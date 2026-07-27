package worker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

const (
	l5aJobTimeout = 10 * 60           // 10 minutes — longer than smoke-run to allow fixture execution
	l5aCommand    = "/bin/sh -c true" // minimal: image must start and exit 0
)

// l5aJobName returns a deterministic DNS-safe Job name for the L5-a validation run.
func l5aJobName(job *work.Job) string {
	return fmt.Sprintf("l5a-%s", sanitizeDNSLabel(job.JobID))
}

// l5aCommandSlice returns the Command slice used in the K8s Job spec.
// It is derived by splitting l5aCommand so that the hash input and the
// actual Job spec share a single source of truth.
func l5aCommandSlice() []string {
	return strings.Fields(l5aCommand)
}

// buildL5aJobSpec constructs the K8s Job for L5-a functional validation.
func buildL5aJobSpec(job *work.Job) *batchv1.Job {
	backoff := int32(0)
	deadline := int64(l5aJobTimeout)
	ttl := int32(120)
	image := job.ImageRepository + "@" + job.ImageDigest

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      l5aJobName(job),
			Namespace: smokeNamespace,
			Labels: map[string]string{
				"app":                 "nodevault-l5a",
				"nodesentinel.io/job": job.JobID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "validate",
							Image:   image,
							Command: l5aCommandSlice(),
						},
					},
				},
			},
		},
	}
}

// runL5a executes L5-a functional validation: creates a K8s Job for the tool
// image, waits for completion, and submits a ToolCheckRecord to NodeVault.
// Infra failures (scheduling, timeout, OOM) produce an "infra_failed" record;
// they are not treated as job failures so L4-successful jobs remain
// succeeded — L5-a never calls FailJob or otherwise feeds back into the
// WorkStore retry loop (see this function's callers in process()). terminal
// tells this call whether L5-a is the last stage this job's plan runs
// (security_scan not requested) — see process()'s isL5ALast: when true,
// L5-a's own record (success or failure) is the job's one Terminal record,
// since nothing else will run after it; when false, L5-b always follows and
// owns the Terminal record instead.
// Returns a non-nil error when the submission itself fails, so the caller can
// reflect the failure in the CompleteJob summary.
func (w *Worker) runL5a(ctx context.Context, logger *slog.Logger, job *work.Job, terminal bool) error {
	if w.vaultClient == nil {
		logger.Info("L5-a skipped: no vault client configured")
		return nil
	}

	checkID := fmt.Sprintf("l5a-%s", sanitizeDNSLabel(job.JobID))
	command := l5aCommand
	startedAt := time.Now()

	jobSpec := buildL5aJobSpec(job)
	l5aCtx, cancel := context.WithTimeout(ctx, time.Duration(l5aJobTimeout)*time.Second)
	defer cancel()

	created, err := w.kube.BatchV1().Jobs(smokeNamespace).Create(l5aCtx, jobSpec, metav1.CreateOptions{})
	if err != nil {
		logger.Warn("L5-a job creation failed", "err", err)
		return w.submitCheckRecord(ctx, logger, job, checkRecordSubmission{
			checkID: checkID, stage: vaultclient.StageL5A, terminal: terminal, command: command,
			validationStatus: "infra_failed", failureKind: vaultclient.FailureKindInfrastructure,
			failureReason: "infra-level: job creation failed: " + err.Error(), retryable: true,
		})
	}
	logger.Info("L5-a validation Job created", "k8s_job", created.Name)

	exitCode, isInfra, runErr := w.waitL5aJob(l5aCtx, logger, job, created.Name)
	durationSec := int64(time.Since(startedAt).Seconds())
	if delErr := w.deleteJob(context.Background(), smokeNamespace, created.Name); delErr != nil {
		logger.Warn("L5-a: failed to delete K8s Job — TTL will clean up",
			"job", created.Name, "err", delErr)
	}

	switch {
	case isInfra && runErr != nil:
		logger.Warn("L5-a infra-level failure", "err", runErr)
		return w.submitCheckRecord(ctx, logger, job, checkRecordSubmission{
			checkID: checkID, stage: vaultclient.StageL5A, terminal: terminal, command: command, exitCode: exitCode, durationSec: durationSec,
			validationStatus: "infra_failed", failureKind: vaultclient.FailureKindInfrastructure,
			failureReason: runErr.Error(), retryable: true,
		})
	case runErr != nil:
		logger.Info("L5-a application-level failure", "exit_code", exitCode, "err", runErr)
		return w.submitCheckRecord(ctx, logger, job, checkRecordSubmission{
			checkID: checkID, stage: vaultclient.StageL5A, terminal: terminal, command: command, exitCode: exitCode, durationSec: durationSec,
			validationStatus: "failed", failureKind: vaultclient.FailureKindApplication,
			failureReason: runErr.Error(), retryable: false,
		})
	default:
		validationHash := computeValidationHash(job.ImageDigest, command, exitCode)
		logger.Info("L5-a validation succeeded", "validation_hash", validationHash)
		return w.submitCheckRecord(ctx, logger, job, checkRecordSubmission{
			checkID: checkID, stage: vaultclient.StageL5A, terminal: terminal, command: command, exitCode: exitCode, durationSec: durationSec,
			validationStatus: "succeeded", validationHash: validationHash, allOutputsPresent: true,
		})
	}
}

// waitL5aJob polls the L5-a Job until it reaches a terminal condition.
// Returns (exitCode, isInfraFailure, err). When err == nil the job succeeded.
func (w *Worker) waitL5aJob(ctx context.Context, logger *slog.Logger, job *work.Job, jobName string) (int, bool, error) {
	pollTick := time.NewTicker(pollFrequency)
	heartbeatTick := time.NewTicker(heartbeatFrequency)
	defer pollTick.Stop()
	defer heartbeatTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, true, fmt.Errorf("L5-a timeout: job did not complete within allotted time")
		case <-heartbeatTick.C:
			if err := w.store.Heartbeat(ctx, job.JobID, w.workerName, leaseDuration); err != nil {
				logger.Warn("L5-a heartbeat failed", "err", err)
			}
		case <-pollTick.C:
			k8sJob, err := w.kube.BatchV1().Jobs(smokeNamespace).Get(ctx, jobName, metav1.GetOptions{})
			if err != nil {
				return 0, true, fmt.Errorf("get L5-a job: %w", err)
			}
			for _, cond := range k8sJob.Status.Conditions {
				switch {
				case cond.Type == "Complete" && cond.Status == "True":
					return 0, false, nil
				case cond.Type == "Failed" && cond.Status == "True":
					code := w.extractPodExitCode(ctx, jobName)
					return code, isInfraReason(cond.Reason), fmt.Errorf("job failed: %s", cond.Message)
				}
			}
		}
	}
}

// extractPodExitCode reads the exit code from the first terminated container
// in any Pod belonging to the Job. Returns -1 if no terminated container found.
func (w *Worker) extractPodExitCode(ctx context.Context, jobName string) int {
	pods, err := w.kube.CoreV1().Pods(smokeNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + jobName,
	})
	if err != nil || len(pods.Items) == 0 {
		return -1
	}
	for _, pod := range pods.Items {
		for _, cs := range pod.Status.ContainerStatuses {
			if t := cs.State.Terminated; t != nil {
				return int(t.ExitCode)
			}
		}
	}
	return -1
}

// isInfraReason returns true for K8s Job-level failure reasons that are
// infrastructure-level (scheduling, deadline) rather than application-level.
// Note: "Evicted" is a Pod-level reason and must not be included here — K8s
// Job Failed condition reasons are BackoffLimitExceeded, DeadlineExceeded, etc.
func isInfraReason(reason string) bool {
	switch reason {
	case "BackoffLimitExceeded", "DeadlineExceeded":
		return true
	}
	return false
}

// submitCheckRecord builds and sends a SubmitCheckRecordRequest to NodeVault.
// Returns the submission error (if any) so the caller can propagate it.
// checkRecordSubmission holds one call site's worth of "what happened at
// this stage" — everything submitCheckRecord needs beyond the job itself.
// Used by L5-a (stage=L5A, terminal only when process()'s isL5ALast says
// L5-a is the last stage this job's plan runs) and by worker.go's L3/L4
// terminal reporting (stage=L3|L4, terminal=true on failure always, and on
// success too when isL4Last — see reportTerminalFailure/reportTerminalSuccess).
// Terminal is therefore computed by the caller from which stage actually
// ends up last in the plan, not hardcoded here — see process()'s
// isL4Last/isL5ALast. submitCheckRecord itself only enforces that whichever
// caller sets terminal=true goes through the one-per-job claim below.
type checkRecordSubmission struct {
	checkID           string
	stage             string // vaultclient.StageL3 | StageL4 | StageL5A
	terminal          bool
	command           string
	exitCode          int
	validationStatus  string // "succeeded" | "infra_failed" | "app_failed"
	validationHash    string
	failureKind       string // vaultclient.FailureKindInfrastructure | FailureKindApplication | ""
	failureReason     string
	retryable         bool
	durationSec       int64
	allOutputsPresent bool
}

// submitCheckRecord builds and sends a SubmitCheckRecordRequest to NodeVault.
// Returns the submission error (if any) so the caller can propagate it. When
// sub.terminal is set, this first claims job's one-time terminal-submission
// slot (see Worker.claimTerminal) — a second call for a job whose terminal
// slot is already claimed is a silent no-op (returns nil without sending
// anything), so a requeued/retried job can never submit two terminal
// records for the same validation request.
func (w *Worker) submitCheckRecord(
	ctx context.Context, logger *slog.Logger, job *work.Job, sub checkRecordSubmission,
) error {
	if sub.terminal && !w.claimTerminal(ctx, logger, job.JobID) {
		return nil
	}

	var contractResult string
	switch sub.validationStatus {
	case "succeeded":
		contractResult = "passed"
	case "infra_failed":
		contractResult = "not_applicable"
	default:
		contractResult = "failed"
	}

	req := vaultclient.SubmitCheckRecordRequest{
		CheckID:             sub.checkID,
		ToolSpecDigest:      job.CasHash,
		ImageDigest:         job.ImageDigest,
		ToolName:            job.ToolName,
		Version:             job.Version,
		ValidationRequestID: job.ValidationRequestID,
		SentinelJobID:       job.JobID,
		Stage:               sub.stage,
		Terminal:            sub.terminal,
		ValidationStatus:    sub.validationStatus,
		ValidationHash:      sub.validationHash,
		Command:             sub.command,
		ExitCode:            sub.exitCode,
		DurationSeconds:     sub.durationSec,
		AllOutputsPresent:   sub.allOutputsPresent,
		ContractResult:      contractResult,
		FailureKind:         sub.failureKind,
		Retryable:           sub.retryable,
		FailureReason:       sub.failureReason,
	}
	if _, err := w.vaultClient.SubmitCheckRecord(ctx, req); err != nil {
		logger.Error("failed to submit check record to NodeVault",
			"check_id", sub.checkID, "stage", sub.stage, "err", err)
		if sub.terminal {
			w.markCheckDeliveryPending(ctx, logger, job, req, err)
		}
		return err
	}
	logger.Info("check record submitted", "check_id", sub.checkID, "stage", sub.stage, "status", sub.validationStatus)
	return nil
}

// computeValidationHash computes a deterministic SHA-256 hash over the inputs
// that define a successful functional validation run. Per spec: timestamps,
// resource profiles, and stdout/stderr are excluded for reproducibility.
func computeValidationHash(imageDigest, command string, exitCode int) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%s|%d", imageDigest, command, exitCode)
	return fmt.Sprintf("%x", h.Sum(nil))
}
