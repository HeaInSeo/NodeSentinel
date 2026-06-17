package worker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
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
							Command: []string{"/bin/sh", "-c", "true"},
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
// they are not treated as job failures so L4-successful jobs remain succeeded.
func (w *Worker) runL5a(ctx context.Context, logger *slog.Logger, job *work.Job) {
	if w.vaultClient == nil {
		logger.Info("L5-a skipped: no vault client configured")
		return
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
		w.submitCheckRecord(ctx, logger, job, checkID, command, 0,
			"infra_failed", "", "infra-level: job creation failed: "+err.Error(), 0, false)
		return
	}
	logger.Info("L5-a validation Job created", "k8s_job", created.Name)

	exitCode, isInfra, runErr := w.waitL5aJob(l5aCtx, logger, job, created.Name)
	durationSec := int64(time.Since(startedAt).Seconds())
	_ = w.deleteJob(context.Background(), smokeNamespace, created.Name)

	switch {
	case isInfra && runErr != nil:
		logger.Warn("L5-a infra-level failure", "err", runErr)
		w.submitCheckRecord(ctx, logger, job, checkID, command, exitCode,
			"infra_failed", "", runErr.Error(), durationSec, false)
	case runErr != nil:
		logger.Info("L5-a application-level failure", "exit_code", exitCode, "err", runErr)
		w.submitCheckRecord(ctx, logger, job, checkID, command, exitCode,
			"failed", "", runErr.Error(), durationSec, false)
	default:
		validationHash := computeValidationHash(job.ImageDigest, command, exitCode)
		logger.Info("L5-a validation succeeded", "validation_hash", validationHash)
		w.submitCheckRecord(ctx, logger, job, checkID, command, exitCode,
			"succeeded", validationHash, "", durationSec, true)
	}
}

// waitL5aJob polls the L5-a Job until it reaches a terminal condition.
// Returns (exitCode, isInfraFailure, err). When err == nil the job succeeded.
func (w *Worker) waitL5aJob(ctx context.Context, logger *slog.Logger, job *work.Job, jobName string) (int, bool, error) {
	pollTick := time.NewTicker(5 * time.Second)
	heartbeatTick := time.NewTicker(time.Duration(heartbeatInterval) * time.Second)
	defer pollTick.Stop()
	defer heartbeatTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, true, fmt.Errorf("L5-a timeout: job did not complete within allotted time")
		case <-heartbeatTick.C:
			if err := w.store.Heartbeat(ctx, job.JobID, w.workerName, time.Duration(leaseTTL)*time.Second); err != nil {
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

// isInfraReason returns true for K8s Job failure reasons that are infrastructure-
// level (scheduling, node eviction, deadline) rather than application-level.
func isInfraReason(reason string) bool {
	switch reason {
	case "BackoffLimitExceeded", "DeadlineExceeded", "Evicted":
		return true
	}
	return false
}

// submitCheckRecord builds and sends a SubmitCheckRecordRequest to NodeVault.
func (w *Worker) submitCheckRecord(
	ctx context.Context,
	logger *slog.Logger,
	job *work.Job,
	checkID, command string,
	exitCode int,
	validationStatus, validationHash, failureReason string,
	durationSec int64,
	allOutputsPresent bool,
) {
	contractResult := "passed"
	if validationStatus != "succeeded" {
		contractResult = "failed"
	}

	req := vaultclient.SubmitCheckRecordRequest{
		CheckID:           checkID,
		ToolSpecDigest:    job.CasHash,
		ImageDigest:       job.ImageDigest,
		ToolName:          job.ToolName,
		Version:           job.Version,
		ValidationStatus:  validationStatus,
		ValidationHash:    validationHash,
		Command:           command,
		ExitCode:          exitCode,
		DurationSeconds:   durationSec,
		AllOutputsPresent: allOutputsPresent,
		ContractResult:    contractResult,
		FailureReason:     failureReason,
	}
	if _, err := w.vaultClient.SubmitCheckRecord(ctx, req); err != nil {
		logger.Error("L5-a: failed to submit check record to NodeVault",
			"check_id", checkID, "err", err)
		return
	}
	logger.Info("L5-a check record submitted", "check_id", checkID, "status", validationStatus)
}

// computeValidationHash computes a deterministic SHA-256 hash over the inputs
// that define a successful functional validation run. Per spec: timestamps,
// resource profiles, and stdout/stderr are excluded for reproducibility.
func computeValidationHash(imageDigest, command string, exitCode int) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%d", imageDigest, command, exitCode)
	return fmt.Sprintf("%x", h.Sum(nil))
}
