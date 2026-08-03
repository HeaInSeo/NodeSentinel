package worker

import (
	"context"
	"crypto/sha256"
	"errors"
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

// l5aJobName returns a DNS-safe Job name for the L5-a validation run,
// suffixed with job.Attempt for the same reason smokeJobName is — see its
// doc comment for the collision this avoids.
func l5aJobName(job *work.Job) string {
	return fmt.Sprintf("l5a-%s-%d", sanitizeDNSLabel(job.JobID), job.Attempt)
}

// l5aCommandSlice returns the Command slice used in the K8s Job spec.
// It is derived by splitting l5aCommand so that the hash input and the
// actual Job spec share a single source of truth.
func l5aCommandSlice() []string {
	return strings.Fields(l5aCommand)
}

// buildL5aJobSpec constructs the K8s Job for L5-a functional validation.
//
// ⚠ 이 Job spec에는 VolumeMounts·Resources·Env가 없다. 따라서
// l5aCommand를 실제 도구 실행으로 바꾸더라도 산출물이 나갈 경로가 없어
// 결과가 Pod와 함께 사라진다. 관측을 성립시키려면 명령 교체(OBS-1)보다
// 먼저 volume·resources 추가(OBS-2)가 필요하다.
// 출력 경로가 없는 것은 VolumeMounts 부재 때문이다. Resources는 별개 축이며
// output digest 비교의 선행 조건이 아니다 —
// docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md §8.1·§8.4가 리소스 수치
// (peakCpu/peakMemory/duration/diskIO 등)를 validationHash에서 명시적으로 제외한다.
// Resources 부재는 실행 환경이 무제한이라는 별도 문제이며 OBS-2에서 volume 추가와 함께 다룬다.
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
// Infra-classified failures (scheduling, timeout) and resource-observation
// failures (OOM) both produce a best-effort record; they are not treated as
// job failures so L4-successful jobs remain succeeded — L5-a never calls
// FailJob or otherwise feeds back into the WorkStore retry loop (see this
// function's callers in process()). terminal tells this call whether L5-a
// is the last stage this job's plan runs (security_scan not requested) —
// see process()'s isL5ALast: when true, L5-a's own record (success or
// failure) is the job's one Terminal record, since nothing else will run
// after it; when false, L5-b always follows and owns the Terminal record
// instead.
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
		w.noteClassification(logger, vaultclient.StageL5A, FailureClassTransientInfra, err.Error())
		return w.submitCheckRecord(ctx, logger, job, l5aFailureSubmission(checkID, command, terminal, 0, 0,
			outcome{class: FailureClassTransientInfra, reason: "infra-level: job creation failed: " + err.Error()}))
	}
	logger.Info("L5-a validation Job created", "k8s_job", created.Name)

	exitCode, result, runErr := w.waitL5aJob(l5aCtx, logger, job, created.Name)
	durationSec := int64(time.Since(startedAt).Seconds())
	if delErr := w.deleteJob(context.Background(), smokeNamespace, created.Name); delErr != nil {
		logger.Warn("L5-a: failed to delete K8s Job — TTL will clean up",
			"job", created.Name, "err", delErr)
	}

	if runErr != nil {
		w.noteClassification(logger, vaultclient.StageL5A, result.class, result.reason)
		if result.class == FailureClassTransientInfra || result.class == FailureClassUnknown {
			logger.Warn("L5-a infra/unknown-level failure", "class", result.class, "err", runErr)
		} else {
			logger.Info("L5-a application-level failure", "exit_code", exitCode, "err", runErr)
		}
		return w.submitCheckRecord(ctx, logger, job, l5aFailureSubmission(checkID, command, terminal, exitCode, durationSec, result))
	}

	// ⚠ 이 validationHash는 검증 증거가 아니다.
	// 입력은 (ImageDigest, command, exitCode)인데 성공 시 command는 항상
	// l5aCommand("/bin/sh -c true") 상수이고 exitCode는 0이므로, 결과적으로
	// ImageDigest의 다른 표현일 뿐이다. 아래 allOutputsPresent도 관측이
	// 아니라 하드코딩된 true다.
	// 정본 CANONICAL-R7 §21.27.3 O1·O3에 따라 이 값을 "검증됨"의 근거로
	// 인용하지 않는다. 실제 관측은 로드맵 OBS-3~OBS-6에서 구현하며
	// validationHash 입력 재정의는 OBS-8이다. gap-register #21 참조.
	validationHash := computeValidationHash(job.ImageDigest, command, exitCode)
	logger.Info("L5-a validation succeeded", "validation_hash", validationHash)
	return w.submitCheckRecord(ctx, logger, job, checkRecordSubmission{
		checkID: checkID, stage: vaultclient.StageL5A, terminal: terminal, command: command, exitCode: exitCode, durationSec: durationSec,
		validationStatus: "succeeded", validationHash: validationHash, allOutputsPresent: true,
	})
}

// l5aFailureSubmission builds the checkRecordSubmission for an L5-a failure
// classified as result — see FailureClass.wireStatus() for the
// class-to-wire mapping, shared with L4's reportTerminalFailure so both
// stages describe the same failure taxonomy to NodeVault. L5-a never
// retries (see runL5a's doc comment), so unlike L4's use of
// decideRetry.Retry, retryable on the wire here is simply "would this class
// of failure be retried in principle" (result.class.wireStatus() doesn't
// return it — recomputed the same way decideRetry's default case would,
// without actually applying decideRetry's stateful maxAttempts/UNKNOWN
// bookkeeping, since none of that applies to a stage that never loops).
func l5aFailureSubmission(checkID, command string, terminal bool, exitCode int, durationSec int64, result outcome) checkRecordSubmission {
	validationStatus, failureKind := result.class.wireStatus()
	retryableInPrinciple := result.class == FailureClassTransientInfra || result.class == FailureClassUnknown
	return checkRecordSubmission{
		checkID: checkID, stage: vaultclient.StageL5A, terminal: terminal, command: command,
		exitCode: exitCode, durationSec: durationSec,
		validationStatus: validationStatus, failureKind: failureKind,
		failureReason: result.reason, retryable: retryableInPrinciple,
	}
}

// waitL5aJob polls the L5-a Job until it reaches a terminal condition.
// Returns (exitCode, outcome, err); when err == nil the job succeeded
// (outcome.success is true). Failed-condition classification is delegated
// to classify.go's classifyFromPods — the same pod-inspection classifier L4
// uses — rather than inspecting only the K8s Job-level condition reason.
//
// Design note (Principle 4 — why L5-a and L4 share one classifier instead
// of two): this function used to call its own isInfraReason(cond.Reason),
// which only recognized the Job-level reasons "BackoffLimitExceeded" and
// "DeadlineExceeded". But buildL5aJobSpec sets BackoffLimit=0 (same as
// buildSmokeJobSpec's L4 spec) — so with backoffLimit 0, the Job controller
// marks the Job Failed with reason "BackoffLimitExceeded" the moment its one
// allowed pod attempt fails, *for any reason*, including a genuine
// deterministic application failure (non-zero exit code) or an OOM kill.
// isInfraReason("BackoffLimitExceeded") == true unconditionally, so L5-a's
// classification was effectively "almost everything is infra" — it never
// actually distinguished OOM, exit-code failures, or scheduling failures
// from one another, unlike L4's classifyFromPods, which inspects pod
// container terminated states for exactly that distinction. This was not a
// deliberate different abstraction layer (contrast with, say, a low-level
// "why did this pod die" classifier vs a higher "should this workflow stage
// retry" policy — those legitimately differ) — both isInfraReason and
// classifyFromPods were answering the identical question ("why did this
// smoke/validation Job's container fail") over the identical Job shape
// (BackoffLimit=0), just with very different granularity. So this is a
// real duplication-with-a-bug, not an intentional split: centralizing L5-a
// onto classifyFromPods both removes the duplication and fixes the bug (L5-a
// now detects OOM specifically, routing it through
// FailureClassResourceObservation instead of a blanket infra_failed).
func (w *Worker) waitL5aJob(ctx context.Context, logger *slog.Logger, job *work.Job, jobName string) (int, outcome, error) {
	pollTick := time.NewTicker(pollFrequency)
	heartbeatTick := time.NewTicker(heartbeatFrequency)
	defer pollTick.Stop()
	defer heartbeatTick.Stop()

	for {
		select {
		case <-ctx.Done():
			result := outcome{class: FailureClassTransientInfra, reason: "L5-a timeout: job did not complete within allotted time"}
			return 0, result, errors.New(result.reason)
		case <-heartbeatTick.C:
			if err := w.store.Heartbeat(ctx, job.JobID, w.workerName, leaseDuration); err != nil {
				logger.Warn("L5-a heartbeat failed", "err", err)
			}
		case <-pollTick.C:
			k8sJob, err := w.kube.BatchV1().Jobs(smokeNamespace).Get(ctx, jobName, metav1.GetOptions{})
			if err != nil {
				result := outcome{class: FailureClassTransientInfra, reason: "get L5-a job: " + err.Error()}
				return 0, result, fmt.Errorf("get L5-a job: %w", err)
			}
			for _, cond := range k8sJob.Status.Conditions {
				switch {
				case cond.Type == "Complete" && cond.Status == "True":
					return 0, outcome{success: true}, nil
				case cond.Type == "Failed" && cond.Status == "True":
					result := classifyFromPods(ctx, w.kube, smokeNamespace, jobName, cond.Reason, cond.Message)
					code := w.extractPodExitCode(ctx, jobName)
					return code, result, fmt.Errorf("job failed: %s", cond.Message)
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
