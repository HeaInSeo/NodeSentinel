package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// Worker polls the WorkStore for queued jobs and runs L3 dry-run + L4 smoke-run
// K8s Jobs per docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md sections 4-6.
// After L4 succeeds, it optionally runs L5-a (functional validation) and
// L5-b (trivy security scan) when vaultClient and dynamicKube are configured.
type Worker struct {
	store       work.Store
	kube        kubernetes.Interface
	workerName  string
	vaultClient *vaultclient.Client // nil → L5 steps skipped
	dynamicKube dynamic.Interface   // nil → L5-b submits not-available record
}

var (
	leaseDuration      = time.Duration(leaseTTL) * time.Second
	heartbeatFrequency = time.Duration(heartbeatInterval) * time.Second
	pollFrequency      = time.Duration(pollInterval) * time.Second
	smokeRunDuration   = time.Duration(smokeRunTimeout) * time.Second
)

// WithVaultClient sets the NodeVault HTTP client used to submit L5 records.
// Returns w for chaining.
func (w *Worker) WithVaultClient(c *vaultclient.Client) *Worker {
	w.vaultClient = c
	return w
}

// New creates a Worker. workerName identifies this instance in LeaseJob records.
func New(store work.Store, kube kubernetes.Interface, workerName string) *Worker {
	return &Worker{store: store, kube: kube, workerName: workerName}
}

// Run polls for queued jobs and processes them. It blocks until ctx is
// canceled, returning ctx.Err(). Deliberately does not also retry pending
// result deliveries — see RunDeliveryLoop, started as an independent
// goroutine (cmd/nodesentinel/main.go) specifically so a NodeVault outage
// stalling redelivery can never stall job leasing.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		job, err := w.store.LeaseJob(ctx, w.workerName, leaseDuration)
		if err != nil {
			if errors.Is(err, work.ErrNoAvailableJob) {
				slog.Debug("LeaseJob: no available job", "err", err)
			} else {
				slog.Error("LeaseJob error", "err", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollFrequency):
			}
			continue
		}
		if job == nil {
			// No queued jobs — wait before polling again.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollFrequency):
			}
			continue
		}
		w.process(ctx, job)
	}
}

func (w *Worker) process(ctx context.Context, job *work.Job) {
	logger := slog.With("job_id", job.JobID, "image_digest", job.ImageDigest)
	logger.Info("processing job", "actions", job.RequestedActions)

	plan, err := planStages(job.RequestedActions)
	if err != nil {
		// An unrecognized requested_actions value must not be silently
		// ignored — NodeVault asked for something this worker doesn't know
		// how to run, and pretending otherwise would either skip a stage
		// NodeVault expects or run stages that were never asked for. Fail
		// the job outright (non-retryable — retrying the same
		// requested_actions can't change the outcome) and still report a
		// Terminal failure so NodeVault's ValidationRequestRecord doesn't
		// stay stuck at Queued/Running forever.
		logger.Error("rejecting job: invalid requested_actions", "err", err)
		reason := "invalid requested_actions: " + err.Error()
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL3, "validate requested_actions",
			reason, vaultclient.FailureKindApplication, false)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, reason, false)
		return
	}

	// isL4Last/isL5ALast identify which stage is the last one this job's
	// plan will actually run, so this job's one Terminal record can be
	// attached to it on success — L5-b (if planned) is always last by
	// construction and keeps its own always-terminal submission (see
	// l5b.go); L4 or L5-a instead need to be told explicitly since neither
	// used to ever submit a Terminal=true record on its own success. See
	// vaultclient's Stage consts doc comment for the bug this fixes:
	// NodeVault today only ever requests smoke_run, so before this fix no
	// Terminal record was ever submitted for a successful smoke_run-only job.
	isL4Last := !plan.runL5a && !plan.runL5b
	isL5ALast := plan.runL5a && !plan.runL5b

	jobSpec := buildSmokeJobSpec(job)
	ns := smokeNamespace

	// L3: dry-run admission check — no actual Job created. A dry-run failure
	// is always retryable (admission webhook / API server issue).
	if err := w.runDryRun(ctx, ns, jobSpec); err != nil {
		logger.Warn("L3 dry-run failed", "err", err)
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL3, "kubectl apply --dry-run",
			err.Error(), vaultclient.FailureKindInfrastructure, true)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, "L3 dry-run: "+err.Error(), true)
		return
	}
	logger.Info("L3 dry-run passed")

	// L4: real smoke-run.
	smokeCtx, cancel := context.WithTimeout(ctx, smokeRunDuration)
	defer cancel()

	result := w.runSmokeRun(smokeCtx, logger, ns, job, jobSpec)
	if !result.success {
		failureKind := vaultclient.FailureKindInfrastructure
		if !result.retryable {
			failureKind = vaultclient.FailureKindApplication
		}
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL4, "smoke-run",
			result.reason, failureKind, result.retryable)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, result.reason, result.retryable)
		return
	}

	if isL4Last {
		w.reportTerminalSuccess(ctx, logger, job, vaultclient.StageL4, "smoke-run")
	}

	// L5-a and L5-b run after L4 success, gated by the plan resolved from
	// job.RequestedActions above — NodeVault should only receive check/scan
	// records for stages it actually requested (see
	// docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md §4.3). An empty
	// RequestedActions list is treated as "run everything" for backward
	// compatibility with callers/rows that predate this gating. They are
	// best-effort: failures are recorded in NodeVault but do not change the
	// WorkStore job status.
	var l5aErr, l5bErr error
	if plan.runL5a {
		l5aErr = w.runL5a(ctx, logger, job, isL5ALast)
	} else {
		logger.Info("L5-a skipped: profile not in requested_actions")
	}
	if plan.runL5b {
		l5bErr = w.runL5b(ctx, logger, job)
	} else {
		logger.Info("L5-b skipped: security_scan not in requested_actions")
	}

	summary := "L3 dry-run passed; L4 smoke-run succeeded"
	switch {
	case l5aErr != nil:
		summary += "; L5-a failed: " + l5aErr.Error()
	case plan.runL5a:
		summary += "; L5-a submitted"
	default:
		summary += "; L5-a skipped (not requested)"
	}
	switch {
	case l5bErr != nil:
		summary += "; L5-b failed: " + l5bErr.Error()
	case plan.runL5b:
		summary += "; L5-b submitted"
	default:
		summary += "; L5-b skipped (not requested)"
	}
	if err := w.store.CompleteJob(ctx, job.JobID, w.workerName, summary); err != nil {
		logger.Error("CompleteJob failed", "err", err)
	}
	logger.Info("job completed", "summary", summary)
}

// reportTerminalFailure submits a CheckRecord for a validation request that
// died at stage (L3 or L4), before ever reaching L5. The record is always
// sent — NodeVault must never be left unaware that this stage failed — but
// its Terminal flag is only set when retryable is false.
//
// This distinction matters because a retryable failure also causes the
// caller to requeue the job via FailJob(..., retryable=true): the job goes
// back to Queued and will be attempted again by this or another worker.
// That job has not finished — it may yet succeed (reportTerminalSuccess
// will then submit the real Terminal record). Submitting Terminal=true here
// regardless of retryable would both (a) tell NodeVault a request is done
// when it is only paused for a retry, and (b) permanently claim this job's
// one-time terminal-submission slot (see claimTerminal/work.Store.ClaimTerminal)
// — so a later successful retry's reportTerminalSuccess call would find the
// slot already claimed and silently suppress the real success Terminal
// record, leaving NodeVault stuck on a stale Failed status forever. See the
// "retryable Terminal suppression" bug this fixes.
//
// A non-retryable failure does mean this validation request is truly done,
// so Terminal=true is submitted and the terminal slot is claimed via
// submitCheckRecord/claimTerminal. Best-effort — a submission failure here
// is only logged; redelivery is handled by the pending-delivery retry in
// Run's poll loop, not inline here.
func (w *Worker) reportTerminalFailure(
	ctx context.Context, logger *slog.Logger, job *work.Job,
	stage, command, failureReason, failureKind string, retryable bool,
) {
	if w.vaultClient == nil {
		return
	}
	validationStatus := "infra_failed"
	if failureKind == vaultclient.FailureKindApplication {
		validationStatus = "failed"
	}
	sub := checkRecordSubmission{
		checkID:          fmt.Sprintf("%s-%s", stageCheckIDPrefix(stage), sanitizeDNSLabel(job.JobID)),
		stage:            stage,
		terminal:         !retryable,
		command:          command,
		validationStatus: validationStatus,
		failureKind:      failureKind,
		failureReason:    failureReason,
		retryable:        retryable,
	}
	if err := w.submitCheckRecord(ctx, logger, job, sub); err != nil {
		logger.Error("failed to report pipeline failure to NodeVault", "stage", stage, "err", err)
	}
}

// stageCheckIDPrefix returns the lowercase CheckID prefix for stage,
// matching the l5a-/l5b- convention already used by pkg/worker's L5 check
// IDs.
func stageCheckIDPrefix(stage string) string {
	switch stage {
	case vaultclient.StageL3:
		return "l3"
	case vaultclient.StageL4:
		return "l4"
	default:
		return "unknown"
	}
}

// reportTerminalSuccess submits a terminal CheckRecord marking stage — and
// therefore this job's whole validation request — as succeeded. Only called
// when stage is the last one this job's plan runs (see process()'s
// isL4Last/isL5ALast): a job that goes on to run L5-a/L5-b afterward gets
// its Terminal record from whichever of those actually ends up last instead.
// Best-effort like reportTerminalFailure: a submission failure here is only
// logged and (via submitCheckRecord) queued for redelivery, never surfaced
// as a WorkStore job failure.
func (w *Worker) reportTerminalSuccess(ctx context.Context, logger *slog.Logger, job *work.Job, stage, command string) {
	if w.vaultClient == nil {
		return
	}
	sub := checkRecordSubmission{
		checkID:          fmt.Sprintf("%s-%s", stageCheckIDPrefix(stage), sanitizeDNSLabel(job.JobID)),
		stage:            stage,
		terminal:         true,
		command:          command,
		validationStatus: "succeeded",
	}
	if err := w.submitCheckRecord(ctx, logger, job, sub); err != nil {
		logger.Error("failed to report pipeline success to NodeVault", "stage", stage, "err", err)
	}
}

// claimTerminal attempts to claim job's one-time terminal-submission slot
// (see work.Store.ClaimTerminal) and reports whether the caller should
// proceed to actually submit the terminal record. Two outcomes prevent
// submission: the slot was already claimed by an earlier call for the same
// job (a requeued/retried job re-reaching this decision point, or any other
// duplicate invocation) — logged at Info and reported as false, no error;
// storage-layer errors are logged at Warn but reported as true (fail open)
// rather than silently dropping what may be this job's only terminal
// record — an occasional duplicate delivery is the safer failure mode,
// since NodeVault's own conflict handling (see vaultclient.SubmitError's 409
// case) is the backstop for that, whereas a job that never reports any
// terminal record leaves its ValidationRequestRecord stuck forever.
func (w *Worker) claimTerminal(ctx context.Context, logger *slog.Logger, jobID string) bool {
	claimed, err := w.store.ClaimTerminal(ctx, jobID)
	if err != nil {
		logger.Warn("claim terminal submission slot failed — proceeding without idempotency guard",
			"job_id", jobID, "err", err)
		return true
	}
	if !claimed {
		logger.Info("terminal record already submitted for this job — skipping duplicate submission",
			"job_id", jobID)
		return false
	}
	return true
}

func (w *Worker) runDryRun(ctx context.Context, ns string, jobSpec *batchv1.Job) error {
	_, err := w.kube.BatchV1().Jobs(ns).Create(
		ctx, jobSpec,
		metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}},
	)
	return err
}

func (w *Worker) runSmokeRun(ctx context.Context, logger *slog.Logger, ns string, job *work.Job, jobSpec *batchv1.Job) outcome {
	created, err := w.kube.BatchV1().Jobs(ns).Create(ctx, jobSpec, metav1.CreateOptions{})
	if err != nil {
		return outcome{success: false, retryable: true, reason: "failed to create smoke-run Job: " + err.Error()}
	}
	logger.Info("L4 smoke-run Job created", "k8s_job", created.Name)

	// Poll until Job completes, context expires, or deadline exceeds.
	pollTick := time.NewTicker(pollFrequency)
	heartbeatTick := time.NewTicker(heartbeatFrequency)
	defer pollTick.Stop()
	defer heartbeatTick.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort cleanup.
			_ = w.deleteJob(context.Background(), ns, created.Name)
			return classifySmokeRun(ctx, w.kube, ns, created.Name, &batchv1.Job{})
		case <-heartbeatTick.C:
			if err := w.store.Heartbeat(ctx, job.JobID, w.workerName, leaseDuration); err != nil {
				logger.Warn("Heartbeat failed", "err", err)
			}
		case <-pollTick.C:
			k8sJob, err := w.kube.BatchV1().Jobs(ns).Get(ctx, created.Name, metav1.GetOptions{})
			if err != nil {
				return outcome{success: false, retryable: true, reason: "get smoke-run Job: " + err.Error()}
			}
			for _, cond := range k8sJob.Status.Conditions {
				if cond.Type == "Complete" && cond.Status == "True" {
					_ = w.deleteJob(context.Background(), ns, created.Name)
					return outcome{success: true}
				}
				if cond.Type == "Failed" && cond.Status == "True" {
					result := classifySmokeRun(ctx, w.kube, ns, created.Name, k8sJob)
					_ = w.deleteJob(context.Background(), ns, created.Name)
					return result
				}
			}
		}
	}
}

// stagePlan is the set of optional L5 stages a job's requested_actions
// selects — see planStages. smoke_run (L3 dry-run + L4 smoke-run) is always
// part of the pipeline, so it has no field here; it's simply never
// conditional.
type stagePlan struct {
	runL5a bool
	runL5b bool
}

// planStages validates job.RequestedActions and resolves which optional L5
// stages this job's pipeline run will execute. An empty RequestedActions
// list means the caller didn't select specific stages — treated as "run
// everything" so old NodeVault versions that don't populate
// requested_actions at all (or pre-existing stored rows from before this
// field was required non-empty) keep today's unconditional-execution
// behavior instead of silently running nothing.
//
// Any action value other than smoke_run/profile/security_scan is rejected
// with an error rather than silently ignored: a typo'd or future-NodeVault
// action must not quietly degrade into "not requested", since that could
// skip a stage NodeVault's caller actually expects to run.
func planStages(requested []work.Action) (stagePlan, error) {
	if len(requested) == 0 {
		return stagePlan{runL5a: true, runL5b: true}, nil
	}
	var plan stagePlan
	for _, a := range requested {
		switch a {
		case work.ActionSmokeRun:
			// Always part of the pipeline — nothing to record.
		case work.ActionProfile:
			plan.runL5a = true
		case work.ActionSecurityScan:
			plan.runL5b = true
		default:
			return stagePlan{}, fmt.Errorf("unknown requested action %q", a)
		}
	}
	return plan, nil
}

func (w *Worker) deleteJob(ctx context.Context, ns, name string) error {
	prop := metav1.DeletePropagationForeground
	return w.kube.BatchV1().Jobs(ns).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &prop,
	})
}
