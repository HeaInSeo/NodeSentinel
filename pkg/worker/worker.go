package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/HeaInSeo/NodeSentinel/pkg/metrics"
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
	metrics     *metrics.Metrics    // nil → metrics recording is a no-op (e.g. in unit tests)
}

var (
	leaseDuration      = time.Duration(leaseTTL) * time.Second
	heartbeatFrequency = time.Duration(heartbeatInterval) * time.Second
	pollFrequency      = time.Duration(pollInterval) * time.Second
	smokeRunDuration   = time.Duration(smokeRunTimeout) * time.Second

	// deletionConfirmWait bounds how long a worker waits for a foreground
	// delete to actually remove a Job — see deleteAndConfirmGone. It covers
	// the default 30s Pod termination grace period and stays below
	// leaseDuration, so the wait cannot by itself let the lease lapse.
	deletionConfirmWait = 60 * time.Second
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

// WithMetrics sets the Metrics instance the worker records pipeline counters
// to. Optional — a nil (default, unset) Metrics leaves recording a no-op, so
// existing tests that construct a Worker via New() without this need no
// changes. Returns w for chaining.
func (w *Worker) WithMetrics(m *metrics.Metrics) *Worker {
	w.metrics = m
	return w
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
		w.incJobsLeased()
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
		decision := decideRetry(FailureClassDeterministic, job, "invalid requested_actions: "+err.Error())
		w.noteClassification(logger, vaultclient.StageL3, decision.Class, decision.Reason)
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL3, "validate requested_actions", decision)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, decision.Reason, decision.Retry, decision.Delay)
		w.incJobsFailed()
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

	// A row that has never had an execution identity may predate the
	// execution_id column, and so may still have a Job running under the old
	// attempt-derived name — one EnsureExecution cannot adopt. Retire any such
	// Job before minting. This runs before EnsureExecution on purpose: if it
	// fails, no identity is minted, so the next attempt still sees an empty
	// ExecutionID and retries the retirement instead of skipping it.
	if job.ExecutionID == "" {
		if retireErr := w.retireLegacyExecutions(ctx, logger, job); retireErr != nil {
			logger.Error("could not retire pre-execution-identity Jobs", "err", retireErr)
			decision := decideRetry(FailureClassTransientInfra, job, "retire legacy execution: "+retireErr.Error())
			w.noteClassification(logger, vaultclient.StageL3, decision.Class, decision.Reason)
			w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL3, "retire legacy execution", decision)
			_ = w.store.FailJob(ctx, job.JobID, w.workerName, decision.Reason, decision.Retry, decision.Delay)
			w.incJobsFailed()
			return
		}
	}

	// Establish this attempt's physical execution identity *before* building
	// any Job spec. EnsureExecution either mints a new identity or hands back
	// the one a previous attempt left in flight; in the latter case this
	// attempt must observe that execution instead of starting a second one.
	// See work.Store.EnsureExecution for the concurrent-duplicate-execution
	// bug this replaces.
	executionID, adopted, err := w.store.EnsureExecution(ctx, job.JobID, w.workerName)
	if err != nil {
		// Without a durable identity there is no safe name to run under:
		// falling back to an attempt-derived one is what allowed duplicate
		// concurrent executions. Requeue instead (retryable — a store error
		// here is infrastructural) and let a later lease try again.
		logger.Error("could not establish execution identity", "err", err)
		decision := decideRetry(FailureClassTransientInfra, job, "execution identity: "+err.Error())
		w.noteClassification(logger, vaultclient.StageL3, decision.Class, decision.Reason)
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL3, "ensure execution identity", decision)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, decision.Reason, decision.Retry, decision.Delay)
		w.incJobsFailed()
		return
	}
	job.ExecutionID = executionID
	logger = logger.With("execution_id", executionID)
	if adopted {
		logger.Info("adopted in-flight execution from a previous attempt — observing, not re-creating")
	}

	// L5-a runs under the same execution identity as L4 (see l5aJobName), so
	// when it will follow, the identity must stay non-terminal after L4 and
	// is released by runL5a once its own Job has ended.
	l5aFollows := plan.runL5a && w.vaultClient != nil

	// An adopted execution may already be past L4: a previous attempt that
	// died during L5-a leaves the L5-a Job running and the L4 Job deleted.
	// Observing L4 would then find NotFound, release the identity and rerun
	// L4 + L5-a under a new one beside the still-running L5-a Job. The L5-a
	// Job is only ever created after L4 succeeded, so its existence is proof
	// enough to go straight to L5-a and adopt it there.
	if adopted && l5aFollows {
		switch found, err := w.jobExists(ctx, smokeNamespace, l5aJobName(job)); {
		case err != nil:
			// State unknown — keep the identity adoptable and try again later.
			decision := decideRetry(FailureClassTransientInfra, job, "get adopted L5-a Job: "+err.Error())
			w.noteClassification(logger, vaultclient.StageL4, decision.Class, decision.Reason)
			w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL4, "observe adopted execution", decision)
			_ = w.store.FailJob(ctx, job.JobID, w.workerName, decision.Reason, decision.Retry, decision.Delay)
			w.incJobsFailed()
			return
		case found:
			logger.Info("adopted execution is already in L5-a — skipping L3/L4 and observing the L5-a Job")
			w.finishAfterL4(ctx, logger, job, plan, isL5ALast)
			return
		}
	}

	jobSpec := buildSmokeJobSpec(job)
	ns := smokeNamespace

	// L3: dry-run admission check — no actual Job created. A dry-run failure
	// is always classified TransientInfra (admission webhook / API server
	// issue) — see decideRetry for how repeated failures are eventually
	// bounded by maxAttempts instead of retrying forever.
	//
	// Skipped entirely when this attempt adopted an in-flight execution: the
	// attempt that created that execution already passed admission for this
	// exact spec, and a dry-run Create against a name that now exists for
	// real would come back AlreadyExists and fail the job for no reason.
	if adopted {
		logger.Info("L3 dry-run skipped: execution already admitted by the attempt that created it")
	} else if err := w.runDryRun(ctx, ns, jobSpec); err != nil {
		logger.Warn("L3 dry-run failed", "err", err)
		// This attempt minted the identity and never created a Job under it,
		// so release it. Leaving it adoptable would make the next attempt skip
		// both the dry-run and the Create, observe a Job that never existed,
		// and spend a retry on the resulting NotFound.
		w.markExecutionTerminal(ctx, logger, job)
		decision := decideRetry(FailureClassTransientInfra, job, "L3 dry-run: "+err.Error())
		w.noteClassification(logger, vaultclient.StageL3, decision.Class, decision.Reason)
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL3, "kubectl apply --dry-run", decision)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, decision.Reason, decision.Retry, decision.Delay)
		w.incJobsFailed()
		return
	} else {
		logger.Info("L3 dry-run passed")
	}

	// L4: real smoke-run.
	smokeCtx, cancel := context.WithTimeout(ctx, smokeRunDuration)
	defer cancel()

	result := w.runSmokeRun(smokeCtx, logger, ns, job, jobSpec, adopted, !l5aFollows)
	if !result.success {
		decision := decideRetry(result.class, job, result.reason)
		w.noteClassification(logger, vaultclient.StageL4, decision.Class, decision.Reason)
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL4, "smoke-run", decision)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, decision.Reason, decision.Retry, decision.Delay)
		w.incJobsFailed()
		return
	}

	if isL4Last {
		w.reportTerminalSuccess(ctx, logger, job, vaultclient.StageL4, "smoke-run")
	}

	w.finishAfterL4(ctx, logger, job, plan, isL5ALast)
}

// finishAfterL4 runs the optional L5 stages of a job whose L4 smoke-run has
// succeeded and completes the job in the WorkStore.
func (w *Worker) finishAfterL4(ctx context.Context, logger *slog.Logger, job *work.Job, plan stagePlan, isL5ALast bool) {
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
		if l5aErr != nil {
			w.incL5aErrors()
		} else {
			w.incL5aSubmitted()
		}
	} else {
		logger.Info("L5-a skipped: profile not in requested_actions")
	}
	if plan.runL5b {
		l5bErr = w.runL5b(ctx, logger, job)
		if l5bErr != nil {
			w.incL5bErrors()
		} else {
			w.incL5bSubmitted()
		}
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
	w.incJobsCompleted()
	logger.Info("job completed", "summary", summary)
}

// reportTerminalFailure submits a CheckRecord for a validation request that
// died at stage (L3 or L4), before ever reaching L5. The record is always
// sent — NodeVault must never be left unaware that this stage failed — but
// its Terminal flag is only set when decision.Retry is false.
//
// This distinction matters because a retryable decision (e.g. L3's admission
// dry-run, or an L4 TRANSIENT_INFRA/UNKNOWN classification still within its
// retry budget) also causes the caller to requeue the job via
// FailJob(..., retryable=true): the job goes back to Queued and will be
// attempted again by this or another worker. That job has not finished — it
// may yet succeed (reportTerminalSuccess will then submit the real Terminal
// record) or eventually fail non-retryably (either its own classification
// says so, or it exhausts maxAttempts — see decideRetry's RETRY_EXHAUSTED/
// UNKNOWN_RETRY_LIMIT reasons). Submitting Terminal=true here regardless of
// decision.Retry would both (a) tell NodeVault a request is done when it is
// only paused for a retry, and (b) permanently claim this job's one-time
// terminal-submission slot (see claimTerminal/work.Store.ClaimTerminal) — so
// a later successful retry's reportTerminalSuccess call would find the slot
// already claimed and silently suppress the real success Terminal record,
// leaving NodeVault stuck on a stale Failed status forever. See the
// "retryable Terminal suppression" bug this fixes.
//
// A non-retryable decision (DETERMINISTIC, RESOURCE_OBSERVATION, or any
// class that has exhausted its retry budget) does mean this validation
// request is truly done, so Terminal=true is submitted and the terminal
// slot is claimed via submitCheckRecord/claimTerminal — exactly like before
// this fix. Best-effort — a submission failure here is only logged;
// redelivery is handled by the pending-delivery retry in Run's poll loop,
// not inline here.
//
// The wire ValidationStatus/FailureKind come from decision.Class.wireStatus()
// — see that method's doc comment for why FailureClass itself (e.g. the
// literal string "RESOURCE_OBSERVATION") is never sent to NodeVault, only
// mapped onto the existing CheckRecord fields. Retryable on the wire is
// decision.Retry (the actual decision, which folds in maxAttempts/UNKNOWN's
// one-retry limit), not the class's "would this kind of failure ever be
// retried in principle" default.
func (w *Worker) reportTerminalFailure(
	ctx context.Context, logger *slog.Logger, job *work.Job,
	stage, command string, decision RetryDecision,
) {
	if w.vaultClient == nil {
		return
	}
	validationStatus, failureKind := decision.Class.wireStatus()
	sub := checkRecordSubmission{
		checkID:          fmt.Sprintf("%s-%s", stageCheckIDPrefix(stage), recordIDLabel(job.JobID)),
		stage:            stage,
		terminal:         !decision.Retry,
		command:          command,
		validationStatus: validationStatus,
		failureKind:      failureKind,
		failureReason:    decision.Reason,
		retryable:        decision.Retry,
	}
	if err := w.submitCheckRecord(ctx, logger, job, sub); err != nil {
		logger.Error("failed to report pipeline failure to NodeVault", "stage", stage, "err", err)
	}
}

// noteClassification records the FailureClass a stage's failure was
// classified as: always via the low-cardinality nodesentinel_failures_classified
// metric (class/stage only — see Metrics.IncFailureClassified's doc comment
// for why the raw reason never becomes a label), and additionally via a
// structured slog.Warn carrying the *full* raw reason when class is
// FailureClassUnknown — an unrecognized signal is exactly the case an
// operator most needs the complete detail for, since no known pattern
// explains it. Called from every L3/L4/L5-a classification site right after
// decideRetry.
func (w *Worker) noteClassification(logger *slog.Logger, stage string, class FailureClass, rawReason string) {
	w.incFailureClassified(class, stage)
	if class == FailureClassUnknown {
		logger.Warn("failure classified as UNKNOWN — full raw signal logged for investigation",
			"stage", stage, "raw_reason", rawReason)
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
		checkID:          fmt.Sprintf("%s-%s", stageCheckIDPrefix(stage), recordIDLabel(job.JobID)),
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

// runSmokeRun runs — or, when adopted is true, observes — the one physical
// smoke-run execution named by jobSpec.
//
// Every return path here falls into exactly one of two categories, and the
// distinction is what keeps at most one execution alive per logical job:
//
//   - The execution is known not to be running (Complete, Failed, or the Job
//     object is gone after it was known to exist). markExecutionTerminal is
//     called, releasing the identity so a later attempt may mint a fresh one.
//     An adopted Job that was never seen is not "gone": it is created under
//     the same name instead.
//   - The execution's state is unknown (a K8s API error, or ctx expiring
//     mid-run without the Job's removal being confirmed). The identity is
//     deliberately left non-terminal, so the next attempt adopts and
//     re-observes this same Job instead of starting a second one alongside it.
//
// releaseOnSuccess is false when more Kubernetes work (L5-a) will run under
// the same identity after a successful smoke-run; that stage releases it.
func (w *Worker) runSmokeRun(
	ctx context.Context, logger *slog.Logger, ns string, job *work.Job, jobSpec *batchv1.Job,
	adopted, releaseOnSuccess bool,
) outcome {
	name := jobSpec.Name
	if adopted {
		logger.Info("L4 observing adopted smoke-run Job", "k8s_job", name)
	} else if res := w.createSmokeJob(ctx, logger, ns, job, jobSpec, true); res != nil {
		return *res
	}
	// confirmed is true once this call knows a Job has been persisted under
	// name: its own Create succeeded or hit AlreadyExists, or a poll saw the
	// Job. Until then, NotFound for an adopted identity proves nothing. The
	// attempt that minted it may have had an ambiguous Create that is still
	// executing server-side (see the poll loop).
	//
	// confirmed is per call, not persisted. If an earlier attempt saw the
	// Job complete and then died before releasing the identity, the Job may
	// since have been deleted, so this attempt re-creates it and L4 runs
	// again. That re-run is sequential, never concurrent: at-least-once.
	confirmed := !adopted

	// Poll until Job completes, context expires, or deadline exceeds.
	pollTick := time.NewTicker(pollFrequency)
	heartbeatTick := time.NewTicker(heartbeatFrequency)
	defer pollTick.Stop()
	defer heartbeatTick.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort cleanup. The delete is what makes this execution
			// non-running, so the identity is released only once the Job is
			// confirmed gone — otherwise the Job may well still be executing
			// and the next attempt must adopt it, not race it.
			if err := w.deleteAndConfirmGone(ns, name); err == nil {
				w.markExecutionTerminal(context.Background(), logger, job)
			} else {
				logger.Warn("L4: smoke-run Job not confirmed gone on context end — "+
					"leaving execution adoptable rather than risking a duplicate", "k8s_job", name, "err", err)
			}
			return classifySmokeRun(ctx, w.kube, ns, name, &batchv1.Job{})
		case <-heartbeatTick.C:
			if err := w.store.Heartbeat(ctx, job.JobID, w.workerName, leaseDuration); err != nil {
				logger.Warn("Heartbeat failed", "err", err)
			}
		case <-pollTick.C:
			k8sJob, err := w.kube.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) && !confirmed {
				// An adopted identity whose Job was never seen. The minting
				// attempt's Create may have been ambiguous and still be in
				// flight, so this NotFound does not show that nothing will
				// run. Releasing here would let the next attempt mint a new
				// name while that Create can still land. Create under the
				// same deterministic name instead: the name is the
				// idempotency key, so one of the two requests wins and the
				// other gets AlreadyExists.
				logger.Info("L4 adopted smoke-run Job not found — creating it under the same identity", "k8s_job", name)
				if res := w.createSmokeJob(ctx, logger, ns, job, jobSpec, false); res != nil {
					return *res
				}
				confirmed = true
				continue
			}
			if apierrors.IsNotFound(err) {
				// The Job is definitively gone — TTL-reaped, or deleted out
				// from under us after it was persisted. Nothing is running,
				// so release the identity and let the next attempt mint a
				// fresh one. Retryable because no outcome was ever observed
				// for this execution.
				w.markExecutionTerminal(ctx, logger, job)
				return outcome{success: false, retryable: true, class: FailureClassUnknown,
					reason: "smoke-run Job no longer exists; outcome was never observed"}
			}
			if err != nil {
				// State unknown — the Job may still be running. Leave the
				// identity adoptable so the next attempt re-observes this
				// same execution instead of creating a second one.
				return outcome{success: false, retryable: true, class: FailureClassTransientInfra,
					reason: "get smoke-run Job: " + err.Error()}
			}
			confirmed = true
			for _, cond := range k8sJob.Status.Conditions {
				if cond.Type == "Complete" && cond.Status == "True" {
					if releaseOnSuccess {
						w.markExecutionTerminal(ctx, logger, job)
					}
					_ = w.deleteJob(context.Background(), ns, name)
					return outcome{success: true}
				}
				if cond.Type == "Failed" && cond.Status == "True" {
					result := classifySmokeRun(ctx, w.kube, ns, name, k8sJob)
					w.markExecutionTerminal(ctx, logger, job)
					_ = w.deleteJob(context.Background(), ns, name)
					return result
				}
			}
		}
	}
}

// createSmokeJob creates jobSpec's smoke-run Job under its deterministic
// name. It returns nil when a Job now exists under that name: this call
// created it, or it already existed, or it exists despite an ambiguous
// Create error. Otherwise it returns the outcome runSmokeRun should report.
//
// releaseOnReject is false when re-creating an adopted Job that was never
// seen. A rejection of this call then proves nothing about the minting
// attempt's own Create, which may still land, so the identity stays
// adoptable. maxAttempts bounds the retries.
func (w *Worker) createSmokeJob(
	ctx context.Context, logger *slog.Logger, ns string, job *work.Job, jobSpec *batchv1.Job,
	releaseOnReject bool,
) *outcome {
	name := jobSpec.Name
	_, err := w.kube.BatchV1().Jobs(ns).Create(ctx, jobSpec, metav1.CreateOptions{})
	switch {
	case apierrors.IsAlreadyExists(err):
		// The object exists under this execution's own name — this worker
		// minted the identity and then died between the mint and a
		// successful Create record, or is retrying the Create itself.
		// Either way the existing object *is* this execution, so observe
		// it. Creating a differently-named Job here is what produced
		// concurrent duplicates before.
		logger.Info("L4 smoke-run Job already exists — adopting", "k8s_job", name)
	case createRejected(err) && !releaseOnReject:
		logger.Warn("L4: re-create of adopted smoke-run Job rejected — leaving execution adoptable",
			"k8s_job", name, "err", err)
		return &outcome{success: false, retryable: true, class: FailureClassTransientInfra,
			reason: "re-create adopted smoke-run Job: " + err.Error()}
	case createRejected(err):
		// The API server refused the object, so nothing is running under
		// this identity. Release it rather than stranding the job: leaving
		// it non-terminal would make every later attempt try to adopt a
		// Job that was never created.
		w.markExecutionTerminal(ctx, logger, job)
		return &outcome{success: false, retryable: true, class: FailureClassTransientInfra,
			reason: "failed to create smoke-run Job: " + err.Error()}
	case err != nil:
		// The Create may have been persisted even though this call failed
		// (lost response, timeout, 5xx). Look the deterministic name up
		// instead of releasing the identity: a fresh identity would put a
		// second Job beside one that may be running.
		found, getErr := w.jobExists(ctx, ns, name)
		if getErr != nil || !found {
			logger.Warn("L4: smoke-run Job create outcome unknown — leaving execution adoptable",
				"k8s_job", name, "err", err, "get_err", getErr)
			return &outcome{success: false, retryable: true, class: FailureClassTransientInfra,
				reason: "create smoke-run Job (outcome unknown): " + err.Error()}
		}
		logger.Info("L4 smoke-run Job exists despite create error — adopting", "k8s_job", name, "err", err)
	default:
		logger.Info("L4 smoke-run Job created", "k8s_job", name)
	}
	return nil
}

// retireLegacyExecutions deletes, and confirms gone, every Job in
// smokeNamespace labeled with job's ID. It is only called for a row that has
// never had an execution identity (see process), so any such Job was created
// by a worker from before durable execution identities existed. Its name was
// derived from the attempt number ("smoke-<id>-<attempt>" / "l5a-<id>-<attempt>"),
// and it may still be running if that worker died and this attempt reclaimed
// its expired lease. Minting a fresh identity beside it would start a second
// concurrent execution of the same validation.
//
// Finished Jobs are deleted too. A legacy Job's result cannot be adopted,
// because nothing records which attempt it belonged to. Deleting finished Jobs
// also avoids relying on Job conditions to decide whether Pods still run.
//
// A job ID that is not a valid label value selects nothing. The API server
// would have rejected a Job carrying it, so no legacy Job can exist.
//
// The caller's empty ExecutionID is a snapshot. If this worker's lease expired
// and another worker has since minted an identity and created its Job, that
// Job carries the same label. Two guards keep it alive: only Jobs whose name
// has the legacy attempt-derived form are deleted (isLegacyJobName), and the
// stored identity is re-read before each delete. If it is no longer empty,
// retirement stops with an error and this attempt mints nothing.
func (w *Worker) retireLegacyExecutions(ctx context.Context, logger *slog.Logger, job *work.Job) error {
	selector, selErr := labels.ValidatedSelectorFromSet(labels.Set{jobIDLabel: job.JobID})
	if selErr != nil {
		logger.Info("job ID is not a valid label value — no legacy Job can exist", "err", selErr)
		return nil
	}
	list, err := w.kube.BatchV1().Jobs(smokeNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return fmt.Errorf("list Jobs for %s: %w", job.JobID, err)
	}
	for i := range list.Items {
		name := list.Items[i].Name
		if !isLegacyJobName(name, job.JobID) {
			logger.Info("not retiring Job: name is not attempt-derived", "k8s_job", name)
			continue
		}
		stored, getErr := w.store.GetJob(ctx, job.JobID)
		if getErr != nil {
			return fmt.Errorf("re-read execution identity for %s: %w", job.JobID, getErr)
		}
		if stored.ExecutionID != "" {
			return fmt.Errorf("%w: %s now has execution %s", errLegacyRetireSuperseded, job.JobID, stored.ExecutionID)
		}
		logger.Warn("retiring Job left by a worker without execution identities", "k8s_job", name)
		if delErr := w.deleteAndConfirmGone(smokeNamespace, name); delErr != nil {
			return delErr
		}
		// Each confirmation can take up to deletionConfirmWait. Extend the
		// lease so a slow retirement cannot let another worker reclaim the job.
		if hbErr := w.store.Heartbeat(ctx, job.JobID, w.workerName, leaseDuration); hbErr != nil {
			logger.Warn("Heartbeat failed", "err", hbErr)
		}
	}
	return nil
}

// errLegacyRetireSuperseded is returned by retireLegacyExecutions when
// another worker minted an execution identity for the row while this one was
// retiring legacy Jobs.
var errLegacyRetireSuperseded = errors.New("execution identity minted by another worker")

// isLegacyJobName reports whether name has the attempt-derived form used by
// workers without execution identities: "smoke-<id>-<attempt>" or
// "l5a-<id>-<attempt>", where <attempt> is a decimal integer. Current names
// end in an execution ID, which starts with "a" (see sqlite.newExecutionID),
// so they never match.
//
// Those workers truncated <id> to legacySanitizedIDMaxLen, not to the current
// sanitizeDNSLabel length, so a long job ID must be matched in that form. The
// current form is accepted too; both are only ever followed by a bare number
// in a legacy name.
func isLegacyJobName(name, jobID string) bool {
	ids := []string{sanitizeDNSLabelMax(jobID, legacySanitizedIDMaxLen), sanitizeDNSLabel(jobID)}
	for _, id := range ids {
		for _, prefix := range []string{"smoke-", "l5a-"} {
			attempt, ok := strings.CutPrefix(name, prefix+id+"-")
			if ok && attempt != "" && strings.Trim(attempt, "0123456789") == "" {
				return true
			}
		}
	}
	return false
}

// markExecutionTerminal releases job's execution identity — the one this
// attempt observed, job.ExecutionID — logging rather than propagating a
// store failure: every caller is already on a path that returns its own
// outcome, and a failure here is self-correcting — the identity simply stays
// adoptable, so the next attempt re-observes a Job that has already finished
// and reaches the same terminal conclusion. That is the safe direction to
// fail in; the unsafe direction would be releasing an identity whose Job is
// still running. The store refuses the release if another worker has since
// replaced this execution (work.ErrExecutionSuperseded).
func (w *Worker) markExecutionTerminal(ctx context.Context, logger *slog.Logger, job *work.Job) {
	err := w.store.MarkExecutionTerminal(ctx, job.JobID, job.ExecutionID)
	switch {
	case errors.Is(err, work.ErrExecutionSuperseded):
		logger.Warn("execution was replaced by another worker — not releasing the replacement", "err", err)
	case err != nil:
		logger.Warn("could not mark execution terminal — it stays adoptable for the next attempt", "err", err)
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

// The incXxx wrappers below record pipeline counters when w.metrics is
// configured (see WithMetrics) and are no-ops otherwise, so unit tests that
// construct a Worker via New() without metrics don't need any changes.
func (w *Worker) incJobsLeased() {
	if w.metrics != nil {
		w.metrics.IncJobsLeased()
	}
}

func (w *Worker) incJobsCompleted() {
	if w.metrics != nil {
		w.metrics.IncJobsCompleted()
	}
}

func (w *Worker) incJobsFailed() {
	if w.metrics != nil {
		w.metrics.IncJobsFailed()
	}
}

func (w *Worker) incL5aSubmitted() {
	if w.metrics != nil {
		w.metrics.IncL5aSubmitted()
	}
}

func (w *Worker) incL5aErrors() {
	if w.metrics != nil {
		w.metrics.IncL5aErrors()
	}
}

func (w *Worker) incL5bSubmitted() {
	if w.metrics != nil {
		w.metrics.IncL5bSubmitted()
	}
}

func (w *Worker) incL5bErrors() {
	if w.metrics != nil {
		w.metrics.IncL5bErrors()
	}
}

func (w *Worker) incFailureClassified(class FailureClass, stage string) {
	if w.metrics != nil {
		w.metrics.IncFailureClassified(string(class), stage)
	}
}

// deleteAndConfirmGone deletes Job name and waits, up to deletionConfirmWait,
// until it is observed NotFound. A nil error from a foreground Delete only
// means the request was accepted: the Job stays (terminating) until its Pods
// are gone, and those Pods may still be executing during their grace period.
// Only the Job's absence shows that nothing of this execution still runs.
func (w *Worker) deleteAndConfirmGone(ns, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), deletionConfirmWait)
	defer cancel()
	if err := w.deleteJob(ctx, ns, name); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Job %s: %w", name, err)
	}
	tick := time.NewTicker(pollFrequency)
	defer tick.Stop()
	for {
		found, err := w.jobExists(ctx, ns, name)
		if err == nil && !found {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("job %s still present after delete: %w", name, ctx.Err())
		case <-tick.C:
		}
	}
}

// createRejected reports whether err, returned by a Job Create, proves the
// API server refused the object so that nothing was persisted. Any other
// error (a timeout, a 5xx, a lost response) leaves the outcome unknown: the
// Job may exist and be running.
func createRejected(err error) bool {
	return apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) ||
		apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) ||
		apierrors.IsNotFound(err) || apierrors.IsMethodNotSupported(err) ||
		apierrors.IsNotAcceptable(err) || apierrors.IsUnsupportedMediaType(err) ||
		apierrors.IsRequestEntityTooLargeError(err) || apierrors.IsTooManyRequests(err)
}

// jobExists reports whether Job name exists. A non-NotFound API error is
// returned as-is: the caller cannot tell either way.
func (w *Worker) jobExists(ctx context.Context, ns, name string) (bool, error) {
	_, err := w.kube.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err):
		return false, nil
	default:
		return false, err
	}
}

func (w *Worker) deleteJob(ctx context.Context, ns, name string) error {
	prop := metav1.DeletePropagationForeground
	return w.kube.BatchV1().Jobs(ns).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &prop,
	})
}
