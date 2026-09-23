package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/metrics"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// ── Required test 1/7: OOM is RESOURCE_OBSERVATION, not retryable ──────────

// TestProcess_L4OOM_NotRetried_MapsToObservedContractFailure verifies that
// an L4 smoke-run OOM (a) does not get re-registered as retryable at the
// WorkStore level — the job ends up permanently Failed, not requeued — and
// (b) is reported to NodeVault via the same ValidationStatus=failed/
// FailureKind=application wire mapping already used for a deterministic
// exit-code failure (see FailureClass.wireStatus's design note), not
// infra_failed — because a valid observation *was* made; the tool just
// failed its resource contract under current conditions.
func TestProcess_L4OOM_NotRetried_MapsToObservedContractFailure(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		return true, fakeJobWithCondition(smokeNamespace, ga.GetName(), batchv1.JobFailed, corev1.ConditionTrue), nil
	})

	req := newSmokeRunOnlyJob()
	req.JobID = "l4-oom-job"
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	// Seed an OOMKilled container for the L4 smoke-run pod. The pod's
	// "job-name" label has to match the K8s Job process() will observe, so
	// the execution identity has to exist before the name is computed.
	mintExecution(t, store, job)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "smoke-oom-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": smokeJobName(job)},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "OOMKilled",
							ExitCode: 137,
						},
					},
				},
			},
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	w := New(store, kube, "test-worker").WithVaultClient(vc)
	w.process(context.Background(), job)

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusFailed {
		t.Errorf("Status = %q, want failed — OOM must not be re-registered as retryable", stored.Status)
	}

	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want 1", len(captured))
	}
	got := captured[0].decodeCheck(t)
	if got.ValidationStatus != "failed" {
		t.Errorf("ValidationStatus = %q, want %q — see wireStatus's design note (not infra_failed)", got.ValidationStatus, "failed")
	}
	if got.FailureKind != "application" {
		t.Errorf("FailureKind = %q, want %q", got.FailureKind, "application")
	}
	if got.Retryable {
		t.Error("Retryable = true, want false")
	}
	if !got.Terminal {
		t.Error("Terminal = false, want true — this is the job's final outcome")
	}
	if !strings.Contains(got.FailureReason, "OOMKilled") {
		t.Errorf("FailureReason = %q, want it to mention OOMKilled", got.FailureReason)
	}
}

// ── Required tests 2/7 + 5/7: bounded retry, then RETRY_EXHAUSTED ──────────

// TestProcess_TransientInfra_RetriedThenExhausted drives a job through a
// persistent (every attempt) transient K8s API error at L4 — a
// FailureClassTransientInfra signal that would, before this change, have
// requeued the job forever (pkg/work/sqlite/store.go's FailJob has no cap).
// It must instead be retried while Attempt < maxAttempts, and once
// maxAttempts is reached, permanently Failed with reason RETRY_EXHAUSTED.
//
// This also doubles as regression coverage for Principle 5's Job-name
// collision finding: runSmokeRun's Get-error branch never deletes the K8s
// Job it just created (see smokeJobName's doc comment), so every one of
// these attempts leaves a stale Job object behind — if attempt-numbered
// names (this PR's fix) were not in place, the second attempt's Create
// would fail with AlreadyExists instead of reaching the Get-error path
// again. This test would fail on Create rather than on Get if that
// regressed.
func TestProcess_TransientInfra_RetriedThenExhausted(t *testing.T) {
	useFastWorkerTicks(t)
	useLowMaxAttempts(t, 3)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewInternalError(fmt.Errorf("internal server error"))
	})

	w := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newSmokeRunOnlyJob()
	req.JobID = "transient-exhaust-job"
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	for i := 1; i <= maxAttempts; i++ {
		job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
		if err != nil {
			t.Fatalf("LeaseJob (attempt %d): %v", i, err)
		}
		if job.JobID != created.JobID {
			t.Fatalf("attempt %d leased a different job: got %q, want %q", i, job.JobID, created.JobID)
		}
		w.process(context.Background(), job)

		stored, err := store.GetJob(context.Background(), created.JobID)
		if err != nil {
			t.Fatalf("GetJob (after attempt %d): %v", i, err)
		}
		if i < maxAttempts {
			if stored.Status != work.StatusQueued {
				t.Fatalf("after attempt %d: Status = %q, want queued (still within retry budget)", i, stored.Status)
			}
			if strings.Contains(stored.LastError, retryExhaustedReason) {
				t.Errorf("after attempt %d: LastError should not yet mention %q, got %q", i, retryExhaustedReason, stored.LastError)
			}
		} else {
			if stored.Status != work.StatusFailed {
				t.Fatalf("after final attempt %d: Status = %q, want failed (RETRY_EXHAUSTED)", i, stored.Status)
			}
			if !strings.Contains(stored.LastError, retryExhaustedReason) {
				t.Errorf("after final attempt %d: LastError = %q, want it to contain %q", i, stored.LastError, retryExhaustedReason)
			}
		}
	}

	if len(captured) != maxAttempts {
		t.Fatalf("submissions captured = %d, want %d (one non-terminal record per retried attempt, "+
			"plus the final terminal one)", len(captured), maxAttempts)
	}
	last := captured[len(captured)-1].decodeCheck(t)
	if !last.Terminal {
		t.Error("final record Terminal = false, want true — retries are exhausted")
	}
	if last.ValidationStatus != "infra_failed" {
		t.Errorf("ValidationStatus = %q, want infra_failed — RETRY_EXHAUSTED doesn't change the wire "+
			"classification, only whether a retry will happen", last.ValidationStatus)
	}
	if last.Retryable {
		t.Error("Retryable = true, want false — no further retry will happen")
	}
	if !strings.Contains(last.FailureReason, retryExhaustedReason) {
		t.Errorf("FailureReason = %q, want it to contain %q", last.FailureReason, retryExhaustedReason)
	}
	for i, c := range captured[:len(captured)-1] {
		rec := c.decodeCheck(t)
		if rec.Terminal {
			t.Errorf("record %d (attempt %d, still within budget) has Terminal=true, want false", i, i+1)
		}
	}
}

// ── Required test 4/7: UNKNOWN retried exactly once, with metric + log ─────

// TestProcess_L4Unknown_RetriedOnce_ThenTerminal_WithMetricAndLog reproduces
// a genuinely unrecognized L4 failure signal (a Failed Job condition with no
// usable Reason/Message and no pods to inspect — classifyFromPods's
// can't-classify fallback). It must be retried exactly once, then
// permanently fail with UNKNOWN_RETRY_LIMIT — and both occurrences must
// increment the low-cardinality nodesentinel_failures_classified{class,stage}
// metric, with the *first* occurrence's full raw signal captured in a
// structured log record (see Worker.noteClassification).
func TestProcess_L4Unknown_RetriedOnce_ThenTerminal_WithMetricAndLog(t *testing.T) {
	useFastWorkerTicks(t)
	useLowMaxAttempts(t, 5) // generous — this test is about UNKNOWN's own limit, not maxAttempts

	handler := &capturingHandler{}
	orig := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(orig) })

	m, err := metrics.New()
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	// Failed condition with no Reason/Message, and no pods ever seeded —
	// classifyFromPods cannot find a recognized pattern.
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		return true, fakeJobWithCondition(smokeNamespace, ga.GetName(), batchv1.JobFailed, corev1.ConditionTrue), nil
	})

	w := New(store, kube, "test-worker").WithVaultClient(vc).WithMetrics(m)

	req := newSmokeRunOnlyJob()
	req.JobID = "unknown-once-job"
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Attempt 1: UNKNOWN, retried once.
	job1, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 1): %v", err)
	}
	w.process(context.Background(), job1)

	afterAttempt1, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob (after attempt 1): %v", err)
	}
	if afterAttempt1.Status != work.StatusQueued {
		t.Fatalf("after attempt 1: Status = %q, want queued (UNKNOWN's one permitted retry)", afterAttempt1.Status)
	}
	if !strings.Contains(afterAttempt1.LastError, unknownRetryMarker) {
		t.Errorf("after attempt 1: LastError = %q, want it to carry the unknown-retry marker", afterAttempt1.LastError)
	}

	// Attempt 2: UNKNOWN again — must NOT be retried a second time.
	job2, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 2): %v", err)
	}
	w.process(context.Background(), job2)

	final, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob (after attempt 2): %v", err)
	}
	if final.Status != work.StatusFailed {
		t.Fatalf("after attempt 2: Status = %q, want failed (UNKNOWN_RETRY_LIMIT)", final.Status)
	}
	if !strings.Contains(final.LastError, unknownRetryLimitReason) {
		t.Errorf("after attempt 2: LastError = %q, want it to contain %q", final.LastError, unknownRetryLimitReason)
	}

	if len(captured) != 2 {
		t.Fatalf("submissions captured = %d, want 2 (one non-terminal record for attempt 1, one terminal for attempt 2)", len(captured))
	}
	if captured[0].decodeCheck(t).Terminal {
		t.Error("attempt 1's record has Terminal=true, want false — it was retried")
	}
	if !captured[1].decodeCheck(t).Terminal {
		t.Error("attempt 2's record has Terminal=false, want true — UNKNOWN's retry limit was reached")
	}

	// Metric: nodesentinel_failures_classified{class="UNKNOWN",stage="L4"}
	// recorded at least twice (once per attempt), with no raw-error label.
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "nodesentinel_failures_classified") ||
		!strings.Contains(body, `class="UNKNOWN"`) || !strings.Contains(body, `stage="L4"`) {
		t.Errorf("expected nodesentinel_failures_classified{class=\"UNKNOWN\",stage=\"L4\"} in metrics output:\n%s", body)
	}
	if strings.Contains(body, "raw_reason") {
		t.Error("metric output must never carry a raw_reason label (unbounded cardinality)")
	}

	// Log: the full raw reason must be captured via a structured log record
	// (not just folded into a metric label).
	var sawRawReason bool
	handler.mu.Lock()
	for _, r := range handler.records {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "raw_reason" {
				sawRawReason = true
			}
			return true
		})
	}
	handler.mu.Unlock()
	if !sawRawReason {
		t.Error("expected a structured log record carrying the raw_reason attribute for the UNKNOWN classification")
	}
}

// ── Required test 6/7: head-of-line regression ─────────────────────────────

// TestHeadOfLine_PermanentlyFailingJob_DoesNotBlockLaterJobs is the direct
// regression test for the bug motivating this whole change: before
// maxAttempts existed, FailJob(..., retryable=true) requeued a job
// unconditionally and LeaseJob selects strictly "ORDER BY created_at ASC",
// so a job that fails in a way classified retryable forever would be
// re-leased ahead of every other (newer) queued job, indefinitely —
// starving them. Once the blocker job exhausts maxAttempts it becomes
// work.StatusFailed, a status LeaseJob's WHERE clause never selects, which
// is what actually frees up the newer job behind it.
func TestHeadOfLine_PermanentlyFailingJob_DoesNotBlockLaterJobs(t *testing.T) {
	useFastWorkerTicks(t)
	useLowMaxAttempts(t, 2)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		if strings.HasPrefix(ga.GetName(), "smoke-hol-blocker") {
			return true, nil, k8serrors.NewInternalError(fmt.Errorf("perpetual transient error"))
		}
		return true, fakeJobWithCondition(smokeNamespace, ga.GetName(), batchv1.JobComplete, corev1.ConditionTrue), nil
	})

	w := New(store, kube, "test-worker")

	blockerReq := newSmokeRunOnlyJob()
	blockerReq.JobID = "hol-blocker"
	blocker, err := store.CreateJob(context.Background(), blockerReq)
	if err != nil {
		t.Fatalf("CreateJob (blocker): %v", err)
	}

	// Created strictly after the blocker (later created_at) — under the
	// pre-fix unbounded-retry behavior this job would never get leased as
	// long as the blocker keeps failing retryably.
	victimReq := newSmokeRunOnlyJob()
	victimReq.JobID = "hol-victim"
	victim, err := store.CreateJob(context.Background(), victimReq)
	if err != nil {
		t.Fatalf("CreateJob (victim): %v", err)
	}

	for i := 1; i <= maxAttempts; i++ {
		job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
		if err != nil {
			t.Fatalf("LeaseJob (blocker attempt %d): %v", i, err)
		}
		if job.JobID != blocker.JobID {
			t.Fatalf("attempt %d: leased %q, want the still-queued, older blocker job %q", i, job.JobID, blocker.JobID)
		}
		w.process(context.Background(), job)
	}

	blockerFinal, err := store.GetJob(context.Background(), blocker.JobID)
	if err != nil {
		t.Fatalf("GetJob (blocker): %v", err)
	}
	if blockerFinal.Status != work.StatusFailed {
		t.Fatalf("blocker Status = %q, want failed (RETRY_EXHAUSTED)", blockerFinal.Status)
	}

	// The blocker is now terminally Failed and no longer matches LeaseJob's
	// WHERE clause — the victim must be leasable despite being newer.
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (victim): %v", err)
	}
	if job.JobID != victim.JobID {
		t.Fatalf("leased %q, want the victim job %q — head-of-line blocking regression", job.JobID, victim.JobID)
	}
	w.process(context.Background(), job)

	victimFinal, err := store.GetJob(context.Background(), victim.JobID)
	if err != nil {
		t.Fatalf("GetJob (victim): %v", err)
	}
	if victimFinal.Status != work.StatusSucceeded {
		t.Errorf("victim Status = %q, want succeeded", victimFinal.Status)
	}
}

// ── Principle 5: Job naming — per-execution, NOT per-attempt ──────────────

// TestJobName_KeyedOnExecutionNotAttempt_Regression pins the naming contract
// both Job builders share, in the direction that actually matters.
//
// An earlier fix made these names unique *per attempt* to avoid an
// AlreadyExists collision. That traded one bug for a worse one: LeaseJob
// increments attempt when it reclaims a merely-expired lease, so a worker
// that died without its K8s Job dying got a different name on the next
// attempt and created a *second* Job beside the one still running — two
// concurrent physical executions of one logical validation (issue #2).
//
// So attempt must NOT move the name, and the execution identity must.
func TestJobName_KeyedOnExecutionNotAttempt_Regression(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*work.Job) string
	}{
		{"smoke", smokeJobName},
		{"l5a", l5aJobName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &work.Job{JobID: "abc123", ExecutionID: "a1-0123456789abcdef", Attempt: 1}
			before := tc.build(job)

			// A reclaimed lease bumps attempt while adopting the same
			// in-flight execution. The name must not budge, or the adopting
			// attempt would address a different Job object and create a
			// duplicate.
			job.Attempt = 2
			if after := tc.build(job); after != before {
				t.Fatalf("name changed with attempt alone (%q -> %q): a reclaimed lease would "+
					"create a second Job beside the one still running", before, after)
			}

			// A genuinely new execution — minted only once the previous one
			// is terminal — must get its own name, or it would collide with
			// the previous execution's leftover object.
			job.ExecutionID = "a2-fedcba9876543210"
			if after := tc.build(job); after == before {
				t.Fatalf("name did not change with a new execution identity (%q): a fresh attempt "+
					"would collide with the previous execution's leftover Job object", after)
			}
		})
	}
}

// TestSmokeJobName_FitsDNSLabelLimit guards the budget sanitizeDNSLabel
// truncates against. K8s rejects an object name longer than 63 characters,
// and the execution-identity suffix is substantially longer than the bare
// attempt number it replaced — so the old 50-character allowance would have
// overflowed and made every Job creation fail for a long job ID.
func TestSmokeJobName_FitsDNSLabelLimit(t *testing.T) {
	job := &work.Job{
		JobID:       strings.Repeat("j", 200),
		ExecutionID: "a9999-0123456789abcdef",
	}
	for _, name := range []string{smokeJobName(job), l5aJobName(job)} {
		if len(name) > 63 {
			t.Errorf("name %q is %d chars, exceeding the 63-character DNS label limit", name, len(name))
		}
	}
}

// TestRunSmokeRun_RetryAfterGetFailure_AdoptsInsteadOfDuplicating is the
// core no-duplicate-execution regression for issue #2.
//
// It reproduces the exact non-crash sequence that used to produce two
// concurrent executions: attempt 1's poll-loop Get fails transiently, and
// runSmokeRun's Get-error branch returns without deleting the K8s Job it
// created — the Job is still running, and its outcome was never observed.
// The job is then requeued and re-leased.
//
// Under the old attempt-keyed naming, attempt 2 computed a *different* name
// and created a second Job beside the first. Now attempt 2 must adopt
// attempt 1's execution and observe that same Job — so the namespace must
// still hold exactly one Job object for this logical validation.
//
// The one-object assertion is the point of the test. Asserting only that
// attempt 2 succeeds would pass just as well under the duplicating behavior.
func TestRunSmokeRun_RetryAfterGetFailure_AdoptsInsteadOfDuplicating(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	store := newTestStore(t)
	w := New(store, kube, "test-worker")

	req := newSmokeRunOnlyJob()
	req.JobID = "adoption-repro-job"
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// getShouldFail toggles a prepended reactor on/off without ever removing
	// it from the chain, so the fake clientset's own default (tracker-backed)
	// reactor stays in place underneath it throughout this test — unlike
	// clearing kube.Fake.ReactionChain, which would also discard that
	// default reactor and make every subsequent call return a zero-value
	// object instead of actually touching the fake tracker.
	var getShouldFail atomic.Bool
	getShouldFail.Store(true)
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if getShouldFail.Load() {
			return true, nil, k8serrors.NewInternalError(fmt.Errorf("transient API error"))
		}
		return false, nil, nil // fall through to the default tracker reactor
	})

	// ── Attempt 1: lease, mint an execution, Get fails transiently ──────────
	job1, err := store.LeaseJob(context.Background(), "worker-1", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 1): %v", err)
	}
	executionID1, adopted1, err := store.EnsureExecution(context.Background(), job1.JobID, "worker-1")
	if err != nil {
		t.Fatalf("EnsureExecution (attempt 1): %v", err)
	}
	if adopted1 {
		t.Fatal("attempt 1 must mint a fresh execution, not adopt one")
	}
	job1.ExecutionID = executionID1

	spec1 := buildSmokeJobSpec(job1)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	result1 := w.runSmokeRun(ctx1, slog.Default(), smokeNamespace, job1, spec1, false, true)
	if result1.success || !result1.retryable {
		t.Fatalf("attempt 1: expected a retryable failure (Get error), got %+v", result1)
	}

	// The scenario this reproduces: attempt 1's Job object is still there,
	// still running as far as anyone knows. Let Get through to check.
	getShouldFail.Store(false)
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Get(
		context.Background(), spec1.Name, metav1.GetOptions{},
	); err != nil {
		t.Fatalf("expected attempt 1's Job to still exist (the Get-error path must not delete it): %v", err)
	}

	// Because the outcome was never observed, the execution must have been
	// left adoptable. Releasing it here is what would let attempt 2 mint a
	// new identity and start a duplicate.
	stored, err := store.GetJob(context.Background(), job1.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionTerminal {
		t.Fatal("execution was marked terminal despite its outcome never being observed — " +
			"the next attempt would mint a new identity and run a second Job alongside this one")
	}

	// Requeue exactly as process() does after a retryable outcome. This step
	// is what production performs between the two attempts, and it is load-
	// bearing here: LeaseJob only hands out a job that is 'queued', or one
	// whose lease has actually expired. Attempt 1's lease still has ~60s to
	// run, so without this the re-lease below would come back
	// ErrNoAvailableJob and the test would never reach the duplicate-Job
	// assertion it exists for.
	if err := store.FailJob(
		context.Background(), job1.JobID, "worker-1", job1.Attempt, result1.reason, true, 0,
	); err != nil {
		t.Fatalf("FailJob (requeue after attempt 1): %v", err)
	}

	// Requeuing must not have released the execution identity — only an
	// observed terminal outcome may do that. If it had, attempt 2 would mint
	// a new one and start a duplicate.
	requeued, err := store.GetJob(context.Background(), job1.JobID)
	if err != nil {
		t.Fatalf("GetJob after requeue: %v", err)
	}
	if requeued.ExecutionTerminal {
		t.Fatal("requeuing a job must not mark its execution terminal — the Job is still running")
	}

	// ── Attempt 2: a different worker reclaims the lease ────────────────────
	job2, err := store.LeaseJob(context.Background(), "worker-2", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 2): %v", err)
	}
	if job2.Attempt == job1.Attempt {
		t.Fatalf("attempt did not advance on re-lease (%d) — this test would not be exercising a retry",
			job2.Attempt)
	}
	executionID2, adopted2, err := store.EnsureExecution(context.Background(), job2.JobID, "worker-2")
	if err != nil {
		t.Fatalf("EnsureExecution (attempt 2): %v", err)
	}
	if !adopted2 {
		t.Fatal("attempt 2 must adopt attempt 1's in-flight execution, not mint a new one")
	}
	if executionID2 != executionID1 {
		t.Fatalf("adopted execution id = %q, want attempt 1's %q", executionID2, executionID1)
	}
	job2.ExecutionID = executionID2

	spec2 := buildSmokeJobSpec(job2)
	if spec2.Name != spec1.Name {
		t.Fatalf("attempt 2 addresses Job %q but attempt 1 created %q — it would create a duplicate",
			spec2.Name, spec1.Name)
	}

	// Prepended on top of the toggleable reactor above — takes priority for
	// every Get from here on, simulating the K8s API recovering.
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	result2 := w.runSmokeRun(context.Background(), slog.Default(), smokeNamespace, job2, spec2, adopted2, true)
	if !result2.success {
		t.Fatalf("attempt 2 should have observed the adopted Job to completion, got: %+v", result2)
	}

	// The assertion this test exists for: at no point did a second physical
	// execution of this logical validation exist.
	jobs, err := kube.BatchV1().Jobs(smokeNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) > 1 {
		names := make([]string, 0, len(jobs.Items))
		for _, item := range jobs.Items {
			names = append(names, item.Name)
		}
		t.Fatalf("found %d concurrent K8s Jobs for one logical validation (%v) — "+
			"duplicate physical execution, which issue #2 forbids", len(jobs.Items), names)
	}

	// Having observed a terminal outcome, the execution must now be released
	// so a later retry is free to mint a fresh identity.
	final, err := store.GetJob(context.Background(), job2.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !final.ExecutionTerminal {
		t.Error("execution should be marked terminal once its outcome was observed")
	}
}

// TestRunSmokeRun_NotFoundDuringPoll_ReleasesExecution covers the other
// direction: a Job that has vanished (TTL-reaped, or deleted out from under
// the worker) is definitively not running, so its identity must be released
// rather than left adoptable. Leaving it adoptable would strand the job —
// every later attempt would try to observe a Job that no longer exists.
//
// This only holds once the Job is known to have been persisted, as it is here
// by this call's own successful Create. An adopted identity whose Job was
// never seen is re-created under the same name instead (see
// TestRunSmokeRun_AdoptedNotFound_RecreatesUnderSameName).
func TestRunSmokeRun_NotFoundDuringPoll_ReleasesExecution(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	// The Create succeeds, then the Job is gone by the first poll.
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		return true, nil, k8serrors.NewNotFound(batchv1.Resource("jobs"), ga.GetName())
	})
	store := newTestStore(t)
	w := New(store, kube, "test-worker")

	req := newSmokeRunOnlyJob()
	req.JobID = "vanished-job-repro"
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	mintExecution(t, store, job)

	spec := buildSmokeJobSpec(job)
	result := w.runSmokeRun(context.Background(), slog.Default(), smokeNamespace, job, spec, false, true)
	if result.success || !result.retryable {
		t.Fatalf("a vanished Job should be a retryable failure, got %+v", result)
	}

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !stored.ExecutionTerminal {
		t.Fatal("a Job observed NotFound is definitively not running — its execution identity must be " +
			"released, or every later attempt would keep adopting a Job that does not exist")
	}
}
