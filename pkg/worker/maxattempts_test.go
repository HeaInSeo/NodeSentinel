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

	// Seed an OOMKilled container for the L4 smoke-run pod.
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

// ── Principle 5: Job-name collision reproduction + fix confirmation ────────

// TestSmokeJobName_UniquePerAttempt_Regression and
// TestL5aJobName_UniquePerAttempt_Regression pin down the fix itself: the
// K8s Job name must differ across attempts of the same WorkStore job.
func TestSmokeJobName_UniquePerAttempt_Regression(t *testing.T) {
	job := &work.Job{JobID: "abc123", Attempt: 1}
	name1 := smokeJobName(job)
	job.Attempt = 2
	name2 := smokeJobName(job)
	if name1 == name2 {
		t.Fatalf("smokeJobName must differ across attempts (got %q for both) — see its doc comment for the "+
			"AlreadyExists collision this prevents", name1)
	}
}

func TestL5aJobName_UniquePerAttempt_Regression(t *testing.T) {
	job := &work.Job{JobID: "abc123", Attempt: 1}
	name1 := l5aJobName(job)
	job.Attempt = 2
	name2 := l5aJobName(job)
	if name1 == name2 {
		t.Fatalf("l5aJobName must differ across attempts (got %q for both)", name1)
	}
}

// TestRunSmokeRun_RetryAfterGetFailure_DoesNotCollideOnStaleJobObject
// reproduces the exact non-crash sequence identified as a real (not just
// crash-recovery) Job-name collision risk: attempt 1's poll-loop Get call
// fails transiently, and runSmokeRun's Get-error branch returns without
// deleting the K8s Job it just created (unlike the Complete/Failed/
// ctx.Done() paths — see runSmokeRun and smokeJobName's doc comment).
// Attempt 2 must still succeed at Create despite that stale object still
// being present, because attempt-numbered names give it a different name.
func TestRunSmokeRun_RetryAfterGetFailure_DoesNotCollideOnStaleJobObject(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	store := newTestStore(t)
	w := New(store, kube, "test-worker")

	job := &work.Job{
		JobID:           "collision-repro-job",
		ImageRepository: "harbor.example.com/library/bwa",
		ImageDigest:     "sha256:abc123",
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

	// Attempt 1: Get fails transiently.
	job.Attempt = 1
	spec1 := buildSmokeJobSpec(job)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	result1 := w.runSmokeRun(ctx1, slog.Default(), smokeNamespace, job, spec1)
	if result1.success || !result1.retryable {
		t.Fatalf("attempt 1: expected a retryable failure (Get error), got %+v", result1)
	}

	// Confirm the scenario this test reproduces: attempt 1's Job object was
	// never cleaned up. Let Get through to the real tracker to check.
	getShouldFail.Store(false)
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Get(context.Background(), spec1.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("expected attempt 1's stale Job object to still exist (Get-error path skips cleanup), got: %v", err)
	}

	// Attempt 2: a fresh lease bumps Attempt — the Job name must differ, so
	// Create must not collide with the leftover object from attempt 1.
	job.Attempt = 2
	spec2 := buildSmokeJobSpec(job)
	if spec2.Name == spec1.Name {
		t.Fatal("attempt 2's Job name must differ from attempt 1's")
	}
	// Prepended on top of the toggleable reactor above — takes priority for
	// every Get from here on, simulating the K8s API recovering.
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	result2 := w.runSmokeRun(context.Background(), slog.Default(), smokeNamespace, job, spec2)
	if !result2.success {
		t.Fatalf("attempt 2 should succeed without a Job-name collision, got: %+v", result2)
	}
}
