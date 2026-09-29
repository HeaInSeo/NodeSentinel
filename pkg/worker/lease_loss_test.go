package worker

import (
	"bufio"
	"context"
	"log/slog"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/metrics"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// takeOver makes another attempt re-lease job exactly like an expired-lease
// reclaim: the current generation is released and "worker-b" leases attempt+1.
func takeOver(t *testing.T, store work.Store, job *work.Job) *work.Job {
	t.Helper()
	if err := store.FailJob(context.Background(), job.JobID, "test-worker", job.Attempt, "lease expired", true, 0); err != nil {
		t.Errorf("release attempt %d: %v", job.Attempt, err)
		return nil
	}
	next, err := store.LeaseJob(context.Background(), "worker-b", 60*time.Second)
	if err != nil || next == nil || next.JobID != job.JobID {
		t.Errorf("worker-b re-lease: job=%v err=%v", next, err)
		return nil
	}
	return next
}

// jobsCompleted reads nodesentinel_jobs_completed_total from the /metrics output
// (0 when the counter was never incremented).
func jobsCompleted(t *testing.T, m *metrics.Metrics) int {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	sc := bufio.NewScanner(strings.NewReader(rec.Body.String()))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "nodesentinel_jobs_completed_total") {
			continue
		}
		f := strings.Fields(line)
		n, err := strconv.ParseFloat(f[len(f)-1], 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return int(n)
	}
	return 0
}

// takeOverOnFirstGet re-leases the job to another attempt at the first poll of
// the L4 Job and then reports that Job Complete, so the stale attempt sees a
// finished L4 only after it has lost the lease.
func takeOverOnFirstGet(t *testing.T, store work.Store, job *work.Job) (k8stesting.ReactionFunc, func() *work.Job) {
	var once sync.Once
	var next *work.Job
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
			ga := action.(k8stesting.GetActionImpl)
			once.Do(func() { next = takeOver(t, store, job) })
			return true, fakeJobWithCondition(ga.GetNamespace(), ga.GetName(), batchv1.JobComplete, corev1.ConditionTrue), nil
		}, func() *work.Job {
			return next
		}
}

// After a confirmed lease loss the stale attempt starts no external report, does
// not complete the job, does not move the completion counter, and leaves the job
// with the attempt that now owns it.
func TestProcess_LeaseLostDuringL4_NoReportNoCompletion(t *testing.T) {
	useFastWorkerTicks(t)
	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)
	store := newTestStore(t)
	m, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}

	req := newSmokeRunOnlyJob()
	req.JobID = "lease-lost-l4-job"
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	react, newOwner := takeOverOnFirstGet(t, store, job)
	kube.PrependReactor("get", "jobs", react)

	New(store, kube, "test-worker").WithVaultClient(vc).WithMetrics(m).process(context.Background(), job)

	if len(captured) != 0 {
		t.Fatalf("stale attempt submitted %d record(s) after losing its lease, want 0", len(captured))
	}
	if got := jobsCompleted(t, m); got != 0 {
		t.Fatalf("jobs_completed = %d after lease loss, want 0", got)
	}
	next := newOwner()
	if next == nil {
		t.Fatal("take-over did not happen")
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status == work.StatusSucceeded || stored.LeaseOwner != "worker-b" || stored.Attempt != next.Attempt {
		t.Fatalf("job = status %q owner %q attempt %d, want still leased by worker-b attempt %d",
			stored.Status, stored.LeaseOwner, stored.Attempt, next.Attempt)
	}
	if stored.TerminalSubmitted {
		t.Fatal("stale attempt took the terminal-submission slot from the new owner")
	}
}

// A lease lost at the L4→L5 boundary starts no L5 stage: no L5-a Job and no record.
func TestProcess_LeaseLostBeforeL5_NoL5Stage(t *testing.T) {
	useFastWorkerTicks(t)
	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)
	store := newTestStore(t)

	req := newTestJob()
	req.JobID = "lease-lost-l5-job"
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.ActionProfile, work.ActionSecurityScan}
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	react, _ := takeOverOnFirstGet(t, store, job)
	kube.PrependReactor("get", "jobs", react)

	New(store, kube, "test-worker").WithVaultClient(vc).process(context.Background(), job)

	for _, a := range kube.Actions() {
		if ca, ok := a.(k8stesting.CreateActionImpl); ok {
			if obj, ok := ca.GetObject().(metav1.Object); ok && strings.HasPrefix(obj.GetName(), "l5a-") {
				t.Fatalf("stale attempt created L5-a Job %q after losing its lease", obj.GetName())
			}
		}
	}
	if len(captured) != 0 {
		t.Fatalf("stale attempt submitted %d record(s), want 0", len(captured))
	}
}

// completionRejectingStore accepts heartbeats but rejects CompleteJob, as the
// fenced store does for a superseded generation.
type completionRejectingStore struct{ work.Store }

func (completionRejectingStore) CompleteJob(context.Context, string, string, int, string) error {
	return work.ErrNotFound
}

// The completion counter moves only after the store accepted CompleteJob.
func TestFinishAfterL4_RejectedCompletion_NoMetric(t *testing.T) {
	store := newTestStore(t)
	m, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateJob(context.Background(), newSmokeRunOnlyJob()); err != nil {
		t.Fatal(err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w := New(completionRejectingStore{store}, fake.NewClientset(), "test-worker").WithMetrics(m)
	ctx, release := withLeaseFence(context.Background())
	defer release(nil)
	w.finishAfterL4(ctx, slog.Default(), job, stagePlan{}, false)
	if got := jobsCompleted(t, m); got != 0 {
		t.Fatalf("jobs_completed = %d after a rejected CompleteJob, want 0", got)
	}

	// Control: an accepted completion counts once.
	New(store, fake.NewClientset(), "test-worker").WithMetrics(m).finishAfterL4(ctx, slog.Default(), job, stagePlan{}, false)
	if got := jobsCompleted(t, m); got != 1 {
		t.Fatalf("jobs_completed = %d after an accepted CompleteJob, want 1", got)
	}
}

// An ambiguous heartbeat failure (not a fenced miss) is not treated as lease loss.
func TestHeartbeat_AmbiguousErrorIsNotLeaseLoss(t *testing.T) {
	store := newTestStore(t)
	if _, err := store.CreateJob(context.Background(), newSmokeRunOnlyJob()); err != nil {
		t.Fatal(err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w := New(store, fake.NewClientset(), "test-worker")
	ctx, release := withLeaseFence(context.Background())
	defer release(nil)

	canceled, cancel := context.WithCancel(ctx)
	cancel() // Heartbeat fails with context.Canceled, not work.ErrNotFound
	w.heartbeat(canceled, slog.Default(), job)
	if leaseLost(ctx) {
		t.Fatal("an ambiguous heartbeat error was treated as a confirmed lease loss")
	}
	if !w.stillLeased(ctx, slog.Default(), job) {
		t.Fatal("stillLeased = false while this attempt holds the lease")
	}

	takeOver(t, store, job)
	if w.stillLeased(ctx, slog.Default(), job) || !leaseLost(ctx) {
		t.Fatal("a fenced heartbeat miss was not treated as lease loss")
	}
}
