package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

// testClock is a settable clock for the store's retry_not_before checks.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// t0 carries sub-second precision on purpose: due times are compared at full
// nanosecond precision.
var t0 = time.Date(2026, 9, 23, 11, 0, 0, 123456789, time.UTC)

func openClockedStore(t *testing.T, path string, clock *testClock) *sqlite.Store {
	t.Helper()
	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	sqlite.SetClock(store, clock.Now)
	return store
}

func newClockedStore(t *testing.T) (*sqlite.Store, *testClock) {
	t.Helper()
	clock := &testClock{now: t0}
	return openClockedStore(t, filepath.Join(t.TempDir(), "nodesentinel.sqlite"), clock), clock
}

func mustLease(t *testing.T, store *sqlite.Store, worker string) *work.Job {
	t.Helper()
	job, err := store.LeaseJob(context.Background(), worker, time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob(%s): %v", worker, err)
	}
	return job
}

func mustGet(t *testing.T, store *sqlite.Store, jobID string) *work.Job {
	t.Helper()
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJob(%s): %v", jobID, err)
	}
	return job
}

func assertNoLeasable(t *testing.T, store *sqlite.Store, why string) {
	t.Helper()
	job, err := store.LeaseJob(context.Background(), "probe-worker", time.Minute)
	if !errors.Is(err, work.ErrNoAvailableJob) {
		t.Fatalf("%s: LeaseJob = (%v, %v), want ErrNoAvailableJob", why, job, err)
	}
}

func TestFailJob_Retryable_RequeuesWithDueTimeInSameWrite(t *testing.T) {
	store, _ := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "transient", true, 3*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}

	got := mustGet(t, store, "job-a")
	if got.Status != work.StatusQueued || got.LeaseOwner != "" || got.LeaseUntil != nil {
		t.Fatalf("after retryable FailJob: status=%q owner=%q leaseUntil=%v, want queued/unowned", got.Status, got.LeaseOwner, got.LeaseUntil)
	}
	if got.RetryNotBefore == nil || !got.RetryNotBefore.Equal(t0.Add(3*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want exactly %v", got.RetryNotBefore, t0.Add(3*time.Second))
	}
}

func TestLeaseJob_RetryNotBefore_SkipBeforeDueEligibleAtDue(t *testing.T) {
	store, clock := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	first := mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "transient", true, 3*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	due := t0.Add(3 * time.Second)

	clock.Set(due.Add(-time.Nanosecond))
	assertNoLeasable(t, store, "1ns before due")

	clock.Set(due)
	second := mustLease(t, store, "worker-b")
	if second.JobID != "job-a" || second.Attempt != first.Attempt+1 {
		t.Fatalf("lease at due = (%s, attempt %d), want (job-a, attempt %d)", second.JobID, second.Attempt, first.Attempt+1)
	}
	if second.RetryNotBefore != nil {
		t.Errorf("leased job still carries RetryNotBefore %v; leasing must clear it", second.RetryNotBefore)
	}
}

func TestLeaseJob_RetryNotBefore_ComparedNumericallyAcrossPrecisionAndZone(t *testing.T) {
	// The due time has a fractional second; "now" is the whole second just
	// before it. A text comparison of RFC3339Nano strings would put
	// "...:00Z" after "...:00.5Z" and lease early.
	store, clock := newClockedStore(t)
	ctx := context.Background()

	whole := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	clock.Set(whole.Add(-2500 * time.Millisecond))
	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "transient", true, 3*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	// due = whole + 500ms

	kst := time.FixedZone("KST", 9*60*60)
	clock.Set(whole.In(kst))
	assertNoLeasable(t, store, "whole second before a fractional due time, non-UTC clock")

	clock.Set(whole.Add(500 * time.Millisecond).In(kst))
	mustLease(t, store, "worker-b")
}

func TestLeaseJob_DelayedOldestDoesNotBlockDueJob(t *testing.T) {
	store, clock := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-old")); err != nil {
		t.Fatalf("CreateJob old: %v", err)
	}
	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-old", "worker-a", 1, "transient", true, 30*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if _, err := store.CreateJob(ctx, sampleRequest("job-new")); err != nil {
		t.Fatalf("CreateJob new: %v", err)
	}

	if got := mustLease(t, store, "worker-b"); got.JobID != "job-new" {
		t.Fatalf("leased %q, want job-new (the only due job)", got.JobID)
	}
	assertNoLeasable(t, store, "old job still delayed, new job leased")

	clock.Set(t0.Add(30 * time.Second))
	if got := mustLease(t, store, "worker-c"); got.JobID != "job-old" {
		t.Fatalf("leased %q after due, want job-old", got.JobID)
	}
}

func TestFailJob_DuplicateAndStaleCalls_ChangeNothing(t *testing.T) {
	store, clock := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "first", true, 5*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	afterFirst := mustGet(t, store, "job-a")

	// Duplicate of the same report: the lease is already released.
	clock.Set(t0.Add(time.Second))
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "duplicate", true, 20*time.Second); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("duplicate FailJob err = %v, want ErrNotFound", err)
	}
	if got := mustGet(t, store, "job-a"); !sameRow(got, afterFirst) {
		t.Fatalf("duplicate FailJob changed the row:\nbefore %+v\nafter  %+v", afterFirst, got)
	}

	// Stale report from worker-a after worker-b re-leased the job.
	clock.Set(t0.Add(5 * time.Second))
	mustLease(t, store, "worker-b")
	leasedByB := mustGet(t, store, "job-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "stale", true, time.Second); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("stale FailJob err = %v, want ErrNotFound", err)
	}
	if got := mustGet(t, store, "job-a"); !sameRow(got, leasedByB) {
		t.Fatalf("stale FailJob changed the row:\nbefore %+v\nafter  %+v", leasedByB, got)
	}

	// A retryable report after the job is terminal must not requeue it.
	if err := store.FailJob(ctx, "job-a", "worker-b", 2, "deterministic", false, 0); err != nil {
		t.Fatalf("terminal FailJob: %v", err)
	}
	terminal := mustGet(t, store, "job-a")
	if err := store.FailJob(ctx, "job-a", "worker-b", 2, "late retry", true, time.Second); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("FailJob on a failed job err = %v, want ErrNotFound", err)
	}
	if got := mustGet(t, store, "job-a"); !sameRow(got, terminal) || got.Status != work.StatusFailed {
		t.Fatalf("late retryable FailJob changed a terminal job: %+v", got)
	}
}

// TestFailJob_StaleAttemptSameWorkerName_ChangesNothing is the DQ-R2.1-N1
// lease-generation case: the same worker name re-leases the job as attempt
// n+1 after its lease on attempt n expired; a late FailJob for attempt n must
// not touch the new lease, its status, or its due time.
func TestFailJob_StaleAttemptSameWorkerName_ChangesNothing(t *testing.T) {
	store, clock := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	first, err := store.LeaseJob(ctx, "worker-a", time.Second)
	if err != nil {
		t.Fatalf("LeaseJob attempt 1: %v", err)
	}
	clock.Set(t0.Add(2 * time.Second)) // attempt 1's lease has expired
	second := mustLease(t, store, "worker-a")
	if second.Attempt != first.Attempt+1 || second.LeaseOwner != "worker-a" {
		t.Fatalf("re-lease = (attempt %d, owner %q), want (attempt %d, worker-a)", second.Attempt, second.LeaseOwner, first.Attempt+1)
	}
	current := mustGet(t, store, "job-a")

	for _, retryable := range []bool{true, false} {
		err := store.FailJob(ctx, "job-a", "worker-a", first.Attempt, "stale attempt", retryable, 10*time.Second)
		if !errors.Is(err, work.ErrNotFound) {
			t.Fatalf("stale FailJob(retryable=%v) err = %v, want ErrNotFound", retryable, err)
		}
		if got := mustGet(t, store, "job-a"); !sameRow(got, current) {
			t.Fatalf("stale FailJob(retryable=%v) changed the live lease:\nbefore %+v\nafter  %+v", retryable, current, got)
		}
	}

	// The live generation can still fail its own attempt.
	if err := store.FailJob(ctx, "job-a", "worker-a", second.Attempt, "transient", true, 3*time.Second); err != nil {
		t.Fatalf("FailJob for the live attempt: %v", err)
	}
	if got := mustGet(t, store, "job-a"); got.Status != work.StatusQueued || got.RetryNotBefore == nil {
		t.Fatalf("live FailJob: status=%q RetryNotBefore=%v, want queued with a due time", got.Status, got.RetryNotBefore)
	}
}

// sameRow compares the fields FailJob could write.
func sameRow(a, b *work.Job) bool {
	return a.Status == b.Status && a.LeaseOwner == b.LeaseOwner && a.LastError == b.LastError &&
		a.Attempt == b.Attempt && a.UpdatedAt.Equal(b.UpdatedAt) &&
		equalTimePtr(a.LeaseUntil, b.LeaseUntil) && equalTimePtr(a.RetryNotBefore, b.RetryNotBefore)
}

func equalTimePtr(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

func TestRetryNotBefore_SurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.sqlite")
	clock := &testClock{now: t0}
	ctx := context.Background()

	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	sqlite.SetClock(store, clock.Now)
	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "transient", true, 20*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openClockedStore(t, path, clock)
	clock.Set(t0.Add(time.Second))
	got := mustGet(t, reopened, "job-a")
	if got.RetryNotBefore == nil || !got.RetryNotBefore.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("RetryNotBefore after reopen = %v, want %v", got.RetryNotBefore, t0.Add(20*time.Second))
	}
	assertNoLeasable(t, reopened, "after reopen, before due")

	clock.Set(t0.Add(20 * time.Second))
	mustLease(t, reopened, "worker-b")
}

func TestFailJob_Retry_PreservesExecutionIdentityAndDeliveryState(t *testing.T) {
	store, clock := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	mustLease(t, store, "worker-a")
	execID, adopted, err := store.EnsureExecution(ctx, "job-a", "worker-a")
	if err != nil || adopted {
		t.Fatalf("EnsureExecution: id=%q adopted=%v err=%v", execID, adopted, err)
	}
	nextDelivery := time.Now().UTC().Add(time.Hour)
	if err := store.MarkResultDeliveryPending(ctx, "job-a", `{"kind":"check"}`, "nv down", nextDelivery); err != nil {
		t.Fatalf("MarkResultDeliveryPending: %v", err)
	}
	before := mustGet(t, store, "job-a")

	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "UNKNOWN [unknown-retry]", true, 2*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	after := mustGet(t, store, "job-a")

	if after.ExecutionID != before.ExecutionID || after.ExecutionTerminal != before.ExecutionTerminal {
		t.Errorf("execution identity changed: (%q, %v) -> (%q, %v)",
			before.ExecutionID, before.ExecutionTerminal, after.ExecutionID, after.ExecutionTerminal)
	}
	if after.ResultDeliveryStatus != before.ResultDeliveryStatus ||
		after.ResultDeliveryPayload != before.ResultDeliveryPayload ||
		after.ResultDeliveryAttempts != before.ResultDeliveryAttempts ||
		after.ResultDeliveryLastError != before.ResultDeliveryLastError ||
		!equalTimePtr(after.NextAttemptAt, before.NextAttemptAt) {
		t.Errorf("delivery state changed:\nbefore %+v\nafter  %+v", before, after)
	}
	if after.Attempt != before.Attempt {
		t.Errorf("Attempt changed by FailJob: %d -> %d", before.Attempt, after.Attempt)
	}

	// The next lease, once due, must adopt the still-live execution rather
	// than be allowed to start a second physical one.
	clock.Set(t0.Add(2 * time.Second))
	mustLease(t, store, "worker-b")
	againID, adopted, err := store.EnsureExecution(ctx, "job-a", "worker-b")
	if err != nil || !adopted || againID != execID {
		t.Fatalf("EnsureExecution after paced retry = (%q, adopted=%v, %v), want (%q, adopted=true)", againID, adopted, err, execID)
	}
}

func TestExpiredLeaseReclaim_NotGatedByRetryPacing(t *testing.T) {
	store, clock := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "transient", true, 30*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	clock.Set(t0.Add(30 * time.Second))
	if _, err := store.LeaseJob(ctx, "worker-b", time.Second); err != nil {
		t.Fatalf("LeaseJob at due: %v", err)
	}

	// worker-b dies; its lease expires. Reclaim must not wait for any pacing.
	clock.Set(t0.Add(32 * time.Second))
	got := mustLease(t, store, "worker-c")
	if got.JobID != "job-a" || got.LeaseOwner != "worker-c" {
		t.Fatalf("expired-lease reclaim = (%s, %s), want (job-a, worker-c)", got.JobID, got.LeaseOwner)
	}
}

func TestMigrateRetryNotBefore_ExistingRowsDueThenPacedOnNextFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-retry-pacing.sqlite")
	seedPreMigrationJobsTable(t, path, "job-old")
	clock := &testClock{now: t0}
	store := openClockedStore(t, path, clock)
	ctx := context.Background()

	if got := mustGet(t, store, "job-old"); got.RetryNotBefore != nil {
		t.Fatalf("migrated row RetryNotBefore = %v, want nil (due)", got.RetryNotBefore)
	}
	jobs, err := store.ListJobs(ctx, "")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobs full-column scan after migration = (%d jobs, %v)", len(jobs), err)
	}

	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-old", "worker-a", 1, "transient", true, 4*time.Second); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if got := mustGet(t, store, "job-old"); got.RetryNotBefore == nil || !got.RetryNotBefore.Equal(t0.Add(4*time.Second)) {
		t.Fatalf("RetryNotBefore after first post-migration failure = %v, want %v", got.RetryNotBefore, t0.Add(4*time.Second))
	}
	assertNoLeasable(t, store, "post-migration paced retry before due")
}

func TestFailJob_RetryableZeroDelay_DueImmediately(t *testing.T) {
	store, _ := newClockedStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	mustLease(t, store, "worker-a")
	if err := store.FailJob(ctx, "job-a", "worker-a", 1, "transient", true, 0); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if got := mustGet(t, store, "job-a"); got.RetryNotBefore != nil {
		t.Fatalf("RetryNotBefore = %v, want nil for a zero delay", got.RetryNotBefore)
	}
	mustLease(t, store, "worker-b")
}
