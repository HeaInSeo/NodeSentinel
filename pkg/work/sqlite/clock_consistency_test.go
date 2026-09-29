package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// Heartbeat, finish and delivery writes use the same injected clock as LeaseJob,
// so a lease-expiry boundary is decided by one clock: a heartbeat extends the
// lease from the injected now, and a reclaim happens exactly when that
// extension has passed on the same clock.
func TestStoreWritesUseInjectedClock(t *testing.T) {
	ctx := context.Background()
	store, clock := newClockedStore(t)
	if _, err := store.CreateJob(ctx, sampleRequest("job-clock")); err != nil {
		t.Fatal(err)
	}
	job := mustLease(t, store, "worker-a")

	clock.Set(t0.Add(30 * time.Second))
	if err := store.Heartbeat(ctx, job.JobID, "worker-a", job.Attempt, time.Minute); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	got := mustGet(t, store, job.JobID)
	if want := t0.Add(90 * time.Second); got.LeaseUntil == nil || !got.LeaseUntil.Equal(want) {
		t.Fatalf("lease_until = %v, want injected now + ttl = %v", got.LeaseUntil, want)
	}
	if !got.UpdatedAt.Equal(t0.Add(30 * time.Second)) {
		t.Fatalf("heartbeat updated_at = %v, want injected now", got.UpdatedAt)
	}

	// Just before the heartbeat's deadline on the injected clock: not reclaimable.
	clock.Set(t0.Add(90*time.Second - time.Nanosecond))
	if _, err := store.LeaseJob(ctx, "worker-b", time.Minute); !errors.Is(err, work.ErrNoAvailableJob) {
		t.Fatalf("reclaim before injected deadline: err = %v, want ErrNoAvailableJob", err)
	}
	// Past it: reclaimed by the next attempt.
	clock.Set(t0.Add(91 * time.Second))
	next := mustLease(t, store, "worker-b")
	if next.JobID != job.JobID || next.Attempt != job.Attempt+1 {
		t.Fatalf("reclaim = %s attempt %d, want %s attempt %d", next.JobID, next.Attempt, job.JobID, job.Attempt+1)
	}

	clock.Set(t0.Add(2 * time.Minute))
	if err := store.CompleteJob(ctx, next.JobID, "worker-b", next.Attempt, "done"); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}
	if got := mustGet(t, store, job.JobID); !got.UpdatedAt.Equal(t0.Add(2 * time.Minute)) {
		t.Fatalf("finish updated_at = %v, want injected now", got.UpdatedAt)
	}

	clock.Set(t0.Add(3 * time.Minute))
	if claimed, err := store.ClaimTerminal(ctx, job.JobID, "worker-b", next.Attempt); err != nil || !claimed {
		t.Fatalf("ClaimTerminal = %v, %v", claimed, err)
	}
	if err := store.MarkFirstDeliveryPending(
		ctx, job.JobID, "worker-b", next.Attempt, "{}", "boom", t0.Add(4*time.Minute),
	); err != nil {
		t.Fatalf("MarkFirstDeliveryPending: %v", err)
	}
	if got := mustGet(t, store, job.JobID); !got.UpdatedAt.Equal(t0.Add(3 * time.Minute)) {
		t.Fatalf("delivery updated_at = %v, want injected now", got.UpdatedAt)
	}
	// Not due on the injected clock yet, then due.
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 0 {
		t.Fatalf("claim before next_attempt_at: %d claimed, err %v", len(claimed), err)
	}
	clock.Set(t0.Add(4 * time.Minute))
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 1 {
		t.Fatalf("claim at next_attempt_at: %d claimed, err %v", len(claimed), err)
	}
}
