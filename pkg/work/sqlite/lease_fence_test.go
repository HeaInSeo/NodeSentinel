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

// openAt opens a Store on path and closes it at test end. Several handles on
// the same file model separate processes sharing one PersistentVolume: a
// replacement Pod and a predecessor that has not yet stopped.
func openAt(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestLeaseGeneration_RestartReclaimFencesStaleOwner walks create → lease →
// restart → reclaim across real Store reopen. The first process leases the
// job and mints an execution, then its lease expires (it crashed or stalled).
// A restarted process reopens the file, reclaims the job on a new attempt and
// adopts the same execution identity. The first process then resumes and
// reports: its Heartbeat, CompleteJob and FailJob for the superseded attempt
// are an ambiguous outcome for a lease it no longer holds, so every one must
// fail closed with ErrNotFound and leave the reclaimed lease untouched.
func TestLeaseGeneration_RestartReclaimFencesStaleOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")

	first, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	if _, err := first.CreateJob(ctx, sampleRequest("job-restart")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// Negative TTL: the lease is already expired, as if the process died
	// without heartbeating again.
	gen1, err := first.LeaseJob(ctx, "owner-gen-1", -time.Second)
	if err != nil {
		t.Fatalf("LeaseJob gen-1: %v", err)
	}
	execID, adopted, err := first.EnsureExecution(ctx, gen1.JobID, "owner-gen-1")
	if err != nil || adopted {
		t.Fatalf("EnsureExecution gen-1 = (%q, adopted=%v, %v), want a fresh mint", execID, adopted, err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restarted := openAt(t, path)
	gen2, err := restarted.LeaseJob(ctx, "owner-gen-2", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob after restart: %v", err)
	}
	if gen2.JobID != gen1.JobID || gen2.Attempt != gen1.Attempt+1 {
		t.Fatalf("reclaimed %s attempt %d, want %s attempt %d", gen2.JobID, gen2.Attempt, gen1.JobID, gen1.Attempt+1)
	}
	adoptedID, adopted, err := restarted.EnsureExecution(ctx, gen2.JobID, "owner-gen-2")
	if err != nil || !adopted || adoptedID != execID {
		t.Fatalf("EnsureExecution after restart = (%q, adopted=%v, %v), want adopt %q", adoptedID, adopted, err, execID)
	}

	// The superseded process resumes on its own handle.
	stale := openAt(t, path)
	if err := stale.Heartbeat(ctx, gen1.JobID, "owner-gen-1", gen1.Attempt, time.Minute); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("stale Heartbeat err = %v, want ErrNotFound", err)
	}
	if err := stale.CompleteJob(ctx, gen1.JobID, "owner-gen-1", gen1.Attempt, "stale success"); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("stale CompleteJob err = %v, want ErrNotFound", err)
	}
	if err := stale.FailJob(ctx, gen1.JobID, "owner-gen-1", gen1.Attempt, "stale failure", false, 0); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("stale FailJob err = %v, want ErrNotFound", err)
	}

	got, err := restarted.GetJob(ctx, gen1.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusLeased || got.LeaseOwner != "owner-gen-2" || got.Attempt != gen2.Attempt {
		t.Fatalf("stale reports changed the reclaimed lease: status=%q owner=%q attempt=%d", got.Status, got.LeaseOwner, got.Attempt)
	}
	if got.ResultSummary != "" || got.LastError != "" {
		t.Fatalf("stale reports leaked an outcome: summary=%q lastError=%q", got.ResultSummary, got.LastError)
	}
	if got.ExecutionID != execID || got.ExecutionTerminal {
		t.Fatalf("execution identity changed: id=%q terminal=%v, want %q live", got.ExecutionID, got.ExecutionTerminal, execID)
	}

	// The current generation still owns the job and can finish it.
	if err := restarted.Heartbeat(ctx, gen2.JobID, "owner-gen-2", gen2.Attempt, time.Minute); err != nil {
		t.Fatalf("current Heartbeat: %v", err)
	}
	if err := restarted.CompleteJob(ctx, gen2.JobID, "owner-gen-2", gen2.Attempt, "done"); err != nil {
		t.Fatalf("current CompleteJob: %v", err)
	}
}

// TestLeaseGeneration_SameOwnerNameStaleAttemptFenced covers the constant
// worker-name deployment NodeSentinel used to run: the same owner string
// leases the job again after its own lease expired. The owner check alone
// would accept the old attempt's Heartbeat/CompleteJob; the attempt fence
// must not.
func TestLeaseGeneration_SameOwnerNameStaleAttemptFenced(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	if _, err := store.CreateJob(ctx, sampleRequest("job-same-owner")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	old, err := store.LeaseJob(ctx, "worker-0", -time.Second)
	if err != nil {
		t.Fatalf("LeaseJob old: %v", err)
	}
	current, err := store.LeaseJob(ctx, "worker-0", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob current: %v", err)
	}
	if current.Attempt != old.Attempt+1 {
		t.Fatalf("attempt = %d, want %d", current.Attempt, old.Attempt+1)
	}

	if err := store.Heartbeat(ctx, old.JobID, "worker-0", old.Attempt, time.Minute); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("stale-attempt Heartbeat err = %v, want ErrNotFound", err)
	}
	if err := store.CompleteJob(ctx, old.JobID, "worker-0", old.Attempt, "stale"); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("stale-attempt CompleteJob err = %v, want ErrNotFound", err)
	}
	got, err := store.GetJob(ctx, old.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusLeased || got.Attempt != current.Attempt {
		t.Fatalf("stale attempt changed the job: status=%q attempt=%d", got.Status, got.Attempt)
	}
	if err := store.CompleteJob(ctx, current.JobID, "worker-0", current.Attempt, "done"); err != nil {
		t.Fatalf("current-attempt CompleteJob: %v", err)
	}
}

// TestTerminalJob_NoReplayAfterRestart proves a terminal job stays terminal
// across a Store reopen: it is not leased again, and a duplicate Heartbeat or
// CompleteJob from the finishing lease generation is rejected rather than
// re-applied.
func TestTerminalJob_NoReplayAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")

	first, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	for _, id := range []string{"job-succeeded", "job-failed"} {
		if _, err := first.CreateJob(ctx, sampleRequest(id)); err != nil {
			t.Fatalf("CreateJob %s: %v", id, err)
		}
	}
	done, err := first.LeaseJob(ctx, "owner-a", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	if err := first.CompleteJob(ctx, done.JobID, "owner-a", done.Attempt, "ok"); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}
	failed, err := first.LeaseJob(ctx, "owner-a", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob second: %v", err)
	}
	if err := first.FailJob(ctx, failed.JobID, "owner-a", failed.Attempt, "contract failed", false, 0); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAt(t, path)
	if job, err := reopened.LeaseJob(ctx, "owner-b", time.Minute); !errors.Is(err, work.ErrNoAvailableJob) {
		t.Fatalf("LeaseJob after restart = (%v, %v), want ErrNoAvailableJob (terminal jobs must not replay)", job, err)
	}
	for _, j := range []*work.Job{done, failed} {
		if err := reopened.Heartbeat(ctx, j.JobID, "owner-a", j.Attempt, time.Minute); !errors.Is(err, work.ErrNotFound) {
			t.Fatalf("%s: duplicate Heartbeat err = %v, want ErrNotFound", j.JobID, err)
		}
		if err := reopened.CompleteJob(ctx, j.JobID, "owner-a", j.Attempt, "replayed"); !errors.Is(err, work.ErrNotFound) {
			t.Fatalf("%s: duplicate CompleteJob err = %v, want ErrNotFound", j.JobID, err)
		}
	}
	wantStatus := map[string]work.Status{done.JobID: work.StatusSucceeded, failed.JobID: work.StatusFailed}
	for id, want := range wantStatus {
		got, err := reopened.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("GetJob %s: %v", id, err)
		}
		if got.Status != want || got.ResultSummary == "replayed" {
			t.Fatalf("%s after replay attempts: status=%q summary=%q, want %q unchanged", id, got.Status, got.ResultSummary, want)
		}
	}
}

// TestLeaseGeneration_ConcurrentHandlesSingleWinner races LeaseJob from two
// Store handles on one file (two processes on one volume) for a single job,
// then races the stale and current generations' CompleteJob. Exactly one
// lease is granted, and only the current generation's completion applies.
func TestLeaseGeneration_ConcurrentHandlesSingleWinner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")
	a := openAt(t, path)
	b := openAt(t, path)
	if _, err := a.CreateJob(ctx, sampleRequest("job-race")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	const callers = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		leases    []*work.Job
		otherErrs []error
	)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := a
			if i%2 == 1 {
				s = b
			}
			job, err := s.LeaseJob(ctx, "racer", time.Minute)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				leases = append(leases, job)
			case !errors.Is(err, work.ErrNoAvailableJob):
				otherErrs = append(otherErrs, err)
			}
		}(i)
	}
	wg.Wait()
	if len(otherErrs) != 0 {
		t.Fatalf("unexpected LeaseJob errors: %v", otherErrs)
	}
	if len(leases) != 1 {
		t.Fatalf("leases granted = %d, want exactly 1", len(leases))
	}
	gen1 := leases[0]

	// Expire gen-1 and let the other handle reclaim as gen-2.
	if err := a.Heartbeat(ctx, gen1.JobID, "racer", gen1.Attempt, -time.Second); err != nil {
		t.Fatalf("expire gen-1: %v", err)
	}
	gen2, err := b.LeaseJob(ctx, "racer", time.Minute)
	if err != nil {
		t.Fatalf("reclaim gen-2: %v", err)
	}

	var staleErr, currentErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		staleErr = a.CompleteJob(ctx, gen1.JobID, "racer", gen1.Attempt, "stale")
	}()
	go func() {
		defer wg.Done()
		currentErr = b.CompleteJob(ctx, gen2.JobID, "racer", gen2.Attempt, "current")
	}()
	wg.Wait()
	if !errors.Is(staleErr, work.ErrNotFound) {
		t.Fatalf("stale CompleteJob err = %v, want ErrNotFound", staleErr)
	}
	if currentErr != nil {
		t.Fatalf("current CompleteJob: %v", currentErr)
	}
	got, err := a.GetJob(ctx, gen1.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusSucceeded || got.ResultSummary != "current" {
		t.Fatalf("final job status=%q summary=%q, want succeeded/current", got.Status, got.ResultSummary)
	}
}
