package sqlite_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

// These tests cover the store half of issue #2: the durable execution
// identity that stops a reclaimed lease from starting a second physical
// execution, and the on-disk survival that makes the WorkStore worth
// putting on a PersistentVolume in the first place.

// TestEnsureExecution_AdoptsUntilTerminal is the core contract. An execution
// that has not been observed terminal must be adopted by every subsequent
// attempt — that is what keeps one logical validation to one physical K8s
// Job — and only once it is marked terminal may a new identity be minted.
func TestEnsureExecution_AdoptsUntilTerminal(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-exec")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	first, adopted, err := store.EnsureExecution(ctx, "job-exec", "worker-1")
	if err != nil {
		t.Fatalf("EnsureExecution (first): %v", err)
	}
	if adopted {
		t.Fatal("the first execution must be minted, not adopted")
	}
	if first == "" {
		t.Fatal("minted execution id is empty")
	}

	// A different worker on a later attempt — the reclaimed-lease case. It
	// must land on the same identity, or it would create a second Job
	// beside one that may still be running.
	second, adopted, err := store.EnsureExecution(ctx, "job-exec", "worker-2")
	if err != nil {
		t.Fatalf("EnsureExecution (second): %v", err)
	}
	if !adopted {
		t.Fatal("a non-terminal execution must be adopted, not re-minted")
	}
	if second != first {
		t.Fatalf("adopted id = %q, want the in-flight %q", second, first)
	}

	// Once the execution is observed terminal, a new one may be minted.
	if err := store.MarkExecutionTerminal(ctx, "job-exec", first); err != nil {
		t.Fatalf("MarkExecutionTerminal: %v", err)
	}
	third, adopted, err := store.EnsureExecution(ctx, "job-exec", "worker-3")
	if err != nil {
		t.Fatalf("EnsureExecution (third): %v", err)
	}
	if adopted {
		t.Fatal("a terminal execution must not be adopted — the retry needs its own identity")
	}
	if third == first {
		t.Fatalf("new execution id %q collides with the terminal one — the retry's Job would "+
			"collide with the previous execution's leftover object", third)
	}

	// Minting must also clear the terminal flag, or the very next attempt
	// would treat this brand-new execution as already finished.
	stored, err := store.GetJob(ctx, "job-exec")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionID != third {
		t.Fatalf("persisted execution id = %q, want %q", stored.ExecutionID, third)
	}
	if stored.ExecutionTerminal {
		t.Fatal("a freshly minted execution must not be marked terminal")
	}
}

// TestEnsureExecution_SurvivesLeaseChurn pins the specific sequence that
// produced duplicate executions: LeaseJob bumps Attempt on every lease,
// including when it reclaims a merely-expired one. The execution identity
// must not move with it.
func TestEnsureExecution_SurvivesLeaseChurn(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-churn")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Attempt 1 leases with an already-expired TTL, so the next LeaseJob
	// reclaims it exactly the way it reclaims a dead worker's job.
	leased1, err := store.LeaseJob(ctx, "worker-1", -time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 1): %v", err)
	}
	first, _, err := store.EnsureExecution(ctx, "job-churn", "worker-1")
	if err != nil {
		t.Fatalf("EnsureExecution (attempt 1): %v", err)
	}

	leased2, err := store.LeaseJob(ctx, "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 2, reclaim): %v", err)
	}
	if leased2.Attempt <= leased1.Attempt {
		t.Fatalf("attempt did not advance on reclaim (%d -> %d); this test would not be "+
			"exercising the duplicate-execution scenario", leased1.Attempt, leased2.Attempt)
	}

	second, adopted, err := store.EnsureExecution(ctx, "job-churn", "worker-2")
	if err != nil {
		t.Fatalf("EnsureExecution (attempt 2): %v", err)
	}
	if !adopted || second != first {
		t.Fatalf("reclaiming attempt got execution %q (adopted=%v), want the in-flight %q — "+
			"a different identity here means a second concurrent K8s Job", second, adopted, first)
	}
}

// TestMarkExecutionTerminal_UnknownAndUnmintedJobs covers the two edge cases
// the worker's cleanup paths can reach.
func TestMarkExecutionTerminal_UnknownAndUnmintedJobs(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if err := store.MarkExecutionTerminal(ctx, "no-such-job", "a1-0000000000000000"); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("MarkExecutionTerminal on a missing job = %v, want ErrNotFound", err)
	}

	// A job that exists but has never minted an execution: nothing to mark,
	// and it must not error — the worker reaches this on a create failure.
	if _, err := store.CreateJob(ctx, sampleRequest("job-unminted")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := store.MarkExecutionTerminal(ctx, "job-unminted", ""); err != nil {
		t.Fatalf("MarkExecutionTerminal on an unminted job = %v, want nil", err)
	}

	// Crucially, that must not have left the job looking like it had run.
	stored, err := store.GetJob(ctx, "job-unminted")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionID != "" {
		t.Fatalf("execution id = %q, want empty", stored.ExecutionID)
	}

	// EnsureExecution keys off the ID being empty, so this job must still
	// mint (not adopt) its first execution.
	if _, adopted, err := store.EnsureExecution(ctx, "job-unminted", "worker-1"); err != nil {
		t.Fatalf("EnsureExecution: %v", err)
	} else if adopted {
		t.Fatal("a job with no execution identity must mint one, not adopt")
	}
}

// TestMarkExecutionTerminal_StaleExecutionCannotReleaseReplacement: a worker
// resumes after its lease expired and reports its execution E1 terminal, but
// another worker has already observed E1 terminal and minted E2. The stale
// report must leave E2 non-terminal. Otherwise the next attempt mints E3
// while E2's Job is still running.
func TestMarkExecutionTerminal_StaleExecutionCannotReleaseReplacement(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-stale")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	e1, _, err := store.EnsureExecution(ctx, "job-stale", "worker-1")
	if err != nil {
		t.Fatalf("EnsureExecution (E1): %v", err)
	}
	// Worker 2 adopted E1, observed it terminal and minted E2.
	if err := store.MarkExecutionTerminal(ctx, "job-stale", e1); err != nil {
		t.Fatalf("MarkExecutionTerminal (E1 by worker 2): %v", err)
	}
	e2, adopted, err := store.EnsureExecution(ctx, "job-stale", "worker-2")
	if err != nil {
		t.Fatalf("EnsureExecution (E2): %v", err)
	}
	if adopted || e2 == e1 {
		t.Fatalf("E2 = %q (adopted=%v), want a fresh identity distinct from E1 %q", e2, adopted, e1)
	}

	// The stale worker 1 now reports E1 terminal.
	if err := store.MarkExecutionTerminal(ctx, "job-stale", e1); !errors.Is(err, work.ErrExecutionSuperseded) {
		t.Fatalf("stale MarkExecutionTerminal(E1) = %v, want ErrExecutionSuperseded", err)
	}
	stored, err := store.GetJob(ctx, "job-stale")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionID != e2 || stored.ExecutionTerminal {
		t.Fatalf("after stale E1 report: execution %q terminal=%v, want E2 %q still non-terminal",
			stored.ExecutionID, stored.ExecutionTerminal, e2)
	}
	e3, adopted, err := store.EnsureExecution(ctx, "job-stale", "worker-3")
	if err != nil {
		t.Fatalf("EnsureExecution (next attempt): %v", err)
	}
	if !adopted || e3 != e2 {
		t.Fatalf("next attempt got %q (adopted=%v), want to adopt running E2 %q — a fresh identity "+
			"here is a second concurrent Job", e3, adopted, e2)
	}

	// The current holder can still release E2.
	if err := store.MarkExecutionTerminal(ctx, "job-stale", e2); err != nil {
		t.Fatalf("MarkExecutionTerminal (E2): %v", err)
	}
}

func TestEnsureExecution_UnknownJob(t *testing.T) {
	store := newStore(t)

	if _, _, err := store.EnsureExecution(context.Background(), "no-such-job", "worker-1"); !errors.Is(
		err, work.ErrNotFound,
	) {
		t.Fatalf("EnsureExecution on a missing job = %v, want ErrNotFound", err)
	}
}

// TestWorkStore_SurvivesReopen is the durability claim itself, at the level
// this repo can actually test: queued/leased/running work — and the
// execution identity attached to it — must still be there after the store
// is closed and reopened from the same path. That is what a Pod replacement
// does to the database, minus the volume.
//
// This is NOT a substitute for verifying real Pod replacement against a
// cluster; see docs/DEPLOYMENT.md §9.
func TestWorkStore_SurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")

	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}

	// One job left queued, one leased-and-running with an execution in
	// flight — the two states a rollout would otherwise destroy.
	if _, err := store.CreateJob(ctx, sampleRequest("job-queued")); err != nil {
		t.Fatalf("CreateJob (queued): %v", err)
	}
	if _, err := store.CreateJob(ctx, sampleRequest("job-running")); err != nil {
		t.Fatalf("CreateJob (running): %v", err)
	}
	leased, err := store.LeaseJob(ctx, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	executionID, _, err := store.EnsureExecution(ctx, leased.JobID, "worker-1")
	if err != nil {
		t.Fatalf("EnsureExecution: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	for _, id := range []string{"job-queued", "job-running"} {
		if _, err := reopened.GetJob(ctx, id); err != nil {
			t.Fatalf("GetJob(%s) after reopen: %v — queued work did not survive", id, err)
		}
	}

	got, err := reopened.GetJob(ctx, leased.JobID)
	if err != nil {
		t.Fatalf("GetJob after reopen: %v", err)
	}
	if got.ExecutionID != executionID {
		t.Fatalf("execution id after reopen = %q, want %q — an in-flight execution that does not "+
			"survive restart is one the next attempt would duplicate", got.ExecutionID, executionID)
	}
	if got.ExecutionTerminal {
		t.Fatal("execution was non-terminal before the restart and must stay adoptable after it")
	}

	// And the adoption decision itself must still come out the same way.
	adoptedID, adopted, err := reopened.EnsureExecution(ctx, leased.JobID, "worker-2")
	if err != nil {
		t.Fatalf("EnsureExecution after reopen: %v", err)
	}
	if !adopted || adoptedID != executionID {
		t.Fatalf("after restart got execution %q (adopted=%v), want the in-flight %q",
			adoptedID, adopted, executionID)
	}
}

// TestNew_UnwritableDirectory_ReportsActionableError covers the permission
// failure that fsGroup exists to prevent. SQLite reports it as "unable to
// open database file", which names neither the directory nor the setting
// that fixes it; New must fail earlier with a diagnostic that does.
func TestNew_UnwritableDirectory_ReportsActionableError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: mode bits do not deny access, so this failure cannot be provoked")
	}

	dir := filepath.Join(t.TempDir(), "readonly")
	if err := os.Mkdir(dir, 0o555); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Restore write permission so t.TempDir's cleanup can remove it.
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	_, err := sqlite.New(filepath.Join(dir, "nodesentinel.sqlite"))
	if err == nil {
		t.Fatal("expected sqlite.New to fail on an unwritable directory")
	}
	if !strings.Contains(err.Error(), "fsGroup") {
		t.Errorf("error %q does not mention fsGroup — the operator is left debugging a bare "+
			"permission error from the outside, which is the whole problem this check exists to fix", err)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Errorf("error %q does not name the offending directory %q", err, dir)
	}
}

// TestNew_MissingDirectory_ReportsMountHint covers the other deployment
// shape of the same class of failure: the volume is not mounted at all.
func TestNew_MissingDirectory_ReportsMountHint(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "not-mounted", "nodesentinel.sqlite")

	_, err := sqlite.New(missing)
	if err == nil {
		t.Fatal("expected sqlite.New to fail when the parent directory does not exist")
	}
	if !strings.Contains(err.Error(), "mounted") {
		t.Errorf("error %q should point at the volume mount", err)
	}
}
