package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

func TestCreateAndGetJob(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	created, err := store.CreateJob(ctx, sampleRequest("job-create"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.Status != work.StatusQueued {
		t.Fatalf("status = %q, want %q", created.Status, work.StatusQueued)
	}

	got, err := store.GetJob(ctx, "job-create")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ToolName != "fastp" {
		t.Fatalf("tool name = %q, want fastp", got.ToolName)
	}
	if len(got.RequestedActions) != 2 {
		t.Fatalf("requested actions = %d, want 2", len(got.RequestedActions))
	}
}

func TestLeaseAndCompleteJob(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-complete")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	leased, err := store.LeaseJob(ctx, "worker-a", 30*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	if leased.LeaseOwner != "worker-a" {
		t.Fatalf("lease owner = %q, want worker-a", leased.LeaseOwner)
	}

	if err := store.Heartbeat(ctx, leased.JobID, "worker-a", 30*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if err := store.CompleteJob(ctx, leased.JobID, "worker-a", "smoke ok"); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	got, err := store.GetJob(ctx, leased.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusSucceeded {
		t.Fatalf("status = %q, want %q", got.Status, work.StatusSucceeded)
	}
	if got.ResultSummary != "smoke ok" {
		t.Fatalf("result summary = %q, want smoke ok", got.ResultSummary)
	}
}

func TestRetryableFailureReturnsJobToQueue(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-retry")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	leased, err := store.LeaseJob(ctx, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	if err := store.FailJob(ctx, leased.JobID, "worker-a", "temporary timeout", true); err != nil {
		t.Fatalf("FailJob retryable: %v", err)
	}

	got, err := store.GetJob(ctx, leased.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusQueued {
		t.Fatalf("status = %q, want %q", got.Status, work.StatusQueued)
	}
	if got.LastError != "temporary timeout" {
		t.Fatalf("last error = %q, want temporary timeout", got.LastError)
	}
}

func TestNonRetryableFailureMarksJobFailed(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-failed")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	leased, err := store.LeaseJob(ctx, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	if err := store.FailJob(ctx, leased.JobID, "worker-a", "contract failed", false); err != nil {
		t.Fatalf("FailJob non-retryable: %v", err)
	}

	got, err := store.GetJob(ctx, leased.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusFailed {
		t.Fatalf("status = %q, want %q", got.Status, work.StatusFailed)
	}
	if got.LastError != "contract failed" {
		t.Fatalf("last error = %q, want contract failed", got.LastError)
	}
}

func TestWrongWorkerCannotCompleteJob(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-owner")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	leased, err := store.LeaseJob(ctx, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	err = store.CompleteJob(ctx, leased.JobID, "worker-b", "should not complete")
	if err != work.ErrNotFound {
		t.Fatalf("CompleteJob wrong worker err = %v, want %v", err, work.ErrNotFound)
	}

	got, err := store.GetJob(ctx, leased.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusLeased {
		t.Fatalf("status = %q, want %q", got.Status, work.StatusLeased)
	}
}

func TestExpiredLeaseCanBeReclaimed(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-expired-lease")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	firstLease, err := store.LeaseJob(ctx, "worker-a", -time.Second)
	if err != nil {
		t.Fatalf("LeaseJob worker-a: %v", err)
	}
	if firstLease.LeaseOwner != "worker-a" {
		t.Fatalf("first lease owner = %q, want worker-a", firstLease.LeaseOwner)
	}

	secondLease, err := store.LeaseJob(ctx, "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob worker-b: %v", err)
	}
	if secondLease.JobID != firstLease.JobID {
		t.Fatalf("reclaimed job = %q, want %q", secondLease.JobID, firstLease.JobID)
	}
	if secondLease.LeaseOwner != "worker-b" {
		t.Fatalf("second lease owner = %q, want worker-b", secondLease.LeaseOwner)
	}
	if secondLease.Attempt != firstLease.Attempt+1 {
		t.Fatalf("attempt = %d, want %d", secondLease.Attempt, firstLease.Attempt+1)
	}
}

// TestExpiredLeaseCanBeReclaimedAfterHeartbeat reproduces the restart-recovery
// bug in https://github.com/HeaInSeo/NodeSentinel/issues/15: a job that has
// been heartbeated at least once is in status='running', not 'leased'.
// Heartbeat runs every 30s while jobs can take up to several minutes, so
// 'running' - not 'leased' - is the status a worker-crashed job is actually
// in most of the time. Before the fix, LeaseJob's reclaim query only matched
// 'queued' or a stale 'leased' row, so a stale 'running' row was never
// reclaimed and the job was stranded forever.
func TestExpiredLeaseCanBeReclaimedAfterHeartbeat(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-stale-running")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	firstLease, err := store.LeaseJob(ctx, "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob worker-a: %v", err)
	}

	// Heartbeat with a negative TTL: transitions the job to 'running' and
	// simultaneously makes that heartbeat's lease_until stale, simulating a
	// worker that heartbeated once and then crashed.
	if err := store.Heartbeat(ctx, firstLease.JobID, "worker-a", -time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	stale, err := store.GetJob(ctx, firstLease.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stale.Status != work.StatusRunning {
		t.Fatalf("status after heartbeat = %q, want %q (test setup invariant)", stale.Status, work.StatusRunning)
	}

	secondLease, err := store.LeaseJob(ctx, "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob worker-b: %v (stale running job was not reclaimed - it would be stranded forever)", err)
	}
	if secondLease.JobID != firstLease.JobID {
		t.Fatalf("reclaimed job = %q, want %q", secondLease.JobID, firstLease.JobID)
	}
	if secondLease.LeaseOwner != "worker-b" {
		t.Fatalf("second lease owner = %q, want worker-b", secondLease.LeaseOwner)
	}
	if secondLease.Status != work.StatusLeased {
		t.Fatalf("status after reclaim = %q, want %q", secondLease.Status, work.StatusLeased)
	}
}

// TestEnqueuedJobSurvivesStoreRestart exercises the real durable-enqueue
// contract end-to-end: CreateJob (the store-level equivalent of
// EnqueueValidationWork) is followed by closing the store and reopening a
// brand-new *Store at the same file path - simulating a process restart -
// then confirming the job is still there and still leasable. Existing
// migration-focused reopen tests seed rows via raw SQL that bypasses
// CreateJob entirely, so they prove file persistence but not this specific
// enqueue-then-restart contract.
func TestEnqueuedJobSurvivesStoreRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")

	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	created, err := store.CreateJob(ctx, sampleRequest("job-survives-restart"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen at the same path - a fresh *Store, exactly what a restarted
	// process would construct.
	reopened, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New (reopen): %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	got, err := reopened.GetJob(ctx, created.JobID)
	if err != nil {
		t.Fatalf("GetJob after restart: %v", err)
	}
	if got.Status != work.StatusQueued {
		t.Fatalf("status after restart = %q, want %q", got.Status, work.StatusQueued)
	}

	// Not just present in the row - actually leasable, proving the restart
	// didn't leave it in some state a worker couldn't pick up.
	leased, err := reopened.LeaseJob(ctx, "worker-after-restart", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob after restart: %v", err)
	}
	if leased.JobID != created.JobID {
		t.Fatalf("leased job = %q, want %q", leased.JobID, created.JobID)
	}
}

func TestWrongWorkerCannotFailJob(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-fail-owner")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	leased, err := store.LeaseJob(ctx, "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	err = store.FailJob(ctx, leased.JobID, "worker-b", "wrong owner", false)
	if err != work.ErrNotFound {
		t.Fatalf("FailJob wrong worker err = %v, want %v", err, work.ErrNotFound)
	}

	got, err := store.GetJob(ctx, leased.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != work.StatusLeased {
		t.Fatalf("status = %q, want %q", got.Status, work.StatusLeased)
	}
	if got.LastError != "" {
		t.Fatalf("last error = %q, want empty", got.LastError)
	}
}

func TestListJobsFiltersByStatus(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-a")); err != nil {
		t.Fatalf("CreateJob job-a: %v", err)
	}
	if _, err := store.CreateJob(ctx, sampleRequest("job-b")); err != nil {
		t.Fatalf("CreateJob job-b: %v", err)
	}
	leased, err := store.LeaseJob(ctx, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	if err := store.CompleteJob(ctx, leased.JobID, "worker-a", "done"); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	queued, err := store.ListJobs(ctx, work.StatusQueued)
	if err != nil {
		t.Fatalf("ListJobs queued: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("queued jobs = %d, want 1", len(queued))
	}

	succeeded, err := store.ListJobs(ctx, work.StatusSucceeded)
	if err != nil {
		t.Fatalf("ListJobs succeeded: %v", err)
	}
	if len(succeeded) != 1 {
		t.Fatalf("succeeded jobs = %d, want 1", len(succeeded))
	}
}

func TestGetJobNotFound(t *testing.T) {
	store := newStore(t)
	_, err := store.GetJob(context.Background(), "missing")
	if err != work.ErrNotFound {
		t.Fatalf("GetJob err = %v, want %v", err, work.ErrNotFound)
	}
}

// TestMigrateValidationRequestID_ExistingRowsSurvive guards the specific
// migration failure mode called out in review: a plain "NOT NULL DEFAULT ”
// + UNIQUE" column would make every pre-existing row collide on the same
// empty string the moment the index is built, since they all predate
// validation_request_id and would migrate to the same value. This seeds a
// jobs table using the pre-migration schema (no validation_request_id/
// request_fingerprint columns) with more than one row, then opens it
// through sqlite.New and checks both that opening succeeds and that the
// pre-existing rows are still readable afterward.
func TestMigrateValidationRequestID_ExistingRowsSurvive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-migration.sqlite")
	seedPreMigrationJobsTable(t, path, "job-old-1", "job-old-2", "job-old-3")

	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New on a pre-migration DB with multiple existing jobs: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for _, id := range []string{"job-old-1", "job-old-2", "job-old-3"} {
		got, getErr := store.GetJob(context.Background(), id)
		if getErr != nil {
			t.Fatalf("GetJob(%q) after migration: %v", id, getErr)
		}
		if got.ValidationRequestID != "" {
			t.Errorf("GetJob(%q).ValidationRequestID = %q, want empty (pre-migration row)", id, got.ValidationRequestID)
		}
	}

	// The migrated DB must still accept new idempotent-enqueue requests.
	req := sampleRequest("job-new-1")
	req.ValidationRequestID = "vr-post-migration"
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob after migration: %v", err)
	}
}

// seedPreMigrationJobsTable creates a jobs table matching the schema before
// validation_request_id/request_fingerprint existed, and inserts one row per
// given job ID directly via SQL — bypassing sqlite.New/CreateJob entirely,
// since those already write the post-migration schema.
func seedPreMigrationJobsTable(t *testing.T, path string, jobIDs ...string) {
	t.Helper()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = `
CREATE TABLE jobs (
  job_id TEXT PRIMARY KEY,
  artifact_kind TEXT NOT NULL,
  image_repository TEXT NOT NULL,
  image_digest TEXT NOT NULL,
  stable_ref TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  version TEXT NOT NULL,
  cas_hash TEXT NOT NULL,
  requested_actions TEXT NOT NULL,
  requested_fixture_set TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_until TEXT,
  last_error TEXT NOT NULL DEFAULT '',
  result_summary TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create pre-migration schema: %v", err)
	}

	const insert = `
INSERT INTO jobs (
  job_id, artifact_kind, image_repository, image_digest, stable_ref, tool_name,
  version, cas_hash, requested_actions, requested_fixture_set, status, created_at, updated_at
) VALUES (?, 'tool', 'harbor.example.local/library/fastp', 'sha256:1234', 'fastp@0.24.0',
  'fastp', '0.24.0', 'sha256:abcd', '["smoke_run"]', 'default', 'queued', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
`
	for _, id := range jobIDs {
		if _, err := db.Exec(insert, id); err != nil {
			t.Fatalf("seed pre-migration row %q: %v", id, err)
		}
	}
}

// TestCreateJob_ConcurrentDuplicateValidationRequestID_OneJobWins is the
// race-safety guard behind CreateJob's design note: two callers racing on
// the same validation_request_id must not both succeed with distinct jobs —
// the partial UNIQUE index (not the pre-INSERT lookup, which only optimizes
// the common non-racing case) is the actual arbiter.
func TestCreateJob_ConcurrentDuplicateValidationRequestID_OneJobWins(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	const workers = 8
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		jobIDs = make(map[string]int)
		errs   []error
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := sampleRequest(fmt.Sprintf("job-race-%d", i)) // distinct job_id per caller
			req.ValidationRequestID = "vr-race"                 // same idempotency key
			job, err := store.CreateJob(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			jobIDs[job.JobID]++
		}(i)
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("CreateJob errors under concurrent dedup: %v", errs)
	}
	if len(jobIDs) != 1 {
		t.Fatalf("distinct job_ids returned = %v, want exactly 1 shared job_id", jobIDs)
	}

	all, err := store.ListJobs(ctx, "")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("jobs actually persisted = %d, want 1 (all %d callers must share one job)", len(all), workers)
	}
}

// TestCreateJob_SameValidationRequestIDActionsContainingComma_Rejected is a
// regression guard for a fingerprint bug an independent review caught:
// requestFingerprint used to join RequestedActions with a bare comma before
// hashing, so an action list containing a literal comma (["a,b"]) fingerprinted
// identically to a two-element list (["a", "b"]). That silently defeated the
// "same validation_request_id + different payload -> rejected" guarantee for
// any caller whose action names happen to contain commas.
func TestCreateJob_SameValidationRequestIDActionsContainingComma_Rejected(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	first := sampleRequest("job-a")
	first.ValidationRequestID = "vr-comma"
	first.RequestedActions = []work.Action{"a,b"}
	if _, err := store.CreateJob(ctx, first); err != nil {
		t.Fatalf("first CreateJob: %v", err)
	}

	second := sampleRequest("job-b")
	second.ValidationRequestID = "vr-comma"
	second.RequestedActions = []work.Action{"a", "b"}
	_, err := store.CreateJob(ctx, second)
	if !errors.Is(err, work.ErrValidationRequestConflict) {
		t.Fatalf(`err = %v, want ErrValidationRequestConflict (["a,b"] and ["a","b"] must not fingerprint-collide)`, err)
	}
}

// TestMigrateValidationRequestID_ConcurrentStoreOpens_BothSucceed is a
// regression guard for a migration race an independent review caught and
// reproduced: opening the same pre-existing (pre-migration) database from
// multiple connections concurrently — modeling two processes starting at
// once, e.g. a rolling restart where the old and new Pod briefly overlap —
// used to race a plain "check column, then ALTER TABLE ADD COLUMN" across
// connections, and one of them would fail with "duplicate column name"
// because its check ran before the other's ALTER committed. New's
// _txlock=immediate DSN option plus running the check-and-ALTER inside one
// transaction (see migrateValidationRequestID) closes that race: the second
// opener's BeginTx blocks until the first's migration transaction commits.
func TestMigrateValidationRequestID_ConcurrentStoreOpens_BothSucceed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-migration-concurrent.sqlite")
	seedPreMigrationJobsTable(t, path, "job-old-1", "job-old-2")

	const openers = 4
	var wg sync.WaitGroup
	errs := make([]error, openers)
	stores := make([]*sqlite.Store, openers)
	for i := 0; i < openers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := sqlite.New(path)
			errs[i] = err
			stores[i] = s
		}(i)
	}
	wg.Wait()

	for _, s := range stores {
		if s != nil {
			t.Cleanup(func() { _ = s.Close() })
		}
	}

	for i, err := range errs {
		if err != nil {
			t.Errorf("opener %d: sqlite.New failed: %v", i, err)
		}
	}
}

// TestMigrateResultDelivery_ExistingRowsSurvive mirrors
// TestMigrateValidationRequestID_ExistingRowsSurvive for the result_delivery_*
// columns: opening a pre-existing jobs table (seeded before this migration
// existed) must succeed and default every existing row to "not_applicable",
// not error or silently leave rows unreadable.
func TestMigrateResultDelivery_ExistingRowsSurvive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-migration-delivery.sqlite")
	seedPreMigrationJobsTable(t, path, "job-old-1", "job-old-2")

	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New on a pre-migration DB: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for _, id := range []string{"job-old-1", "job-old-2"} {
		got, getErr := store.GetJob(context.Background(), id)
		if getErr != nil {
			t.Fatalf("GetJob(%q) after migration: %v", id, getErr)
		}
		if got.ResultDeliveryStatus != work.DeliveryNotApplicable {
			t.Errorf("GetJob(%q).ResultDeliveryStatus = %q, want %q",
				id, got.ResultDeliveryStatus, work.DeliveryNotApplicable)
		}
	}
}

// TestResultDelivery_PendingClaimedThenAcknowledged_Lifecycle exercises the
// store methods directly (independent of pkg/worker): MarkResultDeliveryPending
// makes a job claimable; ClaimPendingDeliveries atomically claims it
// (Delivering) and, once claimed, it's no longer returned by a second
// claim call; MarkResultDeliveryAcknowledged then clears the payload.
func TestResultDelivery_PendingClaimedThenAcknowledged_Lifecycle(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	job, err := store.CreateJob(ctx, sampleRequest("job-delivery"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if job.ResultDeliveryStatus != work.DeliveryNotApplicable {
		t.Fatalf("initial ResultDeliveryStatus = %q, want not_applicable", job.ResultDeliveryStatus)
	}

	past := time.Now().UTC().Add(-time.Second) // already due
	if err := store.MarkResultDeliveryPending(ctx, job.JobID, `{"kind":"check"}`, "boom", past); err != nil {
		t.Fatalf("MarkResultDeliveryPending: %v", err)
	}

	claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPendingDeliveries: %v", err)
	}
	if len(claimed) != 1 || claimed[0].JobID != job.JobID {
		t.Fatalf("ClaimPendingDeliveries = %v, want exactly [%s]", claimed, job.JobID)
	}
	if claimed[0].ResultDeliveryPayload != `{"kind":"check"}` {
		t.Errorf("ResultDeliveryPayload = %q, want the stored payload", claimed[0].ResultDeliveryPayload)
	}
	if claimed[0].ResultDeliveryAttempts != 1 {
		t.Errorf("ResultDeliveryAttempts = %d, want 1", claimed[0].ResultDeliveryAttempts)
	}
	if claimed[0].ResultDeliveryStatus != work.DeliveryDelivering {
		t.Errorf("ResultDeliveryStatus = %q, want delivering", claimed[0].ResultDeliveryStatus)
	}

	// A second claim call must not re-claim the same job — it's already
	// Delivering with an unexpired claim.
	second, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPendingDeliveries (second): %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second ClaimPendingDeliveries = %v, want empty (already claimed)", second)
	}

	if err := store.MarkResultDeliveryAcknowledged(ctx, job.JobID); err != nil {
		t.Fatalf("MarkResultDeliveryAcknowledged: %v", err)
	}

	afterAck, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPendingDeliveries (after ack): %v", err)
	}
	if len(afterAck) != 0 {
		t.Fatalf("ClaimPendingDeliveries after acknowledge = %v, want empty", afterAck)
	}
	got, err := store.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ResultDeliveryStatus != work.DeliveryAcknowledged {
		t.Errorf("ResultDeliveryStatus = %q, want acknowledged", got.ResultDeliveryStatus)
	}
	if got.ResultDeliveryPayload != "" {
		t.Error("ResultDeliveryPayload should be cleared once acknowledged")
	}
}

// TestClaimPendingDeliveries_NotYetDue_NotClaimed guards the backoff gate:
// a pending delivery whose next_attempt_at is still in the future must not
// be claimed — retrying before the computed backoff would defeat the point
// of backing off at all.
func TestClaimPendingDeliveries_NotYetDue_NotClaimed(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	job, err := store.CreateJob(ctx, sampleRequest("job-not-due"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	future := time.Now().UTC().Add(time.Hour)
	if err := store.MarkResultDeliveryPending(ctx, job.JobID, `{"kind":"check"}`, "boom", future); err != nil {
		t.Fatalf("MarkResultDeliveryPending: %v", err)
	}

	claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPendingDeliveries: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("ClaimPendingDeliveries = %v, want empty (not yet due)", claimed)
	}
}

// TestClaimPendingDeliveries_ExpiredClaim_Reclaimed guards the reclaim path:
// a job stuck at Delivering because a prior claimer crashed before
// resolving it (never called Acknowledged/Pending/DeadLetter) must become
// claimable again once its claim TTL has passed — otherwise it would be
// stuck forever, invisible to both this query and the operator.
func TestClaimPendingDeliveries_ExpiredClaim_Reclaimed(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	job, err := store.CreateJob(ctx, sampleRequest("job-reclaim"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	past := time.Now().UTC().Add(-time.Second)
	if err := store.MarkResultDeliveryPending(ctx, job.JobID, `{"kind":"check"}`, "boom", past); err != nil {
		t.Fatalf("MarkResultDeliveryPending: %v", err)
	}

	// Claim with a TTL so short it's already expired by the time we check again.
	if _, err := store.ClaimPendingDeliveries(ctx, 10, time.Nanosecond); err != nil {
		t.Fatalf("first ClaimPendingDeliveries: %v", err)
	}

	reclaimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("second ClaimPendingDeliveries: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].JobID != job.JobID {
		t.Fatalf("reclaimed = %v, want exactly [%s]", reclaimed, job.JobID)
	}
}

// TestMarkResultDeliveryDeadLetter_PreservesPayloadAndStopsClaiming verifies
// that a dead-lettered delivery keeps its payload/error for operator
// inspection (unlike Acknowledged, which clears them) and is never claimed
// again.
func TestMarkResultDeliveryDeadLetter_PreservesPayloadAndStopsClaiming(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	job, err := store.CreateJob(ctx, sampleRequest("job-dead-letter"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	past := time.Now().UTC().Add(-time.Second)
	if err := store.MarkResultDeliveryPending(ctx, job.JobID, `{"kind":"check"}`, "boom", past); err != nil {
		t.Fatalf("MarkResultDeliveryPending: %v", err)
	}
	if _, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute); err != nil {
		t.Fatalf("ClaimPendingDeliveries: %v", err)
	}

	if err := store.MarkResultDeliveryDeadLetter(ctx, job.JobID, "payload undecodable"); err != nil {
		t.Fatalf("MarkResultDeliveryDeadLetter: %v", err)
	}

	got, err := store.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ResultDeliveryStatus != work.DeliveryDeadLetter {
		t.Errorf("ResultDeliveryStatus = %q, want dead_letter", got.ResultDeliveryStatus)
	}
	if got.ResultDeliveryPayload != `{"kind":"check"}` {
		t.Errorf("ResultDeliveryPayload = %q, want preserved for operator inspection", got.ResultDeliveryPayload)
	}
	if got.ResultDeliveryLastError != "payload undecodable" {
		t.Errorf("ResultDeliveryLastError = %q, want the dead-letter reason", got.ResultDeliveryLastError)
	}

	claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPendingDeliveries after dead-letter: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("ClaimPendingDeliveries after dead-letter = %v, want empty", claimed)
	}
}

// TestClaimPendingDeliveries_RespectsLimitAndOrdering verifies the batch
// cap and oldest-first ordering — so a long-stuck delivery isn't starved
// behind a stream of newly-pending ones.
func TestClaimPendingDeliveries_RespectsLimitAndOrdering(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	var jobIDs []string
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("job-order-%d", i)
		if _, err := store.CreateJob(ctx, sampleRequest(id)); err != nil {
			t.Fatalf("CreateJob %s: %v", id, err)
		}
		past := time.Now().UTC().Add(-time.Second)
		if err := store.MarkResultDeliveryPending(ctx, id, `{"kind":"check"}`, "boom", past); err != nil {
			t.Fatalf("MarkResultDeliveryPending %s: %v", id, err)
		}
		jobIDs = append(jobIDs, id)
	}

	claimed, err := store.ClaimPendingDeliveries(ctx, 2, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPendingDeliveries: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2 (limit)", len(claimed))
	}
	if claimed[0].JobID != jobIDs[0] || claimed[1].JobID != jobIDs[1] {
		t.Errorf("claimed order = [%s, %s], want oldest-first [%s, %s]",
			claimed[0].JobID, claimed[1].JobID, jobIDs[0], jobIDs[1])
	}
}

func TestClaimTerminal_FirstCallClaimsSecondCallDoesNot(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if _, err := store.CreateJob(ctx, sampleRequest("job-claim-terminal")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	claimed, err := store.ClaimTerminal(ctx, "job-claim-terminal")
	if err != nil {
		t.Fatalf("first ClaimTerminal: %v", err)
	}
	if !claimed {
		t.Error("first ClaimTerminal should return claimed=true")
	}

	got, err := store.GetJob(ctx, "job-claim-terminal")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !got.TerminalSubmitted {
		t.Error("TerminalSubmitted should be true after claiming")
	}

	claimed, err = store.ClaimTerminal(ctx, "job-claim-terminal")
	if err != nil {
		t.Fatalf("second ClaimTerminal should not error, got: %v", err)
	}
	if claimed {
		t.Error("second ClaimTerminal should return claimed=false — the slot is already taken")
	}
}

func TestClaimTerminal_UnknownJob_ReturnsErrNotFound(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	_, err := store.ClaimTerminal(ctx, "no-such-job")
	if !errors.Is(err, work.ErrNotFound) {
		t.Errorf("ClaimTerminal on unknown job: err = %v, want work.ErrNotFound", err)
	}
}

func newStore(t *testing.T) *sqlite.Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")
	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func sampleRequest(id string) work.JobRequest {
	return work.JobRequest{
		JobID:               id,
		ArtifactKind:        "tool",
		ImageRepository:     "harbor.example.local/library/fastp",
		ImageDigest:         "sha256:1234",
		StableRef:           "fastp@0.24.0",
		ToolName:            "fastp",
		Version:             "0.24.0",
		CasHash:             "sha256:abcd",
		RequestedActions:    []work.Action{work.ActionSmokeRun, work.ActionSecurityScan},
		RequestedFixtureSet: "default-smoke",
	}
}
