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
