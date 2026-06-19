package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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
