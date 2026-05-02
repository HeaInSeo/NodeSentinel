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
