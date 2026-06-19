package ingress_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/HeaInSeo/NodeSentinel/pkg/ingress"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
	nsv1 "github.com/HeaInSeo/NodeSentinel/protos/nodesentinel/v1"
)

type failingStore struct{}

func (f failingStore) CreateJob(context.Context, work.JobRequest) (*work.Job, error) {
	return nil, errors.New("store unavailable")
}

func (f failingStore) LeaseJob(context.Context, string, time.Duration) (*work.Job, error) {
	return nil, errors.New("not implemented")
}

func (f failingStore) Heartbeat(context.Context, string, string, time.Duration) error {
	return errors.New("not implemented")
}

func (f failingStore) CompleteJob(context.Context, string, string, string) error {
	return errors.New("not implemented")
}

func (f failingStore) FailJob(context.Context, string, string, string, bool) error {
	return errors.New("not implemented")
}

func (f failingStore) GetJob(context.Context, string) (*work.Job, error) {
	return nil, errors.New("not implemented")
}

func (f failingStore) ListJobs(context.Context, work.Status) ([]*work.Job, error) {
	return nil, errors.New("not implemented")
}

func (f failingStore) Close() error {
	return nil
}

func newTestServer(t *testing.T) *ingress.Server {
	t.Helper()

	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")
	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	return ingress.NewServer(store)
}

func validRequest() *nsv1.EnqueueValidationWorkRequest {
	return &nsv1.EnqueueValidationWorkRequest{
		ArtifactKind:        "tool",
		ImageRepository:     "harbor.example.local/library/fastp",
		ImageDigest:         "sha256:1234",
		StableRef:           "fastp@0.24.0",
		ToolName:            "fastp",
		Version:             "0.24.0",
		CasHash:             "sha256:abcd",
		RequestedActions:    []string{"smoke_run"},
		RequestedFixtureSet: "default",
	}
}

func TestEnqueueValidationWork_Success(t *testing.T) {
	srv := newTestServer(t)

	resp, err := srv.EnqueueValidationWork(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("EnqueueValidationWork: %v", err)
	}
	if resp.GetJobId() == "" {
		t.Fatal("expected non-empty job_id")
	}
	if resp.GetStatus() == "" {
		t.Fatal("expected non-empty status")
	}
}

func TestEnqueueValidationWork_MissingRequiredField(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name   string
		mutate func(*nsv1.EnqueueValidationWorkRequest)
	}{
		{"empty artifact_kind", func(r *nsv1.EnqueueValidationWorkRequest) { r.ArtifactKind = "" }},
		{"whitespace artifact_kind", func(r *nsv1.EnqueueValidationWorkRequest) { r.ArtifactKind = "   " }},
		{"empty image_repository", func(r *nsv1.EnqueueValidationWorkRequest) { r.ImageRepository = "" }},
		{"empty tool_name", func(r *nsv1.EnqueueValidationWorkRequest) { r.ToolName = "" }},
		{"empty version", func(r *nsv1.EnqueueValidationWorkRequest) { r.Version = "" }},
		{"no requested_actions", func(r *nsv1.EnqueueValidationWorkRequest) { r.RequestedActions = nil }},
		{"blank requested_actions entry", func(r *nsv1.EnqueueValidationWorkRequest) {
			r.RequestedActions = []string{"  "}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
			tt.mutate(req)

			_, err := srv.EnqueueValidationWork(context.Background(), req)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
			}
		})
	}
}

func TestEnqueueValidationWork_NilRequest(t *testing.T) {
	srv := newTestServer(t)

	_, err := srv.EnqueueValidationWork(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestEnqueueValidationWork_StoreFailure(t *testing.T) {
	srv := ingress.NewServer(failingStore{})

	_, err := srv.EnqueueValidationWork(context.Background(), validRequest())
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
}
