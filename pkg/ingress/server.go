// Package ingress implements the gRPC IngressService that NodeKit/NodeVault
// call to enqueue validation work into the NodeSentinel work queue.
package ingress

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	nsv1 "github.com/HeaInSeo/NodeSentinel/protos/nodesentinel/v1"
)

// Server implements nsv1.IngressServiceServer on top of a work.Store.
type Server struct {
	nsv1.UnimplementedIngressServiceServer

	Store work.Store
}

// NewServer constructs an ingress Server backed by the given work.Store.
func NewServer(store work.Store) *Server {
	return &Server{Store: store}
}

// EnqueueValidationWork validates the request, converts it into a
// work.JobRequest, and enqueues it via the underlying work.Store.
func (s *Server) EnqueueValidationWork(ctx context.Context, req *nsv1.EnqueueValidationWorkRequest) (*nsv1.EnqueueValidationWorkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	if err := validate(req); err != nil {
		return nil, err
	}

	actions := make([]work.Action, 0, len(req.GetRequestedActions()))
	for _, a := range req.GetRequestedActions() {
		actions = append(actions, work.Action(a))
	}

	jobReq := work.JobRequest{
		JobID:               newJobID(),
		ArtifactKind:        req.GetArtifactKind(),
		ImageRepository:     req.GetImageRepository(),
		ImageDigest:         req.GetImageDigest(),
		StableRef:           req.GetStableRef(),
		ToolName:            req.GetToolName(),
		Version:             req.GetVersion(),
		CasHash:             req.GetCasHash(),
		RequestedActions:    actions,
		RequestedFixtureSet: req.GetRequestedFixtureSet(),
	}

	job, err := s.Store.CreateJob(ctx, jobReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create job: %v", err)
	}

	return &nsv1.EnqueueValidationWorkResponse{
		JobId:  job.JobID,
		Status: string(job.Status),
	}, nil
}

// newJobID generates a random job identifier. work.Store does not assign
// one itself (job_id is the primary key supplied by the caller).
func newJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failures are effectively unrecoverable on supported
		// platforms; fall back to a fixed prefix rather than panicking.
		return fmt.Sprintf("job-fallback-%x", b)
	}
	return "job-" + hex.EncodeToString(b[:])
}

// validate checks that the required fields of req are present and not
// blank (empty or whitespace-only).
func validate(req *nsv1.EnqueueValidationWorkRequest) error {
	required := map[string]string{
		"artifact_kind":    req.GetArtifactKind(),
		"image_repository": req.GetImageRepository(),
		"tool_name":        req.GetToolName(),
		"version":          req.GetVersion(),
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return status.Errorf(codes.InvalidArgument, "%s must not be empty", field)
		}
	}

	if len(req.GetRequestedActions()) == 0 {
		return status.Error(codes.InvalidArgument, "requested_actions must not be empty")
	}
	for _, a := range req.GetRequestedActions() {
		if strings.TrimSpace(a) == "" {
			return status.Error(codes.InvalidArgument, "requested_actions must not contain blank entries")
		}
	}

	return nil
}
