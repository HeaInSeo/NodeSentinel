package work

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound       = errors.New("workstore: job not found")
	ErrNoAvailableJob = errors.New("workstore: no available job")

	// ErrValidationRequestConflict is returned by CreateJob when
	// ValidationRequestID matches an existing job whose request differs
	// (a different image digest, requested actions, etc.). Reusing a
	// validation_request_id is only valid for retrying the exact same
	// logical request — see EnqueueValidationWorkRequest.validation_request_id.
	ErrValidationRequestConflict = errors.New("workstore: validation_request_id already used with a different request")
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusLeased    Status = "leased"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Action string

const (
	ActionSmokeRun     Action = "smoke_run"
	ActionProfile      Action = "profile"
	ActionSecurityScan Action = "security_scan"
)

type JobRequest struct {
	JobID               string
	ArtifactKind        string
	ImageRepository     string
	ImageDigest         string
	StableRef           string
	ToolName            string
	Version             string
	CasHash             string
	RequestedActions    []Action
	RequestedFixtureSet string

	// ValidationRequestID is the caller's idempotency key (see
	// EnqueueValidationWorkRequest.validation_request_id). CreateJob is
	// idempotent on this field: repeating it with an identical request
	// returns the existing job; repeating it with a different request
	// returns ErrValidationRequestConflict.
	ValidationRequestID string
}

type Job struct {
	JobID               string
	ArtifactKind        string
	ImageRepository     string
	ImageDigest         string
	StableRef           string
	ToolName            string
	Version             string
	CasHash             string
	RequestedActions    []Action
	RequestedFixtureSet string
	ValidationRequestID string
	Status              Status
	Attempt             int
	LeaseOwner          string
	LeaseUntil          *time.Time
	LastError           string
	ResultSummary       string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Store interface {
	CreateJob(ctx context.Context, req JobRequest) (*Job, error)
	LeaseJob(ctx context.Context, worker string, ttl time.Duration) (*Job, error)
	Heartbeat(ctx context.Context, jobID, worker string, ttl time.Duration) error
	CompleteJob(ctx context.Context, jobID, worker, resultSummary string) error
	FailJob(ctx context.Context, jobID, worker, lastError string, retryable bool) error
	GetJob(ctx context.Context, jobID string) (*Job, error)
	ListJobs(ctx context.Context, status Status) ([]*Job, error)
	Close() error
}
