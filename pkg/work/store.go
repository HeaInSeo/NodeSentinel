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

// DeliveryStatus tracks whether a job's terminal validation-result record
// has been durably accepted by NodeVault. Only a job's one terminal record
// is tracked this way — see MarkResultDeliveryPending's doc comment for why
// non-terminal records don't need the same guarantee.
type DeliveryStatus string

const (
	// DeliveryNotApplicable is the default for every job — no terminal
	// record delivery has been attempted (or none is expected) yet.
	DeliveryNotApplicable DeliveryStatus = "not_applicable"
	// DeliveryPending means the terminal record's first delivery attempt
	// failed; ResultDeliveryPayload holds what to redeliver.
	DeliveryPending DeliveryStatus = "pending"
	// DeliveryAcknowledged means NodeVault has durably accepted the
	// terminal record — the payload is cleared once this is set.
	DeliveryAcknowledged DeliveryStatus = "acknowledged"
)

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

	// ResultDelivery* track redelivery of this job's one terminal
	// validation-result record — see MarkResultDeliveryPending.
	ResultDeliveryStatus    DeliveryStatus
	ResultDeliveryPayload   string // opaque to this package — see pkg/worker's pendingDelivery
	ResultDeliveryAttempts  int
	ResultDeliveryLastError string
}

type Store interface {
	CreateJob(ctx context.Context, req JobRequest) (*Job, error)
	LeaseJob(ctx context.Context, worker string, ttl time.Duration) (*Job, error)
	Heartbeat(ctx context.Context, jobID, worker string, ttl time.Duration) error
	CompleteJob(ctx context.Context, jobID, worker, resultSummary string) error
	FailJob(ctx context.Context, jobID, worker, lastError string, retryable bool) error
	GetJob(ctx context.Context, jobID string) (*Job, error)
	ListJobs(ctx context.Context, status Status) ([]*Job, error)

	// MarkResultDeliveryPending durably records that job's terminal
	// validation-result record failed to deliver to NodeVault, along with
	// the (opaque, pre-serialized) payload needed to retry it and the error
	// that caused this attempt to fail. Increments ResultDeliveryAttempts.
	MarkResultDeliveryPending(ctx context.Context, jobID, payload, lastError string) error
	// MarkResultDeliveryAcknowledged records that NodeVault durably
	// accepted job's terminal record — clears the stored payload.
	MarkResultDeliveryAcknowledged(ctx context.Context, jobID string) error
	// ListPendingDeliveries returns every job whose terminal record is
	// still awaiting successful redelivery, oldest first.
	ListPendingDeliveries(ctx context.Context) ([]*Job, error)

	Close() error
}
