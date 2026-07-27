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
//
// State graph:
//
//	NotApplicable -> Pending                (a terminal submission first fails)
//	Pending       -> Delivering             (ClaimPendingDeliveries claims it)
//	Delivering    -> Acknowledged           (redelivery succeeds)
//	Delivering    -> Pending                (redelivery fails with a retryable error)
//	Delivering    -> DeadLetter             (redelivery fails permanently, or the
//	                                          stored payload itself is malformed)
//	Delivering    -> Pending                (claim expires unclaimed — see
//	                                          ClaimPendingDeliveries' reclaim behavior)
type DeliveryStatus string

const (
	// DeliveryNotApplicable is the default for every job — no terminal
	// record delivery has been attempted (or none is expected) yet.
	DeliveryNotApplicable DeliveryStatus = "not_applicable"
	// DeliveryPending means the terminal record needs (re)delivery;
	// ResultDeliveryPayload holds what to send. NextAttemptAt gates when a
	// ClaimPendingDeliveries call may claim it (backoff).
	DeliveryPending DeliveryStatus = "pending"
	// DeliveryDelivering means some RunDeliveryLoop iteration has claimed
	// this job's delivery and is currently attempting it (or crashed before
	// resolving it — see ClaimPendingDeliveries' claim-expiry reclaim).
	DeliveryDelivering DeliveryStatus = "delivering"
	// DeliveryAcknowledged means NodeVault has durably accepted the
	// terminal record — the payload is cleared once this is set.
	DeliveryAcknowledged DeliveryStatus = "acknowledged"
	// DeliveryDeadLetter means redelivery will not be attempted again:
	// either NodeVault permanently rejected the payload (a 4xx response —
	// see vaultclient.Retryable), or the stored payload itself could not be
	// decoded (a corrupted/unrecognized pendingDelivery — see
	// pkg/worker/delivery.go's redeliverOne). The payload and last error are
	// preserved for operator inspection/manual resubmission, not cleared.
	DeliveryDeadLetter DeliveryStatus = "dead_letter"
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
	// NextAttemptAt gates redelivery eligibility (exponential backoff+jitter
	// — see pkg/worker/delivery.go). Nil until the first pending mark.
	NextAttemptAt *time.Time

	// TerminalSubmitted records whether this job has already claimed
	// responsibility for submitting its one terminal validation-result
	// record (see Store.ClaimTerminal). It is separate from
	// ResultDeliveryStatus, which only tracks *redelivery* of a record whose
	// first submission attempt failed — TerminalSubmitted guards against
	// *initiating* a second terminal submission in the first place (e.g. a
	// requeued/retried job re-reaching the point where it would otherwise
	// submit another terminal record).
	TerminalSubmitted bool
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
	// validation-result record needs (re)delivery, along with the (opaque,
	// pre-serialized) payload to send, the error that caused the most
	// recent attempt to fail (empty for the first mark, before any attempt
	// has actually run), and nextAttemptAt — the earliest time
	// ClaimPendingDeliveries may claim it again (backoff, computed by the
	// caller). Increments ResultDeliveryAttempts. Valid from
	// NotApplicable or Delivering (see DeliveryStatus's state graph).
	MarkResultDeliveryPending(ctx context.Context, jobID, payload, lastError string, nextAttemptAt time.Time) error
	// MarkResultDeliveryAcknowledged records that NodeVault durably
	// accepted job's terminal record — clears the stored payload. Valid
	// from Delivering.
	MarkResultDeliveryAcknowledged(ctx context.Context, jobID string) error
	// MarkResultDeliveryDeadLetter records that job's terminal record will
	// not be redelivered again — see DeliveryDeadLetter. The payload and
	// lastError are preserved, not cleared. Valid from Delivering.
	MarkResultDeliveryDeadLetter(ctx context.Context, jobID, lastError string) error
	// ClaimPendingDeliveries atomically selects up to limit jobs eligible
	// for redelivery (status Pending with NextAttemptAt due, or status
	// Delivering whose claim has expired — see DeliveryDelivering) and
	// transitions them to Delivering with a claim expiring after claimTTL,
	// all within one transaction. Two concurrent callers (overlapping
	// RunDeliveryLoop iterations, or a future multi-replica NodeSentinel)
	// therefore never claim the same job — see pkg/work/sqlite's
	// _txlock=immediate DSN option. Returns oldest-updated first.
	ClaimPendingDeliveries(ctx context.Context, limit int, claimTTL time.Duration) ([]*Job, error)

	// ClaimTerminal atomically claims jobID's one-time terminal-submission
	// slot: the first caller for a given job gets claimed=true and is the
	// one responsible for actually submitting that job's one terminal
	// validation-result record; every subsequent caller (a duplicate
	// invocation, a requeued job re-reaching the same terminal decision
	// point) gets claimed=false and must not submit again. Returns
	// ErrNotFound if jobID does not exist.
	ClaimTerminal(ctx context.Context, jobID string) (claimed bool, err error)

	Close() error
}
