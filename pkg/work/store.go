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

	// ErrExecutionSuperseded is returned by MarkExecutionTerminal when the
	// job's current execution identity is not the one the caller observed:
	// a newer execution has replaced it, and it must not be released by a
	// report about the older one.
	ErrExecutionSuperseded = errors.New("workstore: execution identity superseded")
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

	// ExecutionID names this job's current *physical* execution — the
	// identity the K8s Job object carries (see pkg/worker's smokeJobName).
	// It is deliberately NOT derived from Attempt: see EnsureExecution for
	// the concurrent-duplicate-execution bug that derivation caused.
	// Empty until the job's first execution is minted.
	ExecutionID string

	// ExecutionTerminal reports whether ExecutionID's physical execution has
	// been observed to reach a terminal state (Complete/Failed), or to no
	// longer exist at all. Only then may a new attempt mint a new
	// ExecutionID — see EnsureExecution.
	ExecutionTerminal bool

	// RetryNotBefore is the earliest time LeaseJob may lease this job again
	// after a retryable FailJob requeued it (see RetryDelay). Nil when the job
	// is not waiting out a retry delay: never failed retryably, requeued before
	// pacing existed, or leased since.
	RetryNotBefore *time.Time
}

type Store interface {
	CreateJob(ctx context.Context, req JobRequest) (*Job, error)
	LeaseJob(ctx context.Context, worker string, ttl time.Duration) (*Job, error)
	Heartbeat(ctx context.Context, jobID, worker string, ttl time.Duration) error
	CompleteJob(ctx context.Context, jobID, worker, resultSummary string) error
	// FailJob records the failure of attempt, the Job.Attempt value LeaseJob
	// returned to worker. When retryable, the job returns to Queued and, in the
	// same write, becomes ineligible for LeaseJob until retryDelay has elapsed
	// (retryDelay <= 0 leaves it eligible immediately); otherwise it is marked
	// Failed and retryDelay is ignored. It applies only while worker still
	// holds that same lease generation (owner and attempt): a duplicate call,
	// or a stale one for an earlier attempt — even from the same worker name
	// after it re-leased the job — returns ErrNotFound and changes nothing.
	// Execution identity and result-delivery state are untouched.
	FailJob(ctx context.Context, jobID, worker string, attempt int, lastError string, retryable bool, retryDelay time.Duration) error
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

	// EnsureExecution returns the execution identity (see Job.ExecutionID)
	// the caller must use for jobID's physical K8s Job on this attempt, and
	// whether that identity was *adopted* from a previous attempt rather
	// than freshly minted.
	//
	// This exists to close a concurrent-duplicate-execution hole. The K8s
	// Job name used to be derived from Job.Attempt, and LeaseJob increments
	// attempt on *every* lease — including when it reclaims a lease that
	// merely expired (see LeaseJob's 'running' reclaim clause). So a worker
	// that stopped heartbeating without its K8s Job stopping — a crashed,
	// evicted, or network-partitioned worker, or simply a Pod replacement
	// mid-run — left its smoke-run Job still executing in the cluster while
	// the next lease computed a *different* name and created a second Job.
	// Two physical executions of one logical validation then ran at once,
	// which is exactly what NodeSentinel #2's acceptance forbids.
	//
	// Contract:
	//   - If jobID already has an ExecutionID whose execution is not yet
	//     terminal, that same ID is returned with adopted=true. The caller
	//     must then *observe* the existing K8s Job rather than create one.
	//   - Otherwise a new ID is minted, persisted, marked non-terminal, and
	//     returned with adopted=false — the caller creates the Job.
	//
	// A new execution is therefore only ever minted after the previous one
	// is terminal (MarkExecutionTerminal) and the retry policy separately
	// allowed the requeue that produced this lease. Returns ErrNotFound if
	// jobID does not exist.
	EnsureExecution(ctx context.Context, jobID, worker string) (executionID string, adopted bool, err error)

	// MarkExecutionTerminal records that executionID, the execution the
	// caller observed for jobID, has reached a terminal state — it
	// completed, failed, or was observed to no longer exist. It is the only
	// thing that re-enables minting a new execution identity on a later
	// attempt (see EnsureExecution).
	//
	// Callers must only invoke this once the physical execution is known not
	// to be running: a Complete/Failed Job condition, or a NotFound on the
	// Job object itself. Calling it while the Job may still be running would
	// reopen the duplicate-execution hole EnsureExecution closes.
	//
	// The update applies only while executionID is still jobID's current
	// identity. A worker that resumes after its lease expired may report on
	// an execution another worker has already replaced; releasing by jobID
	// alone would mark the replacement terminal while its Job runs, and the
	// next attempt would mint a third beside it. Such a stale report returns
	// ErrExecutionSuperseded and changes nothing. Returns ErrNotFound if
	// jobID does not exist.
	MarkExecutionTerminal(ctx context.Context, jobID, executionID string) error

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
