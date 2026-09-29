package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

const (
	deliveryBaseBackoff  = 10 * time.Second
	deliveryMaxBackoff   = 5 * time.Minute
	deliveryBatchLimit   = 10
	deliveryClaimTTL     = 2 * time.Minute
	deliveryConcurrency  = 4
	deliveryPollInterval = 5 * time.Second
)

// deliveryClock is the clock next-attempt times are computed from. Tests move
// it back so a failed delivery is due at once instead of after the backoff.
var deliveryClock = time.Now

// pendingDelivery is the durable, redeliverable form of the one terminal
// validation-result record a job produces — see work.Job.ResultDeliveryStatus.
// Exactly one of Check/Scan is set, matching Kind. Stored as opaque JSON in
// the jobs table (work.Job.ResultDeliveryPayload); only pkg/worker
// interprets it.
type pendingDelivery struct {
	Kind  string                                `json:"kind"` // "check" | "scan"
	Check *vaultclient.SubmitCheckRecordRequest `json:"check,omitempty"`
	Scan  *vaultclient.SubmitScanRecordRequest  `json:"scan,omitempty"`
}

// backoffDuration computes an exponential-with-cap, jittered delay before
// the next redelivery attempt. attempts is the 1-based count of delivery
// attempts made so far (including the one that just failed) — attempts=1
// (the first failure) waits ~deliveryBaseBackoff, doubling each further
// attempt up to deliveryMaxBackoff. Jitter (0-25% of the base delay) avoids
// every job that failed in the same NodeVault outage retrying in lockstep.
func backoffDuration(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	shift := attempts - 1
	if shift > 8 { // cap the shift — 2^8 * 10s already exceeds deliveryMaxBackoff
		shift = 8
	}
	d := deliveryBaseBackoff * time.Duration(int64(1)<<uint(shift))
	if d > deliveryMaxBackoff || d <= 0 {
		d = deliveryMaxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(d)/4 + 1)) //nolint:gosec // jitter timing, not security-sensitive
	total := d + jitter
	// d alone is already clamped above, but d+jitter can still exceed
	// deliveryMaxBackoff by up to 25% — clamp the total too, so the
	// documented "capped at deliveryMaxBackoff" invariant actually holds
	// for what callers receive, not just for the pre-jitter base.
	if total > deliveryMaxBackoff {
		total = deliveryMaxBackoff
	}
	return total
}

// markCheckDeliveryPending durably records that req (a terminal CheckRecord)
// failed to deliver, so RunDeliveryLoop can redeliver it later without
// re-running the K8s Job that produced it. A permanently-rejected payload
// (see vaultclient.Retryable) goes straight to dead-letter instead of
// entering the retry cycle — resubmitting the same content unchanged can
// never turn a 4xx into success.
func (w *Worker) markCheckDeliveryPending(
	ctx context.Context, logger *slog.Logger, job *work.Job, req vaultclient.SubmitCheckRecordRequest, deliverErr error,
) {
	payload, err := json.Marshal(pendingDelivery{Kind: "check", Check: &req})
	if err != nil {
		logger.Error("failed to marshal pending check delivery — cannot retry later", "err", err)
		return
	}
	w.markFirstDeliveryFailure(ctx, logger, job, string(payload), deliverErr)
}

// markScanDeliveryPending is markCheckDeliveryPending's ScanRecord
// counterpart. L5-b's submissions are always terminal in the current fixed
// pipeline (see checkRecordSubmission's doc comment), so callers don't need
// a terminal guard the way submitCheckRecord does.
func (w *Worker) markScanDeliveryPending(
	ctx context.Context, logger *slog.Logger, job *work.Job, req vaultclient.SubmitScanRecordRequest, deliverErr error,
) {
	payload, err := json.Marshal(pendingDelivery{Kind: "scan", Scan: &req})
	if err != nil {
		logger.Error("failed to marshal pending scan delivery — cannot retry later", "err", err)
		return
	}
	w.markFirstDeliveryFailure(ctx, logger, job, string(payload), deliverErr)
}

// markFirstDeliveryFailure records a failed first delivery of job's terminal
// record: a permanently-rejected payload (vaultclient.Retryable == false)
// goes straight to dead-letter, anything else (network errors, 5xx) to
// pending with a backoff-computed next attempt time. The store accepts it
// only from the attempt holding job's terminal slot (see ClaimTerminal).
func (w *Worker) markFirstDeliveryFailure(
	ctx context.Context, logger *slog.Logger, job *work.Job, payload string, deliverErr error,
) {
	var err error
	if vaultclient.Retryable(deliverErr) {
		nextAttemptAt := deliveryClock().UTC().Add(backoffDuration(1))
		err = w.store.MarkFirstDeliveryPending(
			ctx, job.JobID, w.workerName, job.Attempt, payload, deliverErr.Error(), nextAttemptAt)
	} else {
		logger.Error("delivery permanently rejected — moving to dead letter", "err", deliverErr)
		err = w.store.MarkFirstDeliveryDeadLetter(ctx, job.JobID, w.workerName, job.Attempt, payload, deliverErr.Error())
	}
	w.logDeliveryWriteErr(logger, "first delivery failure", err)
}

// resolveRedelivery records a claimed redelivery's failure under its claim
// token: dead-letter when deadLetter is set or deliverErr is permanent,
// pending with backoff otherwise.
func (w *Worker) resolveRedelivery(
	ctx context.Context, logger *slog.Logger, job *work.Job, deliverErr error, deadLetter bool,
) {
	var err error
	if deadLetter || !vaultclient.Retryable(deliverErr) {
		logger.Error("redelivery cannot succeed — moving to dead letter", "err", deliverErr)
		err = w.store.MarkResultDeliveryDeadLetter(ctx, job.JobID, job.ResultDeliveryClaimToken, deliverErr.Error())
	} else {
		nextAttemptAt := deliveryClock().UTC().Add(backoffDuration(job.ResultDeliveryAttempts + 1))
		err = w.store.MarkResultDeliveryPending(
			ctx, job.JobID, job.ResultDeliveryClaimToken, deliverErr.Error(), nextAttemptAt)
	}
	w.logDeliveryWriteErr(logger, "redelivery failure", err)
}

// logDeliveryWriteErr logs the outcome of a delivery-ledger write. A fenced
// write is not a storage failure: this caller no longer owns the delivery
// (its claim expired and was taken over or resolved, or it does not hold the
// terminal slot), so it leaves the ledger to the current owner.
func (w *Worker) logDeliveryWriteErr(logger *slog.Logger, what string, err error) {
	switch {
	case err == nil:
	case errors.Is(err, work.ErrDeliveryFenced):
		logger.Warn("delivery ledger write fenced — another owner holds this delivery; leaving it unchanged",
			"write", what)
	default:
		logger.Error("failed to record delivery outcome", "write", what, "err", err)
	}
}

// RunDeliveryLoop redelivers terminal validation-result records whose first
// delivery attempt failed. It runs independently of Run's job-leasing loop
// (see cmd/nodesentinel/main.go) specifically so a NodeVault outage —
// however many jobs end up pending redelivery, each bounded by a 10s HTTP
// timeout — cannot stall new job leasing: the two loops share the SQLite
// store but never block on each other. Blocks until ctx is canceled.
func (w *Worker) RunDeliveryLoop(ctx context.Context) error {
	ticker := time.NewTicker(deliveryPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			w.retryPendingDeliveries(ctx)
		}
	}
}

// retryPendingDeliveries claims up to deliveryBatchLimit eligible jobs
// (see work.Store.ClaimPendingDeliveries) and redelivers them with bounded
// concurrency (deliveryConcurrency) — a batch is never processed fully
// sequentially, but also never floods NodeVault with unbounded parallel
// requests.
func (w *Worker) retryPendingDeliveries(ctx context.Context) {
	if w.vaultClient == nil {
		return
	}
	claimed, err := w.store.ClaimPendingDeliveries(ctx, deliveryBatchLimit, deliveryClaimTTL)
	if err != nil {
		slog.Error("claim pending deliveries failed", "err", err)
		return
	}
	if len(claimed) == 0 {
		return
	}

	sem := make(chan struct{}, deliveryConcurrency)
	var wg sync.WaitGroup
	for _, job := range claimed {
		wg.Add(1)
		sem <- struct{}{}
		go func(job *work.Job) {
			defer wg.Done()
			defer func() { <-sem }()
			w.redeliverOne(ctx, job)
		}(job)
	}
	wg.Wait()
}

// redeliverOne re-POSTs job's claimed pending payload to NodeVault. On
// success the job's delivery is marked acknowledged. On failure it's
// returned to pending (with backoff) or moved to dead-letter — see
// resolveRedelivery — except for a payload this process
// itself cannot interpret (unmarshal failure or an unrecognized kind, e.g.
// a schema this binary predates), which goes straight to dead-letter: no
// amount of resubmission fixes a payload this code cannot decode, so
// leaving it 'pending' would just repeat the same failure every cycle
// forever with no operator-visible signal that it's actually stuck.
//
// job must come from ClaimPendingDeliveries: every outcome write carries its
// ResultDeliveryClaimToken, so a call that outlived its claim changes nothing.
func (w *Worker) redeliverOne(ctx context.Context, job *work.Job) {
	logger := slog.With("job_id", job.JobID)

	var pd pendingDelivery
	if err := json.Unmarshal([]byte(job.ResultDeliveryPayload), &pd); err != nil {
		logger.Error("pending delivery payload is not valid JSON — moving to dead letter", "err", err)
		w.resolveRedelivery(ctx, logger, job, fmt.Errorf("unmarshal pending delivery: %w", err), true)
		return
	}

	// malformedShape marks "decodable JSON, but not a shape this code
	// recognizes" (unknown kind, or a kind whose payload pointer is nil) —
	// the same "no amount of resubmission helps" case as an unmarshal
	// failure, so it goes straight to dead-letter too rather than the
	// normal retryable/permanent HTTP-error branch below.
	var deliverErr error
	malformedShape := false
	switch pd.Kind {
	case "check":
		if pd.Check == nil {
			deliverErr = errors.New("pending check delivery payload missing check body")
			malformedShape = true
		} else {
			_, deliverErr = w.vaultClient.SubmitCheckRecord(ctx, *pd.Check)
		}
	case "scan":
		if pd.Scan == nil {
			deliverErr = errors.New("pending scan delivery payload missing scan body")
			malformedShape = true
		} else {
			_, deliverErr = w.vaultClient.SubmitScanRecord(ctx, *pd.Scan)
		}
	default:
		deliverErr = fmt.Errorf("unknown pending delivery kind %q", pd.Kind)
		malformedShape = true
	}

	if deliverErr != nil {
		if malformedShape {
			logger.Error("pending delivery payload has an unrecognized shape", "err", deliverErr)
		} else {
			logger.Warn("redelivery attempt failed", "attempts", job.ResultDeliveryAttempts, "err", deliverErr)
		}
		w.resolveRedelivery(ctx, logger, job, deliverErr, malformedShape)
		return
	}

	logger.Info("redelivery succeeded", "attempts", job.ResultDeliveryAttempts)
	w.logDeliveryWriteErr(logger, "redelivery acknowledged",
		w.store.MarkResultDeliveryAcknowledged(ctx, job.JobID, job.ResultDeliveryClaimToken))
}
