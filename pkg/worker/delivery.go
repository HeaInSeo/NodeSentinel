package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

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

// markCheckDeliveryPending durably records that req (a terminal CheckRecord)
// failed to deliver, so retryPendingDeliveries can redeliver it later
// without re-running the K8s Job that produced it.
func (w *Worker) markCheckDeliveryPending(
	ctx context.Context, logger *slog.Logger, job *work.Job, req vaultclient.SubmitCheckRecordRequest, deliverErr error,
) {
	payload, err := json.Marshal(pendingDelivery{Kind: "check", Check: &req})
	if err != nil {
		logger.Error("failed to marshal pending check delivery — cannot retry later", "err", err)
		return
	}
	if err := w.store.MarkResultDeliveryPending(ctx, job.JobID, string(payload), deliverErr.Error()); err != nil {
		logger.Error("failed to record pending result delivery", "err", err)
	}
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
	if err := w.store.MarkResultDeliveryPending(ctx, job.JobID, string(payload), deliverErr.Error()); err != nil {
		logger.Error("failed to record pending result delivery", "err", err)
	}
}

// retryPendingDeliveries attempts redelivery of every job whose terminal
// validation-result record previously failed to reach NodeVault. Called
// once per Run() poll cycle (see worker.go) alongside normal job leasing.
// Best-effort: a redelivery failure here is logged and left pending for the
// next cycle, not retried inline within this call.
func (w *Worker) retryPendingDeliveries(ctx context.Context) {
	if w.vaultClient == nil {
		return
	}
	pending, err := w.store.ListPendingDeliveries(ctx)
	if err != nil {
		slog.Error("list pending deliveries failed", "err", err)
		return
	}
	for _, job := range pending {
		w.redeliverOne(ctx, job)
	}
}

// redeliverOne re-POSTs job's stored pending payload to NodeVault. On
// success the job's delivery is marked acknowledged; on failure the
// attempt count/last-error are updated and the job stays pending for the
// next retryPendingDeliveries cycle.
func (w *Worker) redeliverOne(ctx context.Context, job *work.Job) {
	logger := slog.With("job_id", job.JobID)

	var pd pendingDelivery
	if err := json.Unmarshal([]byte(job.ResultDeliveryPayload), &pd); err != nil {
		logger.Error("failed to unmarshal pending delivery payload — cannot redeliver", "err", err)
		return
	}

	var deliverErr error
	switch pd.Kind {
	case "check":
		if pd.Check == nil {
			deliverErr = errors.New("pending check delivery payload missing check body")
		} else {
			_, deliverErr = w.vaultClient.SubmitCheckRecord(ctx, *pd.Check)
		}
	case "scan":
		if pd.Scan == nil {
			deliverErr = errors.New("pending scan delivery payload missing scan body")
		} else {
			_, deliverErr = w.vaultClient.SubmitScanRecord(ctx, *pd.Scan)
		}
	default:
		deliverErr = fmt.Errorf("unknown pending delivery kind %q", pd.Kind)
	}

	if deliverErr != nil {
		logger.Warn("redelivery attempt failed", "attempts", job.ResultDeliveryAttempts, "err", deliverErr)
		if err := w.store.MarkResultDeliveryPending(ctx, job.JobID, job.ResultDeliveryPayload, deliverErr.Error()); err != nil {
			logger.Error("failed to update pending delivery attempt count", "err", err)
		}
		return
	}

	logger.Info("redelivery succeeded", "attempts", job.ResultDeliveryAttempts)
	if err := w.store.MarkResultDeliveryAcknowledged(ctx, job.JobID); err != nil {
		logger.Error("failed to mark result delivery acknowledged", "err", err)
	}
}
