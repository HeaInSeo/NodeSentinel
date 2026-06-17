package worker

import (
	"context"
	"log/slog"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// Worker polls the WorkStore for queued jobs and runs L3 dry-run + L4 smoke-run
// K8s Jobs per docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md sections 4-6.
type Worker struct {
	store      work.Store
	kube       kubernetes.Interface
	workerName string
}

// New creates a Worker. workerName identifies this instance in LeaseJob records.
func New(store work.Store, kube kubernetes.Interface, workerName string) *Worker {
	return &Worker{store: store, kube: kube, workerName: workerName}
}

// Run polls for queued jobs and processes them. It blocks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		job, err := w.store.LeaseJob(ctx, w.workerName, time.Duration(leaseTTL)*time.Second)
		if err != nil {
			slog.Error("LeaseJob error", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(pollInterval) * time.Second):
			}
			continue
		}
		if job == nil {
			// No queued jobs — wait before polling again.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(pollInterval) * time.Second):
			}
			continue
		}
		w.process(ctx, job)
	}
}

func (w *Worker) process(ctx context.Context, job *work.Job) {
	logger := slog.With("job_id", job.JobID, "image_digest", job.ImageDigest)
	logger.Info("processing job", "actions", job.RequestedActions)

	jobSpec := buildSmokeJobSpec(job)
	ns := smokeNamespace

	// L3: dry-run admission check — no actual Job created.
	if err := w.runDryRun(ctx, ns, jobSpec); err != nil {
		logger.Warn("L3 dry-run failed", "err", err)
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, "L3 dry-run: "+err.Error(), true)
		return
	}
	logger.Info("L3 dry-run passed")

	// L4: real smoke-run.
	smokeCtx, cancel := context.WithTimeout(ctx, time.Duration(smokeRunTimeout)*time.Second)
	defer cancel()

	result := w.runSmokeRun(smokeCtx, logger, ns, job, jobSpec)
	if !result.success {
		_ = w.store.FailJob(ctx, job.JobID, w.workerName, result.reason, result.retryable)
		return
	}

	summary := "L3 dry-run passed; L4 smoke-run succeeded"
	if err := w.store.CompleteJob(ctx, job.JobID, w.workerName, summary); err != nil {
		logger.Error("CompleteJob failed", "err", err)
	}
	logger.Info("job completed", "summary", summary)
}

func (w *Worker) runDryRun(ctx context.Context, ns string, jobSpec *batchv1.Job) error {
	_, err := w.kube.BatchV1().Jobs(ns).Create(
		ctx, jobSpec,
		metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}},
	)
	return err
}

func (w *Worker) runSmokeRun(ctx context.Context, logger *slog.Logger, ns string, job *work.Job, jobSpec *batchv1.Job) outcome {
	created, err := w.kube.BatchV1().Jobs(ns).Create(ctx, jobSpec, metav1.CreateOptions{})
	if err != nil {
		return outcome{success: false, retryable: true, reason: "failed to create smoke-run Job: " + err.Error()}
	}
	logger.Info("L4 smoke-run Job created", "k8s_job", created.Name)

	// Poll until Job completes, context expires, or deadline exceeds.
	pollTick := time.NewTicker(5 * time.Second)
	heartbeatTick := time.NewTicker(time.Duration(heartbeatInterval) * time.Second)
	defer pollTick.Stop()
	defer heartbeatTick.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort cleanup.
			_ = w.deleteJob(context.Background(), ns, created.Name)
			return classifySmokeRun(ctx, w.kube, ns, created.Name, &batchv1.Job{})
		case <-heartbeatTick.C:
			if err := w.store.Heartbeat(ctx, job.JobID, w.workerName, time.Duration(leaseTTL)*time.Second); err != nil {
				logger.Warn("Heartbeat failed", "err", err)
			}
		case <-pollTick.C:
			k8sJob, err := w.kube.BatchV1().Jobs(ns).Get(ctx, created.Name, metav1.GetOptions{})
			if err != nil {
				return outcome{success: false, retryable: true, reason: "get smoke-run Job: " + err.Error()}
			}
			for _, cond := range k8sJob.Status.Conditions {
				if cond.Type == "Complete" && cond.Status == "True" {
					_ = w.deleteJob(context.Background(), ns, created.Name)
					return outcome{success: true}
				}
				if cond.Type == "Failed" && cond.Status == "True" {
					result := classifySmokeRun(ctx, w.kube, ns, created.Name, k8sJob)
					_ = w.deleteJob(context.Background(), ns, created.Name)
					return result
				}
			}
		}
	}
}

func (w *Worker) deleteJob(ctx context.Context, ns, name string) error {
	prop := metav1.DeletePropagationForeground
	return w.kube.BatchV1().Jobs(ns).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &prop,
	})
}
