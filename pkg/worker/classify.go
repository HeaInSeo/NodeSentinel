package worker

import (
	"context"
	"errors"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// outcome describes the result of an L4 smoke-run, classified per
// docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md section 6.3 / 11.
type outcome struct {
	success   bool
	retryable bool // only meaningful when !success
	reason    string
}

// classifySmokeRun inspects the K8s Job (and, if available, its Pods) to
// decide whether the L4 smoke-run succeeded, and if not, whether the
// failure is infra-level (retryable=true) or application-level
// (retryable=false).
//
// Per spec section 6.3 / 11:
//   - OOMKilled, timeout, scheduling failure, image pull failure -> infra-level, retryable=true
//   - exit code != 0 (container ran) -> application-level, retryable=false
func classifySmokeRun(ctx context.Context, kube kubernetes.Interface, namespace, jobName string, k8sJob *batchv1.Job) outcome {
	for _, cond := range k8sJob.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			return outcome{success: true}
		}
	}

	// ctx deadline exceeded while waiting on the job is always infra-level.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return outcome{success: false, retryable: true, reason: "timeout: smoke-run did not complete within the allotted time"}
	}

	for _, cond := range k8sJob.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return classifyFromPods(ctx, kube, namespace, jobName, cond.Reason, cond.Message)
		}
	}

	// Job has neither Complete nor Failed condition yet (e.g. we returned
	// because the wait loop's own context expired). Inspect pods directly
	// to look for infra-level signals before falling back to a generic
	// infra-level classification (unknown state -> safer to retry).
	return classifyFromPods(ctx, kube, namespace, jobName, "Unknown", "smoke-run ended without a terminal Job condition")
}

// classifyFromPods inspects the smoke-run's pod(s) for terminated container
// states and event-style reasons (OOMKilled, ImagePullBackOff, scheduling
// failure) to refine the infra vs application classification.
func classifyFromPods(ctx context.Context, kube kubernetes.Interface, namespace, jobName, fallbackReason, fallbackMessage string) outcome {
	pods, err := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + jobName,
	})
	if err != nil || len(pods.Items) == 0 {
		// Can't inspect pods; treat as infra-level since we cannot confirm
		// the container ever ran.
		return outcome{success: false, retryable: true, reason: fmt.Sprintf("%s: %s", fallbackReason, fallbackMessage)}
	}

	for _, pod := range pods.Items {
		// Scheduling failure: pod never left Pending.
		if pod.Status.Phase == corev1.PodPending {
			return outcome{success: false, retryable: true, reason: "scheduling failure: pod did not start running"}
		}

		for _, cs := range pod.Status.ContainerStatuses {
			if waiting := cs.State.Waiting; waiting != nil {
				switch waiting.Reason {
				case "ImagePullBackOff", "ErrImagePull":
					return outcome{success: false, retryable: true, reason: "image pull failure: " + waiting.Message}
				}
			}

			term := cs.State.Terminated
			if term == nil {
				continue
			}
			switch term.Reason {
			case "OOMKilled":
				return outcome{success: false, retryable: true, reason: "OOMKilled"}
			case "DeadlineExceeded":
				return outcome{success: false, retryable: true, reason: "timeout: activeDeadlineSeconds exceeded"}
			case "Error", "Completed":
				if term.ExitCode != 0 {
					return outcome{
						success:   false,
						retryable: false,
						reason:    fmt.Sprintf("application-level failure: container exited with code %d", term.ExitCode),
					}
				}
			default:
				if term.ExitCode != 0 {
					return outcome{
						success:   false,
						retryable: false,
						reason:    fmt.Sprintf("application-level failure: container exited with code %d (%s)", term.ExitCode, term.Reason),
					}
				}
			}
		}

		// Pod-level eviction / node problem.
		if pod.Status.Phase == corev1.PodFailed && pod.Status.Reason != "" {
			return outcome{success: false, retryable: true, reason: "infra-level failure: " + pod.Status.Reason}
		}
	}

	return outcome{success: false, retryable: true, reason: fmt.Sprintf("%s: %s", fallbackReason, fallbackMessage)}
}
