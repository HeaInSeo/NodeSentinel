package worker

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

const (
	// smokeNamespace is the namespace L3/L4 Jobs run in, per
	// docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md section 6.1.
	smokeNamespace = "nodevault-smoke"

	// smokeRunTimeout is the L4 default timeout (spec section 6.3 / 11).
	smokeRunTimeout = 5 * 60 // seconds, also used as activeDeadlineSeconds safety net

	leaseTTL          = 2 * 60 // seconds; worker lease duration
	heartbeatInterval = 30     // seconds; how often Heartbeat is called during L4
	pollInterval      = 5      // seconds; LeaseJob polling interval
)

// buildSmokeJobSpec constructs the K8s Job object used for both the L3
// dry-run admission check and the real L4 smoke-run. Per spec section 6.3,
// L4 runs without sample fixtures using only a basic startup command, so L3
// and L4 share the exact same manifest shape (L3 just submits it with
// DryRun: All instead of actually creating it).
func buildSmokeJobSpec(job *work.Job) *batchv1.Job {
	backoff := int32(0)
	deadline := int64(smokeRunTimeout)
	ttlAfterFinished := int32(120)
	image := job.ImageRepository + "@" + job.ImageDigest

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      smokeJobName(job),
			Namespace: smokeNamespace,
			Labels: map[string]string{
				"app":                 "nodevault-smoke",
				"nodesentinel.io/job": job.JobID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttlAfterFinished,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "smoke",
							Image: image,
						},
					},
				},
			},
		},
	}
}

// smokeJobName derives a deterministic, DNS-1123-safe Job name from the
// work.Job's ID so re-leasing (e.g. after a retry) is traceable back to it.
func smokeJobName(job *work.Job) string {
	return fmt.Sprintf("smoke-%s", sanitizeDNSLabel(job.JobID))
}

// sanitizeDNSLabel lowercases and strips characters that are not valid in a
// K8s object name, truncating to fit the 63-character DNS label limit.
func sanitizeDNSLabel(s string) string {
	const maxLen = 50 // leave room for the "smoke-" prefix
	out := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c-'A'+'a')
		default:
			out = append(out, '-')
		}
	}
	if len(out) > maxLen {
		out = out[:maxLen]
	}
	return string(out)
}
