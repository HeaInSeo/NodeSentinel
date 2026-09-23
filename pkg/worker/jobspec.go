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

	// jobIDLabel carries the WorkStore job ID on every Job this worker
	// creates, under every naming scheme it has ever used — see
	// retireLegacyExecutions, which relies on that.
	jobIDLabel = "nodesentinel.io/job"
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
				"app":      "nodevault-smoke",
				jobIDLabel: job.JobID,
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

// smokeJobName derives a DNS-1123-safe Job name from the work.Job's ID and
// its durable *execution identity* (work.Job.ExecutionID, minted/adopted by
// work.Store.EnsureExecution), so it stays traceable back to the WorkStore
// job while naming exactly one physical execution.
//
// History — why this is keyed on ExecutionID and not on Attempt:
//
// Originally the name was job-ID-only, so a retried job reused the exact
// same K8s Job name across attempts. That collided in a reachable,
// non-crash scenario: runSmokeRun's poll loop's Get error path (transient
// K8s API failure) returns a retryable outcome without deleting the
// in-flight K8s Job object first, the job gets requeued and re-leased while
// the previous attempt's object is still present, and the next Create fails
// with AlreadyExists.
//
// The first fix appended job.Attempt. That removed the collision but opened
// a strictly worse hole: work.Store.LeaseJob increments attempt on *every*
// lease, including when it reclaims a merely-expired lease whose worker
// died without its K8s Job dying. The reclaiming attempt then computed a
// *different* name and created a second Job while the first was still
// running — two concurrent physical executions of one logical validation,
// which NodeSentinel #2's acceptance explicitly forbids. Uniqueness per
// attempt and at-most-one-execution are different properties, and attempt
// only ever provided the first.
//
// ExecutionID provides both: it is stable across the attempts that adopt an
// in-flight execution (so no duplicate is ever created), and it changes once
// the previous execution is terminal and the retry policy permits a new one
// (so no AlreadyExists collision either). Nothing here may go back to
// deriving the name from job.Attempt.
func smokeJobName(job *work.Job) string {
	return fmt.Sprintf("smoke-%s-%s", sanitizeDNSLabel(job.JobID), job.ExecutionID)
}

// sanitizeDNSLabel lowercases and strips characters that are not valid in a
// K8s object name, truncating to fit the 63-character DNS label limit.
func sanitizeDNSLabel(s string) string {
	// Budget against the 63-char limit for the longest name built from this:
	// "smoke-" (6) + this label + "-" (1) + an execution ID (see
	// sqlite.newExecutionID: "a" + attempt digits + "-" + 16 hex chars, so
	// 22 even allowing a 4-digit attempt) = 29 chars of overhead. 63-29=34,
	// so 30 leaves headroom. This was 50 back when the suffix was a bare
	// attempt number; keeping 50 would now overflow the limit and make the
	// API server reject every Job for a long job ID.
	return sanitizeDNSLabelMax(s, 30)
}

// legacySanitizedIDMaxLen is the truncation workers without execution
// identities applied to the job ID in their attempt-derived Job names. Only
// isLegacyJobName may use it: recognizing those names needs the historical
// form, and a normal ingress ID ("job-" + 32 hex) is longer than 30.
const legacySanitizedIDMaxLen = 50

// sanitizeDNSLabelMax is sanitizeDNSLabel with an explicit truncation length.
func sanitizeDNSLabelMax(s string, maxLen int) string {
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
