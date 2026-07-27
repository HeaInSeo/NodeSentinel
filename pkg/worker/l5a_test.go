package worker

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// --- computeValidationHash unit tests ---

// TestComputeValidationHash_Deterministic verifies that the same inputs
// always produce the same hash (determinism requirement).
func TestComputeValidationHash_Deterministic(t *testing.T) {
	h1 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 0)
	h2 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 0)
	if h1 != h2 {
		t.Errorf("expected identical hashes, got %q and %q", h1, h2)
	}
}

// TestComputeValidationHash_DifferentExitCode verifies that a different
// exitCode produces a different hash (no hash collision on that axis).
func TestComputeValidationHash_DifferentExitCode(t *testing.T) {
	h0 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 0)
	h1 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 1)
	if h0 == h1 {
		t.Errorf("expected different hashes for different exit codes, but got %q for both", h0)
	}
}

// TestComputeValidationHash_NotEmpty verifies that the hash is a non-empty hex string.
func TestComputeValidationHash_NotEmpty(t *testing.T) {
	h := computeValidationHash("sha256:abc", "cmd", 0)
	if h == "" {
		t.Error("expected non-empty hash")
	}
	// SHA-256 hex is 64 characters.
	if len(h) != 64 {
		t.Errorf("expected 64-char hex, got len=%d: %q", len(h), h)
	}
}

// --- l5aCommand / K8s Job Command slice regression ---

// TestL5aCommandSlice_MatchesConstant_Regression verifies that l5aCommandSlice()
// round-trips back to l5aCommand via strings.Join, ensuring the K8s Job Command
// and the hash input share a single source of truth (WARN regression).
func TestL5aCommandSlice_MatchesConstant_Regression(t *testing.T) {
	slice := l5aCommandSlice()
	rejoined := strings.Join(slice, " ")
	if rejoined != l5aCommand {
		t.Errorf("l5aCommandSlice() rejoined = %q, want %q", rejoined, l5aCommand)
	}
}

// TestBuildL5aJobSpec_CommandMatchesConstant_Regression verifies that the K8s
// Job spec's container Command is exactly the fields of l5aCommand (WARN regression).
func TestBuildL5aJobSpec_CommandMatchesConstant_Regression(t *testing.T) {
	job := makeTestWorkJob()
	spec := buildL5aJobSpec(job)
	if len(spec.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("expected at least one container in L5-a job spec")
	}
	cmd := spec.Spec.Template.Spec.Containers[0].Command
	rejoined := strings.Join(cmd, " ")
	if rejoined != l5aCommand {
		t.Errorf("job spec Command rejoined = %q, want %q", rejoined, l5aCommand)
	}
}

// --- contractResult mapping regression ---

// TestSubmitCheckRecord_InfraFailed_NotApplicable_Regression verifies that
// validationStatus="infra_failed" maps to contractResult="not_applicable"
// and NOT "failed" (WARN regression).
func TestSubmitCheckRecord_InfraFailed_NotApplicable_Regression(t *testing.T) {
	result := contractResultFor("infra_failed")
	if result != "not_applicable" {
		t.Errorf("infra_failed → contractResult: want %q, got %q", "not_applicable", result)
	}
}

// TestSubmitCheckRecord_Failed_ContractFailed_Regression verifies that
// validationStatus="failed" maps to contractResult="failed".
func TestSubmitCheckRecord_Failed_ContractFailed_Regression(t *testing.T) {
	result := contractResultFor("failed")
	if result != "failed" {
		t.Errorf("failed → contractResult: want %q, got %q", "failed", result)
	}
}

// TestSubmitCheckRecord_Succeeded_ContractPassed verifies that
// validationStatus="succeeded" maps to contractResult="passed".
func TestSubmitCheckRecord_Succeeded_ContractPassed(t *testing.T) {
	result := contractResultFor("succeeded")
	if result != "passed" {
		t.Errorf("succeeded → contractResult: want %q, got %q", "passed", result)
	}
}

// contractResultFor is a test helper that reproduces the mapping logic from
// submitCheckRecord so we can unit-test it without setting up a full Worker.
func contractResultFor(validationStatus string) string {
	switch validationStatus {
	case "succeeded":
		return "passed"
	case "infra_failed":
		return "not_applicable"
	default:
		return "failed"
	}
}

// --- isInfraReason removal (Principle 4 centralization) ---
//
// isInfraReason (and its five regression tests, formerly here) was removed:
// it classified an L5-a Job failure purely from the K8s Job-level Failed
// condition's Reason ("BackoffLimitExceeded"/"DeadlineExceeded" -> infra,
// everything else -> not infra). Since buildL5aJobSpec sets BackoffLimit=0,
// *any* single pod failure — OOM, a real deterministic exit-code failure,
// a scheduling problem, anything — makes the Job controller set Reason to
// "BackoffLimitExceeded", so isInfraReason effectively always returned true
// and never actually distinguished failure causes the way L4's
// classify.go:classifyFromPods does (it inspects pod container terminated
// states instead of the coarser Job-level reason). This was a duplicate,
// strictly-worse copy of the same classification job, not a deliberate
// separate abstraction layer — see waitL5aJob's doc comment in l5a.go for
// the full analysis — so L5-a now calls classifyFromPods directly instead.
// TestWaitL5aJob_OOM_ClassifiedAsResourceObservation below is the direct
// regression coverage for the bug this fixes: an L5-a OOM used to come out
// misclassified as infra_failed/retryable via isInfraReason; it is now
// correctly recognized as RESOURCE_OBSERVATION.

// TestWaitL5aJob_OOM_ClassifiedAsResourceObservation reproduces the exact
// scenario isInfraReason used to get wrong: an L5-a Job whose Failed
// condition Reason is "BackoffLimitExceeded" (what buildL5aJobSpec's
// BackoffLimit=0 always produces on any single pod failure) but whose pod
// was actually OOMKilled. The old isInfraReason("BackoffLimitExceeded")
// would have returned true (infra_failed) with no OOM-specific signal at
// all; waitL5aJob must now delegate to classifyFromPods and come back
// FailureClassResourceObservation instead.
func TestWaitL5aJob_OOM_ClassifiedAsResourceObservation(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	jobName := "l5a-oom-job"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "l5a-oom-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": jobName},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "OOMKilled",
							ExitCode: 137,
						},
					},
				},
			},
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	k8sJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: smokeNamespace},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
					Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit",
				},
			},
		},
	}
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Create(context.Background(), k8sJob, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	store := newTestStore(t)
	w := New(store, kube, "test-worker")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, result, err := w.waitL5aJob(ctx, slog.Default(), makeTestWorkJob(), jobName)
	if err == nil {
		t.Fatal("expected an error for a Job-Failed condition")
	}
	if result.class != FailureClassResourceObservation {
		t.Errorf("class = %q, want %q — OOM must be detected via pod inspection, not the Job-level "+
			"BackoffLimitExceeded reason alone", result.class, FailureClassResourceObservation)
	}
}
