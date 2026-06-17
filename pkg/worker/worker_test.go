package worker

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

func newTestStore(t *testing.T) work.Store {
	t.Helper()
	s, err := sqlite.New(":memory:")
	if err != nil {
		t.Fatalf("open in-memory store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newTestJob() work.JobRequest {
	return work.JobRequest{
		ArtifactKind:     "tool",
		ImageRepository:  "harbor.example.com/library/bwa",
		ImageDigest:      "sha256:abc123",
		StableRef:        "bwa@0.7.17",
		ToolName:         "bwa",
		Version:          "0.7.17",
		CasHash:          "deadbeef",
		RequestedActions: []work.Action{work.ActionSmokeRun},
	}
}

// fakeJobWithCondition returns a batchv1.Job stub pre-populated with a condition.
func fakeJobWithCondition(ns, name string, condType batchv1.JobConditionType, status corev1.ConditionStatus) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: condType, Status: status},
			},
		},
	}
}

// TestL3DryRun_SendsDryRunAll verifies runDryRun passes DryRun:All in the
// CreateOptions (fake client doesn't enforce dry-run, so we inspect the action).
func TestL3DryRun_SendsDryRunAll(t *testing.T) {
	kube := fake.NewClientset()
	store := newTestStore(t)
	w := New(store, kube, "test-worker")

	var capturedDryRun []string
	kube.Fake.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateActionImpl)
		capturedDryRun = ca.GetCreateOptions().DryRun
		return false, nil, nil // let the default reactor handle it
	})

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	spec := buildSmokeJobSpec(job)
	if err := w.runDryRun(context.Background(), smokeNamespace, spec); err != nil {
		t.Fatalf("runDryRun: %v", err)
	}

	if len(capturedDryRun) == 0 || capturedDryRun[0] != metav1.DryRunAll {
		t.Errorf("expected DryRun=[All], got %v", capturedDryRun)
	}
}

// TestClassifySmokeRun_Complete verifies success classification.
func TestClassifySmokeRun_Complete(t *testing.T) {
	kube := fake.NewClientset()
	k8sJob := fakeJobWithCondition(smokeNamespace, "smoke-test", batchv1.JobComplete, corev1.ConditionTrue)
	result := classifySmokeRun(context.Background(), kube, smokeNamespace, "smoke-test", k8sJob)
	if !result.success {
		t.Errorf("expected success, got failure: %s", result.reason)
	}
}

// TestClassifySmokeRun_OOMKilled verifies infra-level retryable classification.
func TestClassifySmokeRun_OOMKilled(t *testing.T) {
	kube := fake.NewClientset()
	// Seed a Pod that was OOMKilled.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "smoke-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "smoke-test"},
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

	k8sJob := fakeJobWithCondition(smokeNamespace, "smoke-test", batchv1.JobFailed, corev1.ConditionTrue)
	result := classifySmokeRun(context.Background(), kube, smokeNamespace, "smoke-test", k8sJob)
	if result.success {
		t.Fatal("expected failure")
	}
	if !result.retryable {
		t.Errorf("OOMKilled should be retryable, got reason: %s", result.reason)
	}
}

// TestClassifySmokeRun_ApplicationFailure verifies non-retryable classification.
func TestClassifySmokeRun_ApplicationFailure(t *testing.T) {
	kube := fake.NewClientset()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "smoke-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "smoke-test"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "Error",
							ExitCode: 1,
						},
					},
				},
			},
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	k8sJob := fakeJobWithCondition(smokeNamespace, "smoke-test", batchv1.JobFailed, corev1.ConditionTrue)
	result := classifySmokeRun(context.Background(), kube, smokeNamespace, "smoke-test", k8sJob)
	if result.success {
		t.Fatal("expected failure")
	}
	if result.retryable {
		t.Errorf("exit-code failure should not be retryable, got reason: %s", result.reason)
	}
}

// TestClassifySmokeRun_Timeout verifies context deadline → infra retryable.
func TestClassifySmokeRun_Timeout(t *testing.T) {
	kube := fake.NewClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // ensure deadline exceeded

	result := classifySmokeRun(ctx, kube, smokeNamespace, "smoke-test", &batchv1.Job{})
	if result.success {
		t.Fatal("expected failure on timeout")
	}
	if !result.retryable {
		t.Errorf("timeout should be retryable, reason: %s", result.reason)
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
