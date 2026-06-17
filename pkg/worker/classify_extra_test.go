package worker

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestExtractPodExitCode_Terminated verifies that extractPodExitCode returns
// the correct exit code from a terminated container.
func TestExtractPodExitCode_Terminated(t *testing.T) {
	kube := fake.NewClientset()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "l5a-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "l5a-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 42,
						},
					},
				},
			},
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	store := newTestStore(t)
	w := New(store, kube, "test-worker")
	code := w.extractPodExitCode(context.Background(), "l5a-job")
	if code != 42 {
		t.Errorf("expected exit code 42, got %d", code)
	}
}

// TestExtractPodExitCode_NoPods verifies that extractPodExitCode returns -1
// when no pods exist for the job.
func TestExtractPodExitCode_NoPods(t *testing.T) {
	kube := fake.NewClientset()
	store := newTestStore(t)
	w := New(store, kube, "test-worker")
	code := w.extractPodExitCode(context.Background(), "nonexistent-job")
	if code != -1 {
		t.Errorf("expected -1 for missing pods, got %d", code)
	}
}

// TestExtractPodExitCode_NoTerminatedContainer verifies -1 when pod exists
// but has no terminated container state.
func TestExtractPodExitCode_NoTerminatedContainer(t *testing.T) {
	kube := fake.NewClientset()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "running-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "running-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{},
					},
				},
			},
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	store := newTestStore(t)
	w := New(store, kube, "test-worker")
	code := w.extractPodExitCode(context.Background(), "running-job")
	if code != -1 {
		t.Errorf("expected -1 for running container, got %d", code)
	}
}

// TestClassifyFromPods_Pending verifies scheduling failure classification.
func TestClassifyFromPods_Pending(t *testing.T) {
	kube := fake.NewClientset()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pending-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "pending-job"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	result := classifyFromPods(context.Background(), kube, smokeNamespace, "pending-job", "Unknown", "pending")
	if result.success {
		t.Fatal("expected failure")
	}
	if !result.retryable {
		t.Errorf("pending pod should be retryable, got reason: %s", result.reason)
	}
}

// TestClassifyFromPods_ImagePullBackOff verifies image pull failure classification.
func TestClassifyFromPods_ImagePullBackOff(t *testing.T) {
	kube := fake.NewClientset()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pull-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "pull-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "pull failed",
						},
					},
				},
			},
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	result := classifyFromPods(context.Background(), kube, smokeNamespace, "pull-job", "Unknown", "")
	if result.success {
		t.Fatal("expected failure")
	}
	if !result.retryable {
		t.Errorf("ImagePullBackOff should be retryable, reason: %s", result.reason)
	}
}

// TestClassifyFromPods_DeadlineExceeded verifies timeout classification.
func TestClassifyFromPods_DeadlineExceeded(t *testing.T) {
	kube := fake.NewClientset()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "timeout-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "timeout-job"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "DeadlineExceeded",
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

	result := classifyFromPods(context.Background(), kube, smokeNamespace, "timeout-job", "DeadlineExceeded", "")
	if result.success {
		t.Fatal("expected failure")
	}
	if !result.retryable {
		t.Errorf("DeadlineExceeded should be retryable, reason: %s", result.reason)
	}
}

// TestClassifyFromPods_EvictedPodLevel verifies that pod-level Evicted reason
// (pod.Status.Reason) is treated as infra-level and retryable.
func TestClassifyFromPods_EvictedPodLevel(t *testing.T) {
	kube := fake.NewClientset()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "evicted-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": "evicted-job"},
		},
		Status: corev1.PodStatus{
			Phase:  corev1.PodFailed,
			Reason: "Evicted",
		},
	}
	if _, err := kube.CoreV1().Pods(smokeNamespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	result := classifyFromPods(context.Background(), kube, smokeNamespace, "evicted-job", "Unknown", "")
	if result.success {
		t.Fatal("expected failure")
	}
	if !result.retryable {
		t.Errorf("pod-level Evicted should be retryable, reason: %s", result.reason)
	}
}

// TestSanitizeDNSLabel_Truncation verifies that labels longer than the max
// are truncated.
func TestSanitizeDNSLabel_Truncation(t *testing.T) {
	long := "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz"
	result := sanitizeDNSLabel(long)
	if len(result) > 50 {
		t.Errorf("expected max 50 chars, got %d", len(result))
	}
}

// TestSanitizeDNSLabel_UppercaseConversion verifies uppercase is lowercased.
func TestSanitizeDNSLabel_UppercaseConversion(t *testing.T) {
	result := sanitizeDNSLabel("ABC-def")
	if result != "abc-def" {
		t.Errorf("expected abc-def, got %q", result)
	}
}

// TestSanitizeDNSLabel_SpecialChars verifies special chars are replaced with dash.
func TestSanitizeDNSLabel_SpecialChars(t *testing.T) {
	result := sanitizeDNSLabel("hello_world.test")
	if result != "hello-world-test" {
		t.Errorf("expected hello-world-test, got %q", result)
	}
}
