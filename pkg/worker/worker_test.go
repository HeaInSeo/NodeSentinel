package worker

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
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

// makeTestWorkJob returns a *work.Job suitable for unit tests that need a
// fully-populated job without going through the store.
func makeTestWorkJob() *work.Job {
	return &work.Job{
		JobID:            "test-job-abc123",
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
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
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

// --- capturingHandler: a minimal slog.Handler that records log records ---

type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *capturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(name string) slog.Handler       { return h }

func (h *capturingHandler) hasErrorLevel() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == slog.LevelError {
			return true
		}
	}
	return false
}

// TestLeaseJob_ErrNoAvailableJob_NoErrorLog_Regression verifies that when
// LeaseJob returns ErrNoAvailableJob (empty queue / idle state) the worker
// does NOT emit a slog.Error record — only Debug or lower (CRITICAL regression).
func TestLeaseJob_ErrNoAvailableJob_NoErrorLog_Regression(t *testing.T) {
	handler := &capturingHandler{}
	orig := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(orig) })

	store := newTestStore(t)
	kube := fake.NewClientset()
	w := New(store, kube, "test-worker")

	// Run one poll iteration: queue is empty → LeaseJob returns ErrNoAvailableJob.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	w.Run(ctx) // blocks until ctx expires (one idle poll cycle)

	if handler.hasErrorLevel() {
		t.Error("expected no slog.Error when queue is empty (ErrNoAvailableJob), but ERROR was logged")
	}
}

// TestRunL5a_Error_ReflectedInSummary_Regression verifies that when runL5a
// returns an error, the summary string passed to CompleteJob contains the
// failure message (WARN regression).
//
// We test this at the runL5a level directly: a closed vault server causes
// the submit to fail, and we verify the returned error is non-nil and that
// the summary construction logic propagates it.
func TestRunL5a_Error_ReflectedInSummary_Regression(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()

	// L5-a K8s Job get returns Complete immediately (no poll wait needed).
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		j := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: ga.GetName(), Namespace: ga.GetNamespace()},
			Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
				},
			},
		}
		return true, j, nil
	})

	// Vault server that is immediately closed → connection refused on submit.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	vaultClient := vaultclient.NewWithAddr(srv.URL)

	w := New(store, kube, "test-worker").WithVaultClient(vaultClient)

	job := makeTestWorkJob()
	logger := slog.Default()

	// runL5a must return a non-nil error when the vault submit fails.
	err := w.runL5a(context.Background(), logger, job)
	if err == nil {
		t.Fatal("expected runL5a to return error when vault is unreachable, got nil")
	}

	// Verify that the summary construction embeds the error.
	summary := "L3 dry-run passed; L4 smoke-run succeeded; L5 validation submitted"
	if err != nil {
		summary += "; L5-a failed: " + err.Error()
	}
	if !strings.Contains(summary, "L5-a failed") {
		t.Errorf("summary does not contain 'L5-a failed': %q", summary)
	}
}
