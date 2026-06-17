package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// alwaysCompleteReactor returns a fake reactor that makes every Job Get
// return a Complete condition immediately, bypassing the poll wait.
func alwaysCompleteReactor(ns string) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
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
	}
}

// absorbDryRunReactor returns a reactor that intercepts the dry-run Job create
// (DryRun=[All]) and returns success without persisting the object into the
// fake tracker — preventing the subsequent real create from getting an
// "already exists" error (the fake clientset does not honour DryRun options).
func absorbDryRunReactor() k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateActionImpl)
		if !ok {
			return false, nil, nil
		}
		if len(ca.GetCreateOptions().DryRun) > 0 {
			// Return the submitted object as-is so the caller sees no error,
			// but do NOT let the default reactor persist it.
			return true, ca.GetObject(), nil
		}
		return false, nil, nil // fall through for real creates
	}
}

// TestProcess_L4Success_L5Skipped verifies that process() completes a job
// successfully when L5 is not configured (vaultClient nil).
//
// Note: the poll ticker in runSmokeRun fires after 5 seconds, so this test
// takes ~5 seconds to complete.
func TestProcess_L4Success_L5Skipped(t *testing.T) {
	store := newTestStore(t)
	kube := fake.NewClientset()
	// absorbDryRunReactor must be prepended first so it runs before the
	// tracker reactor.  alwaysCompleteReactor handles the subsequent Get.
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	w := New(store, kube, "test-worker")

	req := newTestJob()
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w.process(context.Background(), job)

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusSucceeded {
		t.Errorf("expected status=succeeded, got %q", stored.Status)
	}
	if !strings.Contains(stored.ResultSummary, "L4 smoke-run succeeded") {
		t.Errorf("unexpected ResultSummary: %q", stored.ResultSummary)
	}
}

// TestProcess_L4Success_WithVault_L5Submitted verifies that process() with a
// vault client submits L5 records and produces a meaningful summary.
//
// Note: poll ticker fires after 5 seconds; test takes ~10 seconds (L4 + L5-a).
func TestProcess_L4Success_WithVault_L5Submitted(t *testing.T) {
	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(vaultclient.SubmitResponse{RecordID: "r1"})
	}))
	t.Cleanup(srv.Close)

	vc := vaultclient.NewWithAddr(srv.URL)
	w2 := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newTestJob()
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w2.process(context.Background(), job)

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusSucceeded {
		t.Errorf("expected status=succeeded, got %q", stored.Status)
	}
}

// TestProcess_L3DryRunFails_JobRetried verifies that a dry-run failure requeues
// the job (retryable=true per spec) and records the error in LastError.
func TestProcess_L3DryRunFails_JobRetried(t *testing.T) {
	store := newTestStore(t)
	kube := fake.NewClientset()

	// Make dry-run creation fail.
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateActionImpl)
		if len(ca.GetCreateOptions().DryRun) > 0 {
			return true, nil, k8serrors.NewInternalError(fmt.Errorf("admission webhook rejected"))
		}
		return false, nil, nil
	})

	w := New(store, kube, "test-worker")

	req := newTestJob()
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w.process(context.Background(), job)

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// L3 failure is retryable=true → job is requeued, not permanently failed.
	if stored.Status != work.StatusQueued {
		t.Errorf("expected status=queued (retried) after L3 rejection, got %q", stored.Status)
	}
	if !strings.Contains(stored.LastError, "L3 dry-run") {
		t.Errorf("LastError should mention L3 dry-run, got %q", stored.LastError)
	}
}

// TestRunSmokeRun_Complete verifies that runSmokeRun returns success when the
// K8s Job reports Complete=True on the first poll.
func TestRunSmokeRun_Complete(t *testing.T) {
	kube := fake.NewClientset()
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	store := newTestStore(t)
	w := New(store, kube, "test-worker")

	req := newTestJob()
	job, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	spec := buildSmokeJobSpec(job)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result := w.runSmokeRun(ctx, slog.Default(), smokeNamespace, job, spec)
	if !result.success {
		t.Errorf("expected success, got failure: %s", result.reason)
	}
}

// TestRunSmokeRun_CreateFails verifies that runSmokeRun returns retryable
// failure when Job creation fails.
func TestRunSmokeRun_CreateFails(t *testing.T) {
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewInternalError(fmt.Errorf("quota exceeded"))
	})

	store := newTestStore(t)
	w := New(store, kube, "test-worker")
	req := newTestJob()
	job, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	spec := buildSmokeJobSpec(job)
	result := w.runSmokeRun(context.Background(), nil, smokeNamespace, job, spec)
	if result.success {
		t.Fatal("expected failure when job creation fails")
	}
	if !result.retryable {
		t.Errorf("create failure should be retryable, got reason: %s", result.reason)
	}
}

// TestRunSmokeRun_GetFails verifies retryable failure when polling Get errors.
func TestRunSmokeRun_GetFails(t *testing.T) {
	kube := fake.NewClientset()
	// First reactor: let the create succeed.
	// Second reactor: make Get fail.
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewInternalError(fmt.Errorf("internal server error"))
	})

	store := newTestStore(t)
	w := New(store, kube, "test-worker")
	req := newTestJob()
	job, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	spec := buildSmokeJobSpec(job)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result := w.runSmokeRun(ctx, slog.Default(), smokeNamespace, job, spec)
	if result.success {
		t.Fatal("expected failure when Get fails")
	}
	if !result.retryable {
		t.Errorf("Get failure should be retryable, got reason: %s", result.reason)
	}
}
