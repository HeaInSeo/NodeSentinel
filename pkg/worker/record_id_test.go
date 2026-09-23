package worker

import (
	"context"
	"log/slog"
	"testing"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// longIngressWorker returns a worker whose store holds one job with a normal
// 36-character ingress ID, and that job. Every NodeVault submission is
// appended to captured.
func longIngressWorker(t *testing.T, kube *fake.Clientset, captured *[]capturedSubmission) (*Worker, *work.Job) {
	t.Helper()
	store := newTestStore(t)
	req := newTestJob()
	req.JobID = longIngressJobID
	job, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if job.JobID != longIngressJobID {
		t.Fatalf("JobID = %q, want %q", job.JobID, longIngressJobID)
	}
	return New(store, kube, "test-worker").WithVaultClient(capturingVaultServer(t, captured)), job
}

// TestRecordIDs_LongIngressID_KeepHistoricalForm: CheckIDs and ScanIDs must
// carry the whole 36-character ingress ID, as they did before Job names moved
// to the 30-character truncation. NodeVault recognizes a resubmitted check by
// its ID, so a pre-upgrade in-flight job resumed after the upgrade would
// otherwise submit the same stage under a new ID and duplicate its evidence.
func TestRecordIDs_LongIngressID_KeepHistoricalForm(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	t.Run("L3 failure", func(t *testing.T) {
		var captured []capturedSubmission
		w, job := longIngressWorker(t, fake.NewClientset(), &captured)
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL3, "cmd",
			RetryDecision{Class: FailureClassTransientInfra, Retry: true, Reason: "boom"})
		assertOneCheckID(t, captured, "l3-"+longIngressJobID)
	})

	t.Run("L4 terminal failure", func(t *testing.T) {
		var captured []capturedSubmission
		w, job := longIngressWorker(t, fake.NewClientset(), &captured)
		w.reportTerminalFailure(ctx, logger, job, vaultclient.StageL4, "cmd",
			RetryDecision{Class: FailureClassDeterministic, Retry: false, Reason: "boom"})
		assertOneCheckID(t, captured, "l4-"+longIngressJobID)
	})

	t.Run("L4 success", func(t *testing.T) {
		var captured []capturedSubmission
		w, job := longIngressWorker(t, fake.NewClientset(), &captured)
		w.reportTerminalSuccess(ctx, logger, job, vaultclient.StageL4, "cmd")
		assertOneCheckID(t, captured, "l4-"+longIngressJobID)
	})

	t.Run("L5-a", func(t *testing.T) {
		var captured []capturedSubmission
		kube := fake.NewClientset()
		kube.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, k8serrors.NewBadRequest("rejected")
		})
		w, job := longIngressWorker(t, kube, &captured)
		job.ExecutionID = "a1-0123456789abcdef"
		if err := w.runL5a(ctx, logger, job, false); err != nil {
			t.Fatalf("runL5a: %v", err)
		}
		assertOneCheckID(t, captured, "l5a-"+longIngressJobID)
	})

	t.Run("L5-b", func(t *testing.T) {
		var captured []capturedSubmission
		w, job := longIngressWorker(t, fake.NewClientset(), &captured)
		if err := w.submitNotAvailableScanRecord(ctx, logger, job); err != nil {
			t.Fatalf("submitNotAvailableScanRecord: %v", err)
		}
		if len(captured) != 1 {
			t.Fatalf("submissions = %d, want 1", len(captured))
		}
		if got, want := captured[0].decodeScan(t).ScanID, "l5b-"+longIngressJobID; got != want {
			t.Errorf("ScanID = %q, want %q", got, want)
		}
	})
}

func assertOneCheckID(t *testing.T, captured []capturedSubmission, want string) {
	t.Helper()
	if len(captured) != 1 {
		t.Fatalf("submissions = %d, want 1", len(captured))
	}
	if got := captured[0].decodeCheck(t).CheckID; got != want {
		t.Errorf("CheckID = %q, want %q", got, want)
	}
}
