package worker

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// Regression coverage for the three execution-identity release rules on
// PR #25: an identity is released exactly when nothing created under it can
// still be running — no sooner (duplicate execution) and no later (a retry
// adopting a Job that never existed).

// realJobCreates returns the names of the non-dry-run Job creates kube saw.
func realJobCreates(kube *fake.Clientset) []string {
	var names []string
	for _, a := range kube.Actions() {
		ca, ok := a.(k8stesting.CreateActionImpl)
		if !ok || ca.GetResource().Resource != "jobs" || len(ca.GetCreateOptions().DryRun) > 0 {
			continue
		}
		if j, ok := ca.GetObject().(*batchv1.Job); ok {
			names = append(names, j.Name)
		}
	}
	return names
}

// TestProcess_L3DryRunFails_ReleasesFreshIdentity: a fresh attempt whose L3
// dry-run fails never created a Job, so its identity must be released.
// Otherwise the retry adopts it, skips dry-run and Create, observes NotFound
// and burns a retry without ever running the smoke-run.
func TestProcess_L3DryRunFails_ReleasesFreshIdentity(t *testing.T) {
	useFastWorkerTicks(t)
	useLowMaxAttempts(t, 2)

	store := newTestStore(t)
	kube := fake.NewClientset()

	var dryRunShouldFail atomic.Bool
	dryRunShouldFail.Store(true)
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateActionImpl)
		if !ok || len(ca.GetCreateOptions().DryRun) == 0 {
			return false, nil, nil
		}
		if dryRunShouldFail.Load() {
			return true, nil, k8serrors.NewInternalError(errTestAdmissionRejected)
		}
		return true, ca.GetObject(), nil
	})
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	w := New(store, kube, "test-worker")

	created, err := store.CreateJob(context.Background(), newSmokeRunOnlyJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	job1, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 1): %v", err)
	}
	w.process(context.Background(), job1)

	afterL3, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if afterL3.Status != work.StatusQueued {
		t.Fatalf("Status after L3 failure = %q, want queued", afterL3.Status)
	}
	if !afterL3.ExecutionTerminal {
		t.Fatal("identity minted by an attempt that failed L3 was left adoptable — no Job exists under it, " +
			"so the retry would observe NotFound instead of running")
	}

	// Attempt 2 is the last one maxAttempts allows. It must get a real
	// dry-run + Create and succeed, not spend itself on a phantom Job.
	dryRunShouldFail.Store(false)
	job2, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 2): %v", err)
	}
	w.process(context.Background(), job2)

	final, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != work.StatusSucceeded {
		t.Fatalf("final Status = %q (LastError %q), want succeeded", final.Status, final.LastError)
	}
	if got := realJobCreates(kube); len(got) != 1 || !strings.HasPrefix(got[0], "smoke-") {
		t.Fatalf("real Job creates = %v, want exactly one smoke-run Job from attempt 2", got)
	}
}

// TestProcess_L5a_IdentityStaysOpenUntilL5aEnds: with profile requested, L5-a
// runs under the same identity after L4, so the identity must still be
// non-terminal when the L5-a Job is created and released only after it ends.
func TestProcess_L5a_IdentityStaysOpenUntilL5aEnds(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	req := newTestJob()
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.ActionProfile}
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	var sawL5aCreate, terminalAtL5aCreate atomic.Bool
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateActionImpl)
		if !ok {
			return false, nil, nil
		}
		if j, ok := ca.GetObject().(*batchv1.Job); ok && strings.HasPrefix(j.Name, "l5a-") {
			stored, err := store.GetJob(context.Background(), created.JobID)
			if err != nil {
				t.Errorf("GetJob inside L5-a create: %v", err)
			} else {
				terminalAtL5aCreate.Store(stored.ExecutionTerminal)
			}
			sawL5aCreate.Store(true)
		}
		return false, nil, nil
	})

	w := New(store, kube, "test-worker").WithVaultClient(vc)
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	w.process(context.Background(), job)

	if !sawL5aCreate.Load() {
		t.Fatal("L5-a Job was never created — this test would not be checking anything")
	}
	if terminalAtL5aCreate.Load() {
		t.Fatal("identity was already terminal when the L5-a Job was created — a worker dying during L5-a " +
			"would let the next lease mint a new identity and run L4 + L5-a beside the running L5-a Job")
	}
	final, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !final.ExecutionTerminal {
		t.Error("identity should be released once the L5-a Job reached a terminal condition")
	}
}

// TestProcess_AdoptedDuringL5a_ObservesL5aInsteadOfRerunning reproduces a
// worker dying mid-L5-a: the L4 Job is gone (deleted after success), the
// L5-a Job is still there, and the identity is still open. The next attempt
// must adopt that L5-a Job — not observe the missing L4 Job, release the
// identity, and rerun L4 + L5-a under a new one.
func TestProcess_AdoptedDuringL5a_ObservesL5aInsteadOfRerunning(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()

	req := newTestJob()
	req.JobID = "died-during-l5a"
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.ActionProfile}
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Attempt 1: minted the identity, finished L4, created the L5-a Job, died.
	job1, err := store.LeaseJob(context.Background(), "worker-1", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 1): %v", err)
	}
	mintExecution(t, store, job1)
	l5aName := l5aJobName(job1)
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Create(
		context.Background(), buildL5aJobSpec(job1), metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("seed running L5-a Job: %v", err)
	}
	if err := store.FailJob(context.Background(), job1.JobID, "worker-1", "lease expired", true); err != nil {
		t.Fatalf("FailJob (requeue): %v", err)
	}
	kube.ClearActions()

	// The API recovers and the L5-a Job finishes while attempt 2 observes it.
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	job2, err := store.LeaseJob(context.Background(), "worker-2", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 2): %v", err)
	}
	w := New(store, kube, "worker-2").WithVaultClient(vc)
	w.process(context.Background(), job2)

	if job2.ExecutionID != job1.ExecutionID {
		t.Fatalf("attempt 2 ran under %q, want adopted %q", job2.ExecutionID, job1.ExecutionID)
	}
	for _, name := range realJobCreates(kube) {
		if strings.HasPrefix(name, "smoke-") {
			t.Fatalf("attempt 2 re-created the L4 smoke-run Job %q — L4 had already succeeded", name)
		}
		if name != l5aName {
			t.Fatalf("attempt 2 created L5-a Job %q beside the running %q — duplicate execution", name, l5aName)
		}
	}
	final, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != work.StatusSucceeded {
		t.Fatalf("final Status = %q (LastError %q), want succeeded", final.Status, final.LastError)
	}
	if !final.ExecutionTerminal {
		t.Error("identity should be released once the adopted L5-a Job reached a terminal condition")
	}
}

// TestRunSmokeRun_Timeout_DeleteAcceptedButJobRemains_StaysAdoptable: a
// foreground Delete returning nil only means the request was accepted. While
// the Job (and its Pods) still exist, the identity must stay adoptable.
func TestRunSmokeRun_Timeout_DeleteAcceptedButJobRemains_StaysAdoptable(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	// Accept every delete without removing anything: the Job stays around,
	// terminating, exactly as it does while its Pods finish their grace period.
	kube.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})

	store := newTestStore(t)
	w := New(store, kube, "test-worker")
	job := leaseAndMint(t, store, "terminating-job")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := w.runSmokeRun(ctx, slog.Default(), smokeNamespace, job, buildSmokeJobSpec(job), false, true)
	if result.success {
		t.Fatal("a timed-out smoke-run must not succeed")
	}

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionTerminal {
		t.Fatal("identity released while the deleted Job was still present — a retry could create a new Job " +
			"while the old Pod is still running in its termination grace period")
	}
}

// TestRunSmokeRun_Timeout_JobConfirmedGone_Releases is the complement: once
// the Job is observed NotFound after the delete, the identity is released.
func TestRunSmokeRun_Timeout_JobConfirmedGone_Releases(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset() // the default tracker removes the Job on Delete
	store := newTestStore(t)
	w := New(store, kube, "test-worker")
	job := leaseAndMint(t, store, "deleted-job")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := w.runSmokeRun(ctx, slog.Default(), smokeNamespace, job, buildSmokeJobSpec(job), false, true)
	if result.success {
		t.Fatal("a timed-out smoke-run must not succeed")
	}

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !stored.ExecutionTerminal {
		t.Fatal("identity should be released once the deleted Job is confirmed gone")
	}
}

func leaseAndMint(t *testing.T, store work.Store, jobID string) *work.Job {
	t.Helper()
	req := newSmokeRunOnlyJob()
	req.JobID = jobID
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	mintExecution(t, store, job)
	return job
}
