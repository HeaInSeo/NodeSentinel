package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
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

// seedLegacyInFlightJob reproduces a row left behind by a worker from before
// execution identities existed: that worker leased the row, created its Job
// under the attempt-derived name, and died. The Job is still running (no
// conditions) and the row has been requeued with an empty ExecutionID, just
// as the migration leaves it. It returns the legacy Job's name.
func seedLegacyInFlightJob(t *testing.T, store work.Store, kube *fake.Clientset, jobID string) string {
	t.Helper()
	return seedLegacyInFlightJobWithPrefix(t, store, kube, jobID, "smoke-")
}

// seedLegacyInFlightJobWithPrefix is seedLegacyInFlightJob for the legacy
// Job of the given check ("smoke-" or "l5a-"). The name is built exactly as
// those workers built it: the job ID sanitized and truncated to 50 characters,
// then the attempt number.
func seedLegacyInFlightJobWithPrefix(t *testing.T, store work.Store, kube *fake.Clientset, jobID, prefix string) string {
	t.Helper()
	req := newSmokeRunOnlyJob()
	req.JobID = jobID
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	legacy, err := store.LeaseJob(context.Background(), "legacy-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (legacy attempt): %v", err)
	}
	legacyName := fmt.Sprintf("%s%s-%d", prefix, sanitizeDNSLabelMax(jobID, legacySanitizedIDMaxLen), legacy.Attempt)
	legacyJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      legacyName,
		Namespace: smokeNamespace,
		Labels:    map[string]string{"app": "nodevault-smoke", jobIDLabel: jobID},
	}}
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Create(
		context.Background(), legacyJob, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("seed legacy Job: %v", err)
	}
	if err := store.FailJob(context.Background(), jobID, "legacy-worker", "lease expired", true); err != nil {
		t.Fatalf("FailJob (requeue): %v", err)
	}
	kube.ClearActions()
	return legacyName
}

// TestProcess_LegacyInFlightJob_RetiredBeforeNewExecution: an upgraded
// worker reclaiming a row whose legacy Job is still running must delete that
// Job, and see it gone, before it creates a Job under a fresh identity.
// Otherwise two executions of one validation run at once.
func TestProcess_LegacyInFlightJob_RetiredBeforeNewExecution(t *testing.T) {
	assertLegacyRetiredBeforeNewExecution(t, "legacy-inflight")
}

// longIngressJobID has the form of every ingress-generated job ID: "job-"
// plus 32 hex characters, 36 in all. That is longer than the current
// sanitizeDNSLabel truncation (30) but shorter than the legacy one (50), so
// the legacy Job name carries the whole ID.
const longIngressJobID = "job-0123456789abcdef0123456789abcdef"

// TestProcess_LegacyInFlightJob_LongIngressID_RetiredBeforeNewExecution: the
// retirement must recognize a legacy Job whose name used the historical
// 50-character truncation. With the current 30-character form it would skip
// the Job and start a second execution beside it.
func TestProcess_LegacyInFlightJob_LongIngressID_RetiredBeforeNewExecution(t *testing.T) {
	if len(longIngressJobID) != 36 {
		t.Fatalf("setup: len(longIngressJobID) = %d, want 36", len(longIngressJobID))
	}
	assertLegacyRetiredBeforeNewExecution(t, longIngressJobID)
}

// assertLegacyRetiredBeforeNewExecution processes a row whose legacy smoke Job
// is still running and checks that the Job is deleted, and seen gone, before
// the first real create, with the run then succeeding.
func assertLegacyRetiredBeforeNewExecution(t *testing.T, jobID string) {
	t.Helper()
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	legacyName := seedLegacyInFlightJob(t, store, kube, jobID)

	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	// New-scheme Jobs complete at once. The legacy Job is left to the default
	// tracker, so it reads NotFound only after it has really been deleted.
	complete := alwaysCompleteReactor(smokeNamespace)
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if ga, ok := action.(k8stesting.GetActionImpl); ok && ga.GetName() == legacyName {
			return false, nil, nil
		}
		return complete(action)
	})

	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	if job.ExecutionID != "" {
		t.Fatalf("setup: ExecutionID = %q, want empty as the migration leaves it", job.ExecutionID)
	}
	New(store, kube, "test-worker").process(context.Background(), job)

	deletedAt, createdAt := -1, -1
	for i, a := range kube.Actions() {
		if da, ok := a.(k8stesting.DeleteActionImpl); ok && da.GetName() == legacyName && deletedAt < 0 {
			deletedAt = i
		}
		if ca, ok := a.(k8stesting.CreateActionImpl); ok && len(ca.GetCreateOptions().DryRun) == 0 && createdAt < 0 {
			createdAt = i
		}
	}
	if deletedAt < 0 {
		t.Fatalf("legacy Job %q was never deleted — it keeps running beside the new execution", legacyName)
	}
	if createdAt < 0 || createdAt < deletedAt {
		t.Fatalf("new Job created at action %d, legacy Job deleted at action %d — want the delete first",
			createdAt, deletedAt)
	}
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Get(
		context.Background(), legacyName, metav1.GetOptions{},
	); !k8serrors.IsNotFound(err) {
		t.Fatalf("legacy Job still present after processing (err=%v)", err)
	}
	final, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != work.StatusSucceeded {
		t.Fatalf("final Status = %q (LastError %q), want succeeded", final.Status, final.LastError)
	}
}

// TestProcess_LegacyJobNotConfirmedGone_MintsNothing: if the legacy Job is
// not confirmed gone, the attempt must requeue without minting an identity or
// creating a Job. Minting would make the next attempt skip the retirement,
// because it only runs for a row with an empty ExecutionID.
func TestProcess_LegacyJobNotConfirmedGone_MintsNothing(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	seedLegacyInFlightJob(t, store, kube, "legacy-stuck")

	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	// Accept every delete without removing anything, as with a Job whose Pods
	// are still in their termination grace period.
	kube.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})

	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	New(store, kube, "test-worker").process(context.Background(), job)

	if got := realJobCreates(kube); len(got) != 0 {
		t.Fatalf("real Job creates = %v, want none while the legacy Job may still run", got)
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionID != "" {
		t.Fatalf("ExecutionID = %q, want empty — a minted identity would skip the retirement on the next attempt",
			stored.ExecutionID)
	}
	if stored.Status != work.StatusQueued {
		t.Fatalf("Status = %q, want queued for a retry", stored.Status)
	}
}

// staleSnapshotAfterReplacement reproduces a stale retirement. A worker
// leased a legacy row (ExecutionID still empty) and lost its lease. A
// replacement worker reclaimed the row, minted an identity and created its Job
// under the new-scheme name. It returns the stale worker's snapshot and the
// replacement Job's name.
func staleSnapshotAfterReplacement(t *testing.T, store work.Store, kube *fake.Clientset, jobID string) (*work.Job, string) {
	t.Helper()
	ctx := context.Background()
	stale, err := store.LeaseJob(ctx, "stale-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (stale): %v", err)
	}
	if stale.ExecutionID != "" {
		t.Fatalf("setup: stale ExecutionID = %q, want empty", stale.ExecutionID)
	}
	// Stands in for lease expiry: the row becomes leasable by another worker.
	if err := store.FailJob(ctx, jobID, "stale-worker", "lease expired", true); err != nil {
		t.Fatalf("FailJob (requeue): %v", err)
	}
	replacement, err := store.LeaseJob(ctx, "replacement-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (replacement): %v", err)
	}
	executionID, _, err := store.EnsureExecution(ctx, jobID, "replacement-worker")
	if err != nil {
		t.Fatalf("EnsureExecution (replacement): %v", err)
	}
	replacement.ExecutionID = executionID
	spec := buildSmokeJobSpec(replacement)
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Create(ctx, spec, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create replacement Job: %v", err)
	}
	kube.ClearActions()
	return stale, spec.Name
}

// deletedJobs returns the names of the Jobs kube saw deleted.
func deletedJobs(kube *fake.Clientset) []string {
	var names []string
	for _, a := range kube.Actions() {
		if da, ok := a.(k8stesting.DeleteActionImpl); ok {
			names = append(names, da.GetName())
		}
	}
	return names
}

// TestRetireLegacyExecutions_StaleWorkerKeepsReplacementJob: the replacement
// Job carries the same job label as legacy Jobs, but its name is not
// attempt-derived. A stale worker's retirement must leave it running.
func TestRetireLegacyExecutions_StaleWorkerKeepsReplacementJob(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	legacyName := seedLegacyInFlightJob(t, store, kube, "legacy-stale")
	// The replacement worker already retired the legacy Job before minting.
	if err := kube.Tracker().Delete(jobsGVR, smokeNamespace, legacyName); err != nil {
		t.Fatalf("remove legacy Job: %v", err)
	}
	stale, replacementName := staleSnapshotAfterReplacement(t, store, kube, "legacy-stale")

	err := New(store, kube, "stale-worker").retireLegacyExecutions(context.Background(), slog.Default(), stale)
	if err != nil {
		t.Fatalf("retireLegacyExecutions: %v", err)
	}
	if got := deletedJobs(kube); len(got) != 0 {
		t.Fatalf("deleted Jobs = %v, want none — the replacement's live execution was removed", got)
	}
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Get(
		context.Background(), replacementName, metav1.GetOptions{},
	); err != nil {
		t.Fatalf("replacement Job %q gone after stale retirement: %v", replacementName, err)
	}
}

// TestRetireLegacyExecutions_IdentityMintedMeanwhile_DeletesNothing: if the
// stored identity is no longer empty, the stale worker's snapshot is out of
// date and it must stop without deleting even an attempt-named Job.
func TestRetireLegacyExecutions_IdentityMintedMeanwhile_DeletesNothing(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	legacyName := seedLegacyInFlightJob(t, store, kube, "legacy-minted")
	stale, replacementName := staleSnapshotAfterReplacement(t, store, kube, "legacy-minted")

	err := New(store, kube, "stale-worker").retireLegacyExecutions(context.Background(), slog.Default(), stale)
	if !errors.Is(err, errLegacyRetireSuperseded) {
		t.Fatalf("retireLegacyExecutions err = %v, want errLegacyRetireSuperseded", err)
	}
	if got := deletedJobs(kube); len(got) != 0 {
		t.Fatalf("deleted Jobs = %v, want none once another worker owns the identity", got)
	}
	for _, name := range []string{legacyName, replacementName} {
		if _, err := kube.BatchV1().Jobs(smokeNamespace).Get(
			context.Background(), name, metav1.GetOptions{},
		); err != nil {
			t.Fatalf("Job %q gone after superseded retirement: %v", name, err)
		}
	}
}

func TestIsLegacyJobName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"smoke-job-1-3", true},
		{"l5a-job-1-12", true},
		// Execution-identity names.
		{"smoke-job-1-a3-0123abcd", false},
		{"l5a-job-1-a1-ffff", false},
		{"smoke-job-1-", false},
		// Another job ID that shares the prefix.
		{"smoke-job-12-3", false},
		{"trivy-job-1-3", false},
	}
	for _, tc := range cases {
		if got := isLegacyJobName(tc.name, "job-1"); got != tc.want {
			t.Errorf("isLegacyJobName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestIsLegacyJobName_LongJobID covers job IDs longer than the current
// 30-character truncation: a normal 36-character ingress ID, and one longer
// than the legacy 50-character truncation.
func TestIsLegacyJobName_LongJobID(t *testing.T) {
	long := longIngressJobID
	veryLong := "job-" + strings.Repeat("0123456789abcdef", 4) // 68 chars
	current := func(id string) string { return sanitizeDNSLabel(id) }
	legacy := func(id string) string { return sanitizeDNSLabelMax(id, legacySanitizedIDMaxLen) }
	cases := []struct {
		name, jobID string
		want        bool
	}{
		// Legacy names, as those workers built them.
		{"smoke-" + long + "-1", long, true},
		{"l5a-" + long + "-2", long, true},
		{"smoke-" + legacy(veryLong) + "-3", veryLong, true},
		{"l5a-" + legacy(veryLong) + "-3", veryLong, true},
		// The current truncation followed by a bare number is accepted too.
		{"smoke-" + current(long) + "-1", long, true},
		// Execution-identity names never match, in either truncation.
		{"smoke-" + current(long) + "-a1-0123456789abcdef", long, false},
		{"l5a-" + current(long) + "-a2-0123456789abcdef", long, false},
		{"smoke-" + long + "-a1-0123456789abcdef", long, false},
		// Another job ID sharing the 30-character prefix.
		{"smoke-" + current(long) + "x-1", long, false},
	}
	for _, tc := range cases {
		if got := isLegacyJobName(tc.name, tc.jobID); got != tc.want {
			t.Errorf("isLegacyJobName(%q, %q) = %v, want %v", tc.name, tc.jobID, got, tc.want)
		}
	}
}

// TestRetireLegacyExecutions_LongIngressID_RetiresSmokeAndL5a: with a normal
// 36-character ingress ID, retirement deletes both the legacy smoke Job and
// the legacy L5-a Job, whose names use the historical 50-character truncation.
func TestRetireLegacyExecutions_LongIngressID_RetiresSmokeAndL5a(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	smokeName := seedLegacyInFlightJobWithPrefix(t, store, kube, longIngressJobID, "smoke-")
	if !strings.HasPrefix(smokeName, "smoke-"+longIngressJobID+"-") {
		t.Fatalf("setup: legacy name %q does not carry the whole 36-char ID", smokeName)
	}
	l5aName := "l5a-" + strings.TrimPrefix(smokeName, "smoke-")
	l5aJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      l5aName,
		Namespace: smokeNamespace,
		Labels:    map[string]string{"app": "nodevault-l5a", jobIDLabel: longIngressJobID},
	}}
	if _, err := kube.BatchV1().Jobs(smokeNamespace).Create(
		context.Background(), l5aJob, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("seed legacy L5-a Job: %v", err)
	}
	kube.ClearActions()

	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	if err := New(store, kube, "test-worker").retireLegacyExecutions(context.Background(), slog.Default(), job); err != nil {
		t.Fatalf("retireLegacyExecutions: %v", err)
	}
	for _, name := range []string{smokeName, l5aName} {
		if _, err := kube.BatchV1().Jobs(smokeNamespace).Get(
			context.Background(), name, metav1.GetOptions{},
		); !k8serrors.IsNotFound(err) {
			t.Fatalf("legacy Job %q still present after retirement (err=%v)", name, err)
		}
	}
}

var jobsGVR = batchv1.SchemeGroupVersion.WithResource("jobs")

// failRealCreate makes every real (non-dry-run) create of a Job whose name
// starts with prefix fail with createErr. When persist is true the Job is
// stored first, as when the API server writes the object and the response is
// lost or times out.
func failRealCreate(kube *fake.Clientset, prefix string, persist bool, createErr error) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateActionImpl)
		if !ok || len(ca.GetCreateOptions().DryRun) > 0 {
			return false, nil, nil
		}
		j, ok := ca.GetObject().(*batchv1.Job)
		if !ok || !strings.HasPrefix(j.Name, prefix) {
			return false, nil, nil
		}
		if persist {
			if err := kube.Tracker().Create(jobsGVR, j, ca.GetNamespace()); err != nil {
				return true, nil, err
			}
		}
		return true, nil, createErr
	}
}

// completeIfStoredReactor reports a stored Job as Complete and a missing one
// as NotFound, so an observation succeeds only for a Job that really exists.
func completeIfStoredReactor(kube *fake.Clientset) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		obj, err := kube.Tracker().Get(jobsGVR, ga.GetNamespace(), ga.GetName())
		if err != nil {
			return true, nil, err
		}
		j := obj.(*batchv1.Job).DeepCopy()
		j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		return true, j, nil
	}
}

func errCreateTimedOut() error {
	return k8serrors.NewTimeoutError("create timed out; the object may have been persisted", 1)
}

// TestProcess_AmbiguousL4Create_JobPersisted_Adopts: Create returns a
// timeout but the API server did store the Job. The attempt must find that
// Job under its deterministic name and observe it. Releasing the identity
// instead would let the retry mint a new one and run a second Job beside it.
func TestProcess_AmbiguousL4Create_JobPersisted_Adopts(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", failRealCreate(kube, "smoke-", true, errCreateTimedOut()))
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", completeIfStoredReactor(kube))

	created, err := store.CreateJob(context.Background(), newSmokeRunOnlyJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	New(store, kube, "test-worker").process(context.Background(), job)

	final, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != work.StatusSucceeded {
		t.Fatalf("final Status = %q (LastError %q), want succeeded by observing the persisted Job",
			final.Status, final.LastError)
	}
	if got := realJobCreates(kube); len(got) != 1 {
		t.Fatalf("real Job creates = %v, want exactly one", got)
	}
}

// TestProcess_AmbiguousL4Create_NotFound_StaysAdoptable: Create times out and
// the Job cannot be found yet. The write may still land, so the identity must
// stay adoptable and the retry must run under the same identity.
func TestProcess_AmbiguousL4Create_NotFound_StaysAdoptable(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", failRealCreate(kube, "smoke-", false, errCreateTimedOut()))
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())

	job := leaseFresh(t, store, "ambiguous-create")
	New(store, kube, "test-worker").process(context.Background(), job)

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusQueued {
		t.Fatalf("Status = %q (LastError %q), want queued for a retry", stored.Status, stored.LastError)
	}
	if stored.ExecutionID == "" || stored.ExecutionTerminal {
		t.Fatalf("identity %q terminal=%v after a create whose outcome is unknown — want it kept adoptable, "+
			"or a late-persisted Job runs beside the retry's Job under a fresh identity",
			stored.ExecutionID, stored.ExecutionTerminal)
	}
}

// TestProcess_RejectedL4Create_ReleasesIdentity: an Invalid error proves the
// API server stored nothing, so the identity is released as before.
func TestProcess_RejectedL4Create_ReleasesIdentity(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	rejected := k8serrors.NewInvalid(batchv1.SchemeGroupVersion.WithKind("Job").GroupKind(), "smoke", nil)
	kube.PrependReactor("create", "jobs", failRealCreate(kube, "smoke-", false, rejected))
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())

	job := leaseFresh(t, store, "rejected-create")
	New(store, kube, "test-worker").process(context.Background(), job)

	if got := realJobCreates(kube); len(got) != 1 {
		t.Fatalf("real Job creates = %v, want exactly one rejected create", got)
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !stored.ExecutionTerminal {
		t.Fatal("identity left adoptable after a rejected create — the retry would observe a Job that never existed")
	}
}

// runL5aWithFailedCreate runs a smoke_run+profile job whose L5-a Create fails
// with a timeout, optionally after the Job was stored. It returns the stored
// row and the L5-a check records sent to NodeVault.
func runL5aWithFailedCreate(t *testing.T, jobID string, persist bool) (*work.Job, []capturedSubmission) {
	t.Helper()
	stored, l5a, _ := runL5aWithCreateReactor(t, jobID, func(kube *fake.Clientset) k8stesting.ReactionFunc {
		return failRealCreate(kube, "l5a-", persist, errCreateTimedOut())
	})
	return stored, l5a
}

// runL5aWithCreateReactor is runL5aWithFailedCreate with the L5-a create
// reactor supplied by the caller. It also returns the fake clientset.
func runL5aWithCreateReactor(
	t *testing.T, jobID string, createReactor func(*fake.Clientset) k8stesting.ReactionFunc,
) (*work.Job, []capturedSubmission, *fake.Clientset) {
	t.Helper()
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", createReactor(kube))
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", completeIfStoredReactor(kube))

	req := newTestJob()
	req.JobID = jobID
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.ActionProfile}
	if _, err := store.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	New(store, kube, "test-worker").WithVaultClient(vc).process(context.Background(), job)

	stored, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	var l5a []capturedSubmission
	for _, c := range captured {
		if c.decodeCheck(t).Stage == vaultclient.StageL5A {
			l5a = append(l5a, c)
		}
	}
	return stored, l5a, kube
}

// l5aCreates returns the names of the real L5-a Job creates kube saw.
func l5aCreates(kube *fake.Clientset) []string {
	var names []string
	for _, n := range realJobCreates(kube) {
		if strings.HasPrefix(n, "l5a-") {
			names = append(names, n)
		}
	}
	return names
}

// TestRunL5a_AmbiguousCreate_JobPersisted_Adopts is the L5-a counterpart of
// the L4 case: the persisted L5-a Job is observed to completion, not reported
// as a creation failure with its identity released while it runs.
func TestRunL5a_AmbiguousCreate_JobPersisted_Adopts(t *testing.T) {
	stored, l5a := runL5aWithFailedCreate(t, "l5a-ambiguous-persisted", true)
	if len(l5a) != 1 {
		t.Fatalf("L5-a check records = %d, want 1", len(l5a))
	}
	if got := l5a[0].decodeCheck(t).ValidationStatus; got != "succeeded" {
		t.Fatalf("L5-a ValidationStatus = %q, want succeeded from observing the persisted Job", got)
	}
	if !stored.ExecutionTerminal {
		t.Error("identity should be released once the adopted L5-a Job reached a terminal condition")
	}
}

// TestRunL5a_AmbiguousCreate_OutcomeStaysUnknown: every L5-a Create, the
// first and each same-name reconcile, times out without storing the Job.
// Nothing re-observes L5-a after the job completes, so the identity is left
// non-terminal (a late Job may still land) and the record must say the
// outcome is unknown rather than that creation failed.
func TestRunL5a_AmbiguousCreate_OutcomeStaysUnknown(t *testing.T) {
	stored, l5a, kube := runL5aWithCreateReactor(t, "l5a-ambiguous-missing", func(kube *fake.Clientset) k8stesting.ReactionFunc {
		return failRealCreate(kube, "l5a-", false, errCreateTimedOut())
	})
	creates := l5aCreates(kube)
	if len(creates) != 1+l5aCreateReconcileAttempts {
		t.Fatalf("L5-a creates = %v, want the first create plus %d reconcile creates", creates, l5aCreateReconcileAttempts)
	}
	for _, n := range creates {
		if n != creates[0] {
			t.Fatalf("L5-a creates = %v, want every reconcile under the same name", creates)
		}
	}
	if stored.ExecutionTerminal {
		t.Fatal("identity released after an L5-a create whose outcome is unknown")
	}
	if len(l5a) != 1 {
		t.Fatalf("L5-a check records = %d, want 1", len(l5a))
	}
	if reason := l5a[0].decodeCheck(t).FailureReason; !strings.Contains(reason, "outcome unknown") {
		t.Fatalf("L5-a FailureReason = %q, want it to say the create outcome is unknown", reason)
	}
}

// TestRunL5a_AmbiguousCreate_ReconcileCreates: the first L5-a Create times
// out without storing the Job and the same-name reconcile Create succeeds.
// L5-a is then observed to completion and the identity released, with one
// Job in the cluster.
func TestRunL5a_AmbiguousCreate_ReconcileCreates(t *testing.T) {
	stored, l5a, kube := runL5aWithCreateReactor(t, "l5a-ambiguous-reconciled", func(kube *fake.Clientset) k8stesting.ReactionFunc {
		failFirst := failRealCreate(kube, "l5a-", false, errCreateTimedOut())
		var failedOnce atomic.Bool
		return func(action k8stesting.Action) (bool, runtime.Object, error) {
			if failedOnce.Load() {
				return false, nil, nil
			}
			handled, obj, err := failFirst(action)
			if handled {
				failedOnce.Store(true)
			}
			return handled, obj, err
		}
	})
	if creates := l5aCreates(kube); len(creates) != 2 || creates[0] != creates[1] {
		t.Fatalf("L5-a creates = %v, want the timed-out create and one reconcile under the same name", creates)
	}
	if len(l5a) != 1 {
		t.Fatalf("L5-a check records = %d, want 1", len(l5a))
	}
	if got := l5a[0].decodeCheck(t).ValidationStatus; got != "succeeded" {
		t.Fatalf("L5-a ValidationStatus = %q, want succeeded from observing the reconciled Job", got)
	}
	if !stored.ExecutionTerminal {
		t.Error("identity should be released once the reconciled L5-a Job reached a terminal condition")
	}
}

// TestMarkExecutionTerminal_StaleWorkerDoesNotReleaseReplacement: a worker
// still holding execution E1 reports it terminal after another worker minted
// E2. The report must leave E2 adoptable, or the next attempt mints a third
// execution beside E2's running Job.
func TestMarkExecutionTerminal_StaleWorkerDoesNotReleaseReplacement(t *testing.T) {
	store := newTestStore(t)
	stale := leaseAndMint(t, store, "stale-worker")
	w := New(store, fake.NewClientset(), "test-worker")

	// Another worker observed E1 terminal and minted E2.
	current := *stale
	w.markExecutionTerminal(context.Background(), slog.Default(), &current)
	e2, adopted, err := store.EnsureExecution(context.Background(), stale.JobID, "other-worker")
	if err != nil || adopted || e2 == stale.ExecutionID {
		t.Fatalf("EnsureExecution = %q, adopted=%v, err=%v; want a fresh E2", e2, adopted, err)
	}

	w.markExecutionTerminal(context.Background(), slog.Default(), stale)

	stored, err := store.GetJob(context.Background(), stale.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionID != e2 || stored.ExecutionTerminal {
		t.Fatalf("execution %q terminal=%v after the stale E1 report, want E2 %q non-terminal",
			stored.ExecutionID, stored.ExecutionTerminal, e2)
	}
}

// TestProcess_AmbiguousL4Create_RetryRecreatesSameName is the full sequence:
// attempt 1's Create times out and the Job is not found, so the identity
// stays adoptable. Attempt 2 adopts it, still finds no Job, and must create
// under the same name rather than release the identity. A released identity
// would let attempt 3 mint a new name while attempt 1's Create could still
// land.
func TestProcess_AmbiguousL4Create_RetryRecreatesSameName(t *testing.T) {
	useFastWorkerTicks(t)

	store := newTestStore(t)
	kube := fake.NewClientset()
	var failedOnce atomic.Bool
	failFirst := failRealCreate(kube, "smoke-", false, errCreateTimedOut())
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failedOnce.Load() {
			return false, nil, nil
		}
		handled, obj, err := failFirst(action)
		if handled {
			failedOnce.Store(true)
		}
		return handled, obj, err
	})
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", completeIfStoredReactor(kube))
	w := New(store, kube, "test-worker")

	job1 := leaseFresh(t, store, "ambiguous-then-adopted")
	w.process(context.Background(), job1)
	if job1.ExecutionID == "" {
		t.Fatal("attempt 1 did not establish an execution identity")
	}

	job2, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 2): %v", err)
	}
	w.process(context.Background(), job2)

	if job2.ExecutionID != job1.ExecutionID {
		t.Fatalf("attempt 2 ran under %q, want adopted %q", job2.ExecutionID, job1.ExecutionID)
	}
	want := smokeJobName(job1)
	got := realJobCreates(kube)
	if len(got) != 2 || got[0] != want || got[1] != want {
		t.Fatalf("real Job creates = %v, want two creates of %q — a different name could run beside a late "+
			"attempt-1 Job", got, want)
	}
	final, err := store.GetJob(context.Background(), job1.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != work.StatusSucceeded {
		t.Fatalf("final Status = %q (LastError %q), want succeeded", final.Status, final.LastError)
	}
	if !final.ExecutionTerminal {
		t.Error("identity should be released once the re-created Job reached a terminal condition")
	}
}

// TestRunSmokeRun_AdoptedNotFound_RecreatesUnderSameName: an adopted Job
// that was never seen is created under the same name and observed. The
// identity is not released on that first NotFound.
func TestRunSmokeRun_AdoptedNotFound_RecreatesUnderSameName(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	kube.PrependReactor("get", "jobs", completeIfStoredReactor(kube))
	store := newTestStore(t)
	job := leaseAndMint(t, store, "adopted-missing")
	spec := buildSmokeJobSpec(job)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := New(store, kube, "test-worker").runSmokeRun(ctx, slog.Default(), smokeNamespace, job, spec, true, true)
	if !result.success {
		t.Fatalf("want success from observing the re-created Job, got %+v", result)
	}
	if got := realJobCreates(kube); len(got) != 1 || got[0] != spec.Name {
		t.Fatalf("real Job creates = %v, want exactly one create of %q", got, spec.Name)
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !stored.ExecutionTerminal {
		t.Error("identity should be released once the re-created Job completed")
	}
}

// TestRunSmokeRun_AdoptedNotFound_OriginalCreateLandsFirst: the minting
// attempt's delayed Create lands just before the re-create. The re-create
// gets AlreadyExists and adopts that Job, so exactly one Job exists.
func TestRunSmokeRun_AdoptedNotFound_OriginalCreateLandsFirst(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateActionImpl)
		if !ok || len(ca.GetCreateOptions().DryRun) > 0 {
			return false, nil, nil
		}
		j, ok := ca.GetObject().(*batchv1.Job)
		if !ok {
			return false, nil, nil
		}
		if err := kube.Tracker().Create(jobsGVR, j.DeepCopy(), ca.GetNamespace()); err != nil {
			t.Errorf("seed the late original Job: %v", err)
		}
		return false, nil, nil // the default reactor now answers AlreadyExists
	})
	kube.PrependReactor("get", "jobs", completeIfStoredReactor(kube))
	store := newTestStore(t)
	job := leaseAndMint(t, store, "adopted-race")
	spec := buildSmokeJobSpec(job)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := New(store, kube, "test-worker").runSmokeRun(ctx, slog.Default(), smokeNamespace, job, spec, true, true)
	if !result.success {
		t.Fatalf("want success from adopting the original Job, got %+v", result)
	}
	list, err := kube.Tracker().List(jobsGVR, batchv1.SchemeGroupVersion.WithKind("Job"), smokeNamespace)
	if err != nil {
		t.Fatalf("list Jobs: %v", err)
	}
	if jobs := list.(*batchv1.JobList).Items; len(jobs) > 1 {
		t.Fatalf("Jobs in cluster = %d, want at most one under %q", len(jobs), spec.Name)
	}
}

// TestRunSmokeRun_AdoptedNotFound_AmbiguousRecreate_StaysAdoptable: the
// re-create is itself ambiguous and the Job is still not found, so the
// identity stays adoptable.
func TestRunSmokeRun_AdoptedNotFound_AmbiguousRecreate_StaysAdoptable(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", failRealCreate(kube, "smoke-", false, errCreateTimedOut()))
	kube.PrependReactor("get", "jobs", completeIfStoredReactor(kube))
	store := newTestStore(t)
	job := leaseAndMint(t, store, "adopted-ambiguous")
	spec := buildSmokeJobSpec(job)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := New(store, kube, "test-worker").runSmokeRun(ctx, slog.Default(), smokeNamespace, job, spec, true, true)
	if result.success || !result.retryable {
		t.Fatalf("want a retryable failure, got %+v", result)
	}
	if got := realJobCreates(kube); len(got) != 1 || got[0] != spec.Name {
		t.Fatalf("real Job creates = %v, want exactly one create of %q", got, spec.Name)
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionTerminal {
		t.Fatal("identity released after an ambiguous re-create — a late Job could run beside a fresh identity's Job")
	}
}

// TestRunSmokeRun_AdoptedNotFound_RejectedRecreate_StaysAdoptable: a
// rejection of the re-create says nothing about the minting attempt's own
// Create, so unlike a first Create's rejection it does not release the
// identity.
func TestRunSmokeRun_AdoptedNotFound_RejectedRecreate_StaysAdoptable(t *testing.T) {
	useFastWorkerTicks(t)

	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", failRealCreate(kube, "smoke-", false,
		k8serrors.NewTooManyRequests("slow down", 1)))
	kube.PrependReactor("get", "jobs", completeIfStoredReactor(kube))
	store := newTestStore(t)
	job := leaseAndMint(t, store, "adopted-rejected")
	spec := buildSmokeJobSpec(job)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := New(store, kube, "test-worker").runSmokeRun(ctx, slog.Default(), smokeNamespace, job, spec, true, true)
	if result.success || !result.retryable {
		t.Fatalf("want a retryable failure, got %+v", result)
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ExecutionTerminal {
		t.Fatal("identity released after a rejected re-create — the original Create may still land")
	}
}

func leaseAndMint(t *testing.T, store work.Store, jobID string) *work.Job {
	t.Helper()
	job := leaseFresh(t, store, jobID)
	mintExecution(t, store, job)
	return job
}

// leaseFresh creates and leases a smoke_run-only row without minting an
// identity, so process mints one and performs a real Create.
func leaseFresh(t *testing.T, store work.Store, jobID string) *work.Job {
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
	return job
}
