package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

func useFastWorkerTicks(t *testing.T) {
	t.Helper()

	originalPoll := pollFrequency
	originalHeartbeat := heartbeatFrequency
	originalSmokeRun := smokeRunDuration
	pollFrequency = time.Millisecond
	heartbeatFrequency = time.Millisecond
	smokeRunDuration = 100 * time.Millisecond
	t.Cleanup(func() {
		pollFrequency = originalPoll
		heartbeatFrequency = originalHeartbeat
		smokeRunDuration = originalSmokeRun
	})
}

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
func TestProcess_L4Success_L5Skipped(t *testing.T) {
	useFastWorkerTicks(t)

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
func TestProcess_L4Success_WithVault_L5Submitted(t *testing.T) {
	useFastWorkerTicks(t)

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

// newSmokeRunOnlyJob returns a JobRequest identical to newTestJob() except
// RequestedActions only asks for smoke_run — the shape NodeVault's
// EnqueueValidationWork call actually sends today (see
// NodeVault/pkg/build/service.go). Used to test that process() gates L5-a/
// L5-b on requested_actions instead of always running them (issue #5).
func newSmokeRunOnlyJob() work.JobRequest {
	req := newTestJob()
	req.RequestedActions = []work.Action{work.ActionSmokeRun}
	return req
}

// TestProcess_SmokeRunOnly_L5aL5bSkipped is a regression guard for issue #5's
// two findings: (1) when only smoke_run is requested (today's real NodeVault
// behavior), L5-a and L5-b must not run — no L5-a ToolCheckRecord or L5-b
// ToolScanRecord should be submitted for stages nobody asked for; and (2) a
// job whose plan never reaches L5-b (previously the only stage that ever
// submitted a Terminal record) must still produce exactly one Terminal
// success record — via L4, since it's the last stage this plan runs — so
// NodeVault's ValidationRequestRecord doesn't stay stuck at Queued/Running
// forever. Before this fix, this scenario produced zero NodeVault
// submissions at all.
func TestProcess_SmokeRunOnly_L5aL5bSkipped(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	w := New(store, kube, "test-worker").WithVaultClient(vc)

	created, err := store.CreateJob(context.Background(), newSmokeRunOnlyJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w.process(context.Background(), job)

	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want exactly 1 (the L4 terminal success record; "+
			"no L5-a/L5-b records — only smoke_run was requested)", len(captured))
	}
	if !strings.Contains(captured[0].path, "check-records") {
		t.Fatalf("expected the one submission to be a check record (L4), got path %q", captured[0].path)
	}
	got := captured[0].decodeCheck(t)
	if got.Stage != vaultclient.StageL4 {
		t.Errorf("Stage = %q, want L4", got.Stage)
	}
	if !got.Terminal {
		t.Error("Terminal = false, want true — L4 is the last (only) stage this plan runs")
	}
	if got.ValidationStatus != "succeeded" {
		t.Errorf("ValidationStatus = %q, want succeeded", got.ValidationStatus)
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusSucceeded {
		t.Errorf("expected status=succeeded, got %q", stored.Status)
	}
	if !strings.Contains(stored.ResultSummary, "L5-a skipped (not requested)") {
		t.Errorf("ResultSummary should note L5-a was skipped, got %q", stored.ResultSummary)
	}
	if !strings.Contains(stored.ResultSummary, "L5-b skipped (not requested)") {
		t.Errorf("ResultSummary should note L5-b was skipped, got %q", stored.ResultSummary)
	}
}

// TestProcess_ProfileOnlyRequested_L5aRunsL5bSkipped verifies partial
// selection: requesting profile without security_scan runs L5-a but not
// L5-b.
func TestProcess_ProfileOnlyRequested_L5aRunsL5bSkipped(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	w := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newTestJob()
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.ActionProfile}
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w.process(context.Background(), job)

	var sawL5A, sawL5B bool
	var l5aTerminal bool
	for _, c := range captured {
		if strings.Contains(c.path, "check-records") && c.decodeCheck(t).Stage == vaultclient.StageL5A {
			sawL5A = true
			l5aTerminal = c.decodeCheck(t).Terminal
		}
		if strings.Contains(c.path, "scan-records") {
			sawL5B = true
		}
	}
	if !sawL5A {
		t.Error("expected an L5-a check record (profile was requested)")
	}
	if sawL5B {
		t.Error("expected no L5-b scan record (security_scan was not requested)")
	}
	if !l5aTerminal {
		t.Error("expected the L5-a record to be Terminal — L5-a is the last stage this plan runs " +
			"(security_scan was not requested)")
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !strings.Contains(stored.ResultSummary, "L5-b skipped (not requested)") {
		t.Errorf("ResultSummary should note L5-b was skipped, got %q", stored.ResultSummary)
	}
}

// TestProcess_EmptyRequestedActions_RunsAllStagesBackwardCompat verifies the
// safe default: a job with no RequestedActions at all (old NodeVault
// versions that never populate the field, or pre-existing stored rows from
// before EnqueueValidationWork required it non-empty) still runs L5-a and
// L5-b, matching pre-issue-#5 unconditional-execution behavior — and still
// produces exactly one Terminal success record, via L5-b since it's the
// last stage the full pipeline runs (regression test 2/5 for the Terminal
// lifecycle redesign).
func TestProcess_EmptyRequestedActions_RunsAllStagesBackwardCompat(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	w := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newTestJob()
	req.RequestedActions = nil
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w.process(context.Background(), job)

	var sawL5A, sawL5B, sawL5BTerminal bool
	for _, c := range captured {
		if strings.Contains(c.path, "check-records") && c.decodeCheck(t).Stage == vaultclient.StageL5A {
			sawL5A = true
		}
		if strings.Contains(c.path, "scan-records") {
			sawL5B = true
			if c.decodeScan(t).Terminal {
				sawL5BTerminal = true
			}
		}
	}
	if !sawL5A {
		t.Error("expected an L5-a check record when RequestedActions is empty (backward-compat default)")
	}
	if !sawL5B {
		t.Error("expected an L5-b scan record when RequestedActions is empty (backward-compat default)")
	}
	if !sawL5BTerminal {
		t.Error("expected the L5-b scan record to be Terminal — it's the last stage the full pipeline runs")
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusSucceeded {
		t.Errorf("expected status=succeeded, got %q", stored.Status)
	}
	if !stored.TerminalSubmitted {
		t.Error("expected TerminalSubmitted=true after a successful full-pipeline run")
	}
}

// TestProcess_ProfileOnlyRequested_L5aFails_TerminalFailedRecord is
// regression test 3/5 for the Terminal lifecycle redesign: when profile is
// requested but security_scan is not, L5-a is the last stage the plan runs
// (see process()'s isL5ALast) — so if L5-a itself fails at the application
// level, that failure must be reported to NodeVault as this job's one
// Terminal record (Terminal=true, ValidationStatus="failed"), not silently
// swallowed as a non-terminal best-effort record with nothing closing out
// the ValidationRequestRecord.
func TestProcess_ProfileOnlyRequested_L5aFails_TerminalFailedRecord(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		if strings.HasPrefix(ga.GetName(), "l5a-") {
			return true, fakeJobWithCondition(smokeNamespace, ga.GetName(), batchv1.JobFailed, corev1.ConditionTrue), nil
		}
		// The L4 smoke-run Job (name prefix "smoke-") completes normally.
		return true, fakeJobWithCondition(smokeNamespace, ga.GetName(), batchv1.JobComplete, corev1.ConditionTrue), nil
	})
	w := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newTestJob()
	// Non-empty JobID: extractPodExitCode's K8s label selector rejects
	// "job-name=l5a-" (trailing '-') built from an empty JobID — see
	// TestProcess_L5aFails_L5bStillRunsAndSubmitsTerminal in pr2b_test.go.
	req.JobID = "profile-only-l5a-fail-job"
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.ActionProfile}
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	// waitL5aJob now classifies a Failed L5-a Job by inspecting its pod(s)
	// (see classifyFromPods, and l5a.go's doc comment on why L5-a was
	// unified onto L4's pod-inspection classifier instead of its own
	// Job-condition-reason-only isInfraReason) rather than trusting the
	// Job-level condition Reason alone. Seed a pod whose container exited
	// non-zero for a non-infra reason, so this test continues to exercise
	// (and assert) a genuine application-level L5-a failure — matching
	// TestClassifySmokeRun_ApplicationFailure/TestProcess_L4NonRetryableFailure_SubmitsTerminalTrueAndClaimsSlot's
	// fixture shape for L4's equivalent case.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "l5a-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": l5aJobName(job)},
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

	w.process(context.Background(), job)

	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want exactly 1 (the L5-a terminal failure record)", len(captured))
	}
	got := captured[0].decodeCheck(t)
	if got.Stage != vaultclient.StageL5A {
		t.Errorf("Stage = %q, want L5A", got.Stage)
	}
	if !got.Terminal {
		t.Error("Terminal = false, want true — L5-a is the last requested stage and it failed")
	}
	if got.ValidationStatus != "failed" {
		t.Errorf("ValidationStatus = %q, want failed", got.ValidationStatus)
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// L5-a/L5-b are best-effort at the WorkStore level — the local job
	// Status still succeeds as long as L3/L4 passed (see
	// TestProcess_L5aFails_L5bStillRunsAndSubmitsTerminal in pr2b_test.go).
	// What changed is that NodeVault now also gets told, via the Terminal
	// record asserted above.
	if stored.Status != work.StatusSucceeded {
		t.Errorf("job Status = %q, want succeeded", stored.Status)
	}
	if !stored.TerminalSubmitted {
		t.Error("expected TerminalSubmitted=true")
	}
}

// TestProcess_UnknownRequestedAction_FailsExplicitly is regression test 4/5
// for the Terminal lifecycle redesign: an unrecognized requested_actions
// value must not be silently ignored (which would either skip a stage
// NodeVault expects, or quietly run stages nobody asked for) — process()
// must fail the job explicitly, without ever touching K8s, and still report
// a Terminal failure so NodeVault's ValidationRequestRecord isn't left
// stuck.
func TestProcess_UnknownRequestedAction_FailsExplicitly(t *testing.T) {
	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	// No K8s reactors configured — if process() ever calls into the fake
	// kube client for this job, that itself would be a bug (validation must
	// happen before L3/L4 ever run).
	kube := fake.NewClientset()
	w := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newTestJob()
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.Action("unknown_stage")}
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
	if stored.Status != work.StatusFailed {
		t.Errorf("expected status=failed (non-retryable — retrying the same requested_actions can't help), got %q", stored.Status)
	}
	if !strings.Contains(stored.LastError, "unknown_stage") {
		t.Errorf("LastError should mention the unrecognized action, got %q", stored.LastError)
	}

	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want 1 (a Terminal failed record so NodeVault isn't left stuck)", len(captured))
	}
	got := captured[0].decodeCheck(t)
	if !got.Terminal {
		t.Error("Terminal = false, want true")
	}
	if got.ValidationStatus != "failed" {
		t.Errorf("ValidationStatus = %q, want failed", got.ValidationStatus)
	}
}

// TestSubmitCheckRecord_DuplicateTerminalSubmission_Suppressed is regression
// test 5/5 for the Terminal lifecycle redesign: submitting a second
// Terminal=true record for a job that already has one must be a silent
// no-op (no error, no second NodeVault submission) — see
// Worker.claimTerminal / Store.ClaimTerminal.
func TestSubmitCheckRecord_DuplicateTerminalSubmission_Suppressed(t *testing.T) {
	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(vc)

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	sub := checkRecordSubmission{
		checkID: "term-" + job.JobID, stage: vaultclient.StageL4, terminal: true,
		validationStatus: "succeeded",
	}
	if err := w.submitCheckRecord(context.Background(), slog.Default(), job, sub); err != nil {
		t.Fatalf("first submitCheckRecord: %v", err)
	}
	if err := w.submitCheckRecord(context.Background(), slog.Default(), job, sub); err != nil {
		t.Fatalf("second (duplicate) submitCheckRecord should not error, got: %v", err)
	}

	if len(captured) != 1 {
		t.Errorf("captured submissions = %d, want 1 — the second terminal submission for the same "+
			"job must be suppressed", len(captured))
	}

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !stored.TerminalSubmitted {
		t.Error("expected TerminalSubmitted=true after the first terminal submission")
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
	useFastWorkerTicks(t)

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
	useFastWorkerTicks(t)

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

// ── Retryable-Terminal suppression bug (independent review finding) ────────
//
// reportTerminalFailure used to submit Terminal=true for every L3/L4
// failure regardless of retryable, which also made submitCheckRecord claim
// the job's one-time terminal-submission slot (see claimTerminal /
// work.Store.ClaimTerminal). But a retryable failure does not mean the
// validation request is done — FailJob(..., retryable=true) sends the same
// job back to Queued so it can be leased and attempted again. If that retry
// later succeeds, reportTerminalSuccess's own terminal submission would
// find the slot already claimed by the earlier (non-final) failure and
// silently suppress the real Terminal=true success record — NodeVault would
// be left with a stale Failed ValidationRequestRecord forever, with no
// error and no way to tell anything went wrong.
//
// The two tests below are the direct end-to-end regression coverage for the
// fix: reportTerminalFailure now only submits Terminal=true (and only then
// claims the slot) when the failure is NOT retryable.

// TestProcess_RetryableL3Failure_ThenSuccess_TerminalSuccessNotSuppressed
// reproduces the exact suppression scenario: attempt 1 fails L3's dry-run
// (always retryable — see process()), attempt 2 (the same job, requeued and
// leased again) succeeds end to end. The success's Terminal record must
// actually reach NodeVault, not be silently dropped by a stale claim from
// attempt 1.
func TestProcess_RetryableL3Failure_ThenSuccess_TerminalSuccessNotSuppressed(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()

	var dryRunShouldFail atomic.Bool
	dryRunShouldFail.Store(true)
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateActionImpl)
		if !ok {
			return false, nil, nil
		}
		if len(ca.GetCreateOptions().DryRun) > 0 {
			if dryRunShouldFail.Load() {
				return true, nil, k8serrors.NewInternalError(errTestAdmissionRejected)
			}
			// Absorb the dry-run create (see absorbDryRunReactor) so the
			// later real create in the same attempt doesn't collide with it
			// in the fake tracker.
			return true, ca.GetObject(), nil
		}
		return false, nil, nil // fall through to the default tracker for the real create
	})
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	w := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newSmokeRunOnlyJob()
	req.JobID = "retry-then-success-job"
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Attempt 1: L3 dry-run fails (retryable).
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 1): %v", err)
	}
	w.process(context.Background(), job)

	if len(captured) != 1 {
		t.Fatalf("after attempt 1: submissions captured = %d, want 1", len(captured))
	}
	firstRecord := captured[0].decodeCheck(t)
	if firstRecord.Terminal {
		t.Fatal("attempt 1's L3 failure record has Terminal=true, want false — a retryable failure must not " +
			"close out the request (see reportTerminalFailure's doc comment)")
	}

	afterAttempt1, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob after attempt 1: %v", err)
	}
	if afterAttempt1.Status != work.StatusQueued {
		t.Fatalf("Status after attempt 1 = %q, want queued (retryable failure requeues the job)", afterAttempt1.Status)
	}
	if afterAttempt1.TerminalSubmitted {
		t.Fatal("TerminalSubmitted=true after only a retryable failure — the terminal slot must stay " +
			"unclaimed so a later successful retry can still submit the real Terminal record")
	}

	// Attempt 2: the same job is leased again and now succeeds end to end.
	dryRunShouldFail.Store(false)
	job2, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob (attempt 2): %v", err)
	}
	if job2.JobID != created.JobID {
		t.Fatalf("attempt 2 leased a different job: got %q, want %q", job2.JobID, created.JobID)
	}
	w.process(context.Background(), job2)

	if len(captured) != 2 {
		t.Fatalf("after attempt 2: submissions captured = %d, want 2 (attempt 1's non-terminal failure "+
			"record + attempt 2's terminal success record)", len(captured))
	}
	secondRecord := captured[1].decodeCheck(t)
	if !secondRecord.Terminal {
		t.Fatal("attempt 2's success record has Terminal=false, want true — this is the exact " +
			"suppression bug: a retried job's real success must not be swallowed by an earlier retryable " +
			"failure's stale terminal claim")
	}
	if secondRecord.ValidationStatus != "succeeded" {
		t.Errorf("ValidationStatus = %q, want succeeded", secondRecord.ValidationStatus)
	}

	final, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob after attempt 2: %v", err)
	}
	if final.Status != work.StatusSucceeded {
		t.Errorf("final Status = %q, want succeeded", final.Status)
	}
	if !final.TerminalSubmitted {
		t.Error("TerminalSubmitted=true expected after the success Terminal record was actually submitted")
	}
}

// TestProcess_L4NonRetryableFailure_SubmitsTerminalTrueAndClaimsSlot is the
// complementary case: a failure that will never be retried again (here, an
// L4 application-level failure — exit code != 0, classified
// retryable=false by classifySmokeRun) must still close out the
// ValidationRequestRecord — Terminal=true, and the terminal slot claimed —
// exactly as before this fix. NodeSentinel currently has no bounded
// retry-count/exhaustion mechanism (work.Job.Attempt is tracked but never
// checked against a cap, and FailJob(retryable=true) requeues
// unconditionally — see pkg/work/sqlite/store.go's FailJob/LeaseJob), so
// "retries exhausted" cannot currently be distinguished from "still
// retryable" other than via this retryable/non-retryable classification
// itself; a non-retryable classification is this codebase's only existing
// notion of "this job's last attempt".
func TestProcess_L4NonRetryableFailure_SubmitsTerminalTrueAndClaimsSlot(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ga := action.(k8stesting.GetActionImpl)
		return true, fakeJobWithCondition(smokeNamespace, ga.GetName(), batchv1.JobFailed, corev1.ConditionTrue), nil
	})

	req := newSmokeRunOnlyJob()
	req.JobID = "l4-app-fail-job"
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	// Seed a pod whose container exited non-zero for a non-infra reason, so
	// classifyFromPods returns retryable=false (application-level failure)
	// — see TestClassifySmokeRun_ApplicationFailure for the same fixture
	// shape at the classifier unit level.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "smoke-pod",
			Namespace: smokeNamespace,
			Labels:    map[string]string{"job-name": smokeJobName(job)},
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

	w := New(store, kube, "test-worker").WithVaultClient(vc)
	w.process(context.Background(), job)

	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want 1", len(captured))
	}
	got := captured[0].decodeCheck(t)
	if got.Stage != vaultclient.StageL4 {
		t.Errorf("Stage = %q, want L4", got.Stage)
	}
	if !got.Terminal {
		t.Error("Terminal = false, want true — a non-retryable failure is this job's final outcome")
	}
	if got.ValidationStatus != "failed" {
		t.Errorf("ValidationStatus = %q, want failed", got.ValidationStatus)
	}
	if got.Retryable {
		t.Error("Retryable = true, want false")
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusFailed {
		t.Errorf("Status = %q, want failed (non-retryable failure is permanent)", stored.Status)
	}
	if !stored.TerminalSubmitted {
		t.Error("TerminalSubmitted=true expected — a non-retryable failure must claim the terminal slot")
	}
}
