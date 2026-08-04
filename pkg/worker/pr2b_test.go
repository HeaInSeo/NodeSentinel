package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// capturingVaultServer starts an httptest server that decodes every POST
// body as JSON into a fresh map and appends it (keyed by path) to captured,
// always responding 200. Returns a vaultclient.Client pointed at it.
func capturingVaultServer(t *testing.T, captured *[]capturedSubmission) *vaultclient.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		*captured = append(*captured, capturedSubmission{path: r.URL.Path, body: body})
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(vaultclient.SubmitResponse{RecordID: "r1"})
	}))
	t.Cleanup(srv.Close)
	return vaultclient.NewWithAddr(srv.URL)
}

type capturedSubmission struct {
	path string
	body []byte
}

func (c capturedSubmission) decodeCheck(t *testing.T) vaultclient.SubmitCheckRecordRequest {
	t.Helper()
	var req vaultclient.SubmitCheckRecordRequest
	if err := json.Unmarshal(c.body, &req); err != nil {
		t.Fatalf("decode check record: %v", err)
	}
	return req
}

func (c capturedSubmission) decodeScan(t *testing.T) vaultclient.SubmitScanRecordRequest {
	t.Helper()
	var req vaultclient.SubmitScanRecordRequest
	if err := json.Unmarshal(c.body, &req); err != nil {
		t.Fatalf("decode scan record: %v", err)
	}
	return req
}

// ── reportTerminalFailure: failure_kind mapping (L3/L4 -> NodeVault) ───────────

func TestReportTerminalFailure_InfrastructureAndApplication(t *testing.T) {
	tests := []struct {
		name                 string
		stage                string
		class                FailureClass
		retryable            bool
		wantFailureKind      string
		wantValidationStatus string
	}{
		{"L3 is always infrastructure", vaultclient.StageL3, FailureClassTransientInfra, true, vaultclient.FailureKindInfrastructure, "infra_failed"},
		{"L4 infra-level (retryable)", vaultclient.StageL4, FailureClassTransientInfra, true, vaultclient.FailureKindInfrastructure, "infra_failed"},
		{"L4 application-level (not retryable)", vaultclient.StageL4, FailureClassDeterministic, false, vaultclient.FailureKindApplication, "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured []capturedSubmission
			vc := capturingVaultServer(t, &captured)
			store := newTestStore(t)
			w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(vc)

			req := newTestJob()
			job, err := store.CreateJob(context.Background(), req)
			if err != nil {
				t.Fatalf("CreateJob: %v", err)
			}

			decision := RetryDecision{Class: tt.class, Retry: tt.retryable, Reason: "boom"}
			w.reportTerminalFailure(context.Background(), slog.Default(), job, tt.stage, "cmd", decision)

			if len(captured) != 1 {
				t.Fatalf("captured submissions = %d, want 1", len(captured))
			}
			got := captured[0].decodeCheck(t)
			if got.Stage != tt.stage {
				t.Errorf("Stage = %q, want %q", got.Stage, tt.stage)
			}
			// A retryable failure is only a pause, not the end of the
			// request — the job is requeued (see FailJob(..., retryable))
			// and may yet succeed, at which point reportTerminalSuccess must
			// still be able to submit the real Terminal record. So Terminal
			// is only true when this failure is NOT retryable. See
			// reportTerminalFailure's doc comment for the bug this guards
			// against (a retryable failure permanently claiming the
			// terminal-submission slot and suppressing a later success).
			wantTerminal := !tt.retryable
			if got.Terminal != wantTerminal {
				t.Errorf("Terminal = %v, want %v (retryable=%v)", got.Terminal, wantTerminal, tt.retryable)
			}
			if got.FailureKind != tt.wantFailureKind {
				t.Errorf("FailureKind = %q, want %q", got.FailureKind, tt.wantFailureKind)
			}
			if got.ValidationStatus != tt.wantValidationStatus {
				t.Errorf("ValidationStatus = %q, want %q", got.ValidationStatus, tt.wantValidationStatus)
			}
			if got.Retryable != tt.retryable {
				t.Errorf("Retryable = %v, want %v", got.Retryable, tt.retryable)
			}
			if got.ValidationRequestID != job.ValidationRequestID {
				t.Errorf("ValidationRequestID = %q, want %q", got.ValidationRequestID, job.ValidationRequestID)
			}
			if got.SentinelJobID != job.JobID {
				t.Errorf("SentinelJobID = %q, want %q", got.SentinelJobID, job.JobID)
			}
		})
	}
}

// TestProcess_L3DryRunFails_SubmitsTerminalCheckRecordToNodeVault is the
// end-to-end guard: before PR2-B, an L3 rejection never reached NodeVault at
// all, so its ValidationRequestRecord would stay stuck at Queued forever.
// L3 failures are always retryable (see process()), so the record this test
// asserts on is non-terminal (Terminal=false): it still tells NodeVault the
// request is progressing (promoting Queued->Running per
// SubmitCheckRecordRequest.Terminal's doc comment) without prematurely
// closing out a request that will be retried — see
// reportTerminalFailure's doc comment for why a retryable failure must not
// claim the one-time terminal-submission slot.
func TestProcess_L3DryRunFails_SubmitsTerminalCheckRecordToNodeVault(t *testing.T) {
	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateActionImpl)
		if len(ca.GetCreateOptions().DryRun) > 0 {
			return true, nil, k8serrors.NewInternalError(errTestAdmissionRejected)
		}
		return false, nil, nil
	})
	w := New(store, kube, "test-worker").WithVaultClient(vc)

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

	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want exactly 1 (the L3 terminal failure record)", len(captured))
	}
	got := captured[0].decodeCheck(t)
	if got.Stage != vaultclient.StageL3 {
		t.Errorf("Stage = %q, want L3", got.Stage)
	}
	if got.Terminal {
		t.Error("Terminal = true, want false — L3 failures are always retryable, so this record must not " +
			"close out the ValidationRequestRecord (the job is requeued and may yet succeed)")
	}
	if got.ValidationRequestID != created.ValidationRequestID {
		t.Errorf("ValidationRequestID = %q, want %q", got.ValidationRequestID, created.ValidationRequestID)
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.TerminalSubmitted {
		t.Error("TerminalSubmitted = true, want false — a retryable failure must not claim the " +
			"terminal-submission slot (see reportTerminalFailure's doc comment)")
	}
	if stored.Status != work.StatusQueued {
		t.Errorf("Status = %q, want queued (retryable L3 failure requeues the job)", stored.Status)
	}
}

// ── L5-a's Terminal flag is caller-supplied (true only when it's the last
// planned stage — see process()'s isL5ALast) ────────────────────────────────

// TestRunL5a_Success_NotTerminal_WhenL5bAlsoPlanned verifies that runL5a
// respects a terminal=false argument (the case where L5-b also runs
// afterward and owns the Terminal record instead).
func TestRunL5a_Success_NotTerminal_WhenL5bAlsoPlanned(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))
	w := New(store, kube, "test-worker").WithVaultClient(vc)

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if err := w.runL5a(context.Background(), slog.Default(), job, false); err != nil {
		t.Fatalf("runL5a: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want 1", len(captured))
	}
	got := captured[0].decodeCheck(t)
	if got.Stage != vaultclient.StageL5A {
		t.Errorf("Stage = %q, want L5A", got.Stage)
	}
	if got.Terminal {
		t.Error("Terminal = true, want false — terminal=false was passed in (L5-b still runs after L5-a)")
	}
	if got.ValidationHash != "" {
		t.Errorf("ValidationHash = %q, want empty without output/resource observation", got.ValidationHash)
	}
	if got.AllOutputsPresent {
		t.Error("AllOutputsPresent = true, want false/omitted without output observation")
	}
}

// TestRunL5a_Success_Terminal_WhenL5aIsLastStage verifies that runL5a honors
// terminal=true (the case where profile was requested but security_scan was
// not, so L5-a is the last stage the plan runs — see process()'s
// isL5ALast).
func TestRunL5a_Success_Terminal_WhenL5aIsLastStage(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))
	w := New(store, kube, "test-worker").WithVaultClient(vc)

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if err := w.runL5a(context.Background(), slog.Default(), job, true); err != nil {
		t.Fatalf("runL5a: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want 1", len(captured))
	}
	got := captured[0].decodeCheck(t)
	if got.Stage != vaultclient.StageL5A {
		t.Errorf("Stage = %q, want L5A", got.Stage)
	}
	if !got.Terminal {
		t.Error("Terminal = false, want true — terminal=true was passed in (L5-a is the last planned stage)")
	}
}

// ── L5-b is always terminal (nothing runs after it) ─────────────────────────

func TestRunL5b_NotAvailable_AlwaysTerminal(t *testing.T) {
	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(vc)
	// No dynamic client configured -> submitNotAvailableScanRecord path.

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if err := w.runL5b(context.Background(), slog.Default(), job); err != nil {
		t.Fatalf("runL5b: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("submissions captured = %d, want 1", len(captured))
	}
	got := captured[0].decodeScan(t)
	if got.Stage != vaultclient.StageL5B {
		t.Errorf("Stage = %q, want L5B", got.Stage)
	}
	if !got.Terminal {
		t.Error("Terminal = false, want true — L5-b is always the last stage in the current fixed pipeline")
	}
}

// ── Delivery-retry: terminal failures are tracked, non-terminal aren't ─────

// forceDeliveryDueNow rewrites jobID's next_attempt_at to the past,
// preserving its current payload/lastError, so a test can exercise
// retryPendingDeliveries immediately after a natural first failure instead
// of waiting out backoffDuration's real delay.
func forceDeliveryDueNow(t *testing.T, store work.Store, jobID string) {
	t.Helper()
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if err := store.MarkResultDeliveryPending(
		context.Background(), jobID, job.ResultDeliveryPayload, job.ResultDeliveryLastError,
		time.Now().Add(-time.Second),
	); err != nil {
		t.Fatalf("MarkResultDeliveryPending (force due now): %v", err)
	}
}

// closedVaultClient returns a vaultclient pointed at an address nothing is
// listening on, so every submit fails deterministically and fast.
func closedVaultClient(t *testing.T) *vaultclient.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()
	return vaultclient.NewWithAddr(srv.URL)
}

func TestSubmitCheckRecord_TerminalDeliveryFailure_MarksPending(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	sub := checkRecordSubmission{
		checkID: "l4-" + job.JobID, stage: vaultclient.StageL4, terminal: true,
		validationStatus: "infra_failed", failureKind: vaultclient.FailureKindInfrastructure, retryable: true,
	}
	if err := w.submitCheckRecord(context.Background(), slog.Default(), job, sub); err == nil {
		t.Fatal("expected submitCheckRecord to return an error against a closed server")
	}

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryPending {
		t.Errorf("ResultDeliveryStatus = %q, want pending", stored.ResultDeliveryStatus)
	}
	if stored.ResultDeliveryPayload == "" {
		t.Error("ResultDeliveryPayload is empty, want the serialized pending submission")
	}
}

func TestSubmitCheckRecord_NonTerminalDeliveryFailure_DoesNotMarkPending(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	sub := checkRecordSubmission{checkID: "l5a-" + job.JobID, stage: vaultclient.StageL5A, terminal: false, validationStatus: "succeeded"}
	if err := w.submitCheckRecord(context.Background(), slog.Default(), job, sub); err == nil {
		t.Fatal("expected submitCheckRecord to return an error against a closed server")
	}

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryNotApplicable {
		t.Errorf("ResultDeliveryStatus = %q, want not_applicable — a non-terminal record's delivery failure "+
			"is not tracked for redelivery (the eventual terminal record's own delivery is what matters)",
			stored.ResultDeliveryStatus)
	}
}

// These exercise redeliverOne directly (rather than through
// retryPendingDeliveries/ClaimPendingDeliveries) so they test the
// retry/backoff/acknowledge decision logic itself without needing to wait
// out backoffDuration's real delay just to make a job claimable again — the
// claim-eligibility scheduling (next_attempt_at gating, batch limit,
// ordering, reclaim) is covered directly at the store level in
// pkg/work/sqlite's tests. TestRetryPendingDeliveries_ClaimsAndRedeliversDueJob
// below covers the full retryPendingDeliveries -> ClaimPendingDeliveries ->
// redeliverOne pipeline end to end.

func TestRedeliverOne_CheckRecord_Acknowledges(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	sub := checkRecordSubmission{
		checkID: "l4-" + job.JobID, stage: vaultclient.StageL4, terminal: true,
		validationStatus: "infra_failed", failureKind: vaultclient.FailureKindInfrastructure, retryable: true,
	}
	if err := w.submitCheckRecord(context.Background(), slog.Default(), job, sub); err == nil {
		t.Fatal("expected initial submission to fail")
	}

	// Point the worker at a working server and redeliver.
	var captured []capturedSubmission
	w.vaultClient = capturingVaultServer(t, &captured)

	pending, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	w.redeliverOne(context.Background(), pending)

	if len(captured) != 1 {
		t.Fatalf("redelivery attempts captured = %d, want 1", len(captured))
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryAcknowledged {
		t.Errorf("ResultDeliveryStatus = %q, want acknowledged", stored.ResultDeliveryStatus)
	}
	if stored.ResultDeliveryPayload != "" {
		t.Error("ResultDeliveryPayload should be cleared once acknowledged")
	}
}

func TestRedeliverOne_StillFailing_IncrementsAttemptsAndStaysPending(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	sub := checkRecordSubmission{
		checkID: "l4-" + job.JobID, stage: vaultclient.StageL4, terminal: true,
		validationStatus: "infra_failed", failureKind: vaultclient.FailureKindInfrastructure, retryable: true,
	}
	if err := w.submitCheckRecord(context.Background(), slog.Default(), job, sub); err == nil {
		t.Fatal("expected initial submission to fail")
	}

	// vaultClient still points at the (now permanently closed) server.
	for i := 0; i < 2; i++ {
		pending, getErr := store.GetJob(context.Background(), job.JobID)
		if getErr != nil {
			t.Fatalf("GetJob: %v", getErr)
		}
		w.redeliverOne(context.Background(), pending)
	}

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryPending {
		t.Errorf("ResultDeliveryStatus = %q, want still pending", stored.ResultDeliveryStatus)
	}
	// 1 from the initial submitCheckRecord failure + 2 redeliverOne calls = 3.
	if stored.ResultDeliveryAttempts != 3 {
		t.Errorf("ResultDeliveryAttempts = %d, want 3", stored.ResultDeliveryAttempts)
	}
	if stored.ResultDeliveryLastError == "" {
		t.Error("ResultDeliveryLastError is empty, want the most recent failure")
	}
	if stored.NextAttemptAt == nil || !stored.NextAttemptAt.After(time.Now()) {
		t.Error("NextAttemptAt should be set in the future (backoff) after a retryable failure")
	}
}

func TestRedeliverOne_ScanRecord_Acknowledges(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := w.runL5b(context.Background(), slog.Default(), job); err == nil {
		t.Fatal("expected initial L5-b submission to fail against a closed server")
	}

	var captured []capturedSubmission
	w.vaultClient = capturingVaultServer(t, &captured)
	pending, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	w.redeliverOne(context.Background(), pending)

	if len(captured) != 1 {
		t.Fatalf("redelivery attempts captured = %d, want 1", len(captured))
	}
	if captured[0].decodeScan(t).Stage != vaultclient.StageL5B {
		t.Errorf("redelivered payload stage = %q, want L5B", captured[0].decodeScan(t).Stage)
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryAcknowledged {
		t.Errorf("ResultDeliveryStatus = %q, want acknowledged", stored.ResultDeliveryStatus)
	}
}

// TestRetryPendingDeliveries_ClaimsAndRedeliversDueJob covers the full
// public pipeline: retryPendingDeliveries -> ClaimPendingDeliveries (claims
// a due job) -> redeliverOne -> acknowledged. Uses forceDeliveryDueNow so
// the test doesn't have to wait out a real backoff delay.
func TestRetryPendingDeliveries_ClaimsAndRedeliversDueJob(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	sub := checkRecordSubmission{
		checkID: "l4-" + job.JobID, stage: vaultclient.StageL4, terminal: true,
		validationStatus: "infra_failed", failureKind: vaultclient.FailureKindInfrastructure, retryable: true,
	}
	if err := w.submitCheckRecord(context.Background(), slog.Default(), job, sub); err == nil {
		t.Fatal("expected initial submission to fail")
	}
	forceDeliveryDueNow(t, store, job.JobID)

	var captured []capturedSubmission
	w.vaultClient = capturingVaultServer(t, &captured)
	w.retryPendingDeliveries(context.Background())

	if len(captured) != 1 {
		t.Fatalf("redelivery attempts captured = %d, want 1", len(captured))
	}
	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryAcknowledged {
		t.Errorf("ResultDeliveryStatus = %q, want acknowledged", stored.ResultDeliveryStatus)
	}
}

// TestProcess_L5aFails_L5bStillRunsAndSubmitsTerminal is a direct regression
// guard for the L5-a/L5-b terminal contract: L5-a is never terminal
// specifically because L5-b always runs after it in the current fixed
// pipeline (see checkRecordSubmission's doc comment) — if that ordering
// invariant ever broke (e.g. a future "return early on l5aErr" added to
// process()), L5-a's failure would silently become the last word on this
// validation request with no terminal record ever submitted, and
// ValidationRequestRecord would stay stuck at Running forever. This locks
// in that L5-b still runs, and still submits a Terminal record, even when
// L5-a fails.
func TestProcess_L5aFails_L5bStillRunsAndSubmitsTerminal(t *testing.T) {
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
	req.JobID = "l5a-fail-job" // non-empty: extractPodExitCode's K8s label selector rejects "job-name=l5a-" (trailing '-') built from an empty JobID
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w.process(context.Background(), job)

	var sawL5A, sawL5BTerminal bool
	for _, c := range captured {
		if strings.Contains(c.path, "check-records") {
			got := c.decodeCheck(t)
			if got.Stage == vaultclient.StageL5A {
				sawL5A = true
				if got.Terminal {
					t.Error("L5-a record must not be Terminal")
				}
				if got.ValidationStatus == "succeeded" {
					t.Error("L5-a record should reflect the injected failure, not succeeded")
				}
			}
		}
		if strings.Contains(c.path, "scan-records") {
			got := c.decodeScan(t)
			if got.Stage == vaultclient.StageL5B && got.Terminal {
				sawL5BTerminal = true
			}
		}
	}
	if !sawL5A {
		t.Error("expected an L5-a check record to have been submitted")
	}
	if !sawL5BTerminal {
		t.Fatal("expected L5-b to still run and submit a Terminal record even though L5-a failed")
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// L5-a/L5-b are best-effort — the WorkStore job itself still succeeds as
	// long as L3/L4 passed. l5aErr in process() only reflects a failure to
	// *submit* the record to NodeVault (an HTTP/transport failure), not an
	// application-level validation failure — that's reported successfully,
	// as a check record whose ValidationStatus is "failed" (asserted above).
	if stored.Status != work.StatusSucceeded {
		t.Errorf("job Status = %q, want succeeded (an L5-a application-level failure doesn't fail the WorkStore job)", stored.Status)
	}
}

// errTestAdmissionRejected simulates an L3 admission-webhook rejection.
var errTestAdmissionRejected = errors.New("admission webhook rejected")
