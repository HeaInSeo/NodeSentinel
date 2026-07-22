package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
		retryable            bool
		wantFailureKind      string
		wantValidationStatus string
	}{
		{"L3 is always infrastructure", vaultclient.StageL3, true, vaultclient.FailureKindInfrastructure, "infra_failed"},
		{"L4 infra-level (retryable)", vaultclient.StageL4, true, vaultclient.FailureKindInfrastructure, "infra_failed"},
		{"L4 application-level (not retryable)", vaultclient.StageL4, false, vaultclient.FailureKindApplication, "failed"},
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

			failureKind := vaultclient.FailureKindInfrastructure
			if !tt.retryable {
				failureKind = vaultclient.FailureKindApplication
			}
			w.reportTerminalFailure(context.Background(), slog.Default(), job, tt.stage, "cmd", "boom", failureKind, tt.retryable)

			if len(captured) != 1 {
				t.Fatalf("captured submissions = %d, want 1", len(captured))
			}
			got := captured[0].decodeCheck(t)
			if got.Stage != tt.stage {
				t.Errorf("Stage = %q, want %q", got.Stage, tt.stage)
			}
			if !got.Terminal {
				t.Error("Terminal = false, want true (L3/L4 failures always end the request)")
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
	if !got.Terminal {
		t.Error("Terminal = false, want true")
	}
	if got.ValidationRequestID != created.ValidationRequestID {
		t.Errorf("ValidationRequestID = %q, want %q", got.ValidationRequestID, created.ValidationRequestID)
	}
}

// ── L5-a is never terminal (L5-b always follows it) ─────────────────────────

func TestRunL5a_Success_NeverTerminal(t *testing.T) {
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

	if err := w.runL5a(context.Background(), slog.Default(), job); err != nil {
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
		t.Error("Terminal = true, want false — L5-b always runs after L5-a in the current fixed pipeline")
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

func TestRetryPendingDeliveries_CheckRecord_Acknowledges(t *testing.T) {
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

	// Point the worker at a working server and retry.
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
	if stored.ResultDeliveryPayload != "" {
		t.Error("ResultDeliveryPayload should be cleared once acknowledged")
	}
}

func TestRetryPendingDeliveries_StillFailing_IncrementsAttempts(t *testing.T) {
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
	w.retryPendingDeliveries(context.Background())
	w.retryPendingDeliveries(context.Background())

	stored, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryPending {
		t.Errorf("ResultDeliveryStatus = %q, want still pending", stored.ResultDeliveryStatus)
	}
	// 1 from the initial submitCheckRecord failure + 2 retries = 3.
	if stored.ResultDeliveryAttempts != 3 {
		t.Errorf("ResultDeliveryAttempts = %d, want 3", stored.ResultDeliveryAttempts)
	}
	if stored.ResultDeliveryLastError == "" {
		t.Error("ResultDeliveryLastError is empty, want the most recent failure")
	}
}

func TestRetryPendingDeliveries_ScanRecord_Acknowledges(t *testing.T) {
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
	w.retryPendingDeliveries(context.Background())

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

// errTestAdmissionRejected simulates an L3 admission-webhook rejection.
var errTestAdmissionRejected = errors.New("admission webhook rejected")
