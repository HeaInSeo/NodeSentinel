package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// ── backoffDuration ───────────────────────────────────────────────────────────

// TestBackoffDuration_NeverExceedsMaxBackoff is a direct regression guard
// for a bug an independent review caught: the base delay was clamped to
// deliveryMaxBackoff BEFORE adding jitter, so the returned total
// (base+jitter) could exceed the documented cap by up to 25%. Sampled
// repeatedly since jitter is randomized.
func TestBackoffDuration_NeverExceedsMaxBackoff(t *testing.T) {
	for attempts := 1; attempts <= 30; attempts++ {
		for sample := 0; sample < 20; sample++ {
			d := backoffDuration(attempts)
			if d > deliveryMaxBackoff {
				t.Fatalf("backoffDuration(%d) = %v, want <= %v (deliveryMaxBackoff)", attempts, d, deliveryMaxBackoff)
			}
			if d <= 0 {
				t.Fatalf("backoffDuration(%d) = %v, want > 0", attempts, d)
			}
		}
	}
}

// TestBackoffDuration_SaturatedAttempts_AlwaysExactlyMaxBackoff verifies the
// specific case the jitter-overflow bug always triggered: once the
// exponential base itself already reached deliveryMaxBackoff (a high
// attempts count), the result must be pinned at exactly deliveryMaxBackoff
// every time, not fluctuate above it with jitter.
func TestBackoffDuration_SaturatedAttempts_AlwaysExactlyMaxBackoff(t *testing.T) {
	for sample := 0; sample < 20; sample++ {
		d := backoffDuration(10)
		if d != deliveryMaxBackoff {
			t.Fatalf("backoffDuration(10) = %v, want exactly %v (base already saturates the cap)", d, deliveryMaxBackoff)
		}
	}
}

// TestBackoffDuration_FirstAttempt_BaseWithBoundedJitter checks the
// unsaturated case: attempts=1 should be deliveryBaseBackoff plus at most
// 25% jitter, never less than the base itself.
func TestBackoffDuration_FirstAttempt_BaseWithBoundedJitter(t *testing.T) {
	maxWithJitter := deliveryBaseBackoff + deliveryBaseBackoff/4
	for sample := 0; sample < 20; sample++ {
		d := backoffDuration(1)
		if d < deliveryBaseBackoff {
			t.Fatalf("backoffDuration(1) = %v, want >= %v (deliveryBaseBackoff)", d, deliveryBaseBackoff)
		}
		if d > maxWithJitter {
			t.Fatalf("backoffDuration(1) = %v, want <= %v (base + 25%% jitter)", d, maxWithJitter)
		}
	}
}

// ── redeliverOne dead-letter paths ───────────────────────────────────────────

func markPendingAndFetch(t *testing.T, store work.Store, payload string) *work.Job {
	t.Helper()
	job, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	past := time.Now().Add(-time.Second)
	if err := store.MarkResultDeliveryPending(context.Background(), job.JobID, payload, "boom", past); err != nil {
		t.Fatalf("MarkResultDeliveryPending: %v", err)
	}
	pending, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	return pending
}

func TestRedeliverOne_UnmarshalFailure_DeadLetter(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	pending := markPendingAndFetch(t, store, `not valid json`)
	w.redeliverOne(context.Background(), pending)

	stored, err := store.GetJob(context.Background(), pending.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryDeadLetter {
		t.Errorf("ResultDeliveryStatus = %q, want dead_letter", stored.ResultDeliveryStatus)
	}
	if stored.ResultDeliveryPayload != `not valid json` {
		t.Error("payload should be preserved for operator inspection")
	}
}

func TestRedeliverOne_UnknownKind_DeadLetter(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	pending := markPendingAndFetch(t, store, `{"kind":"unknown"}`)
	w.redeliverOne(context.Background(), pending)

	stored, err := store.GetJob(context.Background(), pending.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryDeadLetter {
		t.Errorf("ResultDeliveryStatus = %q, want dead_letter", stored.ResultDeliveryStatus)
	}
}

func TestRedeliverOne_NilCheckBody_DeadLetter(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	pending := markPendingAndFetch(t, store, `{"kind":"check"}`) // Check field absent
	w.redeliverOne(context.Background(), pending)

	stored, err := store.GetJob(context.Background(), pending.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryDeadLetter {
		t.Errorf("ResultDeliveryStatus = %q, want dead_letter", stored.ResultDeliveryStatus)
	}
}

func TestRedeliverOne_NilScanBody_DeadLetter(t *testing.T) {
	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(closedVaultClient(t))

	pending := markPendingAndFetch(t, store, `{"kind":"scan"}`) // Scan field absent
	w.redeliverOne(context.Background(), pending)

	stored, err := store.GetJob(context.Background(), pending.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryDeadLetter {
		t.Errorf("ResultDeliveryStatus = %q, want dead_letter", stored.ResultDeliveryStatus)
	}
}

// TestRedeliverOne_PermanentHTTPError_DeadLetter verifies that a real 4xx
// response from NodeVault (not just an undecodable local payload) also
// routes to dead-letter rather than the normal pending/backoff cycle — see
// vaultclient.Retryable.
func TestRedeliverOne_PermanentHTTPError_DeadLetter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)

	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(vaultclient.NewWithAddr(srv.URL))

	payload := `{"kind":"check","check":{"check_id":"c1","image_digest":"sha256:aaa","validation_status":"succeeded"}}`
	pending := markPendingAndFetch(t, store, payload)
	w.redeliverOne(context.Background(), pending)

	stored, err := store.GetJob(context.Background(), pending.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryDeadLetter {
		t.Errorf("ResultDeliveryStatus = %q, want dead_letter (a 4xx must not be retried)", stored.ResultDeliveryStatus)
	}
}

// TestRedeliverOne_TransientHTTPError_StaysPending is the converse: a 5xx
// response must go back to pending (with backoff), not dead-letter.
func TestRedeliverOne_TransientHTTPError_StaysPending(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker").WithVaultClient(vaultclient.NewWithAddr(srv.URL))

	payload := `{"kind":"check","check":{"check_id":"c1","image_digest":"sha256:aaa","validation_status":"succeeded"}}`
	pending := markPendingAndFetch(t, store, payload)
	w.redeliverOne(context.Background(), pending)

	stored, err := store.GetJob(context.Background(), pending.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.ResultDeliveryStatus != work.DeliveryPending {
		t.Errorf("ResultDeliveryStatus = %q, want still pending (a 5xx is retryable)", stored.ResultDeliveryStatus)
	}
}
