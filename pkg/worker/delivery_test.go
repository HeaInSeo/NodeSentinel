package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

// markFirstPendingDue records payload as a new job's failed first delivery,
// as the terminal-slot owner, due at once. It returns the job ID.
func markFirstPendingDue(t *testing.T, store work.Store, payload string) string {
	t.Helper()
	ctx := context.Background()
	job, err := store.CreateJob(ctx, newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if claimed, err := store.ClaimTerminal(ctx, job.JobID, "test-worker", job.Attempt); err != nil || !claimed {
		t.Fatalf("ClaimTerminal = %v, %v", claimed, err)
	}
	past := time.Now().Add(-time.Second)
	if err := store.MarkFirstDeliveryPending(ctx, job.JobID, "test-worker", job.Attempt, payload, "boom", past); err != nil {
		t.Fatalf("MarkFirstDeliveryPending: %v", err)
	}
	return job.JobID
}

// markPendingAndFetch is markFirstPendingDue followed by a claim; it returns
// the row claimed for redelivery.
func markPendingAndFetch(t *testing.T, store work.Store, payload string) *work.Job {
	t.Helper()
	return claimDelivery(t, store, markFirstPendingDue(t, store, payload))
}

// countingVault returns a vaultclient whose server answers every request with
// status, and the counter of requests it received.
func countingVault(t *testing.T, status int) (*vaultclient.Client, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return vaultclient.NewWithAddr(srv.URL), &n
}

// TestRedeliverOne_StaleClaimerCannotUndoReclaimedAck is the worker-level
// form of the C3 interleavings: claimer A stalls past its claim, B reclaims
// and delivers, then A's own POST finishes with a failure. A's outcome must
// not change the ledger — neither back to pending (a redelivery of an
// acknowledged record) nor to a false dead-letter — and nothing is posted
// again afterwards.
func TestRedeliverOne_StaleClaimerCannotUndoReclaimedAck(t *testing.T) {
	const payload = `{"kind":"check","check":{"check_id":"c1","image_digest":"sha256:aaa","validation_status":"succeeded"}}`
	for _, tc := range []struct {
		name       string
		staleReply int
	}{
		{"stale retryable failure", http.StatusInternalServerError},
		{"stale permanent failure", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			jobID := markFirstPendingDue(t, store, payload)

			// A claims with a TTL that has already run out when B looks.
			staleClaims, err := store.ClaimPendingDeliveries(ctx, deliveryBatchLimit, time.Nanosecond)
			if err != nil || len(staleClaims) != 1 {
				t.Fatalf("claim A = %d, %v", len(staleClaims), err)
			}
			a := staleClaims[0]
			b := claimDelivery(t, store, jobID)

			okVault, okPosts := countingVault(t, http.StatusOK)
			New(store, fake.NewClientset(), "worker-b").WithVaultClient(okVault).redeliverOne(ctx, b)

			failVault, failPosts := countingVault(t, tc.staleReply)
			New(store, fake.NewClientset(), "worker-a").WithVaultClient(failVault).redeliverOne(ctx, a)

			got, err := store.GetJob(ctx, jobID)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if got.ResultDeliveryStatus != work.DeliveryAcknowledged {
				t.Fatalf("status = %q, want acknowledged to stand after A's stale outcome", got.ResultDeliveryStatus)
			}
			if got.ResultDeliveryAttempts != 1 || got.ResultDeliveryLastError != "" {
				t.Fatalf("stale outcome leaked: attempts %d, last error %q", got.ResultDeliveryAttempts, got.ResultDeliveryLastError)
			}

			// Nothing is left to redeliver: no further POST from either vault.
			New(store, fake.NewClientset(), "worker-c").WithVaultClient(okVault).retryPendingDeliveries(ctx)
			if posts := okPosts.Load() + failPosts.Load(); posts != 2 {
				t.Fatalf("POSTs = %d, want 2 (B's delivery and A's in-flight one); an acknowledged record was re-sent", posts)
			}
		})
	}
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
