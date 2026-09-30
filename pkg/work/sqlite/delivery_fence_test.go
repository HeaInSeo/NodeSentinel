package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

// claimStaleThenReclaim builds the interleaving every redelivery fence test
// starts from: claimer A claims a due delivery, stalls past the claim TTL,
// and claimer B reclaims it. It returns A's and B's claimed rows.
func claimStaleThenReclaim(t *testing.T, store *sqlite.Store, clock *testClock, jobID string) (a, b *work.Job) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.CreateJob(ctx, sampleRequest(jobID)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	markFirstPending(t, store, jobID, `{"kind":"check"}`, "first failure", t0)

	claimsA, err := store.ClaimPendingDeliveries(ctx, 10, 2*time.Minute)
	if err != nil || len(claimsA) != 1 {
		t.Fatalf("claim A = %d, %v", len(claimsA), err)
	}
	clock.Set(t0.Add(3 * time.Minute)) // A stalls past its 2m claim
	claimsB, err := store.ClaimPendingDeliveries(ctx, 10, 2*time.Minute)
	if err != nil || len(claimsB) != 1 {
		t.Fatalf("reclaim B = %d, %v", len(claimsB), err)
	}
	if claimsA[0].ResultDeliveryClaimToken == claimsB[0].ResultDeliveryClaimToken {
		t.Fatal("reclaim reused the stale claim's token")
	}
	return claimsA[0], claimsB[0]
}

// Counterexample (a): B acknowledges, then A's late retryable failure must
// not reset the row to pending (which would redeliver an acknowledged record).
func TestRedelivery_StalePendingCannotRevertAcknowledged(t *testing.T) {
	ctx := context.Background()
	store, clock := newClockedStore(t)
	a, b := claimStaleThenReclaim(t, store, clock, "job-stale-pending")

	if err := store.MarkResultDeliveryAcknowledged(ctx, b.JobID, b.ResultDeliveryClaimToken); err != nil {
		t.Fatalf("B acknowledge: %v", err)
	}
	ackedAttempts := mustGet(t, store, b.JobID).ResultDeliveryAttempts

	err := store.MarkResultDeliveryPending(ctx, a.JobID, a.ResultDeliveryClaimToken, "A timed out", t0.Add(4*time.Minute))
	if !errors.Is(err, work.ErrDeliveryFenced) {
		t.Fatalf("stale A pending: err = %v, want ErrDeliveryFenced", err)
	}
	got := mustGet(t, store, b.JobID)
	if got.ResultDeliveryStatus != work.DeliveryAcknowledged {
		t.Fatalf("status = %q, want acknowledged to stand", got.ResultDeliveryStatus)
	}
	if got.ResultDeliveryAttempts != ackedAttempts || got.ResultDeliveryLastError != "" {
		t.Fatalf("stale write leaked: attempts %d (want %d), last error %q",
			got.ResultDeliveryAttempts, ackedAttempts, got.ResultDeliveryLastError)
	}
	// Nothing is claimable again, so the acknowledged record is never re-sent.
	clock.Set(t0.Add(time.Hour))
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, 2*time.Minute); err != nil || len(claimed) != 0 {
		t.Fatalf("claim after ack = %d, %v; want nothing", len(claimed), err)
	}
}

// Counterexample (b): A's late permanent failure must not overwrite B's
// acknowledgement with a false dead-letter.
func TestRedelivery_StaleDeadLetterCannotOverwriteAcknowledged(t *testing.T) {
	ctx := context.Background()
	store, clock := newClockedStore(t)
	a, b := claimStaleThenReclaim(t, store, clock, "job-stale-dead-letter")

	if err := store.MarkResultDeliveryAcknowledged(ctx, b.JobID, b.ResultDeliveryClaimToken); err != nil {
		t.Fatalf("B acknowledge: %v", err)
	}
	err := store.MarkResultDeliveryDeadLetter(ctx, a.JobID, a.ResultDeliveryClaimToken, "A got 400")
	if !errors.Is(err, work.ErrDeliveryFenced) {
		t.Fatalf("stale A dead-letter: err = %v, want ErrDeliveryFenced", err)
	}
	if got := mustGet(t, store, b.JobID); got.ResultDeliveryStatus != work.DeliveryAcknowledged {
		t.Fatalf("status = %q, want acknowledged to stand", got.ResultDeliveryStatus)
	}
}

// Counterexample (c): while B is still delivering, A's late pending must not
// clear B's claim; otherwise a third claimer would POST concurrently with B.
func TestRedelivery_StalePendingCannotReleaseLiveClaim(t *testing.T) {
	ctx := context.Background()
	store, clock := newClockedStore(t)
	a, b := claimStaleThenReclaim(t, store, clock, "job-stale-release")

	err := store.MarkResultDeliveryPending(ctx, a.JobID, a.ResultDeliveryClaimToken, "A timed out", t0)
	if !errors.Is(err, work.ErrDeliveryFenced) {
		t.Fatalf("stale A pending: err = %v, want ErrDeliveryFenced", err)
	}
	got := mustGet(t, store, b.JobID)
	if got.ResultDeliveryStatus != work.DeliveryDelivering || got.ResultDeliveryClaimToken != b.ResultDeliveryClaimToken {
		t.Fatalf("B's claim disturbed: status %q token %q", got.ResultDeliveryStatus, got.ResultDeliveryClaimToken)
	}
	clock.Set(t0.Add(4 * time.Minute)) // B's claim (until 3m+2m) is still live
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, 2*time.Minute); err != nil || len(claimed) != 0 {
		t.Fatalf("third claim during B's live claim = %d, %v; want nothing", len(claimed), err)
	}
	// B's own outcome still applies.
	if err := store.MarkResultDeliveryAcknowledged(ctx, b.JobID, b.ResultDeliveryClaimToken); err != nil {
		t.Fatalf("B acknowledge: %v", err)
	}
}

// A resolved claim's token is spent: repeating the outcome, or sending the
// other outcome, is fenced. Acknowledged and dead-letter are final.
func TestRedelivery_ResolvedClaimTokenIsSpent(t *testing.T) {
	ctx := context.Background()
	store, _ := newClockedStore(t)
	if _, err := store.CreateJob(ctx, sampleRequest("job-spent")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	markFirstPending(t, store, "job-spent", `{"kind":"check"}`, "first failure", t0)
	claimed, err := store.ClaimPendingDeliveries(ctx, 10, 2*time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, %v", len(claimed), err)
	}
	token := claimed[0].ResultDeliveryClaimToken
	if err := store.MarkResultDeliveryDeadLetter(ctx, "job-spent", token, "400"); err != nil {
		t.Fatalf("dead-letter: %v", err)
	}
	for name, write := range map[string]func() error{
		"acknowledge": func() error { return store.MarkResultDeliveryAcknowledged(ctx, "job-spent", token) },
		"pending":     func() error { return store.MarkResultDeliveryPending(ctx, "job-spent", token, "x", t0) },
		"dead-letter": func() error { return store.MarkResultDeliveryDeadLetter(ctx, "job-spent", token, "x") },
		"empty token": func() error { return store.MarkResultDeliveryAcknowledged(ctx, "job-spent", "") },
	} {
		if err := write(); !errors.Is(err, work.ErrDeliveryFenced) {
			t.Errorf("%s after dead-letter: err = %v, want ErrDeliveryFenced", name, err)
		}
	}
	if got := mustGet(t, store, "job-spent"); got.ResultDeliveryStatus != work.DeliveryDeadLetter {
		t.Fatalf("status = %q, want dead_letter", got.ResultDeliveryStatus)
	}
}

// Counterexample (d): the first-delivery mark belongs to the attempt that
// holds the terminal slot. A superseded attempt that never took the slot — or
// the right owner under a stale attempt number — cannot write the ledger, and
// once the ledger has left NotApplicable no first mark can rewind it.
func TestFirstDelivery_FencedOnTerminalSlotOwnerAndAttempt(t *testing.T) {
	ctx := context.Background()
	store, _ := newClockedStore(t)
	if _, err := store.CreateJob(ctx, sampleRequest("job-first")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	stale := mustLease(t, store, "worker-a")

	// Before any terminal claim, nobody may mark.
	if err := store.MarkFirstDeliveryPending(ctx, "job-first", "worker-a", stale.Attempt, "{}", "x", t0); !errors.Is(err, work.ErrDeliveryFenced) {
		t.Fatalf("mark without terminal slot: err = %v, want ErrDeliveryFenced", err)
	}

	if claimed, err := store.ClaimTerminal(ctx, "job-first", "worker-b", stale.Attempt+1); err != nil || !claimed {
		t.Fatalf("ClaimTerminal(worker-b) = %v, %v", claimed, err)
	}
	for _, c := range []struct {
		name    string
		worker  string
		attempt int
	}{
		{"superseded attempt", "worker-a", stale.Attempt},
		{"slot owner, stale attempt", "worker-b", stale.Attempt},
		{"other worker, owner's attempt", "worker-a", stale.Attempt + 1},
	} {
		if err := store.MarkFirstDeliveryPending(ctx, "job-first", c.worker, c.attempt, "{}", "x", t0); !errors.Is(err, work.ErrDeliveryFenced) {
			t.Errorf("%s pending: err = %v, want ErrDeliveryFenced", c.name, err)
		}
		if err := store.MarkFirstDeliveryDeadLetter(ctx, "job-first", c.worker, c.attempt, "{}", "x"); !errors.Is(err, work.ErrDeliveryFenced) {
			t.Errorf("%s dead-letter: err = %v, want ErrDeliveryFenced", c.name, err)
		}
	}
	if got := mustGet(t, store, "job-first"); got.ResultDeliveryStatus != work.DeliveryNotApplicable {
		t.Fatalf("status after fenced marks = %q, want not_applicable", got.ResultDeliveryStatus)
	}

	// The slot owner marks once.
	if err := store.MarkFirstDeliveryPending(ctx, "job-first", "worker-b", stale.Attempt+1, `{"kind":"check"}`, "down", t0); err != nil {
		t.Fatalf("owner mark: %v", err)
	}
	// A second first mark, even from the owner, cannot rewind a claimed delivery.
	claimed, err := store.ClaimPendingDeliveries(ctx, 10, 2*time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d, %v", len(claimed), err)
	}
	if err := store.MarkFirstDeliveryPending(ctx, "job-first", "worker-b", stale.Attempt+1, "{}", "again", t0); !errors.Is(err, work.ErrDeliveryFenced) {
		t.Fatalf("second first mark: err = %v, want ErrDeliveryFenced", err)
	}
	got := mustGet(t, store, "job-first")
	if got.ResultDeliveryStatus != work.DeliveryDelivering || got.ResultDeliveryAttempts != 1 {
		t.Fatalf("after second first mark: status %q attempts %d, want delivering/1",
			got.ResultDeliveryStatus, got.ResultDeliveryAttempts)
	}
}

// A first delivery rejected permanently keeps its payload for the operator.
func TestFirstDelivery_DeadLetterKeepsPayload(t *testing.T) {
	ctx := context.Background()
	store, _ := newClockedStore(t)
	if _, err := store.CreateJob(ctx, sampleRequest("job-first-dl")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if claimed, err := store.ClaimTerminal(ctx, "job-first-dl", "worker-a", 1); err != nil || !claimed {
		t.Fatalf("ClaimTerminal = %v, %v", claimed, err)
	}
	if err := store.MarkFirstDeliveryDeadLetter(ctx, "job-first-dl", "worker-a", 1, `{"kind":"scan"}`, "400 bad"); err != nil {
		t.Fatalf("MarkFirstDeliveryDeadLetter: %v", err)
	}
	got := mustGet(t, store, "job-first-dl")
	if got.ResultDeliveryStatus != work.DeliveryDeadLetter || got.ResultDeliveryPayload != `{"kind":"scan"}` ||
		got.ResultDeliveryLastError != "400 bad" {
		t.Fatalf("got status %q payload %q error %q", got.ResultDeliveryStatus, got.ResultDeliveryPayload,
			got.ResultDeliveryLastError)
	}
}
