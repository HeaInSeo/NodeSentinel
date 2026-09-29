package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

// Issue #28 boundaries. A deadline at hh:mm:05.5 compared against a clock at
// exactly hh:mm:05 is the case trimmed RFC3339Nano text got wrong: "…:05Z"
// sorts after "…:05.5Z" because 'Z' > '.', so the deadline looked passed half
// a second early.
var (
	halfSecond  = time.Date(2026, 9, 30, 1, 0, 0, 500000000, time.UTC) // hh:mm:00.5
	deadline    = halfSecond.Add(5 * time.Second)                      // hh:mm:05.5
	wholeSecond = deadline.Truncate(time.Second)                       // hh:mm:05, before deadline
)

func TestIssue28_LeaseExpiry_WholeSecondBeforeFractionalDeadline(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: halfSecond}
	store := openClockedStore(t, filepath.Join(t.TempDir(), "ns.sqlite"), clock)
	if _, err := store.CreateJob(ctx, sampleRequest("job-lease")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := store.LeaseJob(ctx, "worker-a", 5*time.Second); err != nil { // lease_until = deadline
		t.Fatalf("LeaseJob: %v", err)
	}

	clock.Set(wholeSecond)
	if job, err := store.LeaseJob(ctx, "worker-b", time.Minute); !errors.Is(err, work.ErrNoAvailableJob) {
		t.Fatalf("reclaim at %v before lease_until %v: (%v, %v), want ErrNoAvailableJob", wholeSecond, deadline, job, err)
	}
	clock.Set(deadline.Add(time.Nanosecond))
	mustLease(t, store, "worker-b")
}

func TestIssue28_DeliveryDue_WholeSecondBeforeFractionalDueTime(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: halfSecond}
	store := openClockedStore(t, filepath.Join(t.TempDir(), "ns.sqlite"), clock)
	if _, err := store.CreateJob(ctx, sampleRequest("job-due")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	markFirstPending(t, store, "job-due", `{"kind":"check"}`, "down", deadline)

	clock.Set(wholeSecond)
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 0 {
		t.Fatalf("claim at %v before next_attempt_at %v = %d, %v; want nothing", wholeSecond, deadline, len(claimed), err)
	}
	clock.Set(deadline)
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 1 {
		t.Fatalf("claim at next_attempt_at = %d, %v; want 1", len(claimed), err)
	}
}

func TestIssue28_DeliveryClaimExpiry_WholeSecondBeforeFractionalExpiry(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: halfSecond}
	store := openClockedStore(t, filepath.Join(t.TempDir(), "ns.sqlite"), clock)
	if _, err := store.CreateJob(ctx, sampleRequest("job-claim")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	markFirstPending(t, store, "job-claim", `{"kind":"check"}`, "down", halfSecond)
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, 5*time.Second); err != nil || len(claimed) != 1 { // until deadline
		t.Fatalf("claim = %d, %v", len(claimed), err)
	}

	clock.Set(wholeSecond)
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 0 {
		t.Fatalf("reclaim at %v before claimed_until %v = %d, %v; want nothing", wholeSecond, deadline, len(claimed), err)
	}
	clock.Set(deadline.Add(time.Nanosecond))
	if claimed, err := store.ClaimPendingDeliveries(ctx, 10, time.Minute); err != nil || len(claimed) != 1 {
		t.Fatalf("reclaim after claimed_until = %d, %v; want 1", len(claimed), err)
	}
}

// Rows written by an earlier binary keep trimmed RFC3339Nano text. Opening the
// store rewrites them to the fixed-width form, after which comparisons and
// ORDER BY are correct for them too.
func TestIssue28_LegacyTimestampsNormalizedOnOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ns.sqlite")
	clock := &testClock{now: halfSecond}
	legacy := openClockedStore(t, path, clock)
	for _, id := range []string{"job-older", "job-newer", "job-leased"} {
		if _, err := legacy.CreateJob(ctx, sampleRequest(id)); err != nil {
			t.Fatalf("CreateJob(%s): %v", id, err)
		}
	}
	// Legacy text as time.RFC3339Nano wrote it. By text, "05.12Z" sorts before
	// "05.1Z" although it is later; the lease "05.5Z" sorts before "05Z".
	legacyRows := []struct{ id, created, leaseUntil, status string }{
		{"job-older", "2026-09-30T01:00:05.1Z", "", "queued"},
		{"job-newer", "2026-09-30T01:00:05.12Z", "", "queued"},
		{"job-leased", "2026-09-30T01:00:00Z", "2026-09-30T01:00:05.5Z", "running"},
	}
	for _, r := range legacyRows {
		var lease any
		if r.leaseUntil != "" {
			lease = r.leaseUntil
		}
		if err := sqlite.ExecRaw(legacy,
			`UPDATE jobs SET created_at = ?, updated_at = ?, lease_until = ?, status = ?, lease_owner = 'old' WHERE job_id = ?`,
			r.created, r.created, lease, r.status, r.id); err != nil {
			t.Fatalf("write legacy row %s: %v", r.id, err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store := openClockedStore(t, path, clock)
	for _, r := range legacyRows {
		created, err := sqlite.QueryString(store, `SELECT created_at FROM jobs WHERE job_id = ?`, r.id)
		if err != nil {
			t.Fatalf("read created_at: %v", err)
		}
		if want := mustParse(t, r.created).Format("2006-01-02T15:04:05.000000000Z07:00"); created != want {
			t.Errorf("%s created_at = %q, want normalized %q", r.id, created, want)
		}
	}
	lease, err := sqlite.QueryString(store, `SELECT lease_until FROM jobs WHERE job_id = 'job-leased'`)
	if err != nil || lease != "2026-09-30T01:00:05.500000000Z" {
		t.Fatalf("lease_until = %q, %v; want 2026-09-30T01:00:05.500000000Z", lease, err)
	}

	// The legacy lease is not reclaimed at the whole second before it, and
	// queued jobs lease oldest first.
	clock.Set(wholeSecond)
	first := mustLease(t, store, "worker-b")
	if first.JobID != "job-older" {
		t.Fatalf("first lease = %s, want job-older (created 05.1 < 05.12; legacy text order was reversed)", first.JobID)
	}
	second := mustLease(t, store, "worker-b")
	if second.JobID != "job-newer" {
		t.Fatalf("second lease = %s, want job-newer; the running legacy lease must not be reclaimed before 05.5", second.JobID)
	}
	assertNoLeasable(t, store, "legacy lease before its fractional deadline")
}

// A timestamp the store cannot parse fails the open instead of being rewritten:
// scanJob could not read that row either, and rewriting it would destroy it.
func TestIssue28_UnparseableLegacyTimestampFailsOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ns.sqlite")
	legacy := openClockedStore(t, path, &testClock{now: halfSecond})
	if _, err := legacy.CreateJob(ctx, sampleRequest("job-bad")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := sqlite.ExecRaw(legacy, `UPDATE jobs SET lease_until = 'yesterday' WHERE job_id = 'job-bad'`); err != nil {
		t.Fatalf("write bad row: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if s, err := sqlite.New(path); err == nil {
		_ = s.Close()
		t.Fatal("sqlite.New succeeded over an unparseable lease_until; want an error naming the row")
	}
}

func mustParse(t *testing.T, v string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		t.Fatalf("parse %q: %v", v, err)
	}
	return ts.UTC()
}
