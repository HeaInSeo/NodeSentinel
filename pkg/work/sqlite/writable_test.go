package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

// CheckWritable is the store half of /readyz. A store that still answers
// reads but rejects every write — a read-only or full volume — must fail it,
// which is exactly what a ping or SELECT 1 cannot detect.
func TestCheckWritable_ReadOnlyStorePassesSelectButFailsWriteProbe(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	if err := store.CheckWritable(ctx); err != nil {
		t.Fatalf("healthy store: CheckWritable = %v", err)
	}

	if err := sqlite.SetQueryOnly(store, true); err != nil {
		t.Fatalf("SetQueryOnly: %v", err)
	}
	if err := sqlite.Ping(store); err != nil {
		t.Fatalf("read-only store: ping/SELECT 1 = %v; the shallow check should still pass", err)
	}
	if err := store.CheckWritable(ctx); err == nil {
		t.Fatal("read-only store: CheckWritable = nil, want an error")
	}
	if _, err := store.CreateJob(ctx, sampleRequest("job-ro")); err == nil {
		t.Fatal("read-only store accepted CreateJob; the fixture is not read-only")
	}

	// Recovery: once writes work again the probe passes again.
	if err := sqlite.SetQueryOnly(store, false); err != nil {
		t.Fatalf("SetQueryOnly(false): %v", err)
	}
	if err := store.CheckWritable(ctx); err != nil {
		t.Fatalf("recovered store: CheckWritable = %v", err)
	}
}

func TestCheckWritable_ClosedStoreFails(t *testing.T) {
	store := newStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.CheckWritable(context.Background()); err == nil {
		t.Fatal("closed store: CheckWritable = nil, want an error")
	}
}

// A probe whose context has already expired reports failure rather than
// blocking, so a store held by a long transaction reads as not ready.
func TestCheckWritable_HonorsDeadline(t *testing.T) {
	store := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if err := store.CheckWritable(ctx); err == nil {
		t.Fatal("expired context: CheckWritable = nil, want an error")
	}
}
