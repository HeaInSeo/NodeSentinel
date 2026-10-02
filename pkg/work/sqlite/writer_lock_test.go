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

// TestWriterLock_SecondOpenerFailsWhileFirstIsOpen covers FD-07 C2: while one
// Store holds the file, a second sqlite.New on the same path — a second Pod on
// the same volume, or a predecessor that has not stopped — must fail with
// ErrWriterLocked instead of running its own lease and delivery loops, and the
// first writer must keep working and see no writes from it.
func TestWriterLock_SecondOpenerFailsWhileFirstIsOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")

	first := openAt(t, path)
	if _, err := first.CreateJob(ctx, sampleRequest("job-held")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	second, err := sqlite.New(path)
	if err == nil {
		_ = second.Close()
		t.Fatal("second sqlite.New on a held store succeeded; want ErrWriterLocked")
	}
	if !errors.Is(err, sqlite.ErrWriterLocked) {
		t.Fatalf("second sqlite.New err = %v, want ErrWriterLocked", err)
	}

	leased, err := first.LeaseJob(ctx, "owner-first", time.Minute)
	if err != nil {
		t.Fatalf("first writer LeaseJob after rejected opener: %v", err)
	}
	if leased.JobID != "job-held" || leased.Attempt != 1 {
		t.Fatalf("first writer leased %s attempt %d, want job-held attempt 1", leased.JobID, leased.Attempt)
	}
}

// TestWriterLock_ReleasedOnClose covers the normal restart: once the first
// Store closes, a new one opens the same file and sees its work.
func TestWriterLock_ReleasedOnClose(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")

	first, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	if _, err := first.CreateJob(ctx, sampleRequest("job-restart")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restarted := openAt(t, path)
	got, err := restarted.GetJob(ctx, "job-restart")
	if err != nil {
		t.Fatalf("GetJob after restart: %v", err)
	}
	if got.Status != work.StatusQueued {
		t.Fatalf("status after restart = %s, want %s", got.Status, work.StatusQueued)
	}
}

// TestWriterLock_HeldAcrossIdleAndStoreErrors checks the lock is not dropped
// when the Store idles or a statement fails: the pool keeps its one
// connection, so a later opener is still rejected.
func TestWriterLock_HeldAcrossIdleAndStoreErrors(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nodesentinel.sqlite")

	first := openAt(t, path)
	if _, err := first.GetJob(ctx, "missing"); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("GetJob(missing) err = %v, want ErrNotFound", err)
	}
	if err := sqlite.ExecRaw(first, "SELECT * FROM no_such_table"); err == nil {
		t.Fatal("query on a missing table succeeded")
	}
	if err := first.CheckWritable(ctx); err != nil {
		t.Fatalf("CheckWritable: %v", err)
	}

	if second, err := sqlite.New(path); !errors.Is(err, sqlite.ErrWriterLocked) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second sqlite.New after idle/errors err = %v, want ErrWriterLocked", err)
	}
}
