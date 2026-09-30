package sqlite

import (
	"context"
	"time"
)

// SetClock replaces the clock LeaseJob and FailJob use for retry_not_before.
// Test-only: compiled into this package's tests, not the production build.
func SetClock(s *Store, now func() time.Time) { s.now = now }

// ExecRaw runs a statement against the store's database, for tests that need
// a row in a shape the Store API never writes (e.g. a legacy timestamp form).
func ExecRaw(s *Store, query string, args ...any) error {
	_, err := s.db.ExecContext(context.Background(), query, args...)
	return err
}

// QueryString reads one TEXT value, for tests that assert the stored form.
func QueryString(s *Store, query string, args ...any) (string, error) {
	var v string
	err := s.db.QueryRowContext(context.Background(), query, args...).Scan(&v)
	return v, err
}

// SetQueryOnly turns SQLite's query_only mode on or off. The pool holds one
// connection (see New), so the setting applies to every later statement:
// reads keep working and every write fails, as on a read-only volume.
func SetQueryOnly(s *Store, on bool) error {
	mode := "OFF"
	if on {
		mode = "ON"
	}
	_, err := s.db.ExecContext(context.Background(), "PRAGMA query_only = "+mode)
	return err
}

// Ping runs the read-only checks a shallow readiness probe would: a ping and
// SELECT 1.
func Ping(s *Store) error {
	ctx := context.Background()
	if err := s.db.PingContext(ctx); err != nil {
		return err
	}
	var one int
	return s.db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
}
