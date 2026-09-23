package sqlite

import "time"

// SetClock replaces the clock LeaseJob and FailJob use for retry_not_before.
// Test-only: compiled into this package's tests, not the production build.
func SetClock(s *Store, now func() time.Time) { s.now = now }
