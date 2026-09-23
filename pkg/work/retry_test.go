package work_test

import (
	"math"
	"testing"
	"time"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

func TestRetryDelay_CappedExponentialEqualJitterBounds(t *testing.T) {
	cases := []struct {
		attempt int
		cap     time.Duration
	}{
		{math.MinInt, time.Second},
		{-3, time.Second},
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second},
		{7, 30 * time.Second},
		{64, 30 * time.Second},
		{math.MaxInt, 30 * time.Second},
	}
	minJitter := func(int64) int64 { return 0 }
	for _, tc := range cases {
		var gotN int64
		maxJitter := func(n int64) int64 {
			gotN = n
			return n - 1
		}

		low := work.RetryDelay(tc.attempt, minJitter)
		high := work.RetryDelay(tc.attempt, maxJitter)

		if low != tc.cap/2 {
			t.Errorf("attempt %d: minimum delay = %v, want cap/2 = %v", tc.attempt, low, tc.cap/2)
		}
		if high != tc.cap {
			t.Errorf("attempt %d: maximum delay = %v, want cap = %v", tc.attempt, high, tc.cap)
		}
		if want := int64(tc.cap-tc.cap/2) + 1; gotN != want {
			t.Errorf("attempt %d: jitter source asked for n = %d, want %d (uniform over [0, cap/2])", tc.attempt, gotN, want)
		}
		if low <= 0 {
			t.Errorf("attempt %d: delay %v must be positive", tc.attempt, low)
		}
	}
}

func TestRetryDelay_UsesInjectedJitter(t *testing.T) {
	// cap(3) = 4s, so the result is 2s plus whatever the jitter source returns.
	got := work.RetryDelay(3, func(int64) int64 { return int64(1234 * time.Millisecond) })
	if want := 2*time.Second + 1234*time.Millisecond; got != want {
		t.Fatalf("RetryDelay = %v, want %v", got, want)
	}
}
