package work

import "time"

// Retry pacing for requeued jobs (NodeSentinel #8). A job that fails
// retryably is requeued with a durable not-before time (Job.RetryNotBefore)
// so LeaseJob does not hand it straight back to a worker. Only the delay is
// decided here; which failures are retried, and how many times, stays with
// pkg/worker's retry policy.
const (
	// RetryBaseDelay is the backoff cap after a job's first attempt fails.
	RetryBaseDelay = time.Second
	// RetryMaxDelay bounds the backoff cap for every later attempt.
	RetryMaxDelay = 30 * time.Second
)

// RetryDelay returns how long a job whose attempt-th attempt (Job.Attempt,
// 1-based) just failed retryably must wait before it may be leased again.
//
// It is capped exponential backoff with equal jitter:
//
//	cap   = min(RetryMaxDelay, RetryBaseDelay * 2^(attempt-1))
//	delay = cap/2 + jitter, jitter uniform in [0, cap/2]
//
// so the result always lies in [cap/2, cap]. attempt < 1 is treated as 1,
// which keeps the delay above zero, and the doubling stops at RetryMaxDelay,
// so no attempt count can overflow. randInt63n must behave like
// math/rand.Int63n (uniform in [0, n)); it is a parameter so callers and tests
// control the jitter source.
func RetryDelay(attempt int, randInt63n func(n int64) int64) time.Duration {
	capDelay := RetryBaseDelay
	for i := 1; i < attempt && capDelay < RetryMaxDelay; i++ {
		capDelay *= 2
	}
	if capDelay > RetryMaxDelay {
		capDelay = RetryMaxDelay
	}
	half := capDelay / 2
	return half + time.Duration(randInt63n(int64(capDelay-half)+1))
}
