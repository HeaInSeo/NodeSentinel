package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// productionRetryDelayFor is the schedule decideRetry uses outside tests,
// captured before TestMain replaces it.
var productionRetryDelayFor = retryDelayFor

// TestMain runs this package's tests with retry pacing off (Delay 0, so a
// requeued job is due at once). Many existing tests drive a job through
// several retries against a real-clock store and would otherwise wait out
// real backoff. Pacing itself is covered by the tests in this file, which
// install a schedule explicitly, and by pkg/work and pkg/work/sqlite.
func TestMain(m *testing.M) {
	retryDelayFor = func(int) time.Duration { return 0 }
	os.Exit(m.Run())
}

func useRetrySchedule(t *testing.T, f func(attempt int) time.Duration) {
	t.Helper()
	orig := retryDelayFor
	retryDelayFor = f
	t.Cleanup(func() { retryDelayFor = orig })
}

func maxJitterSchedule(attempt int) time.Duration {
	return work.RetryDelay(attempt, func(n int64) int64 { return n - 1 })
}

func TestDecideRetry_RetryableDecisionCarriesPacedDelay(t *testing.T) {
	useRetrySchedule(t, maxJitterSchedule)

	cases := []struct {
		name      string
		class     FailureClass
		attempt   int
		lastError string
		wantRetry bool
		wantDelay time.Duration
	}{
		{"transient first attempt", FailureClassTransientInfra, 1, "", true, time.Second},
		{"transient third attempt", FailureClassTransientInfra, 3, "", true, 4 * time.Second},
		{"unknown first occurrence", FailureClassUnknown, 2, "", true, 2 * time.Second},
		{"transient exhausted", FailureClassTransientInfra, maxAttempts, "", false, 0},
		{"unknown recurred", FailureClassUnknown, 2, "x " + unknownRetryMarker, false, 0},
		{"deterministic", FailureClassDeterministic, 1, "", false, 0},
		{"resource observation", FailureClassResourceObservation, 1, "", false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := makeTestWorkJob()
			job.Attempt = tc.attempt
			job.LastError = tc.lastError
			d := decideRetry(tc.class, job, "boom")
			if d.Retry != tc.wantRetry || d.Delay != tc.wantDelay {
				t.Fatalf("decideRetry = (Retry %v, Delay %v), want (%v, %v)", d.Retry, d.Delay, tc.wantRetry, tc.wantDelay)
			}
		})
	}
}

func TestDecideRetry_ProductionSchedule_WithinEqualJitterBounds(t *testing.T) {
	useRetrySchedule(t, productionRetryDelayFor)

	for attempt := 1; attempt < maxAttempts; attempt++ {
		capDelay := min(work.RetryMaxDelay, work.RetryBaseDelay<<(attempt-1))
		job := makeTestWorkJob()
		job.Attempt = attempt
		for range 200 {
			d := decideRetry(FailureClassTransientInfra, job, "boom")
			if !d.Retry || d.Delay < capDelay/2 || d.Delay > capDelay {
				t.Fatalf("attempt %d: decision (Retry %v, Delay %v), want Retry with Delay in [%v, %v]",
					attempt, d.Retry, d.Delay, capDelay/2, capDelay)
			}
		}
	}
}

func TestProcess_RetryableFailure_RequeuedWithDecisionDelay(t *testing.T) {
	const delay = 7 * time.Second
	useRetrySchedule(t, func(int) time.Duration { return delay })

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if len(action.(k8stesting.CreateActionImpl).GetCreateOptions().DryRun) > 0 {
			return true, nil, k8serrors.NewInternalError(fmt.Errorf("admission webhook rejected"))
		}
		return false, nil, nil
	})
	w := New(store, kube, "test-worker")

	created, err := store.CreateJob(context.Background(), newTestJob())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	before := time.Now()
	w.process(context.Background(), job)
	after := time.Now()

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusQueued {
		t.Fatalf("status = %q, want queued (retryable L3 failure)", stored.Status)
	}
	if stored.RetryNotBefore == nil ||
		stored.RetryNotBefore.Before(before.Add(delay)) || stored.RetryNotBefore.After(after.Add(delay)) {
		t.Fatalf("RetryNotBefore = %v, want within [%v, %v]", stored.RetryNotBefore, before.Add(delay), after.Add(delay))
	}
	if _, err := store.LeaseJob(context.Background(), "other-worker", time.Minute); !errors.Is(err, work.ErrNoAvailableJob) {
		t.Fatalf("immediate re-lease err = %v, want ErrNoAvailableJob while the retry delay runs", err)
	}
}

func TestProcess_NonRetryableFailure_NoRetryNotBefore(t *testing.T) {
	useRetrySchedule(t, maxJitterSchedule)

	store := newTestStore(t)
	w := New(store, fake.NewClientset(), "test-worker")

	req := newTestJob()
	req.RequestedActions = []work.Action{"not-a-real-action"}
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", time.Minute)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}
	w.process(context.Background(), job)

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusFailed || stored.RetryNotBefore != nil {
		t.Fatalf("after deterministic failure: status=%q RetryNotBefore=%v, want failed/nil", stored.Status, stored.RetryNotBefore)
	}
}
