package worker

import (
	"strings"
	"testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// useLowMaxAttempts lowers the package-level maxAttempts var for the
// duration of a test, mirroring process_test.go's useFastWorkerTicks
// override pattern.
func useLowMaxAttempts(t *testing.T, n int) {
	t.Helper()
	orig := maxAttempts
	maxAttempts = n
	t.Cleanup(func() { maxAttempts = orig })
}

// --- decideRetry: per-class policy ---

func TestDecideRetry_Deterministic_NeverRetries(t *testing.T) {
	job := &work.Job{Attempt: 1}
	d := decideRetry(FailureClassDeterministic, job, "application-level failure: exit code 1")
	if d.Retry {
		t.Error("DETERMINISTIC must never retry, regardless of attempt count")
	}
	if d.Reason != "application-level failure: exit code 1" {
		t.Errorf("Reason = %q, want the original reason unchanged (no RETRY_EXHAUSTED wrapping needed here)", d.Reason)
	}
}

// TestDecideRetry_ResourceObservation_NeverRetries is required test 1/7: an
// OOM (RESOURCE_OBSERVATION) must never be retried under the same resource
// conditions — see failureclass.go's wireStatus design note (Principle 1).
func TestDecideRetry_ResourceObservation_NeverRetries(t *testing.T) {
	job := &work.Job{Attempt: 1}
	d := decideRetry(FailureClassResourceObservation, job, "OOMKilled")
	if d.Retry {
		t.Error("RESOURCE_OBSERVATION must never retry — retrying under the same memory ceiling changes nothing")
	}
}

// TestDecideRetry_TransientInfra_RetriesUntilMaxAttempts is required test
// 2/7 (retried within bound) and 5/7 (RETRY_EXHAUSTED once the bound is hit).
func TestDecideRetry_TransientInfra_RetriesUntilMaxAttempts(t *testing.T) {
	useLowMaxAttempts(t, 3)

	job := &work.Job{Attempt: 1}
	d := decideRetry(FailureClassTransientInfra, job, "get smoke-run Job: timeout")
	if !d.Retry {
		t.Fatal("expected a retry while Attempt < maxAttempts")
	}
	if strings.Contains(d.Reason, retryExhaustedReason) {
		t.Errorf("Reason = %q should not yet mention %q", d.Reason, retryExhaustedReason)
	}

	job.Attempt = maxAttempts // this attempt has consumed the whole budget
	d = decideRetry(FailureClassTransientInfra, job, "get smoke-run Job: timeout")
	if d.Retry {
		t.Error("expected no further retry once Attempt >= maxAttempts")
	}
	if !strings.Contains(d.Reason, retryExhaustedReason) {
		t.Errorf("Reason = %q, want it to contain %q", d.Reason, retryExhaustedReason)
	}
}

// TestDecideRetry_Unknown_RetriesExactlyOnce is required test 4/7: UNKNOWN
// gets exactly one retry, using the total attempt budget (no separate
// counter) — see unknownRetryMarker's doc comment.
func TestDecideRetry_Unknown_RetriesExactlyOnce(t *testing.T) {
	useLowMaxAttempts(t, 5) // generous cap — this test is about UNKNOWN's own tighter limit, not maxAttempts

	job := &work.Job{Attempt: 1, LastError: ""}
	d := decideRetry(FailureClassUnknown, job, "can't inspect pods: no pods found")
	if !d.Retry {
		t.Fatal("expected UNKNOWN's first occurrence to be retried once")
	}
	if !strings.Contains(d.Reason, unknownRetryMarker) {
		t.Errorf("Reason = %q, want it to carry unknownRetryMarker so the next attempt can detect a repeat", d.Reason)
	}

	// Attempt 2: LastError is now whatever FailJob would have persisted from
	// attempt 1 — d.Reason, carrying the marker.
	job2 := &work.Job{Attempt: 2, LastError: d.Reason}
	d2 := decideRetry(FailureClassUnknown, job2, "can't inspect pods: no pods found (again)")
	if d2.Retry {
		t.Error("expected no retry for a second consecutive UNKNOWN classification")
	}
	if !strings.Contains(d2.Reason, unknownRetryLimitReason) {
		t.Errorf("Reason = %q, want it to contain %q", d2.Reason, unknownRetryLimitReason)
	}
}

// TestDecideRetry_Unknown_StillBoundByMaxAttempts verifies a *fresh* UNKNOWN
// (no marker in LastError) is still refused a retry once maxAttempts is
// already exhausted — UNKNOWN's one-retry allowance never grants a retry
// beyond the total budget.
func TestDecideRetry_Unknown_StillBoundByMaxAttempts(t *testing.T) {
	useLowMaxAttempts(t, 2)

	job := &work.Job{Attempt: 2, LastError: ""} // fresh UNKNOWN, but already at the cap
	d := decideRetry(FailureClassUnknown, job, "can't inspect pods: no pods found")
	if d.Retry {
		t.Error("expected no retry once Attempt >= maxAttempts, even for a fresh (non-consecutive) UNKNOWN")
	}
	if !strings.Contains(d.Reason, retryExhaustedReason) {
		t.Errorf("Reason = %q, want it to contain %q (the maxAttempts reason, not the UNKNOWN-specific one)", d.Reason, retryExhaustedReason)
	}
}

// --- FailureClass.wireStatus(): the wire mapping table (Principle 1) ---

func TestFailureClass_WireStatus(t *testing.T) {
	tests := []struct {
		class          FailureClass
		wantStatus     string
		wantFailureKnd string
	}{
		{FailureClassDeterministic, "failed", vaultclient.FailureKindApplication},
		{FailureClassResourceObservation, "failed", vaultclient.FailureKindApplication},
		{FailureClassTransientInfra, "infra_failed", vaultclient.FailureKindInfrastructure},
		{FailureClassUnknown, "infra_failed", vaultclient.FailureKindInfrastructure},
	}
	for _, tt := range tests {
		t.Run(string(tt.class), func(t *testing.T) {
			status, kind := tt.class.wireStatus()
			if status != tt.wantStatus {
				t.Errorf("validationStatus = %q, want %q", status, tt.wantStatus)
			}
			if kind != tt.wantFailureKnd {
				t.Errorf("failureKind = %q, want %q", kind, tt.wantFailureKnd)
			}
		})
	}
}

// TestFailureClass_WireStatus_ResourceObservation_NotInfraFailed pins down
// Principle 1's core requirement explicitly (beyond the table above): OOM
// must map to the *same* wire fields as a deterministic exit-code failure —
// NOT infra_failed — since infra_failed would wrongly tell NodeVault no
// valid observation was possible (see OBSERVED_PROFILE_SPEC.md §2.2's
// "inconclusive" vs §2.1's "observed"/contractCheck-failed).
func TestFailureClass_WireStatus_ResourceObservation_NotInfraFailed(t *testing.T) {
	status, _ := FailureClassResourceObservation.wireStatus()
	if status == "infra_failed" {
		t.Error("RESOURCE_OBSERVATION must not map to infra_failed — a valid observation *was* made")
	}
}
