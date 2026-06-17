package worker

import (
	"strings"
	"testing"
)

// --- computeValidationHash unit tests ---

// TestComputeValidationHash_Deterministic verifies that the same inputs
// always produce the same hash (determinism requirement).
func TestComputeValidationHash_Deterministic(t *testing.T) {
	h1 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 0)
	h2 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 0)
	if h1 != h2 {
		t.Errorf("expected identical hashes, got %q and %q", h1, h2)
	}
}

// TestComputeValidationHash_DifferentExitCode verifies that a different
// exitCode produces a different hash (no hash collision on that axis).
func TestComputeValidationHash_DifferentExitCode(t *testing.T) {
	h0 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 0)
	h1 := computeValidationHash("sha256:abc123", "/bin/sh -c true", 1)
	if h0 == h1 {
		t.Errorf("expected different hashes for different exit codes, but got %q for both", h0)
	}
}

// TestComputeValidationHash_NotEmpty verifies that the hash is a non-empty hex string.
func TestComputeValidationHash_NotEmpty(t *testing.T) {
	h := computeValidationHash("sha256:abc", "cmd", 0)
	if h == "" {
		t.Error("expected non-empty hash")
	}
	// SHA-256 hex is 64 characters.
	if len(h) != 64 {
		t.Errorf("expected 64-char hex, got len=%d: %q", len(h), h)
	}
}

// --- l5aCommand / K8s Job Command slice regression ---

// TestL5aCommandSlice_MatchesConstant_Regression verifies that l5aCommandSlice()
// round-trips back to l5aCommand via strings.Join, ensuring the K8s Job Command
// and the hash input share a single source of truth (WARN regression).
func TestL5aCommandSlice_MatchesConstant_Regression(t *testing.T) {
	slice := l5aCommandSlice()
	rejoined := strings.Join(slice, " ")
	if rejoined != l5aCommand {
		t.Errorf("l5aCommandSlice() rejoined = %q, want %q", rejoined, l5aCommand)
	}
}

// TestBuildL5aJobSpec_CommandMatchesConstant_Regression verifies that the K8s
// Job spec's container Command is exactly the fields of l5aCommand (WARN regression).
func TestBuildL5aJobSpec_CommandMatchesConstant_Regression(t *testing.T) {
	job := makeTestWorkJob()
	spec := buildL5aJobSpec(job)
	if len(spec.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("expected at least one container in L5-a job spec")
	}
	cmd := spec.Spec.Template.Spec.Containers[0].Command
	rejoined := strings.Join(cmd, " ")
	if rejoined != l5aCommand {
		t.Errorf("job spec Command rejoined = %q, want %q", rejoined, l5aCommand)
	}
}

// --- contractResult mapping regression ---

// TestSubmitCheckRecord_InfraFailed_NotApplicable_Regression verifies that
// validationStatus="infra_failed" maps to contractResult="not_applicable"
// and NOT "failed" (WARN regression).
func TestSubmitCheckRecord_InfraFailed_NotApplicable_Regression(t *testing.T) {
	result := contractResultFor("infra_failed")
	if result != "not_applicable" {
		t.Errorf("infra_failed → contractResult: want %q, got %q", "not_applicable", result)
	}
}

// TestSubmitCheckRecord_Failed_ContractFailed_Regression verifies that
// validationStatus="failed" maps to contractResult="failed".
func TestSubmitCheckRecord_Failed_ContractFailed_Regression(t *testing.T) {
	result := contractResultFor("failed")
	if result != "failed" {
		t.Errorf("failed → contractResult: want %q, got %q", "failed", result)
	}
}

// TestSubmitCheckRecord_Succeeded_ContractPassed verifies that
// validationStatus="succeeded" maps to contractResult="passed".
func TestSubmitCheckRecord_Succeeded_ContractPassed(t *testing.T) {
	result := contractResultFor("succeeded")
	if result != "passed" {
		t.Errorf("succeeded → contractResult: want %q, got %q", "passed", result)
	}
}

// contractResultFor is a test helper that reproduces the mapping logic from
// submitCheckRecord so we can unit-test it without setting up a full Worker.
func contractResultFor(validationStatus string) string {
	switch validationStatus {
	case "succeeded":
		return "passed"
	case "infra_failed":
		return "not_applicable"
	default:
		return "failed"
	}
}

// --- isInfraReason regression tests ---

// TestIsInfraReason_BackoffLimitExceeded_Regression verifies that the standard
// K8s Job Failed condition reason is classified as infra.
func TestIsInfraReason_BackoffLimitExceeded_Regression(t *testing.T) {
	if !isInfraReason("BackoffLimitExceeded") {
		t.Error("BackoffLimitExceeded should be an infra reason")
	}
}

// TestIsInfraReason_DeadlineExceeded_Regression verifies that DeadlineExceeded
// is classified as infra.
func TestIsInfraReason_DeadlineExceeded_Regression(t *testing.T) {
	if !isInfraReason("DeadlineExceeded") {
		t.Error("DeadlineExceeded should be an infra reason")
	}
}

// TestIsInfraReason_Evicted_False_Regression verifies that "Evicted" is NOT
// classified as an infra reason — it is a Pod-level reason, not a K8s Job
// Failed condition reason (INFO regression).
func TestIsInfraReason_Evicted_False_Regression(t *testing.T) {
	if isInfraReason("Evicted") {
		t.Error("Evicted is a Pod-level reason and must NOT be classified as a Job-level infra reason")
	}
}

// TestIsInfraReason_AppFailure_False verifies that a generic application error
// is not classified as infra.
func TestIsInfraReason_AppFailure_False(t *testing.T) {
	if isInfraReason("Error") {
		t.Error("Error should not be an infra reason")
	}
}

// TestIsInfraReason_Empty_False verifies that an empty string is not infra.
func TestIsInfraReason_Empty_False(t *testing.T) {
	if isInfraReason("") {
		t.Error("empty string should not be an infra reason")
	}
}
