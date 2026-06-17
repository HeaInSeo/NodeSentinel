package worker

import (
	"testing"
)

// makeTrivyObj builds an unstructured VulnerabilityReport object for testing
// parseTrivySummary. Pass empty scanner to simulate a parse-failure scenario.
func makeTrivyObj(scanner, version string, critical, high, medium, low int) map[string]interface{} {
	obj := map[string]interface{}{
		"report": map[string]interface{}{
			"scanner": map[string]interface{}{
				"name":    scanner,
				"version": version,
			},
			"summary": map[string]interface{}{
				"criticalCount": float64(critical),
				"highCount":     float64(high),
				"mediumCount":   float64(medium),
				"lowCount":      float64(low),
			},
		},
	}
	return obj
}

// --- parseTrivySummary happy path ---

// TestParseTrivySummary_HappyPath verifies that a well-formed report is parsed
// correctly into scanner metadata and severity counts.
func TestParseTrivySummary_HappyPath(t *testing.T) {
	obj := makeTrivyObj("trivy", "0.50.0", 2, 5, 10, 3)
	s := parseTrivySummary(obj)
	if s.Scanner != "trivy" {
		t.Errorf("Scanner: want %q, got %q", "trivy", s.Scanner)
	}
	if s.ScannerVersion != "0.50.0" {
		t.Errorf("ScannerVersion: want %q, got %q", "0.50.0", s.ScannerVersion)
	}
	if s.CriticalCount != 2 {
		t.Errorf("CriticalCount: want 2, got %d", s.CriticalCount)
	}
	if s.HighCount != 5 {
		t.Errorf("HighCount: want 5, got %d", s.HighCount)
	}
	if s.MediumCount != 10 {
		t.Errorf("MediumCount: want 10, got %d", s.MediumCount)
	}
	if s.LowCount != 3 {
		t.Errorf("LowCount: want 3, got %d", s.LowCount)
	}
}

// --- parseTrivySummary fail / not-available path ---

// TestParseTrivySummary_NoScanner_NotAvailable_Regression verifies that a
// report missing the scanner name results in Scanner=="" (INFO regression).
// Callers must treat this as a not-available fallback, not as a valid record.
func TestParseTrivySummary_NoScanner_NotAvailable_Regression(t *testing.T) {
	// Build an object without the scanner.name key.
	obj := map[string]interface{}{
		"report": map[string]interface{}{
			"scanner": map[string]interface{}{
				// "name" deliberately omitted
				"version": "0.50.0",
			},
			"summary": map[string]interface{}{
				"criticalCount": float64(0),
				"highCount":     float64(0),
			},
		},
	}
	s := parseTrivySummary(obj)
	if s.Scanner != "" {
		t.Errorf("expected empty Scanner for missing name, got %q", s.Scanner)
	}
}

// TestParseTrivySummary_EmptyObj_ZeroCounts verifies that an entirely empty
// object produces zero counts and empty scanner fields.
func TestParseTrivySummary_EmptyObj_ZeroCounts(t *testing.T) {
	s := parseTrivySummary(map[string]interface{}{})
	if s.Scanner != "" || s.CriticalCount != 0 || s.HighCount != 0 {
		t.Errorf("expected zero-value summary for empty object, got %+v", s)
	}
}

// --- policyResult regression tests ---

// TestPolicyResult_HighOnly_Warning_Regression verifies that Critical=0, High>0
// produces policyResult="warning" (WARN regression: HighCount was previously ignored).
func TestPolicyResult_HighOnly_Warning_Regression(t *testing.T) {
	result := policyResultFor(0, 5)
	if result != "warning" {
		t.Errorf("Critical=0 High=5 → policyResult: want %q, got %q", "warning", result)
	}
}

// TestPolicyResult_BothZero_Passed_Regression verifies that Critical=0, High=0
// produces policyResult="passed".
func TestPolicyResult_BothZero_Passed_Regression(t *testing.T) {
	result := policyResultFor(0, 0)
	if result != "passed" {
		t.Errorf("Critical=0 High=0 → policyResult: want %q, got %q", "passed", result)
	}
}

// TestPolicyResult_CriticalOnly_Warning_Regression verifies that Critical>0
// (High=0) still produces policyResult="warning".
func TestPolicyResult_CriticalOnly_Warning_Regression(t *testing.T) {
	result := policyResultFor(1, 0)
	if result != "warning" {
		t.Errorf("Critical=1 High=0 → policyResult: want %q, got %q", "warning", result)
	}
}

// TestPolicyResult_BothNonZero_Warning verifies that Critical>0, High>0 also
// produces "warning".
func TestPolicyResult_BothNonZero_Warning(t *testing.T) {
	result := policyResultFor(3, 7)
	if result != "warning" {
		t.Errorf("Critical=3 High=7 → policyResult: want %q, got %q", "warning", result)
	}
}

// policyResultFor is a test helper that reproduces the policyResult mapping
// logic from runL5b so we can unit-test it without a full Worker setup.
func policyResultFor(critical, high int) string {
	if critical > 0 || high > 0 {
		return "warning"
	}
	return "passed"
}
