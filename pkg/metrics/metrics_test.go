package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsHandlerExportsPipelineCounters(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	m.IncJobsLeased()
	m.IncJobsCompleted()
	m.IncJobsFailed()
	m.IncL5aSubmitted()
	m.IncL5aErrors()
	m.IncL5bSubmitted()
	m.IncL5bErrors()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	for _, want := range []string{
		"nodesentinel_jobs_leased_total",
		"nodesentinel_jobs_completed_total",
		"nodesentinel_jobs_failed_total",
		"nodesentinel_l5a_check_records_submitted_total",
		"nodesentinel_l5a_submit_errors_total",
		"nodesentinel_l5b_scan_records_submitted_total",
		"nodesentinel_l5b_submit_errors_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in Prometheus output:\n%s", want, body)
		}
	}
}

// TestMetricsHandlerExportsProcessCollectors verifies the standard
// process/Go runtime collectors are registered, so /metrics is meaningful
// (basic process health: memory, goroutines, GC) even before any pipeline
// counter has ever incremented — see issue #6.
func TestMetricsHandlerExportsProcessCollectors(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	for _, want := range []string{
		"go_goroutines",
		"process_resident_memory_bytes",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in Prometheus output (expected default process/Go collectors):\n%s", want, body)
		}
	}
}

func TestMetricsHandlerReturns200(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if rec.Code != 200 {
		t.Fatalf("handler returned %d: %s", rec.Code, rec.Body.String())
	}
}
