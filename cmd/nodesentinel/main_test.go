package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/HeaInSeo/NodeSentinel/pkg/metrics"
)

func TestGRPCPortDefault(t *testing.T) {
	t.Setenv("NODESENTINEL_GRPC_PORT", "")

	port, err := grpcPort()
	if err != nil {
		t.Fatalf("grpcPort: %v", err)
	}
	if port != 50052 {
		t.Fatalf("port = %d, want 50052", port)
	}
}

func TestGRPCPortFromEnv(t *testing.T) {
	t.Setenv("NODESENTINEL_GRPC_PORT", "6000")

	port, err := grpcPort()
	if err != nil {
		t.Fatalf("grpcPort: %v", err)
	}
	if port != 6000 {
		t.Fatalf("port = %d, want 6000", port)
	}
}

func TestGRPCPortRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"not-a-port", "0", "65536"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("NODESENTINEL_GRPC_PORT", value)

			if _, err := grpcPort(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestHTTPPortDefault(t *testing.T) {
	t.Setenv("NODESENTINEL_HTTP_PORT", "")

	port, err := httpPort()
	if err != nil {
		t.Fatalf("httpPort: %v", err)
	}
	if port != 8080 {
		t.Fatalf("port = %d, want 8080", port)
	}
}

func TestHTTPPortFromEnv(t *testing.T) {
	t.Setenv("NODESENTINEL_HTTP_PORT", "9091")

	port, err := httpPort()
	if err != nil {
		t.Fatalf("httpPort: %v", err)
	}
	if port != 9091 {
		t.Fatalf("port = %d, want 9091", port)
	}
}

func TestHTTPPortRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"not-a-port", "0", "65536"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("NODESENTINEL_HTTP_PORT", value)

			if _, err := httpPort(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestNewHTTPServer_HealthzReadyzMetrics is a regression guard for issue #6:
// NodeSentinel had no liveness/readiness probes and no /metrics endpoint,
// unlike JUMI/artifact-handoff/NodeVault. This locks in that all three are
// wired into the mux newHTTPServer builds.
func TestNewHTTPServer_HealthzReadyzMetrics(t *testing.T) {
	m, err := metrics.New()
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	srv := newHTTPServer(m, ":0")

	for _, tc := range []struct {
		path       string
		wantStatus int
	}{
		{"/healthz", http.StatusOK},
		{"/readyz", http.StatusOK},
		{"/metrics", http.StatusOK},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.wantStatus {
				t.Errorf("%s status = %d, want %d", tc.path, rec.Code, tc.wantStatus)
			}
		})
	}
}
