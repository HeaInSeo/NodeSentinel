package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

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

// TestGracefulShutdown_DrainsHTTPAndGRPCOnCtxCancel exercises main()'s actual
// shutdown wiring pattern: an errgroup where the HTTP and gRPC servers are
// each supervised by a goroutine that blocks on <-gCtx.Done() and then calls
// Shutdown/GracefulStop, alongside a goroutine that actually serves. This
// doesn't call main() itself (main has no ctx parameter and calls os.Exit,
// so it isn't directly testable), but it builds the same newHTTPServer this
// package ships and wires it with the identical shutdown pattern main() uses
// for both servers, then cancels the parent ctx (standing in for
// SIGINT/SIGTERM via signal.NotifyContext) and asserts:
//  1. g.Wait() returns within a bound - no hang, no deadlock.
//  2. Both servers are actually stopped afterward (HTTP: connection refused;
//     gRPC: GracefulStop returned, freeing the listener).
func TestGracefulShutdown_DrainsHTTPAndGRPCOnCtxCancel(t *testing.T) {
	m, err := metrics.New()
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	httpServer := newHTTPServer(m, "127.0.0.1:0")
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen http: %v", err)
	}
	httpAddr := httpLis.Addr().String()

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	grpcServer := grpc.NewServer()

	ctx, cancel := context.WithCancel(context.Background())
	g, gCtx := errgroup.WithContext(ctx)

	// Mirrors main()'s two supervised-shutdown + two supervised-serve
	// goroutines exactly.
	g.Go(func() error {
		<-gCtx.Done()
		grpcServer.GracefulStop()
		return nil
	})
	g.Go(func() error {
		if err := grpcServer.Serve(grpcLis); err != nil {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	})
	g.Go(func() error {
		if err := httpServer.Serve(httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	// Confirm both are actually up before triggering shutdown.
	if resp, err := http.Get("http://" + httpAddr + "/healthz"); err != nil {
		t.Fatalf("pre-shutdown healthz check: %v", err)
	} else {
		_ = resp.Body.Close()
	}

	cancel() // stand-in for SIGINT/SIGTERM cancelling main()'s ctx

	waitDone := make(chan error, 1)
	go func() { waitDone <- g.Wait() }()

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("g.Wait() after shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not complete within 10s - goroutines did not drain")
	}

	if resp, err := http.Get("http://" + httpAddr + "/healthz"); err == nil {
		_ = resp.Body.Close()
		t.Fatal("HTTP server still accepting connections after graceful shutdown")
	}
}
