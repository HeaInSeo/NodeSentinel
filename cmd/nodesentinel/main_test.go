package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/HeaInSeo/NodeSentinel/pkg/metrics"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
)

// TestWorkerIdentityUniquePerProcess guards against the constant lease owner
// ("nodesentinel-worker-0") every Pod generation used to share: two identities
// minted in the same host must differ, so a replacement process never
// inherits its predecessor's lease owner.
func TestWorkerIdentityUniquePerProcess(t *testing.T) {
	a, err := workerIdentity()
	if err != nil {
		t.Fatalf("workerIdentity: %v", err)
	}
	b, err := workerIdentity()
	if err != nil {
		t.Fatalf("workerIdentity: %v", err)
	}
	if a == b {
		t.Fatalf("workerIdentity returned %q twice; want a per-process unique owner", a)
	}
	if a == "nodesentinel-worker-0" {
		t.Fatal("workerIdentity returned the legacy constant owner")
	}
}

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
	srv := newHTTPServer(m, ":0", alwaysReady)

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

func alwaysReady(context.Context) error { return nil }

func getStatus(t *testing.T, srv *http.Server, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

// /readyz follows the readiness check both ways: 503 with the reason while it
// fails, 200 once it passes again. /healthz is liveness and stays 200, so a
// store outage makes the Pod unready without restarting it.
func TestNewHTTPServer_ReadyzFollowsReadinessAndHealthzDoesNot(t *testing.T) {
	m, err := metrics.New()
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	failure := errors.New("work store is not writable: disk full")
	srv := newHTTPServer(m, ":0", func(context.Context) error { return failure })

	if code, body := getStatus(t, srv, "/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "disk full") {
		t.Fatalf("/readyz while failing = %d %q, want 503 naming the reason", code, body)
	}
	if code, _ := getStatus(t, srv, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz while not ready = %d, want 200 (liveness is process-only)", code)
	}
	failure = nil
	if code, _ := getStatus(t, srv, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz after recovery = %d, want 200", code)
	}
}

// fakeProber is a writeProber whose result the test sets.
type fakeProber struct{ err error }

func (f *fakeProber) CheckWritable(context.Context) error { return f.err }

func TestReadiness_RequiresWorkerLoopsAndWritableStore(t *testing.T) {
	prober := &fakeProber{}
	r := &readiness{store: prober, workerEnabled: true}
	ctx := context.Background()

	if err := r.check(ctx); err == nil || !strings.Contains(err.Error(), "job worker") {
		t.Fatalf("before the worker loop starts: %v, want not ready (job worker)", err)
	}
	r.workerRunning.Store(true)
	if err := r.check(ctx); err == nil || !strings.Contains(err.Error(), "delivery") {
		t.Fatalf("worker running, delivery loop not: %v, want not ready (delivery)", err)
	}
	r.deliveryRunning.Store(true)
	if err := r.check(ctx); err != nil {
		t.Fatalf("all running, store writable: %v, want ready", err)
	}

	prober.err = errors.New("attempt to write a readonly database")
	if err := r.check(ctx); err == nil || !strings.Contains(err.Error(), "not writable") {
		t.Fatalf("store rejects writes: %v, want not ready (not writable)", err)
	}
	prober.err = nil
	if err := r.check(ctx); err != nil {
		t.Fatalf("store recovered: %v, want ready", err)
	}

	r.workerRunning.Store(false) // the loop exited
	if err := r.check(ctx); err == nil {
		t.Fatal("worker loop exited: ready, want not ready")
	}

	disabled := &readiness{store: &fakeProber{}}
	disabled.workerRunning.Store(true)
	disabled.deliveryRunning.Store(true)
	if err := disabled.check(ctx); err == nil || !strings.Contains(err.Error(), "Kubernetes") {
		t.Fatalf("no K8s client: %v, want not ready (worker disabled)", err)
	}
}

// End to end over a real store: /readyz is 200 while the store commits the
// probe write and 503 once it cannot (here, closed). The read-only case is
// covered in pkg/work/sqlite's CheckWritable tests.
func TestReadyz_RealStore_ClosedStoreIsNotReady(t *testing.T) {
	m, err := metrics.New()
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	store, err := sqlite.New(filepath.Join(t.TempDir(), "ns.sqlite"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	r := &readiness{store: store, workerEnabled: true}
	r.workerRunning.Store(true)
	r.deliveryRunning.Store(true)
	srv := newHTTPServer(m, ":0", r.check)

	if code, body := getStatus(t, srv, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz on a healthy store = %d %q, want 200", code, body)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if code, body := getStatus(t, srv, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz on a closed store = %d %q, want 503", code, body)
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
	httpServer := newHTTPServer(m, "127.0.0.1:0", alwaysReady)
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
