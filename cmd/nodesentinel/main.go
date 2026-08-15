package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/HeaInSeo/NodeSentinel/pkg/ingress"
	"github.com/HeaInSeo/NodeSentinel/pkg/metrics"
	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
	"github.com/HeaInSeo/NodeSentinel/pkg/worker"
	nsv1 "github.com/HeaInSeo/NodeSentinel/protos/nodesentinel/v1"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbPath := os.Getenv("NODESENTINEL_DB_PATH")
	if dbPath == "" {
		dbPath = "./nodesentinel.db"
	}

	port, err := grpcPort()
	if err != nil {
		slog.Error("invalid gRPC port", "err", err)
		os.Exit(1)
	}
	listenAddr := net.JoinHostPort("", strconv.Itoa(port))

	httpPortNum, err := httpPort()
	if err != nil {
		slog.Error("invalid HTTP port", "err", err)
		os.Exit(1)
	}
	httpListenAddr := net.JoinHostPort("", strconv.Itoa(httpPortNum))

	m, err := metrics.New()
	if err != nil {
		slog.Error("initialize metrics", "err", err)
		os.Exit(1)
	}

	store, err := sqlite.New(dbPath)
	if err != nil {
		slog.Error("open work store", "err", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()

	// K8s client for L3/L4 worker (in-cluster preferred, kubeconfig fallback).
	kube, err := worker.NewKubeClient()
	if err != nil {
		slog.Warn("K8s client unavailable — worker will not run", "err", err)
		kube = nil
	}

	var listenConfig net.ListenConfig
	lis, err := listenConfig.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		slog.Error("listen for gRPC", "port", port, "err", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	nsv1.RegisterIngressServiceServer(grpcServer, ingress.NewServer(store))

	// g supervises every long-running loop (job leasing, result-delivery
	// retry, the gRPC server itself) so that any one of them exiting
	// unexpectedly — not just SIGINT/SIGTERM — is surfaced via g.Wait()
	// instead of leaving the process silently running degraded (e.g. still
	// serving gRPC ingress with no worker actually processing jobs). gCtx
	// is canceled both by the outer ctx (SIGINT/SIGTERM) and by any group
	// member returning a non-nil error, which is what drives the graceful
	// shutdown goroutine below on either trigger.
	g, gCtx := errgroup.WithContext(ctx)

	// Start L3/L4/L5 worker + its delivery-retry loop (only when K8s is
	// reachable) as two independent supervised goroutines — see
	// worker.Run's doc comment for why a NodeVault outage stalling
	// redelivery must never stall job leasing.
	if kube != nil {
		dynKube, dynErr := worker.NewDynamicKubeClient()
		if dynErr != nil {
			slog.Warn("dynamic K8s client unavailable — L5-b trivy scan will submit not-available records", "err", dynErr)
		}
		w := worker.New(store, kube, "nodesentinel-worker-0").
			WithVaultClient(vaultclient.New()).
			WithDynamicKubeClient(dynKube).
			WithMetrics(m)

		g.Go(func() error {
			slog.Info("worker started (L3/L4/L5)")
			err := w.Run(gCtx)
			slog.Info("worker stopped", "err", err)
			return err
		})
		g.Go(func() error {
			slog.Info("delivery retry loop started")
			err := w.RunDeliveryLoop(gCtx)
			slog.Info("delivery retry loop stopped", "err", err)
			return err
		})
	}

	g.Go(func() error {
		<-gCtx.Done()
		grpcServer.GracefulStop()
		return nil
	})
	g.Go(func() error {
		slog.Info("NodeSentinel ingress gRPC listening", "port", port)
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("serve grpc: %w", err)
		}
		return nil
	})

	// /healthz, /readyz, /metrics — see docs on newHTTPServer for what each
	// reports. Supervised the same way as the gRPC server: GracefulStop-style
	// shutdown on gCtx cancellation, and a non-nil Serve error propagates via
	// g.Wait() like any other loop.
	httpServer := newHTTPServer(m, httpListenAddr)
	g.Go(func() error {
		<-gCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	})
	g.Go(func() error {
		slog.Info("NodeSentinel HTTP (healthz/readyz/metrics) listening", "port", httpPortNum)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve http: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("nodesentinel exited with error", "err", err)
		os.Exit(1)
	}
}

// newHTTPServer builds the /healthz, /readyz, /metrics HTTP server, mirroring
// the pattern used by JUMI and artifact-handoff (see their pkg/metrics and
// cmd/*/main.go): /healthz and /readyz both simply report the process is up
// — NodeSentinel has no external dependency cheap enough to probe per
// request without its own design work, so a deeper readiness check (e.g.
// WorkStore connectivity) is left as a follow-up, not this change's scope.
func newHTTPServer(m *metrics.Metrics, listenAddr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok")); err != nil {
			slog.Debug("healthz response write failed", "err", err)
		}
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ready")); err != nil {
			slog.Debug("readyz response write failed", "err", err)
		}
	})
	mux.Handle("/metrics", m.Handler())
	return &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func grpcPort() (int, error) {
	return parsePort("NODESENTINEL_GRPC_PORT", 50052)
}

// httpPort returns the port for the /healthz, /readyz, /metrics HTTP server
// (NODESENTINEL_HTTP_PORT, default 8080 — matching JUMI's/artifact-handoff's
// convention for their HTTP port).
func httpPort() (int, error) {
	return parsePort("NODESENTINEL_HTTP_PORT", 8080)
}

func parsePort(envKey string, fallback int) (int, error) {
	value := os.Getenv(envKey)
	if value == "" {
		value = strconv.Itoa(fallback)
	}
	port, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	if port < 1 || port > 65535 {
		return 0, strconv.ErrSyntax
	}
	return port, nil
}
