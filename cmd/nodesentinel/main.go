package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/HeaInSeo/NodeSentinel/pkg/ingress"
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
			WithDynamicKubeClient(dynKube)

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

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("nodesentinel exited with error", "err", err)
		os.Exit(1)
	}
}

func grpcPort() (int, error) {
	value := os.Getenv("NODESENTINEL_GRPC_PORT")
	if value == "" {
		value = "50052"
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
