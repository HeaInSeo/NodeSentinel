package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

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

	// Start L3/L4/L5 worker goroutine (only when K8s is reachable).
	if kube != nil {
		dynKube, dynErr := worker.NewDynamicKubeClient()
		if dynErr != nil {
			slog.Warn("dynamic K8s client unavailable — L5-b trivy scan will submit not-available records", "err", dynErr)
		}
		w := worker.New(store, kube, "nodesentinel-worker-0").
			WithVaultClient(vaultclient.New()).
			WithDynamicKubeClient(dynKube)
		go func() {
			slog.Info("worker started (L3/L4/L5)")
			w.Run(ctx)
			slog.Info("worker stopped")
		}()
	}

	var listenConfig net.ListenConfig
	lis, err := listenConfig.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		slog.Error("listen for gRPC", "port", port, "err", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	nsv1.RegisterIngressServiceServer(grpcServer, ingress.NewServer(store))

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	slog.Info("NodeSentinel ingress gRPC listening", "port", port)
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("serve grpc", "err", err)
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
