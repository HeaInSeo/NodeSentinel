package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/HeaInSeo/NodeSentinel/pkg/ingress"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
	"github.com/HeaInSeo/NodeSentinel/pkg/worker"
	nsv1 "github.com/HeaInSeo/NodeSentinel/protos/nodesentinel/v1"
)

func main() {
	dbPath := os.Getenv("NODESENTINEL_DB_PATH")
	if dbPath == "" {
		dbPath = "./nodesentinel.db"
	}

	port := os.Getenv("NODESENTINEL_GRPC_PORT")
	if port == "" {
		port = "50052"
	}

	store, err := sqlite.New(dbPath)
	if err != nil {
		log.Fatalf("open work store at %q: %v", dbPath, err)
	}
	defer store.Close()

	// K8s client for L3/L4 worker (in-cluster preferred, kubeconfig fallback).
	kube, err := worker.NewKubeClient()
	if err != nil {
		slog.Warn("K8s client unavailable — worker will not run", "err", err)
		kube = nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start L3/L4 worker goroutine (only when K8s is reachable).
	if kube != nil {
		w := worker.New(store, kube, "nodesentinel-worker-0")
		go func() {
			slog.Info("L3/L4 worker started")
			w.Run(ctx)
			slog.Info("L3/L4 worker stopped")
		}()
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen on port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	nsv1.RegisterIngressServiceServer(grpcServer, ingress.NewServer(store))

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	log.Printf("NodeSentinel ingress gRPC listening on :%s (db=%s)", port, dbPath)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve grpc: %v", err)
	}
}
