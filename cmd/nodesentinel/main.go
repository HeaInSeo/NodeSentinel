package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/HeaInSeo/NodeSentinel/pkg/ingress"
	"github.com/HeaInSeo/NodeSentinel/pkg/work/sqlite"
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

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen on port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	nsv1.RegisterIngressServiceServer(grpcServer, ingress.NewServer(store))

	log.Printf("NodeSentinel ingress gRPC listening on :%s (db=%s)", port, dbPath)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve grpc: %v", err)
	}
}
