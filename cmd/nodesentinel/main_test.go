package main

import "testing"

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
