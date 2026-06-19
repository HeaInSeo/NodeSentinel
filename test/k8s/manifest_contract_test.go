package k8s_test

import (
	"os"
	"strings"
	"testing"
)

func readManifest(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}
	return string(b)
}

func requireContains(t *testing.T, body, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("manifest missing %q", want)
	}
}

func TestNodeSentinelDeploymentContract(t *testing.T) {
	body := readManifest(t, "../../deploy/03-nodesentinel.yaml")

	for _, want := range []string{
		"kind: Deployment",
		"name: nodesentinel",
		"namespace: nodesentinel-system",
		"serviceAccountName: nodesentinel",
		"name: NODESENTINEL_GRPC_PORT",
		"value: \"50052\"",
		"name: NODEVAULT_API_ADDR",
		"http://nodevault-controlplane.nodevault-system.svc:8082",
		"name: SMOKE_NAMESPACE",
		"value: nodevault-smoke",
		"allowPrivilegeEscalation: false",
		"readOnlyRootFilesystem: true",
		"drop:",
		"- ALL",
		"kind: Service",
		"targetPort: grpc",
	} {
		requireContains(t, body, want)
	}

	if strings.Contains(body, ":latest") {
		t.Fatal("bori-facing manifest must not pin an implicit latest image")
	}
}

func TestNodeSentinelRBACContract(t *testing.T) {
	body := readManifest(t, "../../deploy/02-rbac.yaml")

	for _, want := range []string{
		"kind: ServiceAccount",
		"name: nodesentinel",
		"kind: ClusterRole",
		"resources: [\"jobs\"]",
		"verbs: [\"create\", \"get\", \"list\", \"watch\", \"delete\"]",
		"resources: [\"pods\"]",
		"resources: [\"pods/log\"]",
		"kind: ClusterRoleBinding",
		"namespace: nodesentinel-system",
	} {
		requireContains(t, body, want)
	}
}

func TestNodeSentinelGRPCRouteContract(t *testing.T) {
	body := readManifest(t, "../../deploy/01-grpcroute.yaml")

	for _, want := range []string{
		"kind: GRPCRoute",
		"name: nodesentinel-grpc",
		"name: nodesentinel",
		"port: 50052",
	} {
		requireContains(t, body, want)
	}
}
