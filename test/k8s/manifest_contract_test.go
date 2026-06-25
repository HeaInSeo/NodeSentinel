package k8s_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

func readManifestObjects(t *testing.T, path string) []unstructured.Unstructured {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}

	parts := strings.Split(string(body), "\n---")
	objects := make([]unstructured.Unstructured, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		var obj map[string]any
		if err := yaml.Unmarshal([]byte(part), &obj); err != nil {
			t.Fatalf("decode manifest %s: %v", path, err)
		}
		objects = append(objects, unstructured.Unstructured{Object: obj})
	}
	return objects
}

func findObject(t *testing.T, objects []unstructured.Unstructured, kind, name string) unstructured.Unstructured {
	t.Helper()

	for _, obj := range objects {
		if obj.GetKind() == kind && obj.GetName() == name {
			return obj
		}
	}
	t.Fatalf("manifest missing %s/%s", kind, name)
	return unstructured.Unstructured{}
}

func nestedString(t *testing.T, obj map[string]any, fields ...string) string {
	t.Helper()

	value, ok, err := unstructured.NestedString(obj, fields...)
	if err != nil {
		t.Fatalf("read %s: %v", strings.Join(fields, "."), err)
	}
	if !ok {
		t.Fatalf("missing %s", strings.Join(fields, "."))
	}
	return value
}

func nestedBool(t *testing.T, obj map[string]any, fields ...string) bool {
	t.Helper()

	value, ok, err := unstructured.NestedBool(obj, fields...)
	if err != nil {
		t.Fatalf("read %s: %v", strings.Join(fields, "."), err)
	}
	if !ok {
		t.Fatalf("missing %s", strings.Join(fields, "."))
	}
	return value
}

func nestedSlice(t *testing.T, obj map[string]any, fields ...string) []any {
	t.Helper()

	value, ok, err := unstructured.NestedSlice(obj, fields...)
	if err != nil {
		t.Fatalf("read %s: %v", strings.Join(fields, "."), err)
	}
	if !ok {
		t.Fatalf("missing %s", strings.Join(fields, "."))
	}
	return value
}

func nestedInt64(t *testing.T, obj map[string]any, fields ...string) int64 {
	t.Helper()

	value, ok, err := unstructured.NestedFieldNoCopy(obj, fields...)
	if err != nil {
		t.Fatalf("read %s: %v", strings.Join(fields, "."), err)
	}
	if !ok {
		t.Fatalf("missing %s", strings.Join(fields, "."))
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		if v != float64(int64(v)) {
			t.Fatalf("%s = %v is not an integer", strings.Join(fields, "."), v)
		}
		return int64(v)
	default:
		t.Fatalf("expected integer at %s, got %T (%s)", strings.Join(fields, "."), value, fmt.Sprint(value))
		return 0
	}
}

func objectMap(t *testing.T, value any) map[string]any {
	t.Helper()

	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected object map, got %T", value)
	}
	return m
}

func stringSlice(t *testing.T, values []any) []string {
	t.Helper()

	out := make([]string, 0, len(values))
	for _, value := range values {
		s, ok := value.(string)
		if !ok {
			t.Fatalf("expected string slice item, got %T", value)
		}
		out = append(out, s)
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestNodeSentinelDeploymentContract(t *testing.T) {
	objects := readManifestObjects(t, "../../deploy/03-nodesentinel.yaml")
	deployment := findObject(t, objects, "Deployment", "nodesentinel")
	service := findObject(t, objects, "Service", "nodesentinel")

	if deployment.GetNamespace() != "nodesentinel-system" {
		t.Fatalf("deployment namespace = %q, want nodesentinel-system", deployment.GetNamespace())
	}
	if got := nestedString(t, deployment.Object, "spec", "template", "spec", "serviceAccountName"); got != "nodesentinel" {
		t.Fatalf("serviceAccountName = %q, want nodesentinel", got)
	}
	if got := nestedBool(t, deployment.Object, "spec", "template", "spec", "securityContext", "runAsNonRoot"); !got {
		t.Fatal("pod securityContext.runAsNonRoot must be true")
	}
	if got := nestedString(t, deployment.Object, "spec", "template", "spec", "securityContext", "seccompProfile", "type"); got != "RuntimeDefault" {
		t.Fatalf("seccompProfile.type = %q, want RuntimeDefault", got)
	}

	containers := nestedSlice(t, deployment.Object, "spec", "template", "spec", "containers")
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(containers))
	}
	container := objectMap(t, containers[0])
	if got := nestedString(t, container, "image"); strings.HasSuffix(got, ":latest") {
		t.Fatalf("bori-facing manifest must not pin an implicit latest image: %s", got)
	}
	if got := nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"); got {
		t.Fatal("container securityContext.allowPrivilegeEscalation must be false")
	}
	if got := nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"); !got {
		t.Fatal("container securityContext.readOnlyRootFilesystem must be true")
	}
	dropped := stringSlice(t, nestedSlice(t, container, "securityContext", "capabilities", "drop"))
	if !containsString(dropped, "ALL") {
		t.Fatalf("container must drop all capabilities, got %v", dropped)
	}
	if got := nestedString(t, container, "resources", "requests", "cpu"); got == "" {
		t.Fatal("container must declare CPU requests")
	}
	if got := nestedString(t, container, "resources", "limits", "memory"); got == "" {
		t.Fatal("container must declare memory limits")
	}

	env := nestedSlice(t, container, "env")
	requiredEnv := map[string]string{
		"NODESENTINEL_GRPC_PORT": "50052",
		"NODEVAULT_API_ADDR":     "http://nodevault-controlplane.nodevault-system.svc:8082",
		"SMOKE_NAMESPACE":        "nodevault-smoke",
	}
	for _, item := range env {
		envVar := objectMap(t, item)
		name := nestedString(t, envVar, "name")
		if want, ok := requiredEnv[name]; ok {
			if got := nestedString(t, envVar, "value"); got != want {
				t.Fatalf("env %s = %q, want %q", name, got, want)
			}
			delete(requiredEnv, name)
		}
	}
	if len(requiredEnv) != 0 {
		t.Fatalf("missing env vars: %v", requiredEnv)
	}

	if service.GetNamespace() != "nodesentinel-system" {
		t.Fatalf("service namespace = %q, want nodesentinel-system", service.GetNamespace())
	}
	ports := nestedSlice(t, service.Object, "spec", "ports")
	if len(ports) != 1 {
		t.Fatalf("service ports = %d, want 1", len(ports))
	}
	port := objectMap(t, ports[0])
	if got := nestedString(t, port, "targetPort"); got != "grpc" {
		t.Fatalf("service targetPort = %q, want grpc", got)
	}
}

func TestNodeSentinelRBACContract(t *testing.T) {
	objects := readManifestObjects(t, "../../deploy/02-rbac.yaml")
	serviceAccount := findObject(t, objects, "ServiceAccount", "nodesentinel")
	clusterRole := findObject(t, objects, "ClusterRole", "nodesentinel-worker")
	binding := findObject(t, objects, "ClusterRoleBinding", "nodesentinel-worker")

	if serviceAccount.GetNamespace() != "nodesentinel-system" {
		t.Fatalf("service account namespace = %q, want nodesentinel-system", serviceAccount.GetNamespace())
	}

	rules := nestedSlice(t, clusterRole.Object, "rules")
	if len(rules) == 0 {
		t.Fatal("cluster role must contain rules")
	}
	for _, rawRule := range rules {
		rule := objectMap(t, rawRule)
		resources := stringSlice(t, nestedSlice(t, rule, "resources"))
		verbs := stringSlice(t, nestedSlice(t, rule, "verbs"))
		if containsString(resources, "*") || containsString(verbs, "*") {
			t.Fatalf("rbac rules must not use wildcards: resources=%v verbs=%v", resources, verbs)
		}
	}

	requiredResources := map[string]bool{"jobs": false, "pods": false, "pods/log": false}
	for _, rawRule := range rules {
		for _, resource := range stringSlice(t, nestedSlice(t, objectMap(t, rawRule), "resources")) {
			if _, ok := requiredResources[resource]; ok {
				requiredResources[resource] = true
			}
		}
	}
	for resource, found := range requiredResources {
		if !found {
			t.Fatalf("cluster role missing resource %q", resource)
		}
	}

	subjects := nestedSlice(t, binding.Object, "subjects")
	if len(subjects) != 1 {
		t.Fatalf("cluster role binding subjects = %d, want 1", len(subjects))
	}
	subject := objectMap(t, subjects[0])
	if got := nestedString(t, subject, "namespace"); got != "nodesentinel-system" {
		t.Fatalf("binding subject namespace = %q, want nodesentinel-system", got)
	}
}

func TestNodeSentinelGRPCRouteContract(t *testing.T) {
	objects := readManifestObjects(t, "../../deploy/01-grpcroute.yaml")
	route := findObject(t, objects, "GRPCRoute", "nodesentinel-grpc")

	rules := nestedSlice(t, route.Object, "spec", "rules")
	if len(rules) != 1 {
		t.Fatalf("grpc route rules = %d, want 1", len(rules))
	}
	backends := nestedSlice(t, objectMap(t, rules[0]), "backendRefs")
	if len(backends) != 1 {
		t.Fatalf("grpc route backendRefs = %d, want 1", len(backends))
	}
	backend := objectMap(t, backends[0])
	if got := nestedString(t, backend, "name"); got != "nodesentinel" {
		t.Fatalf("backend name = %q, want nodesentinel", got)
	}
	if got := nestedInt64(t, backend, "port"); got != 50052 {
		t.Fatalf("backend port = %d, want 50052", got)
	}
}
