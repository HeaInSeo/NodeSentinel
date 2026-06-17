package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
)

// newFakeVaultSrv returns a test HTTP server that accepts any POST and
// responds with a minimal 200 JSON body. It records each request path.
func newFakeVaultSrv(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(vaultclient.SubmitResponse{RecordID: "rec-1"})
	}))
	t.Cleanup(srv.Close)
	return srv, &paths
}

// buildVulnReport returns an unstructured VulnerabilityReport object that
// matches the given image digest, with the provided severity counts.
func buildVulnReport(name, namespace, digest, scanner string, critical, high, medium, low int) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "aquasecurity.github.io/v1alpha1",
			"kind":       "VulnerabilityReport",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"report": map[string]interface{}{
				"artifact": map[string]interface{}{
					"digest": digest,
				},
				"scanner": map[string]interface{}{
					"name":    scanner,
					"version": "0.50.0",
				},
				"summary": map[string]interface{}{
					"criticalCount": float64(critical),
					"highCount":     float64(high),
					"mediumCount":   float64(medium),
					"lowCount":      float64(low),
				},
			},
		},
	}
}

// newDynamicFake builds a fake dynamic client pre-seeded with the given objects.
func newDynamicFake(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	// Register the VulnerabilityReport list kind so the fake client can list it.
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{
			Group:   "aquasecurity.github.io",
			Version: "v1alpha1",
			Kind:    "VulnerabilityReportList",
		},
		&unstructured.UnstructuredList{},
	)
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{
			Group:   "aquasecurity.github.io",
			Version: "v1alpha1",
			Kind:    "VulnerabilityReport",
		},
		&unstructured.Unstructured{},
	)
	return dynamicfake.NewSimpleDynamicClient(scheme, objs...)
}

// TestRunL5b_NoDynamicClient_SubmitsNotAvailable verifies that when
// dynamicKube is nil, L5-b submits a "not-available" scan record.
func TestRunL5b_NoDynamicClient_SubmitsNotAvailable(t *testing.T) {
	srv, paths := newFakeVaultSrv(t)
	vc := vaultclient.NewWithAddr(srv.URL)

	kube := fake.NewClientset()
	w := New(newTestStore(t), kube, "test-worker").WithVaultClient(vc)
	// dynamicKube is deliberately nil

	job := makeTestWorkJob()
	if err := w.runL5b(context.Background(), slog.Default(), job); err != nil {
		t.Fatalf("runL5b returned unexpected error: %v", err)
	}
	if len(*paths) == 0 {
		t.Fatal("expected a scan record to be submitted, but no requests were received")
	}
	if (*paths)[0] != "/v1/validation/scan-records" {
		t.Errorf("expected POST to scan-records, got %q", (*paths)[0])
	}
}

// TestRunL5b_NoVaultClient_Skips verifies that when vaultClient is nil
// runL5b returns nil without error.
func TestRunL5b_NoVaultClient_Skips(t *testing.T) {
	kube := fake.NewClientset()
	w := New(newTestStore(t), kube, "test-worker")
	// No vault client set.
	err := w.runL5b(context.Background(), slog.Default(), makeTestWorkJob())
	if err != nil {
		t.Fatalf("expected nil when vaultClient is nil, got: %v", err)
	}
}

// TestRunL5b_MatchingReport_Passed verifies the happy path: a matching
// VulnerabilityReport with zero critical/high counts → policyResult="passed".
func TestRunL5b_MatchingReport_Passed(t *testing.T) {
	digest := "sha256:abc123"
	// Pass the object directly to newDynamicFake — it pre-seeds at construction.
	report := buildVulnReport("rep-1", smokeNamespace, digest, "trivy", 0, 0, 2, 10)
	dynClient := newDynamicFake(report)

	var capturedBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&capturedBody)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(vaultclient.SubmitResponse{RecordID: "r1"})
	}))
	t.Cleanup(srv.Close)

	vc := vaultclient.NewWithAddr(srv.URL)
	kube := fake.NewClientset()
	w2 := New(newTestStore(t), kube, "test-worker").
		WithVaultClient(vc).
		WithDynamicKubeClient(dynClient)

	job := makeTestWorkJob()
	if runErr := w2.runL5b(context.Background(), slog.Default(), job); runErr != nil {
		t.Fatalf("runL5b error: %v", runErr)
	}
	if capturedBody["policy_result"] != "passed" {
		t.Errorf("expected policy_result=passed, got %v", capturedBody["policy_result"])
	}
}

// TestRunL5b_MatchingReport_HighVulns_Warning_Regression verifies that
// Critical=0, High>0 yields policyResult="warning" (WARN regression).
func TestRunL5b_MatchingReport_HighVulns_Warning_Regression(t *testing.T) {
	digest := "sha256:abc123"
	report := buildVulnReport("rep-2", smokeNamespace, digest, "trivy", 0, 5, 0, 0)
	dynClient := newDynamicFake(report)

	var capturedBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&capturedBody)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(vaultclient.SubmitResponse{RecordID: "r1"})
	}))
	t.Cleanup(srv.Close)

	vc := vaultclient.NewWithAddr(srv.URL)
	kube := fake.NewClientset()
	w2 := New(newTestStore(t), kube, "test-worker").
		WithVaultClient(vc).
		WithDynamicKubeClient(dynClient)

	if runErr := w2.runL5b(context.Background(), slog.Default(), makeTestWorkJob()); runErr != nil {
		t.Fatalf("runL5b error: %v", runErr)
	}
	if capturedBody["policy_result"] != "warning" {
		t.Errorf("expected policy_result=warning for High=5, got %v", capturedBody["policy_result"])
	}
}

// TestRunL5b_NoMatchingReport_SubmitsNotAvailable verifies that when no
// VulnerabilityReport matches the image digest, a not-available record is sent.
func TestRunL5b_NoMatchingReport_SubmitsNotAvailable(t *testing.T) {
	// Seed a report for a different digest.
	report := buildVulnReport("rep-3", smokeNamespace, "sha256:other", "trivy", 0, 0, 0, 0)
	dynClient := newDynamicFake(report)

	var capturedBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&capturedBody)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(vaultclient.SubmitResponse{RecordID: "r1"})
	}))
	t.Cleanup(srv.Close)

	vc := vaultclient.NewWithAddr(srv.URL)
	kube := fake.NewClientset()
	w2 := New(newTestStore(t), kube, "test-worker").
		WithVaultClient(vc).
		WithDynamicKubeClient(dynClient)

	if runErr := w2.runL5b(context.Background(), slog.Default(), makeTestWorkJob()); runErr != nil {
		t.Fatalf("runL5b error: %v", runErr)
	}
	if capturedBody["policy_result"] != "not-available" {
		t.Errorf("expected policy_result=not-available, got %v", capturedBody["policy_result"])
	}
}

// TestRunL5b_EmptyScanner_SubmitsNotAvailable_Regression verifies that a
// VulnerabilityReport with a missing scanner name falls back to not-available
// (INFO regression for parseTrivySummary parse failure).
func TestRunL5b_EmptyScanner_SubmitsNotAvailable_Regression(t *testing.T) {
	// Build a report that matches the digest but has no scanner name.
	digest := "sha256:abc123"
	report := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "aquasecurity.github.io/v1alpha1",
			"kind":       "VulnerabilityReport",
			"metadata": map[string]interface{}{
				"name":      "rep-noscanner",
				"namespace": smokeNamespace,
			},
			"report": map[string]interface{}{
				"artifact": map[string]interface{}{
					"digest": digest,
				},
				"scanner": map[string]interface{}{
					// "name" deliberately absent
					"version": "0.50.0",
				},
				"summary": map[string]interface{}{
					"criticalCount": float64(0),
					"highCount":     float64(0),
				},
			},
		},
	}
	dynClient := newDynamicFake(report)

	var capturedBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&capturedBody)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(vaultclient.SubmitResponse{RecordID: "r1"})
	}))
	t.Cleanup(srv.Close)

	vc := vaultclient.NewWithAddr(srv.URL)
	kube := fake.NewClientset()
	w2 := New(newTestStore(t), kube, "test-worker").
		WithVaultClient(vc).
		WithDynamicKubeClient(dynClient)

	if runErr := w2.runL5b(context.Background(), slog.Default(), makeTestWorkJob()); runErr != nil {
		t.Fatalf("runL5b error: %v", runErr)
	}
	if capturedBody["policy_result"] != "not-available" {
		t.Errorf("expected policy_result=not-available for missing scanner, got %v", capturedBody["policy_result"])
	}
}

// TestRunL5b_VaultError_ReturnsError verifies that a vault submit failure
// propagates back as a non-nil error from runL5b.
func TestRunL5b_VaultError_ReturnsError(t *testing.T) {
	dynClient := newDynamicFake()
	// no reports in store → will take not-available path

	// Vault server is closed → connection refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	vc := vaultclient.NewWithAddr(srv.URL)

	kube := fake.NewClientset()
	w2 := New(newTestStore(t), kube, "test-worker").
		WithVaultClient(vc).
		WithDynamicKubeClient(dynClient)

	err := w2.runL5b(context.Background(), slog.Default(), makeTestWorkJob())
	if err == nil {
		t.Fatal("expected error when vault is unreachable, got nil")
	}
}
