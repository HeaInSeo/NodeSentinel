package vaultclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestClient returns a Client pointed at the given server URL.
func newTestClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{},
	}
}

// makeCheckReq returns a minimal SubmitCheckRecordRequest for testing.
func makeCheckReq() SubmitCheckRecordRequest {
	return SubmitCheckRecordRequest{
		CheckID:          "test-check-1",
		ImageDigest:      "sha256:abc123",
		ValidationStatus: "succeeded",
		ContractResult:   "passed",
	}
}

// makeScanReq returns a minimal SubmitScanRecordRequest for testing.
func makeScanReq() SubmitScanRecordRequest {
	return SubmitScanRecordRequest{
		ScanID:       "test-scan-1",
		ImageDigest:  "sha256:abc123",
		PolicyMode:   "record_only",
		PolicyResult: "passed",
	}
}

// --- Happy path tests ---

// TestSubmitCheckRecord_HappyPath verifies that a 2xx response is decoded correctly.
func TestSubmitCheckRecord_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method: %s", r.Method)
		}
		if r.URL.Path != checkRecordsPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(SubmitResponse{
			RecordID:            "rec-001",
			CertificationStatus: "pending",
		})
	}))
	defer srv.Close()

	c := newTestClient(srv.URL)
	resp, err := c.SubmitCheckRecord(context.Background(), makeCheckReq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RecordID != "rec-001" {
		t.Errorf("expected RecordID=rec-001, got %q", resp.RecordID)
	}
}

// TestSubmitScanRecord_HappyPath verifies that a 2xx response is decoded correctly.
func TestSubmitScanRecord_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != scanRecordsPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SubmitResponse{
			RecordID:            "scan-001",
			CertificationStatus: "scanned",
		})
	}))
	defer srv.Close()

	c := newTestClient(srv.URL)
	resp, err := c.SubmitScanRecord(context.Background(), makeScanReq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RecordID != "scan-001" {
		t.Errorf("expected RecordID=scan-001, got %q", resp.RecordID)
	}
}

// --- Fail path tests ---

// TestPost_Non2xx verifies that a 500 response returns an error.
func TestPost_Non2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL)
	_, err := c.SubmitCheckRecord(context.Background(), makeCheckReq())
	if err == nil {
		t.Fatal("expected error for HTTP 500, got nil")
	}
	if !strings.Contains(err.Error(), "HTTP 500") {
		t.Errorf("error should mention HTTP 500, got: %v", err)
	}
}

// TestPost_BadJSON verifies that a non-JSON response body returns a decode error.
func TestPost_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>not json</html>"))
	}))
	defer srv.Close()

	c := newTestClient(srv.URL)
	_, err := c.SubmitCheckRecord(context.Background(), makeCheckReq())
	if err == nil {
		t.Fatal("expected decode error for HTML response, got nil")
	}
	if !strings.Contains(err.Error(), "decode response") {
		t.Errorf("error should mention decode failure, got: %v", err)
	}
}

// TestPost_ConnectionRefused verifies that a connection failure returns an error.
func TestPost_ConnectionRefused(t *testing.T) {
	// Use a server that is immediately closed so connections are refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // close immediately

	c := newTestClient(srv.URL)
	_, err := c.SubmitCheckRecord(context.Background(), makeCheckReq())
	if err == nil {
		t.Fatal("expected connection error, got nil")
	}
}

// --- Regression tests ---

// TestNew_TrailingSlash_Regression verifies that trailing slashes in the base
// URL do not produce double-slash paths (WARN regression).
func TestNew_TrailingSlash_Regression(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SubmitResponse{RecordID: "r1"})
	}))
	defer srv.Close()

	// Inject trailing slash — should be stripped by New() / newTestClient.
	c := newTestClient(srv.URL + "/")
	_, err := c.SubmitCheckRecord(context.Background(), makeCheckReq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(capturedPath, "//") {
		t.Errorf("URL contained double slash: %q", capturedPath)
	}
	if capturedPath != checkRecordsPath {
		t.Errorf("expected path %q, got %q", checkRecordsPath, capturedPath)
	}
}

// TestPost_ReadBodyError_Regression verifies that io.ReadAll errors are
// propagated as errors instead of being silently ignored (INFO regression).
//
// We simulate a body read error by serving a response whose body is closed
// early via a custom ResponseWriter that fails after the header.
func TestPost_ReadBodyError_Regression(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hijack the connection so we can close it mid-response, causing the
		// client's io.ReadAll to fail.
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("ResponseWriter does not implement http.Hijacker")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		// Write an HTTP 200 header with Content-Length larger than the body
		// we will actually deliver, then close the connection mid-body.
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{")
		_ = buf.Flush()
		_ = conn.Close() // abrupt close — client will get an unexpected EOF
	}))
	defer srv.Close()

	c := newTestClient(srv.URL)
	_, err := c.SubmitCheckRecord(context.Background(), makeCheckReq())
	if err == nil {
		t.Fatal("expected error from truncated body, got nil")
	}
	// The error must originate from the read body path, not from the status check.
	if strings.Contains(err.Error(), "HTTP 200") {
		t.Errorf("error should not be a status error, got: %v", err)
	}
}

// TestNew_DefaultAddr verifies that New() sets the default base URL when the
// env var is absent, and strips any trailing slash.
func TestNew_DefaultAddr(t *testing.T) {
	t.Setenv("NODEVAULT_API_ADDR", "")
	c := New()
	if c.baseURL != defaultVaultAPIAddr {
		t.Errorf("expected default addr %q, got %q", defaultVaultAPIAddr, c.baseURL)
	}
	if strings.HasSuffix(c.baseURL, "/") {
		t.Errorf("baseURL must not end with slash, got %q", c.baseURL)
	}
}

// TestNew_EnvAddr_TrailingSlash verifies that New() strips a trailing slash
// from NODEVAULT_API_ADDR.
func TestNew_EnvAddr_TrailingSlash(t *testing.T) {
	t.Setenv("NODEVAULT_API_ADDR", "http://custom.svc:8082/")
	c := New()
	if strings.HasSuffix(c.baseURL, "/") {
		t.Errorf("baseURL must not end with slash, got %q", c.baseURL)
	}
	if c.baseURL != "http://custom.svc:8082" {
		t.Errorf("expected trimmed addr, got %q", c.baseURL)
	}
}

// TestSubmitCheckRecord_RequestBody verifies that the JSON body is correctly
// marshalled and sent to the server.
func TestSubmitCheckRecord_RequestBody(t *testing.T) {
	var received SubmitCheckRecordRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &received)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SubmitResponse{RecordID: "r1"})
	}))
	defer srv.Close()

	c := newTestClient(srv.URL)
	req := makeCheckReq()
	req.CheckID = "verify-body"
	_, err := c.SubmitCheckRecord(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.CheckID != "verify-body" {
		t.Errorf("expected CheckID=verify-body in request body, got %q", received.CheckID)
	}
}
