// Package vaultclient provides an HTTP client for NodeVault's validation REST API.
// NodeSentinel calls these endpoints after L5-a functional validation and
// L5-b security scan to push ToolCheckRecord and ToolScanRecord to NodeVault.
//
// Default endpoint: NODEVAULT_API_ADDR (default http://nodevault.nodevault-system.svc:8082)
package vaultclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	defaultVaultAPIAddr   = "http://nodevault.nodevault-system.svc:8082"
	checkRecordsPath      = "/v1/validation/check-records"
	scanRecordsPath       = "/v1/validation/scan-records"
	defaultRequestTimeout = 10 * time.Second
)

// Client sends validation records to NodeVault's REST API.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a Client. The base URL is read from NODEVAULT_API_ADDR.
func New() *Client {
	addr := os.Getenv("NODEVAULT_API_ADDR")
	if addr == "" {
		addr = defaultVaultAPIAddr
	}
	return &Client{
		baseURL: addr,
		http:    &http.Client{Timeout: defaultRequestTimeout},
	}
}

// PortObservation is the JSON wire type for port I/O observation.
type PortObservation struct {
	Port      string `json:"port"`
	FileCount int    `json:"file_count"`
	NonEmpty  bool   `json:"non_empty"`
}

// SubmitCheckRecordRequest is the payload for POST /v1/validation/check-records.
type SubmitCheckRecordRequest struct {
	CheckID           string            `json:"check_id"`
	ToolSpecDigest    string            `json:"tool_spec_digest,omitempty"`
	ImageDigest       string            `json:"image_digest"`
	ToolName          string            `json:"tool_name,omitempty"`
	Version           string            `json:"version,omitempty"`
	ValidationStatus  string            `json:"validation_status"`
	ValidationHash    string            `json:"validation_hash,omitempty"`
	Command           string            `json:"command,omitempty"`
	ExitCode          int               `json:"exit_code,omitempty"`
	ObservedInputs    []PortObservation `json:"observed_inputs,omitempty"`
	ObservedOutputs   []PortObservation `json:"observed_outputs,omitempty"`
	PeakCPUMilli      int64             `json:"peak_cpu_millicores,omitempty"`
	PeakMemoryMiB     int64             `json:"peak_memory_mib,omitempty"`
	DurationSeconds   int64             `json:"duration_seconds,omitempty"`
	Timeout           bool              `json:"timeout,omitempty"`
	AllOutputsPresent bool              `json:"all_outputs_present,omitempty"`
	ContractResult    string            `json:"contract_result,omitempty"`
	FailureReason     string            `json:"failure_reason,omitempty"`
}

// SubmitScanRecordRequest is the payload for POST /v1/validation/scan-records.
type SubmitScanRecordRequest struct {
	ScanID         string `json:"scan_id"`
	ImageDigest    string `json:"image_digest"`
	ToolName       string `json:"tool_name,omitempty"`
	Scanner        string `json:"scanner,omitempty"`
	ScannerVersion string `json:"scanner_version,omitempty"`
	Source         string `json:"source,omitempty"`
	CriticalCount  int    `json:"critical_count"`
	HighCount      int    `json:"high_count"`
	MediumCount    int    `json:"medium_count"`
	LowCount       int    `json:"low_count"`
	PolicyMode     string `json:"policy_mode,omitempty"`
	PolicyResult   string `json:"policy_result,omitempty"`
}

// SubmitResponse is the JSON response from NodeVault validation endpoints.
type SubmitResponse struct {
	RecordID            string `json:"record_id"`
	CertificationStatus string `json:"certification_status"`
}

// SubmitCheckRecord sends a ToolCheckRecord to NodeVault.
func (c *Client) SubmitCheckRecord(ctx context.Context, req SubmitCheckRecordRequest) (*SubmitResponse, error) {
	return c.post(ctx, c.baseURL+checkRecordsPath, req)
}

// SubmitScanRecord sends a ToolScanRecord to NodeVault.
func (c *Client) SubmitScanRecord(ctx context.Context, req SubmitScanRecordRequest) (*SubmitResponse, error) {
	return c.post(ctx, c.baseURL+scanRecordsPath, req)
}

func (c *Client) post(ctx context.Context, url string, body any) (*SubmitResponse, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("vaultclient: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("vaultclient: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vaultclient: POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("vaultclient: POST %s: HTTP %d: %s", url, resp.StatusCode, respBody)
	}

	var result SubmitResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("vaultclient: decode response: %w", err)
	}
	return &result, nil
}
