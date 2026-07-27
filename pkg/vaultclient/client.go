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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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
// Trailing slashes in the address are stripped to prevent double-slash URLs.
func New() *Client {
	addr := os.Getenv("NODEVAULT_API_ADDR")
	if addr == "" {
		addr = defaultVaultAPIAddr
	}
	return NewWithAddr(addr)
}

// NewWithAddr creates a Client with an explicit base URL, ignoring
// NODEVAULT_API_ADDR. Trailing slashes are stripped.
func NewWithAddr(addr string) *Client {
	addr = strings.TrimRight(addr, "/")
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

// Stage identifies which validation pipeline stage produced a record.
// NodeVault uses this (together with Terminal) to decide whether a record
// only promotes a ValidationRequestRecord to Running, or closes it out to
// Succeeded/Failed. In the current worker pipeline (see pkg/worker) stages
// always run in this fixed relative order — L3, L4, then (if requested)
// L5A, then (if requested) L5B — and pkg/worker's process() skips L5A/L5B
// whose action isn't in job.RequestedActions. Terminal is computed from
// which stage actually ends up last in that plan (see process()'s
// isL4Last/isL5ALast and l5b.go's doc comment for L5B, which is always last
// whenever it runs): a smoke_run-only job gets its Terminal record from L4,
// a smoke_run+profile job from L5A, and a job that also requests
// security_scan from L5B — so every successfully completed job produces
// exactly one Terminal record regardless of which optional stages it asked
// for. Exactly one, because every Terminal submission first claims the
// job's one-time terminal-submission slot (see Worker.claimTerminal /
// Store.ClaimTerminal) — a requeued/retried job can't submit a second one.
const (
	StageL3  = "L3"
	StageL4  = "L4"
	StageL5A = "L5A"
	StageL5B = "L5B"
)

// FailureKind classifies why a stage failed, distinct from which stage
// failed (Stage) — an infra-level failure (pod scheduling, OOM, timeout)
// can happen at any stage, not just the ones historically assumed
// infra-only. See pkg/worker's classifyFromPods/waitL5aJob, which already
// compute this distinction; this just carries it over the wire instead of
// discarding it.
const (
	FailureKindInfrastructure = "infrastructure"
	FailureKindApplication    = "application"
	FailureKindPolicy         = "policy"
	FailureKindInternal       = "internal"
)

// SubmitCheckRecordRequest is the payload for POST /v1/validation/check-records.
type SubmitCheckRecordRequest struct {
	CheckID        string `json:"check_id"`
	ToolSpecDigest string `json:"tool_spec_digest,omitempty"`
	ImageDigest    string `json:"image_digest"`
	ToolName       string `json:"tool_name,omitempty"`
	Version        string `json:"version,omitempty"`

	// ValidationRequestID/SentinelJobID correlate this record back to the
	// NodeVault-issued ValidationRequestRecord and this record's own
	// NodeSentinel job — see index.ValidationRequestRecord (NodeVault).
	ValidationRequestID string `json:"validation_request_id,omitempty"`
	SentinelJobID       string `json:"sentinel_job_id,omitempty"`

	// Stage/Terminal tell NodeVault where this record sits in the pipeline:
	// Terminal=false only ever promotes Queued->Running; only a
	// Terminal=true record closes the ValidationRequestRecord out to
	// Succeeded/Failed. See the Stage consts' doc comment.
	Stage    string `json:"stage"`
	Terminal bool   `json:"terminal"`

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

	// FailureKind/FailureCode/Retryable are set only when ValidationStatus
	// != "succeeded" — see the FailureKind consts' doc comment.
	FailureKind   string `json:"failure_kind,omitempty"`
	FailureCode   string `json:"failure_code,omitempty"`
	Retryable     bool   `json:"retryable,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
}

// SubmitScanRecordRequest is the payload for POST /v1/validation/scan-records.
type SubmitScanRecordRequest struct {
	ScanID      string `json:"scan_id"`
	ImageDigest string `json:"image_digest"`
	ToolName    string `json:"tool_name,omitempty"`

	// See SubmitCheckRecordRequest's doc comments — same correlation and
	// stage-position contract. A scan record has no ValidationStatus/
	// FailureKind of its own (see submitNotAvailableScanRecord — an
	// unavailable scanner is not a validation failure); PolicyResult
	// carries the closest equivalent when this record is Terminal.
	ValidationRequestID string `json:"validation_request_id,omitempty"`
	SentinelJobID       string `json:"sentinel_job_id,omitempty"`
	Stage               string `json:"stage"`
	Terminal            bool   `json:"terminal"`

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

// SubmitError wraps a non-2xx HTTP response from NodeVault's validation
// endpoints, carrying the status code so callers (see pkg/worker/delivery.go)
// can decide whether resubmitting the same payload could plausibly succeed.
type SubmitError struct {
	StatusCode int
	Body       string
}

func (e *SubmitError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

// Retryable reports whether err is worth resubmitting the same payload for:
// true for a network-level failure (no SubmitError in the chain at all —
// the request never got a response) or a 5xx server error; false for a 4xx
// response, since NodeVault rejected THIS payload specifically — a 400
// (malformed/invalid field, e.g. an unknown policy_result), a 409
// (validation_request_id/CheckID content conflict, or an image_digest
// mismatch) — and retrying it unchanged can never succeed.
func Retryable(err error) bool {
	var se *SubmitError
	if errors.As(err, &se) {
		return se.StatusCode >= 500
	}
	return true
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
	defer func() { _ = resp.Body.Close() }()

	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if readErr != nil {
		return nil, fmt.Errorf("vaultclient: read response body: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("vaultclient: POST %s: %w", url, &SubmitError{StatusCode: resp.StatusCode, Body: string(respBody)})
	}

	var result SubmitResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("vaultclient: decode response: %w", err)
	}
	return &result, nil
}
