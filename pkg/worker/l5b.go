package worker

import (
	"context"
	"fmt"
	"log/slog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

var trivyVulnReportGVR = schema.GroupVersionResource{
	Group:    "aquasecurity.github.io",
	Version:  "v1alpha1",
	Resource: "vulnerabilityreports",
}

// runL5b queries trivy-operator VulnerabilityReport CRDs for the tool image
// and submits a ToolScanRecord to NodeVault. When the CRD is absent or no
// matching report exists, a "not-available" record is submitted so certification
// can proceed without being blocked by missing scan infrastructure.
// Returns a non-nil error when the submission itself fails, so the caller can
// reflect the failure in the CompleteJob summary.
func (w *Worker) runL5b(ctx context.Context, logger *slog.Logger, job *work.Job) error {
	if w.vaultClient == nil {
		logger.Info("L5-b skipped: no vault client configured")
		return nil
	}
	if w.dynamicKube == nil {
		logger.Info("L5-b: no dynamic k8s client — submitting not-available scan record")
		return w.submitNotAvailableScanRecord(ctx, logger, job)
	}

	scanID := fmt.Sprintf("l5b-%s", sanitizeDNSLabel(job.JobID))

	reports, err := w.dynamicKube.Resource(trivyVulnReportGVR).Namespace(smokeNamespace).List(
		ctx, metav1.ListOptions{},
	)
	if err != nil {
		logger.Warn("L5-b: trivy VulnerabilityReport CRD not available", "err", err)
		return w.submitNotAvailableScanRecord(ctx, logger, job)
	}

	// Find the VulnerabilityReport whose artifact digest matches the tool image.
	var matched *trivyVulnSummary
	for i := range reports.Items {
		obj := reports.Items[i].Object
		digest, ok := nestedStr(obj, "report", "artifact", "digest")
		if !ok || digest != job.ImageDigest {
			continue
		}
		matched = parseTrivySummary(obj)
		break
	}

	if matched == nil {
		logger.Info("L5-b: no matching VulnerabilityReport found, submitting not-available")
		return w.submitNotAvailableScanRecord(ctx, logger, job)
	}

	// parseTrivySummary returns a summary with empty Scanner when parsing fails.
	// Treat missing Scanner as a not-available fallback to avoid submitting a
	// structurally invalid record.
	if matched.Scanner == "" {
		logger.Warn("L5-b: VulnerabilityReport parsed but Scanner is empty — submitting not-available")
		return w.submitNotAvailableScanRecord(ctx, logger, job)
	}

	policyResult := "passed"
	if matched.CriticalCount > 0 || matched.HighCount > 0 {
		policyResult = "warning"
	}

	req := vaultclient.SubmitScanRecordRequest{
		ScanID:         scanID,
		ImageDigest:    job.ImageDigest,
		ToolName:       job.ToolName,
		Scanner:        matched.Scanner,
		ScannerVersion: matched.ScannerVersion,
		Source:         "trivy-operator",
		CriticalCount:  matched.CriticalCount,
		HighCount:      matched.HighCount,
		MediumCount:    matched.MediumCount,
		LowCount:       matched.LowCount,
		PolicyMode:     "record_only",
		PolicyResult:   policyResult,
	}
	if _, err := w.vaultClient.SubmitScanRecord(ctx, req); err != nil {
		logger.Error("L5-b: failed to submit scan record", "scan_id", scanID, "err", err)
		return err
	}
	logger.Info("L5-b scan record submitted",
		"scan_id", scanID, "critical", matched.CriticalCount, "high", matched.HighCount)
	return nil
}

func (w *Worker) submitNotAvailableScanRecord(ctx context.Context, logger *slog.Logger, job *work.Job) error {
	scanID := fmt.Sprintf("l5b-%s", sanitizeDNSLabel(job.JobID))
	req := vaultclient.SubmitScanRecordRequest{
		ScanID:       scanID,
		ImageDigest:  job.ImageDigest,
		ToolName:     job.ToolName,
		Scanner:      "trivy-operator",
		Source:       "not-available",
		PolicyMode:   "record_only",
		PolicyResult: "not-available",
	}
	if _, err := w.vaultClient.SubmitScanRecord(ctx, req); err != nil {
		logger.Error("L5-b: failed to submit not-available scan record", "err", err)
		return err
	}
	return nil
}

// trivyVulnSummary holds the fields we extract from a VulnerabilityReport.
type trivyVulnSummary struct {
	Scanner        string
	ScannerVersion string
	CriticalCount  int
	HighCount      int
	MediumCount    int
	LowCount       int
}

// parseTrivySummary extracts scanner metadata and severity counts from the
// unstructured VulnerabilityReport object returned by the dynamic client.
func parseTrivySummary(obj map[string]interface{}) *trivyVulnSummary {
	s := &trivyVulnSummary{}
	s.Scanner, _ = nestedStr(obj, "report", "scanner", "name")
	s.ScannerVersion, _ = nestedStr(obj, "report", "scanner", "version")
	if summary, ok := nestedMap(obj, "report", "summary"); ok {
		s.CriticalCount = nestedInt(summary, "criticalCount")
		s.HighCount = nestedInt(summary, "highCount")
		s.MediumCount = nestedInt(summary, "mediumCount")
		s.LowCount = nestedInt(summary, "lowCount")
	}
	return s
}

// nestedStr walks nested map[string]interface{} and returns the string at
// the last key. Returns ("", false) if any key is missing or the value is
// not a string.
func nestedStr(obj map[string]interface{}, fields ...string) (string, bool) {
	cur := obj
	for i, f := range fields {
		v, ok := cur[f]
		if !ok {
			return "", false
		}
		if i == len(fields)-1 {
			value, isString := v.(string)
			return value, isString
		}
		cur, ok = v.(map[string]interface{})
		if !ok {
			return "", false
		}
	}
	return "", false
}

// nestedMap walks nested map[string]interface{} and returns the map at the
// last key. Returns (nil, false) if any key is missing or not a map.
func nestedMap(obj map[string]interface{}, fields ...string) (map[string]interface{}, bool) {
	cur := obj
	for _, f := range fields {
		v, ok := cur[f]
		if !ok {
			return nil, false
		}
		cur, ok = v.(map[string]interface{})
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// nestedInt reads an integer value from a map. JSON numbers decoded by the
// dynamic client arrive as float64, so we handle that as the primary case.
func nestedInt(obj map[string]interface{}, key string) int {
	v, ok := obj[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	}
	return 0
}

// WithDynamicKubeClient sets the dynamic Kubernetes client used by L5-b to
// query trivy-operator VulnerabilityReport CRDs. If not set, L5-b submits a
// "not-available" scan record instead.
func (w *Worker) WithDynamicKubeClient(d dynamic.Interface) *Worker {
	w.dynamicKube = d
	return w
}
