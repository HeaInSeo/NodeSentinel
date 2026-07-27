// Package metrics exposes NodeSentinel operational counters via a
// Prometheus-format /metrics endpoint. It follows the same OpenTelemetry +
// Prometheus-exporter pattern used by JUMI's and artifact-handoff's
// pkg/metrics packages, so NodeSentinel's metrics look and behave the same
// way to anything scraping the platform's other data-plane apps.
//
// Kept intentionally small per issue #6's scope: the standard process/Go
// runtime collectors (so /metrics is meaningful the moment the process
// starts) plus a handful of validation-pipeline counters. Broader custom
// instrumentation is a follow-up, not this change.
package metrics

import (
	"context"
	"net/http"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics holds NodeSentinel's instrumentation. A single instance is shared
// across the worker so every counter appears on one /metrics endpoint.
type Metrics struct {
	jobsLeased    metric.Int64Counter
	jobsCompleted metric.Int64Counter
	jobsFailed    metric.Int64Counter
	l5aSubmitted  metric.Int64Counter
	l5aErrors     metric.Int64Counter
	l5bSubmitted  metric.Int64Counter
	l5bErrors     metric.Int64Counter

	handler http.Handler
}

// New creates a Metrics instance backed by an OTel SDK MeterProvider that
// exports to an isolated Prometheus registry. The registry also carries the
// standard process and Go runtime collectors, so /metrics reports basic
// process health (CPU, memory, goroutines, GC) even before any pipeline
// counter has incremented.
func New() (*Metrics, error) {
	reg := promclient.NewRegistry()
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())

	exporter, err := promexporter.New(promexporter.WithRegisterer(reg))
	if err != nil {
		return nil, err
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("github.com/HeaInSeo/NodeSentinel")

	m := &Metrics{handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{})}

	counters := []struct {
		dest *metric.Int64Counter
		name string
	}{
		{&m.jobsLeased, "nodesentinel_jobs_leased"},
		{&m.jobsCompleted, "nodesentinel_jobs_completed"},
		{&m.jobsFailed, "nodesentinel_jobs_failed"},
		{&m.l5aSubmitted, "nodesentinel_l5a_check_records_submitted"},
		{&m.l5aErrors, "nodesentinel_l5a_submit_errors"},
		{&m.l5bSubmitted, "nodesentinel_l5b_scan_records_submitted"},
		{&m.l5bErrors, "nodesentinel_l5b_submit_errors"},
	}
	for _, c := range counters {
		if *c.dest, err = meter.Int64Counter(c.name); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// IncJobsLeased records that the worker successfully leased a queued job.
func (m *Metrics) IncJobsLeased() { m.jobsLeased.Add(context.Background(), 1) }

// IncJobsCompleted records that a job reached WorkStore status Succeeded.
func (m *Metrics) IncJobsCompleted() { m.jobsCompleted.Add(context.Background(), 1) }

// IncJobsFailed records that a job's L3 or L4 stage ended the job (either
// requeued for retry or permanently failed — see worker.go's FailJob calls).
func (m *Metrics) IncJobsFailed() { m.jobsFailed.Add(context.Background(), 1) }

// IncL5aSubmitted records a successful L5-a ToolCheckRecord submission to NodeVault.
func (m *Metrics) IncL5aSubmitted() { m.l5aSubmitted.Add(context.Background(), 1) }

// IncL5aErrors records a failed L5-a ToolCheckRecord submission attempt.
func (m *Metrics) IncL5aErrors() { m.l5aErrors.Add(context.Background(), 1) }

// IncL5bSubmitted records a successful L5-b ToolScanRecord submission to NodeVault.
func (m *Metrics) IncL5bSubmitted() { m.l5bSubmitted.Add(context.Background(), 1) }

// IncL5bErrors records a failed L5-b ToolScanRecord submission attempt.
func (m *Metrics) IncL5bErrors() { m.l5bErrors.Add(context.Background(), 1) }

// Handler returns the /metrics HTTP handler (Prometheus text exposition format).
func (m *Metrics) Handler() http.Handler { return m.handler }
