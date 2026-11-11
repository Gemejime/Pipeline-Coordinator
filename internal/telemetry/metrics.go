// Package telemetry provides dependency-injected Prometheus metrics registries
// and Temporal SDK metric handler bridges with zero global mutable state.
package telemetry

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
)

// Metrics owns an isolated Prometheus registry and all coordinator telemetry collectors.
type Metrics struct {
	registry                      *prometheus.Registry
	WorkflowStartsTotal           *prometheus.CounterVec
	ChildWorkflowsSpawned         prometheus.Histogram
	RedisEventsPublishedTotal     prometheus.Counter
	RedisEventsErrorsTotal        prometheus.Counter
	TemporalActivityScheduleStart *prometheus.HistogramVec
}

// NewMetrics constructs a Metrics instance with a dedicated Prometheus registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	workflowStarts := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coordinator_workflow_starts_total",
			Help: "Total initiated root ingestion workflows partitioned by tenant_id and status.",
		},
		[]string{"tenant_id", "status"},
	)

	childWorkflows := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "coordinator_child_workflows_spawned",
			Help:    "Distribution of child DocumentPartitionWorkflow executions spawned per document.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
	)

	redisPublished := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "coordinator_redis_events_published_total",
			Help: "Total progress milestone events successfully published to Redis Pub/Sub.",
		},
	)

	redisErrors := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "coordinator_redis_events_errors_total",
			Help: "Total Redis Pub/Sub milestone event publication failures.",
		},
	)

	activityScheduleToStart := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "temporal_activity_schedule_to_start_seconds",
			Help:    "Temporal activity schedule-to-start latency across polyglot task queues.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
		},
		[]string{"task_queue"},
	)

	reg.MustRegister(
		workflowStarts,
		childWorkflows,
		redisPublished,
		redisErrors,
		activityScheduleToStart,
	)

	return &Metrics{
		registry:                      reg,
		WorkflowStartsTotal:           workflowStarts,
		ChildWorkflowsSpawned:         childWorkflows,
		RedisEventsPublishedTotal:     redisPublished,
		RedisEventsErrorsTotal:        redisErrors,
		TemporalActivityScheduleStart: activityScheduleToStart,
	}
}

// Registry returns the underlying isolated Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// Handler returns the HTTP handler serving Prometheus metrics from this isolated registry.
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		Registry: m.registry,
	})
}

// IncWorkflowStart increments coordinator_workflow_starts_total for tenantID and status.
func (m *Metrics) IncWorkflowStart(tenantID, status string) {
	if m == nil || m.WorkflowStartsTotal == nil {
		return
	}
	t := strings.TrimSpace(tenantID)
	if t == "" {
		t = "unknown"
	}
	s := strings.TrimSpace(status)
	if s == "" {
		s = "UNKNOWN"
	}
	m.WorkflowStartsTotal.WithLabelValues(t, s).Inc()
}

// ObserveChildWorkflowsSpawned records the count of child workflows spawned for a document.
func (m *Metrics) ObserveChildWorkflowsSpawned(count int) {
	if m == nil || m.ChildWorkflowsSpawned == nil || count < 0 {
		return
	}
	m.ChildWorkflowsSpawned.Observe(float64(count))
}

// IncRedisEventPublished increments coordinator_redis_events_published_total.
func (m *Metrics) IncRedisEventPublished() {
	if m == nil || m.RedisEventsPublishedTotal == nil {
		return
	}
	m.RedisEventsPublishedTotal.Inc()
}

// IncRedisEventError increments coordinator_redis_events_errors_total.
func (m *Metrics) IncRedisEventError() {
	if m == nil || m.RedisEventsErrorsTotal == nil {
		return
	}
	m.RedisEventsErrorsTotal.Inc()
}

// ObserveActivityScheduleToStart records schedule-to-start latency for a Temporal task queue.
func (m *Metrics) ObserveActivityScheduleToStart(taskQueue string, d time.Duration) {
	if m == nil || m.TemporalActivityScheduleStart == nil {
		return
	}
	tq := strings.TrimSpace(taskQueue)
	if tq == "" {
		tq = "unknown"
	}
	secs := d.Seconds()
	if secs < 0 {
		secs = 0
	}
	m.TemporalActivityScheduleStart.WithLabelValues(tq).Observe(secs)
}

// TemporalMetricsHandler returns a Temporal SDK client.MetricsHandler that bridges
// Temporal activity schedule-to-start timers into Prometheus.
func (m *Metrics) TemporalMetricsHandler() client.MetricsHandler {
	if m == nil {
		return client.MetricsNopHandler
	}
	return &temporalMetricsHandler{
		metrics: m,
		tags:    map[string]string{},
	}
}

type temporalMetricsHandler struct {
	metrics *Metrics
	tags    map[string]string
}

func (h *temporalMetricsHandler) WithTags(tags map[string]string) client.MetricsHandler {
	merged := make(map[string]string, len(h.tags)+len(tags))
	for k, v := range h.tags {
		merged[k] = v
	}
	for k, v := range tags {
		merged[k] = v
	}
	return &temporalMetricsHandler{
		metrics: h.metrics,
		tags:    merged,
	}
}

func (h *temporalMetricsHandler) Counter(string) client.MetricsCounter {
	return nopCounter{}
}

func (h *temporalMetricsHandler) Gauge(name string) client.MetricsGauge {
	if name == "coordinator_child_workflows_spawned" {
		return temporalGaugeFunc(func(v float64) {
			h.metrics.ObserveChildWorkflowsSpawned(int(v))
		})
	}
	return nopGauge{}
}

func (h *temporalMetricsHandler) Timer(name string) client.MetricsTimer {
	if strings.Contains(name, "activity_schedule_to_start") {
		taskQueue := h.tags["task_queue"]
		return temporalTimerFunc(func(d time.Duration) {
			h.metrics.ObserveActivityScheduleToStart(taskQueue, d)
		})
	}
	return nopTimer{}
}

type nopCounter struct{}

func (nopCounter) Inc(int64) {}

type nopGauge struct{}

func (nopGauge) Update(float64) {}

type nopTimer struct{}

func (nopTimer) Record(time.Duration) {}

type temporalTimerFunc func(time.Duration)

func (f temporalTimerFunc) Record(d time.Duration) { f(d) }

type temporalGaugeFunc func(float64)

func (f temporalGaugeFunc) Update(v float64) { f(v) }
