package http

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

// ReadinessCheckFunc probes external dependencies (Temporal, PostgreSQL, Redis)
// and returns a map of component -> status string ("connected" when healthy).
type ReadinessCheckFunc func(ctx context.Context) map[string]string

// RouterConfig configures the HTTP transport router and middleware stack.
type RouterConfig struct {
	UploadHandler    *UploadHandler
	ExecutionHandler *ExecutionHandler
	TokenValidator   middleware.TokenValidator
	RateLimiter      *middleware.TokenBucketLimiter
	Logger           *slog.Logger
	ReadinessCheck   ReadinessCheckFunc
	DrainState       *atomic.Bool
	MetricsHandler   http.Handler
	OpenAPIPath      string
	OpenAPISpec      []byte
}

// NewRouter constructs the standard library net/http router with RequestID,
// StructuredLogger, RateLimit, and RequireAuth middleware.
func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	validator := cfg.TokenValidator
	if validator == nil {
		validator = middleware.NewFailClosedTokenValidator()
	}

	limiter := cfg.RateLimiter
	if limiter == nil {
		limiter = middleware.NewTokenBucketLimiter(middleware.RateLimiterConfig{})
	}

	uploadHandler := cfg.UploadHandler
	if uploadHandler != nil {
		createChain := middleware.RequireAuth(
			validator,
			middleware.ScopeIngestWrite,
			middleware.RateLimit(
				limiter,
				http.HandlerFunc(uploadHandler.HandleCreateUploadURL),
			),
		)
		completeChain := middleware.RequireAuth(
			validator,
			middleware.ScopeIngestWrite,
			middleware.RateLimit(
				limiter,
				http.HandlerFunc(uploadHandler.HandleCompleteMultipartUpload),
			),
		)
		mux.Handle("POST /v1/ingest/upload-url", createChain)
		mux.Handle("POST /v1/ingest/upload-url/complete", completeChain)
	}

	executionHandler := cfg.ExecutionHandler
	if executionHandler != nil {
		startChain := middleware.RequireAuth(
			validator,
			middleware.ScopeIngestWrite,
			middleware.RateLimit(
				limiter,
				http.HandlerFunc(executionHandler.HandleStartIngestion),
			),
		)
		statusChain := middleware.RequireAuth(
			validator,
			middleware.ScopeIngestRead,
			middleware.RateLimit(
				limiter,
				http.HandlerFunc(executionHandler.HandleGetIngestionStatus),
			),
		)
		mux.Handle("POST /v1/ingest/workflows/start", startChain)
		mux.Handle("GET /v1/ingest/workflows/{id}/status", statusChain)
	}

	mux.HandleFunc("GET /healthz", handleLivenessProbe)
	mux.HandleFunc("GET /ready", makeReadinessHandler(cfg.ReadinessCheck, cfg.DrainState))
	if cfg.MetricsHandler != nil {
		mux.Handle("GET /metrics", cfg.MetricsHandler)
	} else {
		mux.HandleFunc("GET /metrics", handlePrometheusMetrics)
	}
	mux.HandleFunc("GET /openapi.yaml", makeOpenAPIHandler(cfg.OpenAPISpec, cfg.OpenAPIPath))
	mux.HandleFunc("GET /docs", handleSwaggerDocs)

	return middleware.RequestID(middleware.StructuredLogger(logger, mux))
}

func handleLivenessProbe(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

func makeReadinessHandler(checkFn ReadinessCheckFunc, drainState *atomic.Bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if drainState != nil && drainState.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "draining",
				"checks": map[string]string{
					"drain": "shutting_down",
				},
			})
			return
		}

		checks := map[string]string{
			"temporal": "connected",
			"postgres": "connected",
			"redis":    "connected",
		}
		if checkFn != nil {
			checks = checkFn(r.Context())
		}
		allReady := len(checks) > 0
		for _, state := range checks {
			if state != "connected" {
				allReady = false
				break
			}
		}
		statusCode := http.StatusOK
		statusLabel := "ready"
		if !allReady {
			statusCode = http.StatusServiceUnavailable
			statusLabel = "unavailable"
		}
		writeJSON(w, statusCode, map[string]any{
			"status": statusLabel,
			"checks": checks,
		})
	}
}

func handlePrometheusMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(strings.Join([]string{
		"# HELP coordinator_workflow_starts_total Total initiated root workflows by tenant.",
		"# TYPE coordinator_workflow_starts_total counter",
		"coordinator_workflow_starts_total 0",
		"# HELP coordinator_child_workflows_spawned Fan-out child workflows per document.",
		"# TYPE coordinator_child_workflows_spawned histogram",
		"coordinator_child_workflows_spawned_count 0",
		"# HELP coordinator_child_workflows_completed_total Total completed child DocumentPartitionWorkflow executions.",
		"# TYPE coordinator_child_workflows_completed_total counter",
		"coordinator_child_workflows_completed_total 0",
		"# HELP coordinator_child_workflows_failed_total Total failed child DocumentPartitionWorkflow executions.",
		"# TYPE coordinator_child_workflows_failed_total counter",
		"coordinator_child_workflows_failed_total 0",
		"# HELP coordinator_redis_events_published_total Telemetry publication success count.",
		"# TYPE coordinator_redis_events_published_total counter",
		"coordinator_redis_events_published_total 0",
		"# HELP coordinator_redis_events_errors_total Telemetry publication error count.",
		"# TYPE coordinator_redis_events_errors_total counter",
		"coordinator_redis_events_errors_total 0",
		"# HELP temporal_activity_schedule_to_start_seconds Task queue schedule-to-start latency.",
		"# TYPE temporal_activity_schedule_to_start_seconds histogram",
		"temporal_activity_schedule_to_start_seconds_count 0",
		"",
	}, "\n")))
}

func makeOpenAPIHandler(embeddedSpec []byte, specPath string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		if len(embeddedSpec) > 0 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(embeddedSpec)
			return
		}
		path := strings.TrimSpace(specPath)
		if path == "" {
			path = "api/openapi/openapi.yaml"
		}
		// #nosec G304 -- specPath is a static server configuration path, never user input.
		data, err := os.ReadFile(path)
		if err != nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("openapi: 3.1.0\ninfo:\n  title: RAG Ingestion Coordinator API\n  version: 1.0.0\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}
}

func handleSwaggerDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>RAG Ingestion Coordinator API Docs</title>
</head>
<body>
  <div id="swagger-ui" data-spec-url="/openapi.yaml">RAG Ingestion Coordinator OpenAPI 3.1 Specification</div>
</body>
</html>`))
}
