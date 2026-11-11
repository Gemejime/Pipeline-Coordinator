# Implementation Plan: Phase 4 — Telemetry & Event Offloading

## 1. Context & Objectives

Following the architectural blueprints in [`tasks/RAG Ingestion Coordinator Implementation Plan.pdf`](../tasks/RAG%20Ingestion%20Coordinator%20Implementation%20Plan.pdf), [`tasks/Production-Grade Multimodal RAG Architectural Specification.pdf`](../tasks/Production-Grade%20Multimodal%20RAG%20Architectural%20Specification.pdf), [`tasks/Go and Python Microservices Architecture.pdf`](../tasks/Go%20and%20Python%20Microservices%20Architecture.pdf), [`tasks/Production Grade Backend Concepts.pdf`](../tasks/Production%20Grade%20Backend%20Concepts.pdf), [`tasks/My Naming Convention.pdf`](../tasks/My%20Naming%20Convention.pdf), and the system invariants in [`.ai/`](../.ai/), **Phase 4: Telemetry & Event Offloading** implements decoupled real-time progress event streaming, terminal execution cataloging in PostgreSQL, live Prometheus observability, production AWS S3 & Temporal worker wiring, GitOps ArgoCD rollout readiness, and downstream chunk lifecycle integration verification:

1. **Database Schema & SQL Migrations (`migrations/`)**:
   - `migrations/000001_init_schema.up.sql`:
     - Idempotent DDL (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`) matching [`.ai/doc-api.md`](../.ai/doc-api.md):
       - `pipeline_configs` (`id VARCHAR(64) PRIMARY KEY`, `tenant_id VARCHAR(64) NOT NULL`, `name VARCHAR(255) NOT NULL`, `chunk_size_tokens INT NOT NULL DEFAULT 300`, `chunk_overlap_tokens INT NOT NULL DEFAULT 50`, `ocr_enabled BOOLEAN NOT NULL DEFAULT TRUE`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`, `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`).
       - `pipeline_runs` (`id UUID PRIMARY KEY DEFAULT gen_random_uuid()`, `workflow_id VARCHAR(255) NOT NULL UNIQUE`, `run_id VARCHAR(255) NOT NULL`, `tenant_id VARCHAR(64) NOT NULL`, `document_id VARCHAR(255) NOT NULL`, `content_hash VARCHAR(64) NOT NULL`, `status VARCHAR(32) NOT NULL CHECK (status IN ('COMPLETED', 'FAILED', 'TERMINATED'))`, `total_pages INT NOT NULL DEFAULT 0`, `total_slices INT NOT NULL DEFAULT 0`, `completed_slices INT NOT NULL DEFAULT 0`, `failed_slices INT NOT NULL DEFAULT 0`, `duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0.0`, `error_message TEXT`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`, `completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`).
       - Indexes: `idx_pipeline_runs_tenant_created` on `pipeline_runs(tenant_id, created_at DESC)` and `idx_pipeline_configs_tenant` on `pipeline_configs(tenant_id)`.
   - `migrations/000001_init_schema.down.sql`:
     - Safely drops indexes and tables (`DROP INDEX IF EXISTS`, `DROP TABLE IF EXISTS`).
   - Supports ArgoCD `PreSync` Hook Jobs via idempotent SQL and a `-migrate` flag in `cmd/coordinator/main.go`.

2. **PostgreSQL Storage Adapters (`internal/store/postgres/`)**:
   - `client.go`:
     - Configures and establishes `*pgxpool.Pool` using `github.com/jackc/pgx/v5/pgxpool`.
     - **PgBouncer Transaction-Pooling Compatibility**: Explicitly sets `ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec` (or configurable `QueryExecModeSimpleProtocol`) to disable server-side prepared statement caching (`stmtcache_*`) that fails behind PgBouncer `pool_mode = transaction`.
     - Enforces pool bounds (`MaxConns`, `MinConns`, `MaxConnLifetime`, `MaxConnIdleTime`, `HealthCheckPeriod`), TLS `sslmode` configuration ([`.ai/security.md`](../.ai/security.md)), and DSN password redaction in logs/errors.
     - Provides `Ping(ctx)` for `/ready` probing and an idempotent `RunMigrations(ctx, migrationFS)` helper.
   - `run_repo.go`:
     - Implements `activity.TerminalCatalogStore`:
       - `RecordTerminalRun(ctx context.Context, req coordinatorv1.CatalogTerminalRunRequest) (string, error)`
       - Atomic UPSERT into `pipeline_runs` (`ON CONFLICT (workflow_id) DO UPDATE`) enforcing tenant ownership (`WHERE pipeline_runs.tenant_id = EXCLUDED.tenant_id`), updating `run_id`, `status`, slice/page counters, `duration_seconds`, `error_message`, and `completed_at = NOW()`, and returning `id::text`.
       - Enforces ADR-003 zero dual-writes: invoked strictly at pipeline boundaries (`COMPLETED`, `FAILED`, `TERMINATED`), never during in-flight slice execution.
     - Implements `service.TerminalRunReader`:
       - `GetTerminalRun(ctx context.Context, tenantID, workflowID string) (*coordinatorv1.GetIngestionStatusResponse, error)`
       - Queries `pipeline_runs` by `workflow_id`, verifies `tenant_id` match (returning a typed tenant-mismatch error without leaking cross-tenant rows, and `service.ErrWorkflowNotFound` when absent), safely scans nullable `error_message` (`*string`), and formats `created_at` / `completed_at` as RFC 3339 UTC timestamps.
   - `pipeline_repo.go`:
     - Implements `PipelineConfigRepository` (`GetPipelineConfig(ctx, tenantID, configID)` and `SavePipelineConfig(ctx, config)`).
     - Enforces cross-tenant isolation on `SavePipelineConfig` (`ON CONFLICT (id) DO UPDATE ... WHERE pipeline_configs.tenant_id = EXCLUDED.tenant_id`, returning an explicit tenant-mismatch error when `RowsAffected() == 0` so Tenant B cannot overwrite Tenant A's `id`).

3. **Redis Pub/Sub Telemetry Publisher (`internal/store/redis/`)**:
   - `publisher.go`:
     - Production Redis adapter using `github.com/redis/go-redis/v9` (`redis.NewUniversalClient(&redis.UniversalOptions{...})`) supporting **Standalone**, **Sentinel** (`MasterName`), and **Cluster** (`Addrs` > 1) topologies with optional TLS (`TLSConfig`).
     - Implements `activity.MilestonePublisher`:
       - `PublishMilestone(ctx context.Context, channel string, event coordinatorv1.ProgressMilestoneEvent) error`
     - Validates required fields (`event_id`, `tenant_id`, `document_id`, `workflow_id`, `stage`, `status`) and `sequence_number >= 1`.
     - **Memory-Bounded Monotonic Sequence Guard (<256 MiB RSS)**: Uses a bounded, TTL/capacity-evicted per-workflow sequence tracker (`MaxTrackedWorkflowSequences = 10000`, matching `middleware.MaxTrackedBuckets`) that allows idempotent Temporal activity retries (`seq == lastSeq`) and rejects out-of-order regressions (`seq < lastSeq`) without leaking memory across long-running pods.
     - Serializes structured JSON payload matching [`.ai/doc-api.md`](../.ai/doc-api.md) Section 5.
     - Increments injected Prometheus counters (`coordinator_redis_events_published_total` on success; `coordinator_redis_events_errors_total` on failure) and never logs Redis credentials/URLs.
     - Provides `Ping(ctx)` for `/ready` probing.

4. **Prometheus Telemetry Registry & Activity/Service Wiring (`internal/telemetry/`, `internal/activity/`, `internal/service/`)**:
   - `internal/telemetry/metrics.go`:
     - Defines a dependency-injected `Metrics` struct backed by a dedicated `*prometheus.Registry` (zero package-level global mutable state per [`.ai/code-standards.md`](../.ai/code-standards.md)) exposing:
       - `coordinator_workflow_starts_total` (`CounterVec` labeled by `tenant_id`, `status`)
       - `coordinator_child_workflows_spawned` (`Histogram`)
       - `coordinator_redis_events_published_total` (`Counter`)
       - `coordinator_redis_events_errors_total` (`Counter`)
       - `temporal_activity_schedule_to_start_seconds` (`HistogramVec` labeled by `task_queue`, plus Temporal SDK `client.MetricsHandler` adapter)
     - Provides `Handler() http.Handler` via `promhttp.HandlerFor` for `/metrics` (required by Prometheus Alert Rules and **ArgoCD Rollouts** canary promotion gates).
   - `internal/activity/notification.go`:
     - Adds `PublishMilestoneActivityName = "PublishMilestoneActivity"`, `ErrNilMilestonePublisher`, and `func (a *NotificationActivities) Register(reg worker.ActivityRegistry) error` (matching `CatalogActivities` and `PresignActivities`) so production workers fail-fast if `Publisher == nil` instead of silently dropping events.
   - `internal/activity/inspection.go`:
     - Adds `InspectDocumentHeaderActivityName = "InspectDocumentHeaderActivity"` and `func (a *InspectionActivities) Register(reg worker.ActivityRegistry) error` for wiring `ByteRangeReader` cleanly in workers.
   - `internal/service/execution_service.go`:
     - Wires optional `PipelineConfigReader` (`GetPipelineConfig`) so `StartIngestion` resolves caller-specified `pipeline_config_id` from PostgreSQL into `WorkflowIngestionRequest.PipelineConfig` (falling back safely to defaults when unset or using `default-multimodal-v1`), propagates `UseHashPrefixS3Layout` config (Runbook 2), and increments `coordinator_workflow_starts_total`.
     - Fixes closed-workflow `GetWorkflowStatus` resolution: when `QueryWorkflow` reports a terminal status (`COMPLETED`, `FAILED`, `TERMINATED`, `CANCELED`, `TIMED_OUT`), enriches/resolves full terminal metadata (`run_id`, `duration_seconds`, `created_at`, `completed_at`) via `TerminalRunReader` (and `DescribeWorkflowExecution` fallback) instead of returning incomplete query timestamps.

5. **AWS SDK v2 S3 Adapter (`internal/store/s3/aws_client.go`)**:
   - Implements `s3.PresignAPI` and `s3.RangeStreamGetter` using `github.com/aws/aws-sdk-go-v2` (`config`, `service/s3`, `aws`) supporting standard AWS IAM/STS credentials and custom S3-compatible endpoints (e.g., LocalStack/MinIO with path-style addressing for integration testing and staging).

6. **Readiness Probe, GitOps ArgoCD Rollout Safety & Service Lifecycle (`internal/transport/http/`, `cmd/coordinator/`)**:
   - `internal/transport/http/router.go`:
     - Replaces static hardcoded-zero `handlePrometheusMetrics` with `RouterConfig.MetricsHandler` (`http.Handler`) backed by live `telemetry.Metrics` (retaining a clean zero-metric fallback when `nil` in isolated unit tests).
     - Keeps `ReadinessCheckFunc` probing Temporal (`CheckHealth`), PostgreSQL (`pgxpool.Ping`), and Redis (`publisher.Ping`), and supports a drain state returning `503 Service Unavailable` immediately during graceful shutdown (`draining = true`) for zero-dropped-connection **ArgoCD** canary/rolling deploys.
   - `cmd/coordinator/main.go`:
     - Bootstraps all 5 backend layers from 12-factor environment variables:
       - Optional `-migrate` flag (executes `migrations/000001_init_schema.up.sql` and exits `0` for ArgoCD `PreSync` Job compatibility).
       - Structured JSON `log/slog` logger (never logging secrets, DSNs, Redis passwords, or presigned URLs).
       - Production `TokenValidator` wiring (configured HMAC/JWKS or static token map via environment/secret mount; strictly never `TestTokenValidator`).
       - PostgreSQL `pgxpool`, Redis `UniversalClient`, AWS SDK v2 S3 `Presigner`, and Temporal `client.Client` (wired with `telemetry.Metrics` handler).
       - Temporal `worker.Worker` polling `coordinator-task-queue` with `IngestionCoordinatorWorkflow`, `DocumentPartitionWorkflow`, `PresignActivities`, `NotificationActivities`, and `CatalogActivities` registered (plus optional `ENABLE_INSPECTION_WORKER` toggle for `inspection-queue`).
       - HTTP server (`:8080`) configured with explicit `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, and `IdleTimeout` (`gosec` G112/G114 compliant) and gRPC server (`:50051`) implementing `CoordinatorServiceServer`.
       - Graceful `SIGINT`/`SIGTERM` shutdown sequence: flip `/ready` to `503` (drain signal), stop Temporal worker, `Shutdown` HTTP server with timeout, `GracefulStop` gRPC server, and close Temporal, Redis, and PostgreSQL pools cleanly.

7. **Downstream Integration Verification (`rag-chunk-lifecycle-service`) & Automated Test Suite**:
   - `test/unit/postgres_repo_test.go`: Unit tests for `run_repo.go` and `pipeline_repo.go` (atomic upsert, cross-tenant isolation on read and upsert, nullable `error_message`, RFC 3339 timestamps, PgBouncer `QueryExecModeExec` config verification).
   - `test/unit/redis_publisher_test.go`: Unit tests for `publisher.go` validating `ingestion:events:<tenant_id>` channel routing, JSON schema conformance, bounded sequence tracking + idempotent retry handling, standalone/cluster/sentinel option building, and Prometheus `published`/`errors` counter increments.
   - `test/unit/http_readiness_test.go`: HTTP tests for `/ready` (all combinations of healthy/failing Temporal, Postgres, Redis + shutdown drain `503`), and live `/metrics` Prometheus exposition output.
   - `test/unit/chunk_lifecycle_contract_test.go`: Downstream contract tests verifying the handoff from `workers/python/rag_document_ai_worker` Parquet schema (`slice_id`, `slice_index`, `unit_index`, `content`, `metadata_json`, `tokens_estimate`) and `ExtractSegmentResponse` into `CompileChunksActivity` on `chunk-lifecycle-queue` (including `HeartbeatDetails` checkpoint recovery and custom `PipelineConfig` chunk/overlap propagation).
   - `test/workflow/ingestion_workflow_test.go`: Extended workflow tests verifying `NotificationActivities.Register`, `PublishMilestoneActivity` sequence monotonicity, `CatalogTerminalRunActivity` single boundary persistence, and `ExecutionService` closed-workflow `TerminalRunReader` fallback.
   - `test/integration/telemetry_catalog_integration_test.go`: Ephemeral Testcontainers integration tests (PostgreSQL 16, Redis 7, LocalStack S3 / Temporal) verifying zero DB rows during active execution, single atomic terminal row in `pipeline_runs`, and real Redis Pub/Sub frame delivery on `ingestion:events:<tenant_id>` (cleanly skipped with `t.Skip` when Docker is unavailable so unit/workflow CI stays fast and deterministic).

---

## 2. Files to Create or Modify

| File Path | Action | Responsibility |
| :--- | :--- | :--- |
| `go.mod` & `go.sum` | Modify | Add `github.com/jackc/pgx/v5`, `github.com/redis/go-redis/v9`, `github.com/prometheus/client_golang`, `github.com/aws/aws-sdk-go-v2` (`config`, `service/s3`), and `github.com/testcontainers/testcontainers-go` (test). |
| `migrations/000001_init_schema.up.sql` | Create | Idempotent DDL migrations for `pipeline_configs`, `pipeline_runs` (with status check constraint), and indexes (`idx_pipeline_runs_tenant_created`, `idx_pipeline_configs_tenant`). |
| `migrations/000001_init_schema.down.sql` | Create | Reversible rollback DDL migration script. |
| `internal/telemetry/metrics.go` | Create | Dependency-injected Prometheus registry (`Metrics`), HTTP `/metrics` handler, and Temporal SDK `MetricsHandler` bridge. |
| `internal/store/postgres/client.go` | Create | `pgxpool` setup with `QueryExecModeExec` PgBouncer compatibility, TLS, `Ping`, secret-safe logging, and embedded SQL migration runner. |
| `internal/store/postgres/run_repo.go` | Create | Implements `activity.TerminalCatalogStore` and `service.TerminalRunReader` with atomic upsert, nullable `error_message` handling, and tenant isolation. |
| `internal/store/postgres/pipeline_repo.go` | Create | Implements `PipelineConfigRepository` with cross-tenant conflict protection on upsert. |
| `internal/store/redis/publisher.go` | Create | Implements `activity.MilestonePublisher` using `go-redis/v9` `UniversalClient` (standalone/cluster/sentinel + TLS), bounded sequence guard, `Ping`, and metrics. |
| `internal/store/s3/aws_client.go` | Create | Concrete AWS SDK v2 implementation of `s3.PresignAPI` and `s3.RangeStreamGetter`. |
| `internal/activity/notification.go` | Modify | Add `PublishMilestoneActivityName`, `ErrNilMilestonePublisher`, and `Register(worker.ActivityRegistry)` matching `CatalogActivities` / `PresignActivities`. |
| `internal/activity/inspection.go` | Modify | Add `InspectDocumentHeaderActivityName` and `Register(worker.ActivityRegistry)` for dependency-backed inspection worker wiring. |
| `internal/service/execution_service.go` | Modify | Wire optional `PipelineConfigReader`, `UseHashPrefixS3Layout`, `coordinator_workflow_starts_total` metric, and terminal-state enrichment via `TerminalRunReader`. |
| `internal/transport/http/router.go` | Modify | Wire live `MetricsHandler` for `/metrics` and drain-aware `ReadinessCheckFunc` for `/ready`. |
| `cmd/coordinator/main.go` | Create | Service entry point: 12-factor config, `-migrate` ArgoCD PreSync mode, dependency wiring, Temporal `coordinator-task-queue` worker, HTTP/gRPC servers with timeouts, and drain-aware graceful shutdown. |
| `test/unit/postgres_repo_test.go` | Create | Unit tests for Postgres run catalog, pipeline config repo, and PgBouncer config. |
| `test/unit/redis_publisher_test.go` | Create | Unit tests for Redis publisher formatting, bounded sequence tracking, topologies, and metrics. |
| `test/unit/http_readiness_test.go` | Create | Unit tests verifying `/ready` (200 vs 503 + drain) and live `/metrics` output. |
| `test/unit/chunk_lifecycle_contract_test.go` | Create | Contract tests validating DocAI Parquet schema & `CompileChunksActivity` (`chunk-lifecycle-queue`) integration. |
| `test/workflow/ingestion_workflow_test.go` | Modify | Extend workflow assertions for `NotificationActivities.Register`, milestone emissions, and terminal catalog persistence. |
| `test/integration/telemetry_catalog_integration_test.go` | Create | Testcontainers integration tests for Postgres 16, Redis 7, and terminal boundary / Pub/Sub verification. |
| `.ai/doc-api.md` & `api/openapi/openapi.yaml` | Modify | Synchronize `/ready` `503` JSON schema, `pipeline_configs` index/constraints, and live `/metrics` specification. |
| `.ai/progress-tracker.md` | Modify | Record Phase 4 completion and pressure-test refinement notes (`GAP-14`). |

---

## 3. Detailed Design & Invariants

### 3.1 PgBouncer & Tenant-Isolated Persistence (`internal/store/postgres/`)
- **PgBouncer Compatibility**: `pgxpool.ParseConfig` sets `cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec` by default so transaction-pooled PgBouncer connections never fail on cached prepared statement names.
- **ADR-003 Zero Dual-Writes**: `run_repo.go` is invoked solely by `CatalogTerminalRunActivity` at pipeline completion, failure, or cancellation; zero `INSERT`/`UPDATE` statements execute per slice.
- **Cross-Tenant Write Guard**: `SavePipelineConfig` uses `ON CONFLICT (id) DO UPDATE SET ... WHERE pipeline_configs.tenant_id = EXCLUDED.tenant_id` and returns `ErrTenantMismatch` if `0` rows are affected.

### 3.2 Bounded Memory Telemetry & Redis Topologies (`internal/store/redis/`, `internal/telemetry/`)
- **No Global Mutable Metrics**: `telemetry.Metrics` owns an isolated `*prometheus.Registry` injected via constructors (`ExecutionServiceConfig`, `RedisPublisherConfig`, `RouterConfig`), keeping `t.Parallel()` tests race-free and compliant with [`.ai/code-standards.md`](../.ai/code-standards.md).
- **Bounded Sequence State**: `publisher.go` caps in-memory `lastSeqByWorkflow` tracking at `10,000` entries with stale/LRU eviction so RSS remains strictly `<256 MiB`, treats `seq == lastSeq` as idempotent on Temporal activity retry, and rejects `seq <= 0` or `seq < lastSeq`.

### 3.3 GitOps ArgoCD & Graceful Drain Readiness (`cmd/coordinator/main.go`, `internal/transport/http/router.go`)
- **PreSync Migration Entrypoint**: Running `coordinator -migrate` applies `migrations/000001_init_schema.up.sql` idempotently and exits `0`, supporting ArgoCD `PreSync` hook Jobs before canary pods roll out.
- **Zero-Drop Rolling/Canary Drain**: On `SIGINT`/`SIGTERM`, `main.go` flips an atomic `draining` boolean so `GET /ready` immediately returns `503 Service Unavailable` (`{"status":"unavailable","checks":{...}}`), stops the Temporal worker, drains HTTP/gRPC listeners, and then closes Redis and PostgreSQL pools.

---

## 4. Verification Plan

1. **Formatting & Static Analysis**:
   - `gofmt -s -w .`
   - `go vet ./...`
   - `golangci-lint run`
2. **Security & Contract Gates**:
   - Verify `gosec` (including `ReadHeaderTimeout` on `http.Server`) and `govulncheck ./...`.
   - Verify Secured Logging audit (no raw presigned URLs, Bearer tokens, Postgres DSNs, or Redis passwords logged).
   - Validate `api/openapi/openapi.yaml` with Spectral (`.spectral.yaml`) and sync with [`.ai/doc-api.md`](../.ai/doc-api.md).
3. **Automated Tests**:
   - Unit & Workflow suites: `go test -v -race ./test/unit/... ./test/workflow/...`
   - History threshold gate: `go test -v -race -run "Test.*History" ./test/workflow/...`
   - Integration suite (when Docker is available): `go test -v -race ./test/integration/...`

---

## 5. Human-in-the-Loop Confirmation Gate

In accordance with [`.ai/ai-workflow-rules.md`](../.ai/ai-workflow-rules.md):
- This plan defines the complete, pressure-tested specification and deliverables for **Phase 4: Telemetry & Event Offloading** (`GAP-14`).
- Awaiting human confirmation before applying code modifications and adding new dependencies.
