# System Architecture Specification - RAG Ingestion Coordinator

## 1. System Stack & Specifications

The **`rag-ingestion-coordinator`** is engineered as a pure control-plane microservice orchestrating end-to-end multimodal document ingestion across heterogeneous compute workers.

| Layer | Technology | Operational Characteristics & Invariants |
| :--- | :--- | :--- |
| **Language Runtime** | Go 1.26+ (64-bit) | High-concurrency network I/O, control-plane coordination, minimal memory overhead. |
| **Memory Ceiling** | Zero-Heap Ingestion ($< 256\text{ MiB}$ RSS) | Zero buffering/streaming of binary payloads or Arrow columnar data; strictly flat memory ceiling regardless of upload size ($\le 10\text{ GB}$). |
| **Workflow Orchestration**| Temporal Go SDK (`go.temporal.io/sdk`) | Workflow DAGs: Root `IngestionCoordinatorWorkflow` and child `DocumentPartitionWorkflow` executions. |
| **Task Queue Registry** | Polyglot Task Queues | `inspection-queue` (Go/Python header inspection), `doc-ai-task-queue` (Python Docling/OCR/ASR), `chunk-lifecycle-queue` (Go chunk compilation). |
| **Metadata Persistence** | PostgreSQL 16+ via `pgxpool` / PgBouncer | Pipeline configurations and terminal execution records (`COMPLETED`, `FAILED`); zero dual-writes. |
| **Real-Time Telemetry** | Redis 7.2+ Cluster (Bitnami 6-Node) | Asynchronous Pub/Sub telemetry broadcast (`ingestion:events:<tenant_id>`) for decoupled client progress streaming. |
| **Object Storage** | AWS S3 / Compatible (AWS SDK v2) | Presigned upload URLs (15-minute lease) and deterministic content-addressed artifact keys (`s3://<bucket>/artifacts/<tenant_id>/<doc_hash>/...`). |
| **Ingress Protocols** | HTTP REST & gRPC (Protobuf v1) | OpenAPI 3.1 endpoints (`:8080`) and Protobuf v3 gRPC server (`:50051`) with tenant authentication enforcement. |
| **Container Hardening** | Distroless (`static:nonroot`) | Multi-stage minimal container image with zero shell, non-root user (UID 65532), read-only root filesystem. |

---

## 2. 5-Layer Backend Architecture

Aligned with [Production Grade Backend Concepts.pdf](tasks/Production%20Grade%20Backend%20Concepts.pdf):

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               Layer 1: API & Routing                                   │
│  - HTTP REST Router (:8080) & gRPC Server (:50051)                                     │
│  - Bearer JWT Authentication, Tenant Isolation & Scopes (ingest:write, ingest:read)   │
│  - Token-Bucket Rate Limiter (MaxTrackedBuckets = 10,000) & ULID Correlation Tracing   │
│  - Drain-Aware Readiness (/ready), Liveness (/healthz), Prometheus Metrics (/metrics) │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
┌───────────────────────────────────────────▼────────────────────────────────────────────┐
│                       Layer 2: Data Persistence & State                                │
│  - Single Source of Truth for Checkpoints (ADR-003: Zero Dual-Writes)                  │
│  - PostgreSQL 16+ via pgxpool (QueryExecModeExec, PgBouncer transaction-mode safe)     │
│  - Atomic Terminal Upsert on pipeline_runs; pipeline_configs caching & multi-tenancy   │
│  - Embedded Version-Controlled SQL Migrations (embed.go & -migrate PreSync flag)      │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
┌───────────────────────────────────────────▼────────────────────────────────────────────┐
│                        Layer 3: Architecture & Scaling                                 │
│  - Temporal Polyglot Task Queues: [inspection-queue], [doc-ai-task-queue]              │
│  - Resilient Child Workflow Fan-Out (ADR-002: DocumentPartitionWorkflow)               │
│  - History Event Bounding (<10,000 hard ceiling, <2,500 target via ContinueAsNew)      │
│  - Zero-Heap S3 Presigner with strict 15-minute lease and deterministic prefix hashing │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
┌───────────────────────────────────────────▼────────────────────────────────────────────┐
│                          Layer 4: Security & Identity                                  │
│  - Cross-Tenant Boundary Enforcement across HTTP headers, JWT claims, and gRPC context │
│  - AWS STS / IRSA Presigned Leases (strictly zero persistent static AWS keys in code) │
│  - Secured Logging Invariant: mandatory redaction of X-Amz-Signature, tokens, and PII  │
│  - Distroless Non-Root Execution (UID 65532, zero shell binaries, no /dev/shm)         │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
┌───────────────────────────────────────────▼────────────────────────────────────────────┐
│                        Layer 5: Observability & Quality                                │
│  - Structured JSON Logging (slog) with trace_id, tenant_id, document_id, workflow_id  │
│  - Isolated Prometheus Registry (coordinator_*, temporal_activity_schedule_to_start)   │
│  - Alert Rules: Task Queue Backlog (>60s), Failure Rate (>2%), Redis Drops (>0.1%)     │
│  - Verified Chaos & 20,000-Page Resiliency Test Suites with zero data corruption       │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Layer 1: API & Routing
- **Entry Points (`cmd/coordinator/main.go`)**:
  - Initializes runtime environment, loads `.env` locally (preserving environment variable overrides), wires dependency injection containers, and starts HTTP (`:8080`) and gRPC (`:50051`) listeners.
  - Intercepts `SIGINT` / `SIGTERM` OS signals and executes clean two-stage connection draining: marks `/ready` as `503 Service Unavailable ("status": "draining")` and sleeps for `SHUTDOWN_DRAIN_DELAY` (default 5s) before shutting down listeners.
- **Transports (`internal/transport/`)**:
  - `POST /v1/ingest/upload-url`: Evaluates payload parameters, selecting single-part PUT ($< 100\text{ MiB}$) or multipart initiation ($> 100\text{ MiB}$ up to $10\text{ GB}$).
  - `POST /v1/ingest/upload-url/complete`: Finalizes S3 multipart upload with client-submitted ETag manifests.
  - `POST /v1/ingest/workflows/start`: Idempotently schedules `IngestionCoordinatorWorkflow`.
  - `GET /v1/ingest/workflows/{id}/status`: Resolves live Temporal workflow state via `QueryTypeIngestionStatus` or falls back to terminal PostgreSQL records.
- **Middleware & Protection (`internal/transport/http/middleware/`)**:
  - High-entropy ULID correlation injection via `X-Request-ID`.
  - Token-bucket rate limiting capped at $10,000$ active buckets to prevent memory exhaustion and DoS presign attacks.
  - Strict 64 KiB request body ceiling (`413 Content Too Large`).

### Layer 2: Data Persistence & State
- **ADR-003: Zero Dual-Writes**:
  - Relational database checkpoint tables are deprecated. Active execution progress and crash recovery checkpoints are persisted exclusively via native Temporal heartbeat details (`activity.GetHeartbeatDetails`).
  - PostgreSQL records **zero intermediate rows** during slice processing. Exactly one atomic record is inserted or updated upon workflow termination (`COMPLETED`, `FAILED`, or `TERMINATED`).
- **Connection Management & Migrations (`internal/store/postgres/`)**:
  - Bounded connection pooling via `pgxpool` using `QueryExecModeExec` to ensure compatibility with transaction-mode connection poolers like PgBouncer.
  - Version-controlled migrations embedded into the coordinator binary (`embed.go`), runnable via CLI flag (`coordinator -migrate`) as an ArgoCD PreSync or Helm pre-install hook.

### Layer 3: Architecture & Scaling
- **Child Workflow Decomposition (ADR-002)**:
  - Decomposes massive documents (up to 20,000 pages or 10 GB) into independent `DocumentPartitionWorkflow` executions.
  - Root coordinator manages bounded batches (e.g., $\le 100$ parallel child workflows per generation) and issues `workflow.NewContinueAsNewError`, carrying forward pending slice indexes and aggregated completion counters.
- **Polyglot Execution Model**:
  - `inspection-queue`: Light Go workers inspecting byte headers via S3 `Range: bytes=0-65535` requests.
  - `doc-ai-task-queue`: Scalable Python GPU workers running Docling and OCR, saving intermediate Parquet slices to S3.
  - `chunk-lifecycle-queue`: Downstream Go chunk compilation service pulling Parquet slices from S3.

### Layer 4: Security & Identity
- **Tenant Isolation**:
  - Authenticated caller claims (`claims.TenantID`) are strictly cross-checked against payload `tenant_id` and URL routes. Cross-tenant execution or status querying is rejected with `403 Forbidden` / `codes.PermissionDenied`.
- **Short-Lived S3 Leases**:
  - All S3 upload and download URLs are issued with a strict 15-minute lease ceiling. Presigned URLs are generated via AWS STS or IAM Roles for Service Accounts (IRSA).
- **Secured Logging Invariant**:
  - Structured JSON logs only. Full URLs containing query parameters and AWS credentials (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`) are stripped before emission.

### Layer 5: Observability & Quality
- **Prometheus Telemetry**:
  - `coordinator_workflow_starts_total`: Counter by tenant.
  - `coordinator_child_workflows_spawned`: Histogram of child workflow slice distributions.
  - `coordinator_redis_events_published_total` / `_errors_total`: Pub/Sub delivery success and drop counters.
  - `temporal_activity_schedule_to_start_seconds`: Latency tracking task queue backlog per queue.
- **Operational Runbooks**:
  - Complete runbooks for diagnosing stuck workflows, recovering from S3 rate limiting, isolating poison-pill document partitions, and handling manual Redis failover.

---

## 3. Architecture Decision Records (ADRs)

### ADR-001: Native Polyglot Temporal Workflows
- **Decision**: Replace synchronous gRPC task proxies between Go activities and Python workers with direct Temporal Python SDK workers polling dedicated task queues (`doc-ai-task-queue`).
- **Rationale**: Removes intermediate network hops, eliminates fragile keep-alive tunnels, and enables native activity retries and backoff driven by the Temporal server.

### ADR-002: Entity / Child Workflow Fan-Out
- **Decision**: Decompose large document ingestion into root (`IngestionCoordinatorWorkflow`) and child (`DocumentPartitionWorkflow`) workflows.
- **Rationale**: Guarantees Temporal history event bounds ($< 10,000$ hard limit, $< 2,500$ operational target) on documents with thousands of pages or audio segments.

### ADR-003: Single Source of Truth for Checkpoints (Zero Dual-Writes)
- **Decision**: Deprecate relational database checkpoint tables. All in-flight execution progress is tracked natively via Temporal heartbeat details (`HeartbeatDetails`), writing to PostgreSQL only at pipeline termination boundaries.
- **Rationale**: Eliminates dual-write desynchronization, removes write contention on PostgreSQL during heavy ingestion fan-out, and ensures deterministic crash recovery.

### ADR-004: Redis Pub/Sub Telemetry Layer
- **Decision**: Offload real-time client streaming from coordinator pods to a dedicated 6-node Redis 7.2 Cluster (`ingestion:events:<tenant_id>`).
- **Rationale**: Preserves coordinator pod statelessness, enabling seamless rolling zero-drop deployments without dropping client event streaming connections.

### ADR-005: Control-Plane Arrow Exclusion
- **Decision**: Strictly prohibit Apache Arrow C/Go libraries within the coordinator binary.
- **Rationale**: Prevents unmanaged off-heap memory growth and maintains the $< 256\text{ MiB}$ RSS memory ceiling. Columnar Parquet transfers occur exclusively between compute workers and S3.
