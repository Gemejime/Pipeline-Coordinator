# Architecture Context - RAG Ingestion Coordinator

## System Stack & Specifications

| Layer | Technology | Role & Operational Characteristics |
| :--- | :--- | :--- |
| **Language Runtime** | Go 1.23+ (64-bit) | High-concurrency network I/O, control-plane coordination, minimal memory overhead |
| **Memory Ceiling** | Zero-Heap Ingestion ($<256\text{ MiB}$ RSS) | Zero buffering/streaming of binary payloads or Arrow columnar data; flat memory ceiling |
| **Orchestration** | Temporal Go SDK (`go.temporal.io/sdk`) | Workflow DAGs: Root `IngestionCoordinatorWorkflow` & Child `DocumentPartitionWorkflow` |
| **Temporal Queues** | Polyglot Task Queue Registry | `inspection-queue` (random-access headers), `doc-ai-task-queue` (Docling/OCR/ASR), `chunk-lifecycle-queue` |
| **State Persistence** | PostgreSQL 16+ (via `pgxpool` / PgBouncer) | Pipeline configurations and terminal execution records (`COMPLETED`, `FAILED`); zero dual-writes |
| **Telemetry Bus** | Redis 7.2+ Cluster / Sentinel | High-throughput, low-latency Pub/Sub event broadcasting for decoupled client progress streaming |
| **Object Storage** | AWS S3 / Compatible (AWS SDK v2) | Presigned upload URLs (15-min lease) and deterministic content-addressed artifact keys |
| **API & Probes** | Standard HTTP/Router + gRPC | OpenAPI 3.1 endpoints (`/v1/ingest/upload-url`, `/healthz`, `/ready`, `/metrics`) & Protobuf v3 gRPC |
| **Containerization** | Scratch / Distroless (`static:nonroot`) | Multi-stage minimal container image with zero shell, non-root user (UID 65532) |

---

## Production-Grade 5-Layer Backend Architecture

Aligned with [Production Grade Backend Concepts.pdf](tasks/Production%20Grade%20Backend%20Concepts.pdf):

### Layer 1: API & Routing
- **Entry Points (`cmd/coordinator/main.go`)**:
  - Initializes runtime environment, parses 12-factor configuration, establishes connection pools (PostgreSQL `pgxpool`, Redis client, Temporal client, AWS S3 presign client).
  - Bootstraps the HTTP listener (`:8080`) and gRPC control listener (`:50051`) with graceful shutdown handling OS signals (`SIGINT`, `SIGTERM`).
- **Endpoints & Routing (`internal/transport/http/`)**:
  - `POST /v1/ingest/upload-url`: REST endpoint issuing presigned S3 upload URLs with parameter validation and lease expiration policies (supports single-part and multipart upload handoffs for payloads up to 10 GB).
  - `POST /v1/ingest/upload-url/complete`: REST endpoint completing multipart S3 uploads with client-submitted ETag manifests.
  - `POST /v1/ingest/workflows/start`: Initiates `IngestionCoordinatorWorkflow` with tenant, document hash, and execution params.
  - `GET /v1/ingest/workflows/{id}/status`: Queries Temporal workflow execution state and terminal PostgreSQL metrics.
  - `GET /healthz`: Liveness probe verifying process event loop and basic readiness.
  - `GET /ready`: Readiness probe verifying active connectivity to PostgreSQL pool, Redis broker, and Temporal frontend.
  - `GET /metrics`: Prometheus telemetry scrape endpoint.
  - `GET /openapi.yaml` & `GET /docs`: OpenAPI 3.1 specification and Swagger documentation.
- **Middleware & Interception (`internal/transport/http/middleware/`)**:
  - Request ID tracing middleware injecting high-entropy ULIDs (`X-Request-ID`).
  - Structured logging middleware capturing request duration, tenant ID, HTTP method, and response status.
  - Multi-tenant authentication pass-through and validation.
- **Input Validation & Sanitization**:
  - Strict validation of incoming payloads (file size $\le 10\text{ GB}$, permitted MIME types, non-empty tenant identifiers, valid UUID/ULIDs).
- **Rate Limiting & Throttling**:
  - Token-bucket rate limiter capping upload requests per tenant/IP to guard against DoS spikes and S3 presign floods.
- **Idempotency**:
  - Idempotent workflow start via Temporal Workflow ID deduplication (`workflow_id = fmt.Sprintf("ingest-%s-%s", tenantID, docHash)`).
  - Deterministic S3 artifact key generation based on content hash and range offsets (`s3://<bucket>/artifacts/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet`).

### Layer 2: Data Persistence & State
- **Single Source of Truth for Checkpoints (ADR-003: Zero Dual-Writes)**:
  - Eliminates transactional database checkpoint tables in PostgreSQL. In-flight task progress and crash recovery rely entirely on native Temporal workflow state and heartbeat payload details (`activity.GetHeartbeatDetails`).
  - PostgreSQL records **zero rows** during active slice processing; writes a single, atomic terminal record only when the workflow reaches `COMPLETED` or `FAILED`.
- **Database Migrations & Schemas**:
  - Version-controlled SQL migration scripts managing schema evolution for `pipeline_configs` and `pipeline_runs`.
- **Database Transactions (ACID) & Connection Pooling**:
  - PostgreSQL 16+ accessed through `pgxpool` with PgBouncer compatibility (bounded max open/idle connections, max connection lifetime).
  - Atomic transactions for terminal pipeline registration and run catalog updates.
- **Resource Identification Strategies**:
  - High-entropy UUIDv4/ULIDs for `run_id`, `pipeline_id`, and `document_id` preventing enumeration attacks.

### Layer 3: Architecture & Scale
- **Asynchronous Task Queues & Workers**:
  - Discrete Temporal Task Queues:
    - `inspection-queue`: Light Go/Python inspection worker pods performing random-access byte inspection (PDF headers, cross-reference tables, audio container atoms).
    - `doc-ai-task-queue`: Autoscaling GPU Python workers executing Docling, OCR, and ASR.
    - `chunk-lifecycle-queue`: Polled by `rag-chunk-lifecycle-service` for parent-child chunk compilation and Parquet slices.
- **Resilient Child Workflow Fan-Out (ADR-002)**:
  - Decomposes large document ingestion into root (`IngestionCoordinatorWorkflow`) and child (`DocumentPartitionWorkflow`) workflows to guarantee Temporal history event boundaries ($<10,000$ events per execution history, $<2,500$ in practice).
  - Enforces bounded child-workflow batches (e.g., $\le 100$ concurrent child workflows per root workflow generation) combined with `workflow.NewContinueAsNewError` (carrying forward remaining pending slice manifests and aggregated progress counters) or hierarchical sub-coordinator batches, ensuring both parent and child execution histories remain strictly below the $2,500$-event operational target and $10,000$-event hard ceiling.
- **High Availability & Redundancy**:
  - Stateless coordinator pods deployed across multi-AZ Kubernetes node groups behind load balancer with `PodDisruptionBudget` (`minAvailable: 2`).
  - Rolling deploy safety: Zero dropped client connections or interrupted ingestion runs during coordinator restarts.
- **Creational Design Patterns**:
  - Explicit dependency injection container wiring database pools, Redis clients, S3 presigners, and Temporal clients.

### Layer 4: Security & Identity
- **Tenant Isolation & Access Control (AuthN vs AuthZ)**:
  - Strict tenant boundary validation on every endpoint and workflow execution.
- **Storage Access & Lease Management**:
  - Scoped IAM roles via AWS STS issuing short-lived ($15\text{-minute}$) presigned S3 URLs for client uploads and worker access.
- **Secured Logging Invariant**:
  - Structured JSON logs only.
  - Automated redaction of AWS STS credentials, presigned S3 query parameters (`X-Amz-Signature`), bearer tokens, and tenant PII/PHI.
- **Container Hardening**:
  - Minimal distroless container image (`gcr.io/distroless/static:nonroot`) with zero OS shell or external utilities, non-root user execution, read-only root filesystem.

### Layer 5: Observability & Quality
- **Structured Logging**: Machine-parseable JSON logs containing `trace_id`, `tenant_id`, `document_id`, `workflow_id`, and duration.
- **Prometheus Observability Matrix**:
  - `coordinator_workflow_starts_total`: Counter tracking initiated root workflows by tenant.
  - `coordinator_child_workflows_spawned`: Histogram tracking fan-out slices per document.
  - `coordinator_redis_events_published_total`: Counter tracking telemetry publication success.
  - `coordinator_redis_events_errors_total`: Counter tracking telemetry publication failures.
  - `temporal_activity_schedule_to_start_seconds`: Latency tracking task queue backlog across worker pools.
- **Alert Rules**:
  - *High Task Queue Backlog*: Alert fires if `schedule_to_start` latency on `doc-ai-task-queue` exceeds $60\text{ seconds}$ for $>5\text{ minutes}$ (triggers GPU worker horizontal autoscaling).
  - *Elevated Workflow Failure Rate*: Alert fires if child workflow failure rate exceeds $2\%$ over a $15\text{-minute}$ rolling window.
  - *Redis Publish Drops*: Alert fires if Redis publish error rate exceeds $0.1\%$ over a $2\text{-minute}$ rolling window.
- **Health Checks & Graceful Error Handling**:
  - HTTP `/healthz` (liveness) and `/ready` (readiness probing PostgreSQL, Redis, and Temporal).
  - Clean signal interception, connection draining, and graceful shutdown.

---

## Pragmatic Monorepo Directory Boundaries

```
rag-ingestion-coordinator/
├── cmd/
│   └── coordinator/
│       └── main.go                 # Service lifecycle, config parsing, dependency wiring
├── api/
│   ├── proto/v1/
│   │   ├── coordinator.proto       # Public control plane gRPC service definitions
│   │   └── types.proto             # Shared domain payloads and manifest definitions
│   └── openapi/
│       └── openapi.yaml            # REST API specifications (presigning, run triggering)
├── internal/
│   ├── workflow/                   # Temporal Workflow definitions (deterministic code only)
│   │   ├── ingestion_root.go       # IngestionCoordinatorWorkflow (inspection & child fan-out)
│   │   ├── document_partition.go   # DocumentPartitionWorkflow (per-slice activity chain)
│   │   └── retry.go                # Shared retry and backoff definitions
│   ├── activity/                   # Temporal Activities & polyglot contracts
│   │   ├── presign.go              # S3 presigned URL generation activities
│   │   ├── inspection.go           # Header inspection activity (or dispatch to inspect queue)
│   │   ├── polyglot.go             # Polyglot DocAI extract & chunk compilation activity contracts
│   │   ├── catalog.go              # Terminal database persistence activities
│   │   └── notification.go         # Activity publishing milestones to Redis Pub/Sub
│   ├── service/                    # Core application logic
│   │   ├── upload_service.go       # Presigned URL generation and parameter validation
│   │   ├── execution_service.go    # Starting and querying Temporal workflows
│   │   └── manifest_planner.go     # Pure algorithm for calculating slice intervals
│   ├── store/                      # External data adapters
│   │   ├── postgres/
│   │   │   ├── client.go           # Connection pool setup
│   │   │   ├── pipeline_repo.go    # Pipeline configuration queries
│   │   │   └── run_repo.go         # Terminal run history and metric persistence
│   │   ├── redis/
│   │   │   └── publisher.go        # Redis Pub/Sub publisher for progress streaming
│   │   └── s3/
│   │       └── presigner.go        # AWS SDK v2 S3 Presign client wrapper
│   └── transport/                  # Inbound protocol handlers
│       ├── http/
│       │   ├── router.go           # Standard HTTP routing setup
│       │   ├── upload_handler.go   # REST endpoint: POST /v1/ingest/upload-url
│       │   └── middleware/         # Auth, rate limiting, and request logging
│       └── grpc/
│           └── coordinator_server.go # gRPC server implementation for internal control calls
├── deployments/
│   ├── helm/                       # Kubernetes Helm chart configuration
│   │   └── values.yaml
│   └── docker/
│       └── Dockerfile              # Multi-stage scratch/distroless build
├── test/
│   ├── unit/                       # Pure logic tests (manifest planner, validation)
│   ├── workflow/                   # Temporal TestSuite workflow execution tests
│   └── integration/                # Testcontainers tests for Postgres, Redis, LocalStack
├── Makefile
├── go.mod
└── go.sum
```

---

## Architectural Decision Records (ADRs)

- **ADR-001: Native Polyglot Temporal Workflows**: Replaces synchronous gRPC task proxying from Go activities to Python workers with direct Temporal Python SDK workers polling dedicated task queues (`doc-ai-task-queue`).
- **ADR-002: Entity / Child Workflow Fan-Out**: Mandates decomposing large document workflows into root (`IngestionCoordinatorWorkflow`) and child (`DocumentPartitionWorkflow`) workflows to guarantee Temporal history event bounds ($<10,000$ events).
- **ADR-003: Single Source of Truth for Checkpoints**: Deprecates transactional database checkpoint tables in favor of native Temporal heartbeat details, removing dual-write desynchronization.
- **ADR-004: Redis Pub/Sub Telemetry Layer**: Offloads real-time client streaming from coordinator pods to Redis to support stateless rolling deployments.
- **ADR-005: Control-Plane Arrow Exclusion**: Prohibits Apache Arrow libraries within the coordinator; data-plane columnar transfers remain strictly between compute workers and S3.
