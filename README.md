# RAG Ingestion Coordinator (`rag-ingestion-coordinator`)

[![CI](https://img.shields.io/badge/CI-passing-2ea44f?logo=github-actions&logoColor=white)](.github/workflows/ci.yml)
[![CD](https://img.shields.io/badge/CD-ready-blue?logo=github-actions&logoColor=white)](.github/workflows/cd-deploy.yml)
[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)](go.mod)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

The **`rag-ingestion-coordinator`** is the authoritative Go-based control plane microservice for the Multimodal RAG platform. It orchestrates document ingestion workflows across polyglot execution workers using Temporal workflow DAGs, S3 presigned URL leases, Redis Pub/Sub telemetry, and PostgreSQL metadata persistence.

The service operates strictly on the **Control Plane** (stateless orchestration), completely decoupled from the heavy binary and columnar data transfers on the **Data Plane** (**Zero-Heap Ingestion**).

---

## Architecture Overview

```
CONTROL PLANE (Stateless Orchestration & Coordination)
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                       API Gateway                                         │
└────────────────┬───────────────────────────────────────────▲──────────────────────────────┘
                 │ 1. Request Presigned URL                  │ Real-Time Telemetry (Redis Pub/Sub)
                 ▼                                           │
┌───────────────────────────────────────────┐                │
│         rag-ingestion-coordinator         │                │
│  - Issues presigned S3 upload URLs        │                │
│  - Triggers Root Ingestion DAG Workflow   │────────────────┘
│  - Emits milestone updates to Redis       │
└─────────────────────┬─────────────────────┘
                      │ Starts Root Workflow
                      ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                                     Temporal Cluster                                      │
│  - Root Workflow: IngestionCoordinatorWorkflow                                            │
│  - Child Workflows: DocumentPartitionWorkflow (<10,000 events ceiling)                   │
│  - Tracks Heartbeat Details & Checkpoints natively (ADR-003: Zero Dual-Writes)             │
└─────────┬─────────────────────────────┬───────────────────────────┬───────────────────────┘
          │ [inspection-queue]          │ [doc-ai-task-queue]       │ [chunk-lifecycle-queue]
          ▼                             ▼                           ▼
┌───────────────────┐         ┌───────────────────┐       ┌─────────────────────────────────┐
│Inspection Activity│         │Python Worker Pool │       │   rag-chunk-lifecycle-service   │
│   (Go / Python)   │         │ (Docling/OCR/ASR) │       │   (Go Chunk Compiler Worker)    │
│(Range Byte Header)│         │(Temporal SDK v1.9)│       │    (Pulls Parquet from S3)      │
└─────────┬─────────┘         └─────────┬─────────┘       └─────────────────┬───────────────┘
          │                             │                                   │
          └─────────────────────────────┼───────────────────────────────────┘
                                        ▼ DATA PLANE (Zero Coordinator Heap)
                      ┌───────────────────────────────────┐
                      │     S3 / Cloud Object Storage     │
                      │ - Raw Multimodal Binaries (≤10GB) │
                      │ - Content-Addressed Parquet Slices│
                      └───────────────────────────────────┘
```

---

## Core System Invariants

1. **Zero-Heap Ingestion ($< 256\text{ MiB}$ RSS)**:
   The coordinator never buffers, streams, or parses raw binary files or columnar Arrow buffers in-memory. Binary payloads up to $10\text{ GB}$ travel directly from clients to S3 via presigned multipart URLs.
2. **Deterministic Temporal Workflow DAGs**:
   Root `IngestionCoordinatorWorkflow` fans out slices into discrete child `DocumentPartitionWorkflow` executions. To prevent event history exhaustion, child workflows are bounded to batches with `workflow.NewContinueAsNewError`, ensuring execution histories stay strictly below $2,500$ operational events ($10,000$ hard limit).
3. **Zero Dual-Writes (ADR-003)**:
   In-flight task progress and crash recovery checkpoints rely solely on native Temporal workflow state and heartbeat payload details (`activity.GetHeartbeatDetails`). PostgreSQL records **zero intermediate rows**, executing a single atomic write only when the workflow reaches `COMPLETED` or `FAILED`.
4. **Decoupled Real-Time Telemetry (ADR-004)**:
   Pipeline milestone progress is published asynchronously to Redis Pub/Sub (`ingestion:events:<tenant_id>`), allowing coordinator pods to be restarted or rolled out without interrupting client progress streams.
5. **Continuous Contract Synchronization**:
   API contracts defined in [`.ai/doc-api.md`](.ai/doc-api.md) and [`api/openapi/openapi.yaml`](api/openapi/openapi.yaml) are kept strictly in sync on every commit, enforced by automated CI gates (`spectral` + contract verification).

---

## Polyglot Task Queues & Workers

| Queue Name | Worker Technology | Primary Responsibility |
| :--- | :--- | :--- |
| `inspection-queue` | Go (`internal/activity/inspection.go`) | Non-destructive byte-range header inspection (PDF, audio, video, tabular) via 64 KiB chunks. |
| `doc-ai-task-queue` | Python (`workers/python/rag_document_ai_worker/`) | Heavy compute extraction using Docling, OCR, Whisper ASR, outputting content-addressed Parquet slices to S3. |
| `chunk-lifecycle-queue` | Go (`rag-chunk-lifecycle-service`) | Downstream parent-child chunk compilation, tokenization, and vector store preparation. |

---

## Directory Structure

```
.
├── .ai/                       # Engineering specifications, ADRs, and calibrated roadmaps
├── .github/                   # CI/CD workflows (Go/Python lint, govulncheck, gosec, Helm CD)
├── api/
│   ├── openapi/               # OpenAPI 3.1 specification (openapi.yaml)
│   └── proto/v1/              # Protobuf v1 contracts and generated Go bindings
├── cmd/
│   └── coordinator/           # Microservice entrypoint (main.go)
├── deployments/
│   ├── argocd/                # GitOps Application manifests (Redis cluster, Argo Rollouts)
│   ├── docker/                # Multi-stage distroless Dockerfile & Docker Compose
│   ├── helm/                  # Production Helm chart & canary Rollout configurations
│   ├── redis-cluster/         # Bitnami Redis Cluster 7.2 production values
│   └── SECRET_MANAGEMENT.md   # Production secret inventory and rotation procedures
├── internal/
│   ├── activity/              # Temporal activity implementations (Inspection, Presign, Catalog)
│   ├── config/                # 12-factor configuration & pure stdlib dotenv parser
│   ├── model/                 # Domain models, database entities, and status enums
│   ├── service/               # Core business services (UploadService, ExecutionService)
│   ├── store/                 # Storage adapters (PostgreSQL pgxpool, Redis Pub/Sub, AWS S3)
│   ├── telemetry/             # Prometheus metrics registry and Temporal metric bridge
│   ├── transport/             # Ingress transports (HTTP REST router & gRPC server)
│   └── workflow/              # Temporal workflow DAGs (Root Coordinator, Child Partition)
├── migrations/                # Embedded PostgreSQL schema migrations (init_schema.up.sql)
├── scripts/                   # Operations & deployment automation (sync, secret injection, Redis)
├── test/
│   ├── chaos/                 # High-scale chaos & 20,000-page partition resilience suites
│   ├── integration/           # Testcontainers integration tests (PostgreSQL & Redis)
│   └── unit/                  # Comprehensive Go unit tests
└── workers/
    └── python/                # Native Temporal Python Document AI worker
```

---

## API Endpoints & Interfaces

### HTTP REST API (`:8080`)

All endpoints require Bearer JWT authentication with tenant-bound claims (`sub`, `tenant_id`, `scopes`):

| Method | Path | Required Scope | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/v1/ingest/upload-url` | `ingest:write` | Issues presigned single-part ($<100\text{ MiB}$) or multipart S3 upload URLs with 15-minute lease. |
| `POST` | `/v1/ingest/upload-url/complete` | `ingest:write` | Finalizes S3 multipart upload with client-supplied part ETags. |
| `POST` | `/v1/ingest/workflows/start` | `ingest:write` | Schedules idempotent `IngestionCoordinatorWorkflow` DAG execution. |
| `GET` | `/v1/ingest/workflows/{id}/status` | `ingest:read` | Queries live Temporal status or terminal PostgreSQL run metadata. |
| `GET` | `/healthz` | *public* | Liveness probe verifying process runtime health. |
| `GET` | `/ready` | *public* | Readiness probe verifying PostgreSQL, Redis, and Temporal connectivity (drain-aware). |
| `GET` | `/metrics` | *public* | Prometheus metrics scrape endpoint. |
| `GET` | `/openapi.yaml` | *public* | OpenAPI 3.1 specification. |

### gRPC Service (`:50051`)

Defined in [`api/proto/v1/types.proto`](api/proto/v1/types.proto):
- `CreateUploadURL(CreateUploadURLRequest) -> CreateUploadURLResponse`
- `CompleteUploadURL(CompleteMultipartUploadRequest) -> CompleteMultipartUploadResponse`
- `StartIngestion(StartIngestionRequest) -> StartIngestionResponse`
- `GetIngestionStatus(GetIngestionStatusRequest) -> GetIngestionStatusResponse`

---

## Local Development Quickstart

### Prerequisites
- **Go**: `1.26+` (or `1.23+`)
- **Python**: `3.10+`
- **Docker & Docker Compose**: For local infrastructure (PostgreSQL, Redis, Temporal)
- **kubectl & helm**: For Kubernetes deployment testing

### 1. Start Local Infrastructure
```bash
# Start standalone Redis 7.2 with AOF persistence
docker compose -f deployments/docker/docker-compose.redis.yml up -d
```

### 2. Configure Environment
```bash
# Copy example configuration for local development
cp .env.example .env

# Verify configuration drift between .env.prod and Helm values
python scripts/sync_env_to_deployments.py --check-only
```

### 3. Run the Go Coordinator
```bash
# Run database migrations and start HTTP (:8080) and gRPC (:50051) servers
go run cmd/coordinator/main.go
```

### 4. Run the Python Document AI Worker
```bash
cd workers/python
python -m venv .venv
source .venv/bin/activate  # Or on Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m rag_document_ai_worker.worker
```

---

## Quality Gates & Verification

Run all test suites and static analysis gates prior to committing:

```bash
# 1. Format code
gofmt -s -w .

# 2. Run Go vet analysis
go vet ./...

# 3. Run Go unit & integration tests
go test -v -race ./test/unit/...

# 4. Run Python unit tests
python -m unittest discover -s workers/python/tests

# 5. Run GolangCI-Lint
golangci-lint run --timeout=5m

# 6. Verify zero configuration drift
python scripts/sync_env_to_deployments.py --check-only
```

---

## Security & Secrets Management

- **No Cleartext Credentials**: Raw presigned URLs containing `X-Amz-Signature` or AWS session tokens must **never** appear in application logs or standard error streams.
- **Secret Rotation Runbooks**: Detailed procedures for zero-downtime PostgreSQL credential rotation and planned HMAC token signing updates are documented in [`deployments/SECRET_MANAGEMENT.md`](deployments/SECRET_MANAGEMENT.md).
- **Distroless Containerization**: Production container images are built from `gcr.io/distroless/static:nonroot` with UID 65532, zero shell binaries, and a read-only root filesystem.
