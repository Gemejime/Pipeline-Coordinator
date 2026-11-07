# RAG Ingestion Coordinator - Project Overview

## Overview

The **`rag-ingestion-coordinator`** is the authoritative Go-based control plane microservice responsible for pipeline configuration, presigned ingestion handoffs, multi-part document pre-partitioning, and distributed Temporal workflow DAG coordination across multimodal ingestion pipelines.

The service functions purely on the **Control Plane** (stateless orchestration), decoupling ingestion signaling from heavy binary and columnar data transfers on the **Data Plane** (Zero Heap Ingestion):

```
CONTROL PLANE (Stateless & Orchestration)
┌───────────────────────────────────────────────────────────────────────────────────────────────┐
│                                         API Gateway                                           │
└───────────────┬───────────────────────────────────────────────▲───────────────────────────────┘
                │ 1. Request Presigned URL                      │ Real-time Events (Redis Pub/Sub)
                ▼                                               │
┌───────────────────────────────────────────────┐               │
│           rag-ingestion-coordinator           │               │
│ - Issues presigned S3 upload URLs             │               │
│ - Triggers Root Ingestion Temporal Workflow   │───────────────┘
│ - Emits milestone updates to Redis Pub/Sub    │
└───────────────────────┬───────────────────────┘
                        │ Starts Workflow
                        ▼
┌───────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       Temporal Cluster                                        │
│ - Orchestrates Root Ingestion Workflow & Child Workflows                                     │
│ - Manages Task Queues: [inspection-queue], [doc-ai-task-queue], [chunk-lifecycle-queue]       │
│ - Tracks Heartbeat Details & Checkpoints natively (Zero Dual-Writes)                          │
└───────┬───────────────────────────────┬───────────────────────────────┬───────────────────────┘
        │ Task: Inspect & Manifest      │ Task: Extract Segment         │ Task: Compile Chunks
        ▼                               ▼                               ▼
┌───────────────────────┐       ┌───────────────────────┐       ┌───────────────────────────────┐
│  Inspection Activity  │       │  Python Worker Pool   │       │  rag-chunk-lifecycle-service  │
│      Worker Pod       │       │   (Docling/OCR/ASR)   │       │   (Pulls Parquet / Slices)    │
│ (Range Byte Parsing)  │       │ (Temporal Python SDK) │       │     (Temporal Go Worker)      │
└───────┬───────────────┘       └───────┬───────────────┘       └───────────────┬───────────────┘
        │                               │                                       │
        └───────────────────────────────┼───────────────────────────────────────┘
                                        ▼ DATA PLANE (Zero Heap Ingestion)
                        ┌───────────────────────────────────────┐
                        │       S3 / Cloud Object Storage       │
                        │ - Raw Monolithic Binaries (≤ 10 GB)   │
                        │ - Content-Addressed Parquet Slices    │
                        └───────────────────────────────────────┘
```

---

## Strategic Objectives

1. **Pure Control Plane Orchestration**: Drive complex multimodal ingestion workflows via Temporal without buffering, parsing, or streaming binary payloads or columnar Arrow buffers through the coordinator.
2. **Polyglot Activity Scheduling**: Coordinate language-specialized workers (Go for storage/metadata, Python for Document AI/GPU operations) using native Temporal task queues, decoupling task execution from synchronous transport protocols.
3. **Resilient Child Workflow Execution**: Isolate document slice executions into independent child workflows (`DocumentPartitionWorkflow`) to prevent event history exhaustion on multi-gigabyte or high-page-count documents ($<10,000$ events per execution history).
4. **Single Source of Truth for State (Zero Dual-Writes)**: Rely solely on Temporal workflow state and heartbeat payload details for in-flight task progress and crash recovery, writing to PostgreSQL only at pipeline boundaries (`COMPLETED` or `FAILED`).
5. **Scalable Real-Time Telemetry**: Publish pipeline stage events asynchronously via Redis Pub/Sub to decouple ingestion coordination from client connection lifecycles.
6. **Continuous Documentation & Contract Synchronization**: Ensure internal OpenAPI 3.1 specifications (`api/openapi/openapi.yaml`) and `.ai/doc-api.md` are continuously synchronized with every code change.

---

## Calibrated Quantitative SLOs & SLIs

- **P99 Presigned URL Generation Latency**: $<35\text{ ms}$ under sustained loads of $1,000\text{ requests per second}$.
- **Coordinator Memory Ceiling**: Strictly $<256\text{ MiB}$ resident set size (RSS) per pod, maintaining a flat memory profile regardless of incoming file size (tested up to $10\text{ GB}$).
- **Workflow Scale Capacity**: Ability to orchestrate documents exceeding $20,000\text{ pages}$ ($400+\text{ child workflows}$) without exceeding $10,000$ events in any single workflow execution history ($<2,500$ events per workflow in practice).
- **Zero Dual-Write Desynchronization**: $100\%$ state consistency during compute worker crashes, with failed activities resuming from their last recorded heartbeat detail without duplicate work or database conflicts.
- **Rolling Deploy Safety**: $0$ dropped client connections or interrupted ingestion runs during coordinator pod restarts, achieved via Redis-backed event pub/sub and Temporal workflow durability.

---

## Scope & Exclusions

### In-Scope
- **Upload Coordination**: Tenant authentication pass-through, payload validation, and presigned S3 upload URI generation with cryptographically secure expiration policies (AWS STS, 15-minute lease), supporting both single-part and multipart upload handoffs for payloads up to $10\text{ GB}$.
- **Workflow Root Scheduling**: Starting `IngestionCoordinatorWorkflow` instances and registering pipeline metadata in PostgreSQL.
- **Child Workflow Fan-Out**: Spawning parallel `DocumentPartitionWorkflow` instances across pre-calculated slice intervals via `manifest_planner`.
- **Telemetry Emission**: Emitting structured progress frames to Redis Pub/Sub for down-funnel consumption by API gateways and notification dispatchers.
- **Lifecycle Persistence**: Recording terminal states, execution duration, token/page metrics, and error logs in PostgreSQL upon workflow completion or fatal failure.

### Exclusions (Explicitly Out-of-Scope)
- **Byte-Level Header Inspection**: Direct extraction of PDF cross-reference tables or media container atoms inside the coordinator process (delegated to an isolated inspection activity worker).
- **Direct Compute Proxying**: Proxied gRPC calls to Python parsers inside Go activities (Python workers poll Temporal directly on `doc-ai-task-queue`).
- **Columnar Data Transport**: Importing Apache Arrow Flight or serializing large tensor/chunk embeddings (handled directly between Python workers, S3, and chunk lifecycle services).
- **Client WebSocket/SSE Connection Termination**: Managing long-lived HTTP client streams (delegated to the edge API Gateway or notification daemon via Redis).
- **In-Flight Checkpoint Tracking in PostgreSQL**: Maintaining intermediate segment execution states in relational database tables during active runs (handled via native Temporal heartbeat details).
