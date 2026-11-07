# Documented APIs & Contracts - RAG Ingestion Coordinator

This document is the authoritative specification for all public and internal interfaces of the `rag-ingestion-coordinator`.

> [!IMPORTANT]
> **Continuous Doc API Updates**: This file and `api/openapi/openapi.yaml` MUST be kept synchronized on **every code change or addition**, regardless of how small the modification is.

---

## 1. OpenAPI 3.1 REST API Specification

### Base URL
`http://localhost:8080` (Internal Service / Ingress Gateway)

### Endpoints Overview

| Method | Endpoint | Description | Auth Scope |
| :--- | :--- | :--- | :--- |
| `POST` | `/v1/ingest/upload-url` | Generate presigned S3 upload URL with expiration lease | `ingest:write` |
| `POST` | `/v1/ingest/workflows/start` | Trigger Root Ingestion Temporal Workflow | `ingest:write` |
| `GET` | `/v1/ingest/workflows/{id}/status` | Query in-flight workflow status and terminal run metrics | `ingest:read` |
| `GET` | `/healthz` | Kubernetes Liveness Probe | Public |
| `GET` | `/ready` | Kubernetes Readiness Probe (checks DB, Redis, Temporal) | Public |
| `GET` | `/metrics` | Prometheus Metrics Scrape Endpoint | Public / Ops |
| `GET` | `/openapi.yaml` | Raw OpenAPI 3.1 YAML specification | Public |
| `GET` | `/docs` | Swagger UI documentation interface | Public |

---

### Detailed Endpoint Contracts

#### 1. `POST /v1/ingest/upload-url`
Issues a cryptographically signed, short-lived S3 upload URL for zero-heap client uploads.

- **Request Headers**:
  - `Authorization: Bearer <token>`
  - `Content-Type: application/json`
  - `X-Tenant-ID: <string>`
- **Request Body**:
```json
{
  "tenant_id": "tenant-corp-01",
  "filename": "annual-report-2026.pdf",
  "content_type": "application/pdf",
  "content_length_bytes": 104857600,
  "content_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}
```
- **Response `200 OK`**:
```json
{
  "upload_id": "upl_01HXYZ1234567890ABCDEF",
  "tenant_id": "tenant-corp-01",
  "s3_bucket": "rag-ingestion-bucket",
  "s3_key": "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/annual-report-2026.pdf",
  "presigned_url": "https://rag-ingestion-bucket.s3.amazonaws.com/raw/...?[REDACTED_IN_LOGS]",
  "expires_at": "2026-11-07T08:35:00Z",
  "lease_seconds": 900
}
```
- **Response `400 Bad Request`**:
```json
{
  "error": "BAD_REQUEST",
  "message": "File size exceeds 10GB maximum limit or unsupported MIME type"
}
```

---

#### 2. `POST /v1/ingest/workflows/start`
Starts an `IngestionCoordinatorWorkflow` instance on Temporal.

- **Request Body**:
```json
{
  "tenant_id": "tenant-corp-01",
  "document_id": "doc-01HXYZ1234567890",
  "s3_bucket": "rag-ingestion-bucket",
  "s3_key": "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/annual-report-2026.pdf",
  "content_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "total_bytes": 104857600,
  "mime_type": "application/pdf",
  "pipeline_config_id": "default-multimodal-v1"
}
```
- **Response `202 Accepted`**:
```json
{
  "workflow_id": "ingest-tenant-corp-01-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "run_id": "4b68a85c-197e-4074-a698-c9ceca2ef11d",
  "status": "RUNNING",
  "started_at": "2026-11-07T08:20:00Z"
}
```

---

#### 3. `GET /v1/ingest/workflows/{id}/status`
Retrieves in-flight state from Temporal or terminal record from PostgreSQL.

- **Response `200 OK`**:
```json
{
  "workflow_id": "ingest-tenant-corp-01-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "run_id": "4b68a85c-197e-4074-a698-c9ceca2ef11d",
  "tenant_id": "tenant-corp-01",
  "status": "COMPLETED",
  "total_slices": 8,
  "completed_slices": 8,
  "failed_slices": 0,
  "total_pages": 400,
  "duration_seconds": 45.2,
  "created_at": "2026-11-07T08:20:00Z",
  "completed_at": "2026-11-07T08:20:45Z"
}
```

---

#### 4. `GET /healthz` & `GET /ready`
- **`GET /healthz`**: Returns `200 OK` with `{"status": "healthy"}`.
- **`GET /ready`**: Returns `200 OK` when PostgreSQL, Redis, and Temporal clients are connected:
```json
{
  "status": "ready",
  "checks": {
    "temporal": "connected",
    "postgres": "connected",
    "redis": "connected"
  }
}
```

---

## 2. Protobuf v3 gRPC Contracts

### Service Definition (`api/proto/v1/coordinator.proto`)

```protobuf
syntax = "proto3";

package rag.coordinator.v1;

option go_package = "rag-ingestion-coordinator/api/proto/v1;coordinatorv1";

service CoordinatorService {
  rpc CreateUploadURL(CreateUploadURLRequest) returns (CreateUploadURLResponse);
  rpc StartIngestion(StartIngestionRequest) returns (StartIngestionResponse);
  rpc GetIngestionStatus(GetIngestionStatusRequest) returns (GetIngestionStatusResponse);
}

message CreateUploadURLRequest {
  string tenant_id = 1;
  string filename = 2;
  string content_type = 3;
  int64 content_length_bytes = 4;
  string content_hash = 5;
}

message CreateUploadURLResponse {
  string upload_id = 1;
  string tenant_id = 2;
  string s3_bucket = 3;
  string s3_key = 4;
  string presigned_url = 5;
  int64 expires_at_unix = 6;
  int32 lease_seconds = 7;
}

message StartIngestionRequest {
  string tenant_id = 1;
  string document_id = 2;
  string s3_bucket = 3;
  string s3_key = 4;
  string content_hash = 5;
  int64 total_bytes = 6;
  string mime_type = 7;
  string pipeline_config_id = 8;
}

message StartIngestionResponse {
  string workflow_id = 1;
  string run_id = 2;
  string status = 3;
  int64 started_at_unix = 4;
}

message GetIngestionStatusRequest {
  string workflow_id = 1;
  string tenant_id = 2;
}

message GetIngestionStatusResponse {
  string workflow_id = 1;
  string run_id = 2;
  string tenant_id = 3;
  string status = 4;
  int32 total_slices = 5;
  int32 completed_slices = 6;
  int32 failed_slices = 7;
  int32 total_pages = 8;
  double duration_seconds = 9;
}
```

---

## 3. Temporal Workflow & Activity Signatures

### Workflows (`internal/workflow/`)

```go
// IngestionCoordinatorWorkflow orchestrates the end-to-end ingestion DAG.
// It executes inspection, plans slice manifests, and fans out child workflows.
func IngestionCoordinatorWorkflow(ctx workflow.Context, req IngestionRequest) (*IngestionResult, error)

// DocumentPartitionWorkflow executes the per-slice activity chain in isolation
// to prevent Temporal history event exhaustion.
func DocumentPartitionWorkflow(ctx workflow.Context, slice PartitionSlice) (*PartitionResult, error)
```

### Activities (`internal/activity/`)

| Activity | Task Queue | Description | Heartbeat Cadence |
| :--- | :--- | :--- | :--- |
| `InspectDocumentHeaderActivity` | `inspection-queue` | Performs random-access byte inspection on S3 objects to read page counts and layout metadata | 10s |
| `PresignS3ArtifactActivity` | Go Coordinator | Generates presigned URLs for child worker artifact emission | N/A (<50ms) |
| `DispatchDocAIExtractActivity` | `doc-ai-queue` | Polled by Python Doc AI workers for layout parsing, OCR, and chunking | 15s |
| `CompileChunksActivity` | `chunk-lifecycle-queue` | Polled by `rag-chunk-lifecycle-service` to build chunk trees | 10s |
| `PublishMilestoneActivity` | Go Coordinator | Publishes stage transition events to Redis Pub/Sub | N/A (<20ms) |
| `CatalogTerminalRunActivity` | Go Coordinator | Writes final `COMPLETED` or `FAILED` record to PostgreSQL | N/A (Atomic write) |

---

## 4. PostgreSQL Database Schema

```sql
-- Pipeline Configuration Catalog
CREATE TABLE IF NOT EXISTS pipeline_configs (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    chunk_size_tokens INT NOT NULL DEFAULT 300,
    chunk_overlap_tokens INT NOT NULL DEFAULT 50,
    ocr_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Terminal Pipeline Run History (Written ONLY upon workflow completion or fatal failure)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id VARCHAR(255) NOT NULL UNIQUE,
    run_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    document_id VARCHAR(255) NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL, -- 'COMPLETED', 'FAILED', 'TERMINATED'
    total_pages INT NOT NULL DEFAULT 0,
    total_slices INT NOT NULL DEFAULT 0,
    duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_tenant_created 
ON pipeline_runs (tenant_id, created_at DESC);
```

---

## 5. Redis Pub/Sub Event Schemas

### Channel Pattern
`ingestion:events:<tenant_id>`

### Payload Schema
```json
{
  "event_id": "evt_01HXYZ1234567890",
  "tenant_id": "tenant-corp-01",
  "document_id": "doc-01HXYZ1234567890",
  "workflow_id": "ingest-tenant-corp-01-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "stage": "EXTRACT_SEGMENTS",
  "status": "PROGRESS",
  "progress_pct": 65.0,
  "slices_completed": 5,
  "total_slices": 8,
  "timestamp": "2026-11-07T08:20:30Z"
}
```
