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
| `POST` | `/v1/ingest/upload-url` | Generate presigned S3 upload URL (single-part or multipart lease) | `ingest:write` |
| `POST` | `/v1/ingest/upload-url/complete` | Finalize S3 multipart upload with part ETag list | `ingest:write` |
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
Issues a cryptographically signed, short-lived S3 upload URL for zero-heap client uploads (supports single-part for ≤100MB and multipart for >100MB up to 10GB).

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
- **Response `200 OK` (Single-Part / <= 100MB)**:
```json
{
  "upload_id": "upl_01HXYZ1234567890ABCDEF",
  "tenant_id": "tenant-corp-01",
  "s3_bucket": "rag-ingestion-bucket",
  "s3_key": "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/annual-report-2026.pdf",
  "presigned_url": "https://rag-ingestion-bucket.s3.amazonaws.com/raw/...?[REDACTED_IN_LOGS]",
  "expires_at_unix": 1794040500,
  "lease_seconds": 900
}
```
- **Response `200 OK` (Multipart / > 100MB up to 10GB)**:
```json
{
  "upload_id": "mp_upload_01HXYZ1234567890ABCDEF",
  "tenant_id": "tenant-corp-01",
  "s3_bucket": "rag-ingestion-bucket",
  "s3_key": "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/annual-report-2026.pdf",
  "part_size_bytes": 104857600,
  "parts": [
    {
      "part_number": 1,
      "presigned_url": "https://rag-ingestion-bucket.s3.amazonaws.com/raw/...?partNumber=1&[REDACTED_IN_LOGS]"
    },
    {
      "part_number": 2,
      "presigned_url": "https://rag-ingestion-bucket.s3.amazonaws.com/raw/...?partNumber=2&[REDACTED_IN_LOGS]"
    }
  ],
  "expires_at_unix": 1794040500,
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

#### 2. `POST /v1/ingest/upload-url/complete`
Finalizes an S3 multipart upload after the client has uploaded all parts.

- **Request Body**:
```json
{
  "upload_id": "mp_upload_01HXYZ1234567890ABCDEF",
  "tenant_id": "tenant-corp-01",
  "s3_bucket": "rag-ingestion-bucket",
  "s3_key": "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/annual-report-2026.pdf",
  "parts": [
    {
      "part_number": 1,
      "etag": "\"d41d8cd98f00b204e9800998ecf8427e\""
    },
    {
      "part_number": 2,
      "etag": "\"a1b2c3d4e5f6789012345678abcdef01\""
    }
  ]
}
```
- **Response `200 OK`**:
```json
{
  "upload_id": "mp_upload_01HXYZ1234567890ABCDEF",
  "status": "COMPLETED",
  "s3_location": "s3://rag-ingestion-bucket/raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/annual-report-2026.pdf",
  "completed_at": "2026-11-07T08:30:00Z"
}
```

---

#### 3. `POST /v1/ingest/workflows/start`
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
  "started_at_unix": 1794039600
}
```

---

#### 4. `GET /v1/ingest/workflows/{id}/status`
Retrieves in-flight state from Temporal or terminal record from PostgreSQL.
- **Query Parameters**:
  - `tenant_id` (required): Tenant identifier filter; validated strictly against the authenticated Bearer token claims to prevent cross-tenant access.

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

#### 5. `GET /healthz` & `GET /ready`
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
  rpc CompleteUploadURL(CompleteMultipartUploadRequest) returns (CompleteMultipartUploadResponse);
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

message PresignedPart {
  int32 part_number = 1;
  string presigned_url = 2;
}

message CreateUploadURLResponse {
  string upload_id = 1;
  string tenant_id = 2;
  string s3_bucket = 3;
  string s3_key = 4;
  string presigned_url = 5;
  int64 expires_at_unix = 6;
  int32 lease_seconds = 7;
  int64 part_size_bytes = 8;
  repeated PresignedPart parts = 9;
}

message CompletedPart {
  int32 part_number = 1;
  string etag = 2;
}

message CompleteMultipartUploadRequest {
  string upload_id = 1;
  string tenant_id = 2;
  string s3_bucket = 3;
  string s3_key = 4;
  repeated CompletedPart parts = 5;
}

message CompleteMultipartUploadResponse {
  string upload_id = 1;
  string status = 2;
  string s3_location = 3;
  string completed_at = 4; // RFC 3339 / ISO-8601 UTC timestamp
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
  string created_at = 10;   // RFC 3339 / ISO-8601 UTC timestamp
  string completed_at = 11; // RFC 3339 / ISO-8601 UTC timestamp
}
```

### Shared Domain & Polyglot Activity Payloads (`api/proto/v1/types.proto`)

```protobuf
syntax = "proto3";

package rag.coordinator.v1;

option go_package = "rag-ingestion-coordinator/api/proto/v1;coordinatorv1";

enum MultimodalModality {
  MULTIMODAL_MODALITY_UNSPECIFIED = 0;
  MULTIMODAL_MODALITY_DOCUMENT_PDF = 1;
  MULTIMODAL_MODALITY_AUDIO = 2;
  MULTIMODAL_MODALITY_VIDEO = 3;
  MULTIMODAL_MODALITY_TABULAR = 4;
}

message PartitionSlice {
  string slice_id = 1;
  int32 slice_index = 2;
  int32 total_slices = 3;
  string tenant_id = 4;
  string document_id = 5;
  string content_hash = 6;
  string s3_input_bucket = 7;
  string s3_input_key = 8;
  string s3_output_parquet_key = 9;
  MultimodalModality modality = 10;
  int32 start_page = 11;
  int32 end_page = 12;
  double start_time_seconds = 13;
  double end_time_seconds = 14;
  double overlap_seconds = 15;
  string pipeline_config_id = 16;
  PipelineConfig pipeline_config = 17;
}

// HeartbeatDetails represents the native Temporal activity heartbeat payload (ADR-003)
message HeartbeatDetails {
  string slice_id = 1;
  int32 slice_index = 2;
  int32 last_processed_unit = 3; // e.g. last page number or seconds offset
  int32 processed_units = 4;
  int32 total_units = 5;
  string intermediate_s3_key = 6;
  int64 updated_at_unix = 7;
}

message PipelineConfig {
  string id = 1;
  string tenant_id = 2;
  string name = 3;
  int32 chunk_size_tokens = 4;
  int32 chunk_overlap_tokens = 5;
  bool ocr_enabled = 6;
}

message ProgressMilestoneEvent {
  string event_id = 1;
  int64 sequence_number = 2;
  string tenant_id = 3;
  string document_id = 4;
  string workflow_id = 5;
  string stage = 6;
  string status = 7;
  double progress_pct = 8;
  int32 slices_completed = 9;
  int32 total_slices = 10;
  int64 timestamp_unix = 11;
}

// Polyglot Activity Contracts across Go and Python Workers

message InspectDocumentRequest {
  string tenant_id = 1;
  string document_id = 2;
  string s3_bucket = 3;
  string s3_key = 4;
  string content_hash = 5;
  int64 total_bytes = 6;
  string mime_type = 7;
}

message InspectDocumentResponse {
  MultimodalModality modality = 1;
  int32 total_pages = 2;
  double duration_seconds = 3;
  int32 recommended_slice_pages = 4;
  double recommended_window_seconds = 5;
}

message PresignSliceArtifactRequest {
  PartitionSlice slice = 1;
  int32 lease_seconds = 2;
}

message PresignSliceArtifactResponse {
  string s3_bucket = 1;
  string s3_input_key = 2;
  string s3_output_parquet_key = 3;
  string presigned_input_get_url = 4;    // Redacted in logs
  string presigned_output_put_url = 5;   // Redacted in logs
  int64 expires_at_unix = 6;
}

message ExtractSegmentRequest {
  PartitionSlice slice = 1;
  string presigned_input_get_url = 2;    // Redacted in logs
  string presigned_output_put_url = 3;   // Redacted in logs
  PipelineConfig pipeline_config = 4;
}

message ExtractSegmentResponse {
  string slice_id = 1;
  int32 slice_index = 2;
  string s3_output_bucket = 3;
  string s3_output_parquet_key = 4;
  int32 extracted_pages = 5;
  int32 extracted_segments = 6;
  int64 parquet_bytes = 7;
}

message CompileChunksRequest {
  PartitionSlice slice = 1;
  string s3_output_bucket = 2;
  string s3_output_parquet_key = 3;
  PipelineConfig pipeline_config = 4;
}

message CompileChunksResponse {
  string slice_id = 1;
  int32 slice_index = 2;
  int32 compiled_chunks = 3;
  int32 total_tokens = 4;
}

message PublishMilestoneRequest {
  ProgressMilestoneEvent event = 1;
}

message PublishMilestoneResponse {
  string channel = 1;
  int64 sequence_number = 2;
  bool published = 3;
}

message CatalogTerminalRunRequest {
  string workflow_id = 1;
  string run_id = 2;
  string tenant_id = 3;
  string document_id = 4;
  string content_hash = 5;
  string status = 6; // COMPLETED, FAILED, TERMINATED
  int32 total_pages = 7;
  int32 total_slices = 8;
  int32 completed_slices = 9;
  int32 failed_slices = 10;
  double duration_seconds = 11;
  string error_message = 12;
}

message CatalogTerminalRunResponse {
  string record_id = 1;
  bool persisted = 2;
}
```

---

## 3. Temporal Workflow & Activity Signatures

### Workflows, Queries & Search Attributes (`internal/workflow/`)

```go
// QueryTypeIngestionStatus is the Temporal query handler name for in-flight status inspection (ADR-003).
const QueryTypeIngestionStatus = "ingestion_status"

// Typed Temporal Search Attributes enabled on the cluster:
// - TenantID (Keyword)
// - DocumentID (Keyword)
// - PipelineRunID (Keyword)

// IngestionCoordinatorWorkflow orchestrates the end-to-end ingestion DAG.
// It executes inspection, plans slice manifests, fans out child workflows in bounded
// batches (<=100 slices per generation with ContinueAsNew), isolates poison-pill
// slice failures (Runbook 3), and persists a single terminal catalog record (ADR-003).
func IngestionCoordinatorWorkflow(ctx workflow.Context, req IngestionRequest) (*IngestionResult, error)

// DocumentPartitionWorkflow executes the per-slice activity chain in isolation
// to prevent Temporal history event exhaustion.
func DocumentPartitionWorkflow(ctx workflow.Context, slice PartitionSlice) (*PartitionResult, error)
```

### Activities (`internal/activity/`)

| Activity | Task Queue | Description | Heartbeat Cadence |
| :--- | :--- | :--- | :--- |
| `InspectDocumentHeaderActivity` | `inspection-queue` | Performs random-access byte inspection on S3 objects to read page counts and layout metadata | 10s |
| `PresignS3ArtifactActivity` | `coordinator-task-queue` | Generates 15-minute presigned URLs for child worker input range reading and Parquet emission | N/A (<50ms) |
| `DispatchDocAIExtractActivity` | `doc-ai-task-queue` | Polled by Python Doc AI workers for layout parsing, OCR, ASR, and Parquet slice emission | 15s |
| `CompileChunksActivity` | `chunk-lifecycle-queue` | Polled by `rag-chunk-lifecycle-service` to build parent-child chunk trees from Parquet slices | 10s |
| `PublishMilestoneActivity` | `coordinator-task-queue` | Publishes stage transition events with monotonic `sequence_number` to Redis Pub/Sub | N/A (<20ms) |
| `CatalogTerminalRunActivity` | `coordinator-task-queue` | Writes final atomic `COMPLETED`, `FAILED`, or `TERMINATED` record to PostgreSQL | N/A (Atomic write) |

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
    completed_slices INT NOT NULL DEFAULT 0,
    failed_slices INT NOT NULL DEFAULT 0,
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
  "sequence_number": 1,
  "tenant_id": "tenant-corp-01",
  "document_id": "doc-01HXYZ1234567890",
  "workflow_id": "ingest-tenant-corp-01-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "stage": "EXTRACT_SEGMENTS",
  "status": "PROGRESS",
  "progress_pct": 65.0,
  "slices_completed": 5,
  "total_slices": 8,
  "timestamp_unix": 1794039630
}
```
