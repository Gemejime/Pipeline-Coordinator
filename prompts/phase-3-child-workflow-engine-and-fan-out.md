# Implementation Plan: Phase 3 — Child Workflow Engine & Fan-Out

## 1. Context & Objectives

Following the architectural blueprints in [`tasks/RAG Ingestion Coordinator Implementation Plan.pdf`](../tasks/RAG%20Ingestion%20Coordinator%20Implementation%20Plan.pdf), [`tasks/Production-Grade Multimodal RAG Architectural Specification.pdf`](../tasks/Production-Grade%20Multimodal%20RAG%20Architectural%20Specification.pdf), [`tasks/Go and Python Microservices Architecture.pdf`](../tasks/Go%20and%20Python%20Microservices%20Architecture.pdf), [`tasks/Production Grade Backend Concepts.pdf`](../tasks/Production%20Grade%20Backend%20Concepts.pdf), and the system invariants in [`.ai/`](../.ai/), **Phase 3: Child Workflow Engine & Fan-Out** implements dynamic orchestration, execution control services, polyglot worker integration, and zero dual-write state tracking:

1. **Go gRPC Contract Service Definitions (`api/proto/v1/contracts.go`)**:
   - Declares `coordinatorv1.CoordinatorServiceServer` interface matching `api/proto/v1/coordinator.proto`.
   - Provides `RegisterCoordinatorServiceServer(s grpc.ServiceRegistrar, srv CoordinatorServiceServer)` with `grpc.ServiceDesc` to enable compile-time safety and in-memory `bufconn` testing without requiring external `protoc` binaries.
2. **Execution Service & Dynamic Orchestration (`internal/service/execution_service.go`)**:
   - Implements `ExecutionCoordinator` interface:
     - `StartIngestion(ctx, req)`: Strict input validation: `tenant_id`, `document_id`, `s3_bucket`, `s3_key` (must start with `raw/<tenant_id>/`), lowercase 64-hex SHA-256 `content_hash`, `total_bytes > 0`, and supported MIME type.
     - Idempotent Temporal Workflow ID deduplication: `ingest-<tenant_id>-<content_hash>` with typed Search Attributes (`TenantID`, `DocumentID`, `PipelineRunID`) scheduled on `coordinator-task-queue`.
     - Sets `WorkflowExecutionErrorWhenAlreadyStarted: false`, `WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING`, and `WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY`, guaranteeing that concurrent or re-submitted duplicate payloads return the existing running workflow handle (`202 Accepted`) rather than erroring out.
     - `GetWorkflowStatus(ctx, workflowID, tenantID)`: Enforces tenant isolation. Queries live workflow state via `QueryTypeIngestionStatus` ("ingestion_status") query handler (ADR-003: zero dual-writes). Falls back gracefully to `DescribeWorkflowExecution` (and optional `TerminalRunReader` interface) when workflows have completed.
3. **HTTP Execution Handlers & Routing (`internal/transport/http/execution_handler.go`, `router.go`)**:
   - `POST /v1/ingest/workflows/start`: Accepts JSON payload (enforcing `MaxControlPlaneBodyBytes = 64 KiB`), validates Bearer auth claim matches `tenant_id` with `ScopeIngestWrite`, and returns `202 Accepted` with `workflow_id`, `run_id`, `status: RUNNING`, and `started_at_unix`.
   - `GET /v1/ingest/workflows/{id}/status`: Accepts `id` path parameter and `tenant_id` query parameter, verifies token tenant claim with `ScopeIngestRead` (returning `400 Bad Request` if missing, `401 Unauthorized` if unauthenticated, or `403 Forbidden` on tenant mismatch), and returns `200 OK` with full in-flight or terminal status (`total_slices`, `completed_slices`, `failed_slices`, `total_pages`, `duration_seconds`).
   - Handles `400`, `401`, `403`, `404`, `413`, and `429` error responses uniformly using `ErrorResponse`.
   - Wired into `net/http` router with token-bucket rate limiting and structured JSON logging.
4. **gRPC Coordinator Server (`internal/transport/grpc/coordinator_server.go`)**:
   - Implements `coordinatorv1.CoordinatorServiceServer` delegating to `UploadCoordinator` and `ExecutionCoordinator`:
     - `CreateUploadURL`: Presigned single-part or multipart S3 upload URL generation.
     - `CompleteUploadURL`: Finalizing S3 multipart uploads.
     - `StartIngestion`: Initiating `IngestionCoordinatorWorkflow`.
     - `GetIngestionStatus`: Querying in-flight or terminal workflow status.
   - Maps errors to canonical gRPC status codes (`codes.InvalidArgument`, `codes.NotFound`, `codes.PermissionDenied`, `codes.ResourceExhausted`, `codes.Internal`), preserving structured logging and tracing correlation.
5. **Temporal Python SDK Integration (`workers/python/rag_document_ai_worker/`)**:
   - Idiomatic Temporal Python SDK worker (`rag_document_ai_worker`):
     - `worker.py`: Connects to Temporal cluster, registers activity `DispatchDocAIExtractActivity`, polls `doc-ai-task-queue` with graceful shutdown.
     - `models.py`: Pydantic v2 schemas matching `types.proto` (`PartitionSlice`, `HeartbeatDetails`, `ExtractSegmentRequest`, `ExtractSegmentResponse`, `PipelineConfig`).
     - `activity.py`: Implements `@activity.defn(name="DispatchDocAIExtractActivity")`:
       - Inspects `activity.info().heartbeat_details` for `HeartbeatDetails` checkpoint.
       - Resumes from `last_processed_unit + 1` without repeating already processed pages/segments (ADR-003).
       - Emits heartbeats with updated progress (`last_processed_unit`, `total_units`, `intermediate_s3_key`, `updated_at_unix`).
       - Downloads binary range from `presigned_input_get_url`, parses content, generates a standardized PyArrow Parquet slice (`slice_id`, `slice_index`, `unit_index`, `content`, `metadata_json`, `tokens_estimate`), and uploads to `presigned_output_put_url`.
     - `requirements.txt`: Specifying `temporalio>=1.5.0`, `pyarrow>=14.0.0`, `pydantic>=2.0.0`, `pytest>=7.0.0`.
6. **Native Temporal Heartbeat Detail Checkpoints & Verification**:
   - Go unit tests covering `ExecutionService`, `ExecutionHandler`, and `CoordinatorServer` (via `bufconn`).
   - Python unit tests (`workers/python/tests/test_document_ai_worker.py`) verifying Pydantic v2 model validation, heartbeat recovery from `last_processed_unit + 1`, and PyArrow Parquet slice generation.
   - OpenAPI 3.1 & `.ai/doc-api.md` contract synchronization for all response codes (`200`, `202`, `400`, `401`, `403`, `404`, `413`, `429`).

---

## 2. Files to Create or Modify

| File Path | Action | Responsibility |
| :--- | :--- | :--- |
| `api/proto/v1/contracts.go` | Modify | Define `CoordinatorServiceServer` Go interface and `RegisterCoordinatorServiceServer` gRPC service descriptor helper. |
| `internal/service/execution_service.go` | Create | Implements `ExecutionCoordinator` interface: `StartIngestion` and `GetWorkflowStatus` against Temporal `client.Client`, enforcing tenant isolation, idempotent workflow IDs, conflict policies, and `QueryTypeIngestionStatus` query resolution with closed workflow fallback. |
| `internal/transport/http/execution_handler.go` | Create | HTTP REST handlers for `POST /v1/ingest/workflows/start` and `GET /v1/ingest/workflows/{id}/status` with body limits (64 KiB), tenant authentication cross-checks, and comprehensive HTTP error responses. |
| `internal/transport/http/router.go` | Modify | Wire `ExecutionHandler` into HTTP router with authentication scopes (`ingest:write`, `ingest:read`) and token-bucket rate limiting. |
| `internal/transport/grpc/coordinator_server.go` | Create | Implements `coordinatorv1.CoordinatorServiceServer` for internal control plane RPCs with gRPC status error mapping and structured logging, injecting `UploadCoordinator` and `ExecutionCoordinator`. |
| `workers/python/rag_document_ai_worker/__init__.py` | Create | Python package initialization for Document AI worker. |
| `workers/python/rag_document_ai_worker/models.py` | Create | Pydantic v2 schemas matching `types.proto` (`PartitionSlice`, `HeartbeatDetails`, `ExtractSegmentRequest`, `ExtractSegmentResponse`, `PipelineConfig`). |
| `workers/python/rag_document_ai_worker/activity.py` | Create | Temporal Python activity `@activity.defn(name="DispatchDocAIExtractActivity")` with heartbeat checkpoint resumption, PyArrow Parquet generation, and zero dual-write state tracking. |
| `workers/python/rag_document_ai_worker/worker.py` | Create | Standalone Temporal Python worker connecting to cluster and polling `doc-ai-task-queue`. |
| `workers/python/rag_document_ai_worker/requirements.txt` | Create | Dependencies: `temporalio>=1.5.0`, `pydantic>=2.0.0`, `pyarrow>=14.0.0`, `pytest>=7.0.0`. |
| `workers/python/tests/test_document_ai_worker.py` | Create | Python unit tests verifying Pydantic v2 model parsing, heartbeat detail resumption, and Parquet table construction. |
| `test/unit/execution_service_test.go` | Create | Unit tests for `ExecutionService`: input validation, idempotent workflow ID formatting, search attribute injection, query handling, and closed workflow fallback. |
| `test/unit/http_execution_handler_test.go` | Create | HTTP transport tests for `POST /v1/ingest/workflows/start` and `GET /v1/ingest/workflows/{id}/status`: scope checks, cross-tenant isolation, 400/401/403/404/413/429 handling, rate limiting. |
| `test/unit/grpc_coordinator_server_test.go` | Create | gRPC server tests verifying all 4 RPC methods over in-memory `bufconn` listener. |
| `.ai/doc-api.md` & `api/openapi/openapi.yaml` | Modify | Synchronize endpoints, query/header parameters, and error codes (`400`, `401`, `403`, `413`, `429`). |
| `.ai/progress-tracker.md` | Modify | Update Phase 3 status and pressure-test refinement notes (GAP-12). |

---

## 3. Detailed Design & Invariants

### 3.1 Idempotent Temporal Workflow Initiation (`internal/service/execution_service.go`)
- **Workflow ID Generation**:
  `fmt.Sprintf("ingest-%s-%s", sanitizeTenant(req.TenantID), req.ContentHash)`
  Guarantees that re-submitting the same document payload for the same tenant maps to the identical workflow instance, preventing duplicate execution DAGs.
- **Workflow Options**:
  - `TaskQueue`: `coordinator-task-queue` (`workflow.CoordinatorTaskQueue`)
  - `TypedSearchAttributes`: `TenantID`, `DocumentID`, `PipelineRunID`
  - `WorkflowIDConflictPolicy`: `enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING`
  - `WorkflowIDReusePolicy`: `enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY`
  - `WorkflowExecutionErrorWhenAlreadyStarted`: `false` (returns existing running workflow info rather than throwing an error on duplicate start requests).

### 3.2 Status Query Resolution & Zero Dual-Writes (ADR-003)
- In-flight workflow state is resolved strictly by querying the running workflow via Temporal's `QueryWorkflow(ctx, workflowID, runID, workflow.QueryTypeIngestionStatus)` query handler.
- If the workflow run has closed:
  - If a `TerminalRunReader` interface is configured (PostgreSQL `pipeline_runs`), retrieve the recorded terminal run metrics.
  - If no database reader is configured or the query returns not found, fall back gracefully to `DescribeWorkflowExecution` to retrieve execution status (`COMPLETED`, `FAILED`, `TERMINATED`, `TIMED_OUT`) and duration timestamps.
- Under no circumstances does the coordinator or compute workers write in-flight intermediate checkpoints to PostgreSQL.

### 3.3 Python Worker Heartbeat Recovery (ADR-001 & ADR-003)
- `rag-document-ai-worker` runs as an independent worker process using the official Temporal Python SDK (`temporalio.worker.Worker`).
- Polling `doc-ai-task-queue` with a 15-second activity heartbeat timeout.
- On each invocation:
  ```python
  info = activity.info()
  checkpoint = None
  if info.heartbeat_details:
      checkpoint = HeartbeatDetails.model_validate(info.heartbeat_details[0])
  start_unit = (checkpoint.last_processed_unit + 1) if checkpoint else 1
  ```
- Any unhandled worker SIGKILL causes Temporal to re-route the activity on heartbeat expiration; the next worker reads `info.heartbeat_details` and resumes immediately without repeating already processed units.
- PyArrow Parquet Output Schema:
  - `slice_id` (`pa.string()`)
  - `slice_index` (`pa.int32()`)
  - `unit_index` (`pa.int32()`)
  - `content` (`pa.string()`)
  - `metadata_json` (`pa.string()`)
  - `tokens_estimate` (`pa.int32()`)

---

## 4. Verification Plan

1. **Static Analysis & Linting**:
   - `gofmt -s -w .`
   - `go vet ./...`
   - `golangci-lint run`
2. **Unit Tests**:
   - Go unit tests: `go test -v ./test/unit/execution_service_test.go`
   - HTTP transport tests: `go test -v ./test/unit/http_execution_handler_test.go`
   - gRPC server tests: `go test -v ./test/unit/grpc_coordinator_server_test.go`
   - Python unit tests: `pytest workers/python/tests/` (or `python -m unittest discover workers/python/tests`)
   - Full suite: `go test -v ./test/unit/... ./test/workflow/...`
3. **Contract & Spec Sync**:
   - Verify `api/openapi/openapi.yaml` and `.ai/doc-api.md` match across all endpoints and response codes.
4. **Secured Logging Audit**:
   - Confirm zero logging of presigned URLs, auth headers, or raw signatures.
5. **Git Commits**:
   - Minimum 2 commits (`feat: ...`, `tests: ...`, `docs: ...`).
