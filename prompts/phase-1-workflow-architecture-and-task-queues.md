# Implementation Plan: Phase 1 — Workflow Architecture & Task Queues

## 1. Context & Objective

Following the architectural blueprints in [`tasks/RAG Ingestion Coordinator Implementation Plan.pdf`](<../tasks/RAG%20Ingestion%20Coordinator%20Implementation%20Plan.pdf>), [`tasks/Production Grade Backend Concepts.pdf`](<../tasks/Production%20Grade%20Backend%20Concepts.pdf>), and the specifications in [`.ai/`](../.ai/), **Phase 1: Workflow Architecture & Task Queues** establishes:
1. The polyglot Temporal topology and discrete task queue registry (`inspection-queue`, `doc-ai-task-queue`, `chunk-lifecycle-queue`, `coordinator-task-queue`).
2. Complete Protobuf v3 contracts (`api/proto/v1/coordinator.proto` and `api/proto/v1/types.proto`) covering control-plane gRPC RPCs (single-part and multipart S3 handoffs) and cross-language activity inputs/outputs across Go and Python workers.
3. Pure deterministic slice manifest planning and content-addressed S3 artifact key generation (`internal/service/manifest_planner.go`).
4. Deterministic root (`IngestionCoordinatorWorkflow`) and child (`DocumentPartitionWorkflow`) workflow definitions with native Temporal query handlers, search attributes, poison-pill partition isolation (Runbook 3), and bounded `ContinueAsNew` history management (ADR-002).
5. Comprehensive unit and Temporal workflow simulation test suites (`test/unit/`, `test/workflow/`) including 20,000-page `ContinueAsNew` history boundary assertions and `GetHeartbeatDetails` crash recovery verification (ADR-003).

---

## 2. Scope & Deliverables

### 2.1 Go Module & Dependencies (`go.mod`, `go.sum`)

- Initialize Go module `rag-ingestion-coordinator` targeting **Go 1.23+**.
- Add core dependencies:
  - `go.temporal.io/sdk` (Temporal Go SDK + `testsuite` for deterministic workflow and activity testing)
  - `go.temporal.io/api` (Temporal enums and protocol types such as `ParentClosePolicy`)
  - `github.com/stretchr/testify` (assertions and mock matching for unit and workflow tests)

### 2.2 Protobuf v3 Contracts (`api/proto/v1/`)

- **`api/proto/v1/coordinator.proto`** (synchronized with OpenAPI 3.1 & [`.ai/doc-api.md`](../.ai/doc-api.md)):
  - Define `CoordinatorService` with:
    - `CreateUploadURL(CreateUploadURLRequest) returns (CreateUploadURLResponse)` (supports single-part `presigned_url` and multipart `part_size_bytes` + `repeated PresignedPart parts` for uploads up to 10 GB)
    - `CompleteUploadURL(CompleteMultipartUploadRequest) returns (CompleteMultipartUploadResponse)`
    - `StartIngestion(StartIngestionRequest) returns (StartIngestionResponse)`
    - `GetIngestionStatus(GetIngestionStatusRequest) returns (GetIngestionStatusResponse)`
- **`api/proto/v1/types.proto`** (polyglot domain & activity input/output contracts across Go and Python workers):
  - Domain models: `MultimodalModality`, `PartitionSlice`, `HeartbeatDetails` (ADR-003 native checkpoint schema), `PipelineConfig`, and `ProgressMilestoneEvent` (with monotonic `sequence_number`).
  - Polyglot Activity I/O contracts:
    - `InspectDocumentRequest` / `InspectDocumentResponse` (`inspection-queue`)
    - `PresignSliceArtifactRequest` / `PresignSliceArtifactResponse` (`coordinator-task-queue`)
    - `ExtractSegmentRequest` / `ExtractSegmentResponse` (`doc-ai-task-queue` — Temporal Python SDK workers)
    - `CompileChunksRequest` / `CompileChunksResponse` (`chunk-lifecycle-queue` — `rag-chunk-lifecycle-service`)
    - `PublishMilestoneRequest` / `PublishMilestoneResponse` (`coordinator-task-queue`)
    - `CatalogTerminalRunRequest` / `CatalogTerminalRunResponse` (`coordinator-task-queue`)

### 2.3 Deterministic Manifest Planner (`internal/service/manifest_planner.go`)

- Pure, zero-I/O algorithm for decomposing documents and continuous media into deterministic `PartitionSlice` manifests:
  - **PDF / Tabular Documents**: Partitions pages into non-overlapping intervals (default `50` pages per slice, configurable within `1..100` pages). Validates extreme boundaries: single-page PDFs (`1..1`), massive 20,000+ page documents (`400+` slices), and rejects zero-byte uploads, negative/zero page counts, or corrupted ranges as non-retryable validation errors.
  - **Audio / Video Media**: Partitions continuous media durations into sliding windows (default `600s` / 10-minute segments with configurable `overlap_seconds`).
  - **Deterministic Slice Identity & Content-Addressed S3 Keys**:
    - Computes deterministic `slice_id` via `SHA-256(document_id + ":" + content_hash + ":" + range_spec)`.
    - Generates idempotent S3 output Parquet keys:
      - Standard: `artifacts/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet`
      - Hash-prefixed mode (Runbook 2 mitigation for S3 HTTP 503 `SlowDown`): `artifacts/<hash_prefix>/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet` where `<hash_prefix>` is the 4-hex-character prefix of `SHA-256(document_id + page_range)`.

### 2.4 Task Queues, Retry Policies, Search Attributes & Activity Contracts (`internal/workflow/`, `internal/activity/`)

- **Discrete Task Queue Registry (`internal/workflow/retry.go`)**:
  - `CoordinatorTaskQueue = "coordinator-task-queue"` (Go coordinator activities: presigning, Redis milestone publishing, PostgreSQL terminal cataloging)
  - `InspectionTaskQueue = "inspection-queue"` (`InspectDocumentHeaderActivity`, `10s` heartbeat timeout)
  - `DocAITaskQueue = "doc-ai-task-queue"` (`DispatchDocAIExtractActivity`, `15s` heartbeat timeout)
  - `ChunkLifecycleTaskQueue = "chunk-lifecycle-queue"` (`CompileChunksActivity`, `10s` heartbeat timeout)
- **Temporal Search Attributes (`internal/workflow/retry.go`)**:
  - Typed keyword search attribute keys:
    - `SearchAttrTenantID = temporal.NewSearchAttributeKeyKeyword("TenantID")`
    - `SearchAttrDocumentID = temporal.NewSearchAttributeKeyKeyword("DocumentID")`
    - `SearchAttrPipelineRunID = temporal.NewSearchAttributeKeyKeyword("PipelineRunID")`
- **Retry & Timeout Configurations (`internal/workflow/retry.go`)**:
  - Exponential backoff policies distinguishing transient compute/network failures from non-retryable validation errors (`INVALID_ARGUMENT`, `CORRUPT_INPUT`, `UNSUPPORTED_MIME_TYPE`, `TENANT_MISMATCH`).
- **Activity Definitions & Heartbeat Resumption (`internal/activity/`)**:
  - `internal/activity/inspection.go`: `InspectDocumentHeaderActivity` with `activity.GetHeartbeatDetails` resumption and `activity.RecordHeartbeat` progress emission.
  - `internal/activity/presign.go`: `PresignS3ArtifactActivity` issuing 15-minute (`900s`) scoped presigned PUT/GET URLs for worker slice reading and Parquet emission (redacting all query signatures from logs).
  - `internal/activity/polyglot.go`: Polyglot activity contracts and reference heartbeat-aware handlers for `DispatchDocAIExtractActivity` (`doc-ai-task-queue`) and `CompileChunksActivity` (`chunk-lifecycle-queue`), demonstrating `HeartbeatDetails` checkpoint recovery (`LastProcessedUnit`) so crashed workers skip already-processed pages/sub-segments without duplicate work (ADR-003).
  - `internal/activity/notification.go`: `PublishMilestoneActivity` publishing `ProgressMilestoneEvent` payloads with monotonic `sequence_number` to `ingestion:events:<tenant_id>`.
  - `internal/activity/catalog.go`: `CatalogTerminalRunActivity` persisting a single atomic terminal record (`COMPLETED`, `FAILED`, `TERMINATED`) with `total_pages`, `total_slices`, `completed_slices`, `failed_slices`, and `duration_seconds` to PostgreSQL.

### 2.5 Deterministic Temporal Workflows (`internal/workflow/`)

- **`internal/workflow/document_partition.go` (`DocumentPartitionWorkflow`)**:
  - Executes the isolated per-slice activity pipeline:
    1. `PresignS3ArtifactActivity` on `coordinator-task-queue`
    2. `DispatchDocAIExtractActivity` on `doc-ai-task-queue` (`15s` heartbeat timeout)
    3. `CompileChunksActivity` on `chunk-lifecycle-queue` (`10s` heartbeat timeout)
  - Strictly deterministic: no `time.Now()`, `go` routines, `rand.*`, or direct I/O.
  - Never logs raw presigned URLs returned by `PresignS3ArtifactActivity`; logs only `s3_bucket`, `s3_key`, `tenant_id`, `document_id`, `workflow_id`, and `slice_id`.
- **`internal/workflow/ingestion_root.go` (`IngestionCoordinatorWorkflow`)**:
  - Orchestrates end-to-end document ingestion:
    1. **Query Handler Registration (ADR-003)**: Registers `QueryTypeIngestionStatus = "ingestion_status"` via `workflow.SetQueryHandler` immediately upon startup to serve live in-flight status (`status`, `total_pages`, `total_slices`, `completed_slices`, `failed_slices`, `current_generation`) without database dual-writes.
    2. **Search Attributes Upsert**: Upserts `TenantID`, `DocumentID`, and `PipelineRunID` typed search attributes on initial generation.
    3. **Header Inspection & Manifest Planning**: On initial generation (`Generation == 0`), executes `InspectDocumentHeaderActivity` on `inspection-queue` and invokes `manifest_planner.PlanSlices` to build the deterministic slice manifest.
    4. **Milestone Telemetry**: Publishes stage milestones (`INSPECTING`, `EXTRACT_SEGMENTS`, `COMPLETED` / `FAILED`) via `PublishMilestoneActivity` with monotonically increasing `sequence_number` carried across `ContinueAsNew` generations.
    5. **Bounded Child Workflow Fan-Out (`MaxSlicesPerGeneration = 100`)**:
       - Spawns at most `100` concurrent `DocumentPartitionWorkflow` child workflows per generation with deterministic `WorkflowID = fmt.Sprintf("%s-slice-%05d", rootWorkflowID, slice.SliceIndex)` and `ParentClosePolicy = PARENT_CLOSE_POLICY_REQUEST_CANCEL`.
       - Awaits completion of all child workflow futures in the current generation batch before transitioning.
       - **Poison-Pill Partition Isolation (Runbook 3)**: If an individual child workflow fails with a non-cancellation error after exhausting activity retries, increments `failed_slices` and records the failed `slice_id` while allowing sibling slices to complete; fails the entire root run only if all slices fail, a fatal inspection/validation error occurs, or the workflow is cancelled.
    6. **Bounded History via `ContinueAsNew` (ADR-002)**:
       - When `len(remainingSlices) > 0` after completing a 100-slice batch, returns `workflow.NewContinueAsNewError` carrying forward `RemainingSlices`, `TotalPages`, `TotalSlices`, `CompletedSlices`, `FailedSlices`, `FailedSliceIDs`, `StartedAtUnix`, `NextSequenceNumber`, and `Generation + 1`.
       - Guarantees every workflow generation stays strictly below `<2,500` events (and `<10,000` hard limit), even for 20,000+ page documents (`400+` child workflows).
    7. **Terminal Boundary Cataloging (ADR-003)**:
       - Persists a single terminal record to PostgreSQL at the final pipeline boundary via `CatalogTerminalRunActivity` (using `workflow.NewDisconnectedContext` on workflow cancellation so `FAILED`/`TERMINATED` runs are always cataloged).

### 2.6 Unit & Workflow Simulation Test Suites (`test/unit/`, `test/workflow/`)

- Note: `.github/workflows/ci.yml` executes both `./test/unit/...` and `./test/workflow/...` (including `-run "Test.*History"`) with `-race`.
- **`test/unit/manifest_planner_test.go` & `test/unit/workflow_contracts_test.go`**:
  - Validates manifest boundary mathematics against single-page PDFs, zero-byte uploads, negative/corrupted ranges, 20,000-page boundaries (`400` non-overlapping slices), and audio/video sliding windows.
  - Validates deterministic S3 content-addressed key generation (both standard and Runbook 2 hash-prefixed paths).
  - Validates task queue constants, search attribute keys, and retry policy non-retryable error types.
- **`test/workflow/ingestion_workflow_test.go`**:
  - Uses `go.temporal.io/sdk/testsuite` (`WorkflowTestSuite` & `TestActivityEnvironment`) to test:
    - `TestDocumentPartitionWorkflow_Success`: Verifies per-slice activity sequence (`PresignS3ArtifactActivity` -> `DispatchDocAIExtractActivity` -> `CompileChunksActivity`).
    - `TestActivity_HeartbeatRecovery_ResumesFromLastCheckpoint`: Simulates worker failure midway through extraction (`LastProcessedUnit = 25`), restarts the activity with recorded `HeartbeatDetails`, and asserts it resumes at unit `26` without duplicate processing (ADR-003).
    - `TestIngestionCoordinatorWorkflow_EndToEnd_And_QueryStatus`: Verifies end-to-end execution, monotonic Redis milestone sequence numbers, and live `QueryTypeIngestionStatus` query responses.
    - `TestIngestionCoordinatorWorkflow_HistoryLimit_20000Pages`: Simulates a 20,000-page document (`400` child workflows) across `4` bounded `ContinueAsNew` generations (`100` slices/generation), iterating through `*workflow.ContinueAsNewError` transitions and asserting that every root generation and child workflow stays strictly below `2,500` history events (`<10,000` hard limit).
    - `TestIngestionCoordinatorWorkflow_PoisonPillSliceIsolation`: Simulates a poison-pill slice failing while sibling slices succeed (Runbook 3), verifying partial failure tracking (`failed_slices = 1`, `completed_slices = 3`) and terminal cataloging.
    - `TestIngestionCoordinatorWorkflow_FailureCataloging`: Verifies that fatal inspection errors or cancellations record a single `FAILED` terminal state via `CatalogTerminalRunActivity` using disconnected context.

### 2.7 Documentation & Contract Synchronization

- Synchronize [`.ai/doc-api.md`](../.ai/doc-api.md), [`.ai/architecture.md`](../.ai/architecture.md), and [`api/openapi/openapi.yaml`](../api/openapi/openapi.yaml) with all Protobuf messages, workflow/activity signatures, query handlers, and `pipeline_runs` columns (`completed_slices`, `failed_slices`).
- Update [`.ai/progress-tracker.md`](../.ai/progress-tracker.md) to reflect Phase 1 completion once verified.

---

## 3. Security & Invariant Verification

1. **Zero-Heap Ingestion**: Workflows and activities pass only metadata and S3 key pointers (`s3_bucket`, `s3_key`); zero file bytes or Arrow buffers are allocated in coordinator heap memory.
2. **Secured Logging**: All structured logs use `workflow.GetLogger(ctx)` or `log/slog` with correlation keys (`tenant_id`, `document_id`, `workflow_id`) and never log presigned URL query strings (`X-Amz-Signature`, `X-Amz-Credential`, `AWSAccessKeyId`), bearer tokens, or PII.
3. **Zero Dual-Writes (ADR-003)**: Zero database writes occur during active slice processing; in-flight state is served via Temporal Query Handler and `HeartbeatDetails`, and `CatalogTerminalRunActivity` executes exclusively at terminal workflow boundaries.
4. **Git Commit Discipline**: Create at least two conventional commits (`feat:`, `tests:`, `docs:`) after passing `gofmt -s -w .`, `go vet ./...`, `golangci-lint run`, and `go test -v -race ./...`.
