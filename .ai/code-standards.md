# Code Standards - RAG Ingestion Coordinator

## General Principles

- **Pure Control Plane Responsibility**: Decouple ingestion coordination and workflow scheduling from data-plane binary streaming and columnar serialization. The coordinator must never buffer file bytes in memory.
- **Fail-Fast on Corrupt Input**: Distinguish workflow-level validation errors from transient worker failures. Bad request schemas, invalid MIME types, or corrupted range requests fail immediately as non-retryable errors (`temporal.NewNonRetryableApplicationError`), while worker preemption or transient network errors use exponential backoff retries.
- **Root-Cause Focus**: Never patch race conditions or activity timeouts with arbitrary sleep statements; use deterministic Temporal timers (`workflow.Sleep`), proper heartbeat timeouts, and token-bucket rate limiters.

---

## Go 1.23+ & Code Quality Standards

- **Runtime Target**: Go 1.23+ (64-bit).
- **Linter & Formatter**:
  - Formatter: `gofmt -s -w .`
  - Vet: `go vet ./...`
  - Linter: `golangci-lint run` (enforcing `errcheck`, `govet`, `staticcheck`, `unused`, `gosec`, `revive`).
- **Standard Library Utilization**:
  - Prefer Go standard library (`net/http`, `context`, `log/slog`, `sync`, `time`, `crypto/sha256`).
  - Context propagation: All functions performing I/O or Temporal activities MUST accept `context.Context` as the first argument and respect cancellation (`ctx.Done()`).
- **Idiomatic Go Structuring**:
  - Keep `internal/` packages decoupled with explicit interfaces.
  - Avoid global variables or package-level state; wire dependencies via constructor functions (`NewUploadService(...)`, `NewIngestionCoordinator(...)`).
  - Package names must be lowercase, short, and singular (e.g., `activity`, `workflow`, `service`, `store`, `http`).

---

## Temporal Workflow & Activity Invariants

### 1. Deterministic Workflow Code
- Code inside `internal/workflow/` MUST be completely deterministic:
  - **No non-deterministic standard functions**: Never call `time.Now()`, `time.Sleep()`, `rand.*`, or generate random UUIDs inside workflow functions. Use `workflow.Now(ctx)`, `workflow.Sleep(ctx, d)`, or `workflow.SideEffect`.
  - **No raw goroutines**: Use `workflow.Go(ctx, ...)` instead of `go ...`.
  - **No direct I/O or network calls**: All database queries, Redis publishing, S3 operations, and HTTP requests MUST execute inside Temporal Activities (`internal/activity/`).
  - **Child Workflow Fan-Out**: Partition large documents into discrete `DocumentPartitionWorkflow` executions to prevent event history exhaustion ($<10,000$ events per history).

### 2. Activity Implementation & Heartbeat Continuity
- Activities inside `internal/activity/` must:
  - Accept `context.Context` and regularly check for cancellation (`activity.GetHeartbeatDetails(ctx, ...)`).
  - Emit heartbeats periodically (`activity.RecordHeartbeat(ctx, progress)`) for long-running operations.
  - Inspect `activity.GetHeartbeatDetails` on restart to resume from previous progress checkpoints without duplicate work.
  - Configure explicit activity options: `StartToCloseTimeout`, `HeartbeatTimeout`, and retry policies with exponential backoff and jitter.

---

## Memory & Zero-Heap Ingestion Invariants

1. **Strict RSS Memory Ceiling**:
   - Maintain $<256\text{ MiB}$ resident set size per coordinator pod under sustained load.
2. **Zero Ingestion Buffering**:
   - Coordinator code must NEVER read, buffer, or stream multipart file payloads or Apache Arrow Flight streams through heap memory.
   - S3 presigned URLs allow clients and workers to read and write directly to object storage.
   - Large file inspections (e.g. PDF headers) are delegated to isolated inspection activities using HTTP range byte reads (`Range: bytes=0-4096`).

---

## PostgreSQL & Redis Invariants

1. **Single Source of Truth for Checkpoints (Zero Dual-Writes - ADR-003)**:
   - Relational PostgreSQL checkpoint tables for in-flight tasks are deprecated.
   - PostgreSQL is accessed only at pipeline boundaries: recording configuration metadata at initiation and terminal records (`COMPLETED` or `FAILED`) with run duration and token/page metrics.
2. **Redis Pub/Sub Telemetry**:
   - Publish real-time milestone events asynchronously via Redis Pub/Sub channels (`ingestion:events:<tenant_id>`).
   - Do not maintain stateful WebSocket/SSE connections within coordinator pods.

---

## Secured Logging Invariants

- **Structured JSON Logging**: All logs must be emitted as machine-parseable JSON lines via `log/slog` or structured logger.
- **Mandatory Correlation IDs**: Every log message must include:
  - `trace_id`
  - `tenant_id`
  - `document_id`
  - `workflow_id`
- **Security Invariant (Secured Logging)**:
  - **Strict Redaction**: Never log raw presigned S3 URLs containing query credentials (`X-Amz-Signature`, `X-Amz-Credential`), authorization headers, bearer tokens, or tenant PII/PHI.
  - When logging S3 artifact paths, log only the bucket and sanitized object key (`s3://<bucket>/artifacts/<tenant_id>/<doc_hash>/slice_...`), never presigned query parameters.
  - Error logs must sanitize internal system paths and database connection strings.

---

## Doc API Mandatory Maintenance

- **Continuous Synchronization**: Whenever any endpoint, workflow parameter, activity signature, gRPC proto, or database schema is added or modified, [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` **MUST** be updated immediately in the same change step, no matter how small the code modification is.

---

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
