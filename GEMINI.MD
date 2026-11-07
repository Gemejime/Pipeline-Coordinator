# GEMINI.MD - RAG Ingestion Coordinator Workspace Guide

This file provides the primary agent and system operational instructions for the **`rag-ingestion-coordinator`** microservice.

> [!IMPORTANT]
> **Primary Persona & Execution Protocol**: Refer directly to [`.ai/agents.md`](.ai/agents.md) for the principal engineer persona, execution guidelines, and implementation workflows.
>
> All developers and AI agents must review the files in the [`.ai/`](.ai/) folder in sequence:
> 1. [`.ai.md`](.ai.md) — Root project context and core system invariants.
> 2. [`.ai/agents.md`](.ai/agents.md) — Principal AI Systems & Go Backend Engineer persona and execution protocol.
> 3. [`.ai/project-overview.md`](.ai/project-overview.md) — Product definition, pure control plane orchestration, and SLIs/SLOs.
> 4. [`.ai/architecture.md`](.ai/architecture.md) — 5-Layer Backend Architecture (from `Production Grade Backend Concepts.pdf`), system boundaries, and ADRs.
> 5. [`.ai/doc-api.md`](.ai/doc-api.md) — OpenAPI 3.1 contracts, gRPC Protobuf contracts, Temporal workflows/activities, and DB schemas.
> 6. [`.ai/code-standards.md`](.ai/code-standards.md) — Go 1.23+ conventions, deterministic workflows, zero-heap ingestion, and quality gates.
> 7. [`.ai/security.md`](.ai/security.md) — Threat model, AWS STS presigned URL leases, distroless non-root containers, and secured logging.
> 8. [`.ai/ai-workflow-rules.md`](.ai/ai-workflow-rules.md) — Spec-driven development rules and pre-commit verification checklist.
> 9. [`.ai/progress-tracker.md`](.ai/progress-tracker.md) — 5-phase calibrated milestones, ADRs, and operational runbooks.

---

## Service Overview: `rag-ingestion-coordinator`

The `rag-ingestion-coordinator` is the authoritative Go-based control plane microservice for the Multimodal RAG platform:
- **Control Plane Only**: Pure workflow orchestration without buffering, parsing, or streaming binary payloads or columnar Arrow buffers through the coordinator process.
- **Zero-Heap Ingestion**: Strict $<256\text{ MiB}$ RSS memory ceiling per pod regardless of upload size (up to $10\text{ GB}$).
- **Temporal Workflow DAGs**: Manages root `IngestionCoordinatorWorkflow` and fans out child `DocumentPartitionWorkflow` executions to prevent event history exhaustion ($<10,000$ events).
- **Polyglot Task Queues**: Coordinates `inspection-queue` (Go/Python byte inspection), `doc-ai-queue` (Python Docling/OCR/ASR), and `chunk-lifecycle-queue` (Go chunk compilation).
- **Zero Dual-Writes (ADR-003)**: In-flight execution progress is tracked natively via Temporal heartbeat details; writes to PostgreSQL only at pipeline boundaries (`COMPLETED` or `FAILED`).
- **Real-Time Telemetry**: Publishes pipeline milestone events asynchronously via Redis Pub/Sub (`ingestion:events:<tenant_id>`).

---

## Non-Negotiable Invariants

### 1. Continuous Doc API & OpenAPI Maintenance
The API contracts defined in [`.ai/doc-api.md`](.ai/doc-api.md) and the OpenAPI 3.1 specification at [`api/openapi/openapi.yaml`](api/openapi/openapi.yaml) **MUST be kept synchronized on every code change or addition, no matter how little the created or modified code is**.
- Whenever an endpoint, request payload, response schema, error model, workflow argument, or database column is added or altered, both `api/openapi/openapi.yaml` and `.ai/doc-api.md` must be updated in the exact same change set.
- CI includes an automated contract sync gate (`openapi-and-contract-sync`) that fails if endpoints or schemas diverge.

### 2. Secured Logging Invariant
- **Structured JSON Logging**: All logs must be output as machine-parseable JSON lines via `log/slog` or structured logger.
- **Mandatory Redaction**:
  - Full presigned S3 URLs containing query parameters and signatures (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`, `AWSAccessKeyId`) must **NEVER** be logged.
  - Log only the bucket and sanitized object key: `s3_bucket`, `s3_key`.
  - Client authorization headers (`Authorization: Bearer ...`), tenant secrets, and PII/PHI must be strictly stripped before log emission.
  - Database connection strings and internal file system paths must be sanitized.
- **Mandatory Correlation**: Every log event must carry `trace_id`, `tenant_id`, `document_id`, and `workflow_id`.

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

---

## Pre-Commit Verification Checklist

Before completing any task or committing changes:
1. `gofmt -s -w .` (ensure all Go code is properly formatted).
2. `go vet ./...` and `golangci-lint run`.
3. `go test -v -race ./...`.
4. Verify [`.ai/doc-api.md`](.ai/doc-api.md) and [`api/openapi/openapi.yaml`](api/openapi/openapi.yaml) reflect any API or schema changes.
5. Verify Secured Logging rules (no secrets, raw presigned URLs, or PII logged).
6. Update [`.ai/progress-tracker.md`](.ai/progress-tracker.md).
