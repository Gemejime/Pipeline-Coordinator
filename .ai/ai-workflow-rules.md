# AI Workflow Rules - RAG Ingestion Coordinator

## Development Approach

Build this microservice incrementally following a spec-driven workflow:
- The specification documents in [`tasks/`](../tasks) and context files in [`.ai/`](.) define system invariants, data structures, and operational boundaries.
- Adhere strictly to the Go control plane coordinator architecture and the 5 backend architectural layers documented in [`.ai/architecture.md`](architecture.md) aligned with `Production Grade Backend Concepts.pdf`.
- Implement against these specs without inferring undocumented dependencies or deviating from architectural decision records (ADR-001 through ADR-005).

---

## Human-in-the-Loop Rules

- **Plan First**: Propose an implementation plan or prompt in `prompts/<task-name>.md` before making substantial code changes, and await human confirmation.
- **Ambiguity Escalation**: If S3 bucket strategies, task queue naming, Temporal activity timeouts, or database indices are ambiguous, prompt for clarification before proceeding.
- **Destructive Action Guards**: Always request human confirmation before running destructive operations, dropping tables, or altering published proto contracts.
- **Demonstrate & Verify**: Provide clear verification steps, unit/workflow test output, and linting checks after completing any unit.

---

## Non-Negotiable Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: The [`.ai/doc-api.md`](doc-api.md) file and OpenAPI 3.1 specification (`api/openapi/openapi.yaml`) MUST be updated on **every code change or addition**, regardless of how small the modification is.
2. **Secured Logging**: All logs emitted by coordinator services or activities must be structured JSON. Strictly redact presigned S3 URLs containing credentials (`X-Amz-Signature`), authorization tokens, bearer headers, and tenant PII/PHI before emission.
3. **Pure Control Plane Orchestration (Zero Heap Ingestion)**: Drive complex multimodal ingestion workflows via Temporal without buffering, parsing, or streaming binary payloads or columnar Arrow buffers through the coordinator. Maintain $<256\text{ MiB}$ RSS per pod.
4. **Resilient Child Workflow Execution**: Isolate document slice executions into independent `DocumentPartitionWorkflow` child workflows to prevent event history exhaustion ($<10,000$ events per execution history).
5. **Single Source of Truth for Checkpoints (Zero Dual-Writes)**: Rely solely on Temporal workflow state and heartbeat payload details for in-flight task progress and crash recovery; write to PostgreSQL only at pipeline boundaries (`COMPLETED` or `FAILED`).
6. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features 
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup
7. **Quality Gates**: Go formatting (`gofmt -s`), `go vet`, and `golangci-lint` must pass cleanly before any commit.

---

## Pre-Commit Verification Checklist

Before committing code, submitting changes, or marking a task complete:

1. **Formatting**: Run `gofmt -s -w .` (zero unformatted files).
2. **Static Analysis & Linting**: Run `go vet ./...` and `golangci-lint run`.
3. **Automated Tests**: Ensure unit and workflow simulation tests pass with `-race`: `go test -v -race ./...`.
4. **Secured Logging Audit**: Verify no raw presigned URLs, AWS credentials, bearer tokens, or PII are logged.
5. **Doc API & OpenAPI Sync**: Confirm [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` reflect any new or modified endpoints, parameters, activities, or database columns.
6. **Progress Tracker**: Update [`.ai/progress-tracker.md`](progress-tracker.md) with session notes and completed items.
