# Progress Tracker & Implementation Roadmap - RAG Ingestion Coordinator

## 1. Calibrated Implementation Phases

This roadmap tracks the implementation milestones detailed in [RAG Ingestion Coordinator Implementation Plan.pdf](tasks/RAG%20Ingestion%20Coordinator%20Implementation%20Plan.pdf):

| Phase | Duration | Focus Area | Deliverables & Exit Gates | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1: Workflow Architecture & Task Queues** | Weeks 1–2 | Polyglot Temporal Topology & Contracts | - Protobuf contracts for activity inputs/outputs across Go and Python workers.<br>- Root `IngestionCoordinatorWorkflow` and child `DocumentPartitionWorkflow` definitions.<br>- Configuration of discrete Temporal Task Queues (`inspection-queue`, `doc-ai-queue`, `chunk-lifecycle-queue`). | In Progress |
| **Phase 2: Ingress & Inspection Activity** | Weeks 3–4 | Presigning & Isolated Container Inspection | - Presigned S3 multipart URL issuance with 15-minute lease management.<br>- Isolated Inspection Activity worker inspecting PDF/audio headers via byte ranges.<br>- Deterministic S3 artifact key generation based on content and range hashes. | Pending |
| **Phase 3: Child Workflow Engine & Fan-Out** | Weeks 5–6 | Dynamic Orchestration & State Tracking | - Root workflow dynamic fan-out to child workflows via `manifest_planner`.<br>- Integration with Temporal Python SDK in `rag-document-ai-worker`.<br>- Native Temporal heartbeat detail checkpoints without relational database dual-writes. | Pending |
| **Phase 4: Telemetry & Event Offloading** | Weeks 7–8 | Redis Event Bus & Terminal Cataloging | - Redis Pub/Sub integration for decoupled progress event broadcast.<br>- Terminal execution logging in PostgreSQL upon workflow completion.<br>- Integration verification with downstream `rag-chunk-lifecycle-service`. | Pending |
| **Phase 5: Resiliency, Chaos & Production Cutover** | Weeks 9–10 | High-Scale Validation & Rollout | - Chaos testing: pod evictions during 20,000-page document ingestion.<br>- Verification of Temporal history event boundaries ($<10,000$ events per workflow).<br>- Production Helm chart deployment with canary validation. | Pending |

---

## 2. Architecture Decision Records (ADRs)

- **ADR-001: Native Polyglot Temporal Workflows**: Replaces synchronous gRPC task proxying from Go activities to Python workers with direct Temporal Python SDK workers polling dedicated task queues (`doc-ai-task-queue`).
- **ADR-002: Entity / Child Workflow Fan-Out**: Mandates decomposing large document workflows into root (`IngestionCoordinatorWorkflow`) and child (`DocumentPartitionWorkflow`) workflows to guarantee Temporal history event bounds ($<10,000$ events).
- **ADR-003: Single Source of Truth for Checkpoints**: Deprecates transactional database checkpoint tables in favor of native Temporal heartbeat details, removing dual-write desynchronization.
- **ADR-004: Redis Pub/Sub Telemetry Layer**: Offloads real-time client streaming from coordinator pods to Redis to support stateless rolling deployments.
- **ADR-005: Control-Plane Arrow Exclusion**: Prohibits Apache Arrow libraries within the coordinator; data-plane columnar transfers remain strictly between compute workers and S3.

---

## 3. Operational Runbooks

### Runbook 1: Diagnosing Stuck Temporal Workflows
1. Inspect Temporal Web UI for workflows in `Running` state exceeding SLA thresholds.
2. Check the active task queue backlog for `doc-ai-task-queue` using Prometheus metrics.
3. Identify if tasks are stalled on unallocated GPU worker pods or failing heartbeat intervals.
4. Terminate or signal stalled runs using the Temporal CLI (`temporal workflow terminate`).

### Runbook 2: Recovering from S3 Rate Limiting (HTTP 503 SlowDown)
1. Verify if multiple parallel child workflows are issuing simultaneous range requests to the same S3 prefix.
2. Update the S3 key structure in configuration to include hash prefixes (`s3://<bucket>/artifacts/<hash_prefix>/...`) to distribute load across S3 partitions.

### Runbook 3: Handling Poison-Pill Document Partitions
1. Identify failing child workflows flagged with maximum activity retry errors.
2. Inspect the S3 input artifact pointer from the child workflow input parameters.
3. Manually route the slice to an isolated quarantine bucket for manual inspection.
4. Allow the root workflow to mark the individual slice as failed while completing the remaining document partitions.

---

## 4. Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
