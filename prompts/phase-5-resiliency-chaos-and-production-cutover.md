# Implementation Plan: Phase 5 — Resiliency, Chaos & Production Cutover

## 1. Context & Objectives

Following the architectural blueprints in [`tasks/RAG Ingestion Coordinator Implementation Plan.pdf`](../tasks/RAG%20Ingestion%20Coordinator%20Implementation%20Plan.pdf), [`tasks/Production Grade Backend Concepts.pdf`](../tasks/Production%20Grade%20Backend%20Concepts.pdf), [`tasks/Go and Python Microservices Architecture.pdf`](../tasks/Go%20and%20Python%20Microservices%20Architecture.pdf), and the system invariants in [`.ai/`](../.ai/), **Phase 5: Resiliency, Chaos & Production Cutover** hardens runtime resiliency bugs uncovered during pressure-testing, delivers production distroless containerization, Kubernetes/Helm & ArgoCD canary manifests, accurate Prometheus alerting rules, developer/CI automation, and high-scale chaos & memory profiling verification:

1. **Runtime Resiliency, Telemetry & Zero-Drop Drain Fixes (Pre-Cutover Hardening)**:
   - **Heartbeat Checkpoint Continuity Across Re-Presign Retries (`internal/workflow/`, `internal/activity/`, `api/proto/v1/`, `workers/python/`)**:
     - Currently `DocAIActivityOptions()` sets `MaximumAttempts = 1` so `DocumentPartitionWorkflow` can re-issue fresh 15-minute presigned S3 URLs per attempt in a workflow retry loop. Because each `workflow.ExecuteActivity` call in Temporal starts a new `ActivityExecution`, native `HeartbeatDetails` recorded on a timed-out/crashed attempt are not automatically carried to the next `ExecuteActivity` invocation.
     - Extract `LastHeartbeatDetails` from `*temporal.TimeoutError` / `*temporal.ActivityError` on retry in `DocumentPartitionWorkflow`, propagate an explicit `ResumeCheckpoint *HeartbeatDetails` on `ExtractSegmentRequest` (across `types.proto`, `contracts.go`, and Python `models.py`), and update `DispatchDocAIExtract` (Go) and `DispatchDocAIExtractActivity` (Python) to check `HasHeartbeatDetails` first and fall back to `req.ResumeCheckpoint`, guaranteeing zero duplicate work from `LastProcessedUnit + 1` even when re-presigning fresh 15-minute URLs on worker `SIGKILL` / `HeartbeatTimeout`.
   - **Child Workflow Outcome Telemetry (`internal/telemetry/metrics.go`, `internal/workflow/ingestion_root.go`)**:
     - Add `coordinator_child_workflows_completed_total` and `coordinator_child_workflows_failed_total` (or `coordinator_child_workflows_total` partitioned by `status="COMPLETED"|"FAILED"`) to `telemetry.Metrics` and wire `temporalMetricsHandler.Counter(...)` in `internal/telemetry/metrics.go` + `IngestionCoordinatorWorkflow` child result handling so the Prometheus `Elevated Workflow Failure Rate` alert and Argo Rollouts `AnalysisTemplate` evaluate real, live scraped counters instead of `no data`.
   - **Zero-Drop Rolling Deploy Drain Window (`cmd/coordinator/main.go`)**:
     - Because `gcr.io/distroless/static:nonroot` has no OS shell (`sh`) or `sleep` binary, Kubernetes `preStop` shell hooks cannot be used to wait for `kube-proxy`/Ingress endpoint removal.
     - Add configurable `SHUTDOWN_DRAIN_DELAY` (default `5s`, configurable to `0s` in tests) in `cmd/coordinator/main.go` immediately after `draining.Store(true)` (so `/ready` returns `503` while existing traffic drains and endpoints de-register) before invoking `httpSrv.Shutdown` and `grpcSrv.GracefulStop`, paired with `terminationGracePeriodSeconds >= 30s` in Helm.
   - **Readiness `503` Drain Contract Alignment (`internal/transport/http/router.go`, `.ai/doc-api.md`, `api/openapi/openapi.yaml`)**:
     - Reconcile the `/ready` `503` JSON payload between `router.go` (`"status": "unavailable"`, `"checks": {"drain": "draining"}`) and `.ai/doc-api.md` / `api/openapi/openapi.yaml` (`"status": "draining"`, `"checks": {"drain": "shutting_down"}`) so runtime behavior, unit tests, and OpenAPI 3.1 match 1:1.

2. **Multi-Stage Distroless Container Build (`deployments/docker/Dockerfile`, `.dockerignore`)**:
   - Multi-stage Go `1.26.10` static build (`CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`).
   - Minimal runtime image `gcr.io/distroless/static:nonroot` (UID/GID `65532`, zero OS shell or package utilities, bundled root CA certificates for S3/STS HTTPS and TLS).
   - Explicit `WORKDIR /` (matching `main.go` default `OPENAPI_SPEC_PATH=api/openapi/openapi.yaml`) copying `/coordinator` binary and `/api/openapi/openapi.yaml`, setting `ENV TMPDIR=/tmp HOME=/tmp`, exposing `8080` and `50051`, with read-only root filesystem compatibility.

3. **Production Kubernetes Helm Chart & Canary Cutover (`deployments/helm/`)**:
   - `Chart.yaml`, `values.yaml`, `.helmignore`, and `templates/`:
     - **Compute Workload (`deployment.yaml` + optional Argo Rollouts `rollout.yaml` / `analysistemplate.yaml`)**:
       - Resource requests `100m` CPU / `128Mi` RAM; limits `500m` CPU / `256Mi` RAM (`< 256 MiB` RSS invariant).
       - Hardened Pod & Container security context: `runAsNonRoot: true`, `runAsUser: 65532`, `runAsGroup: 65532`, `fsGroup: 65532`, `seccompProfile: { type: RuntimeDefault }`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities: drop: ["ALL"]`, with a bounded ephemeral `emptyDir` (`sizeLimit: 64Mi`) mounted at `/tmp` and **strictly zero `/dev/shm` mounts** (`Go and Python Microservices Architecture.pdf`).
       - Multi-AZ High Availability & Disaggregated Node Placement: configurable `nodeSelector`, `tolerations`, `topologySpreadConstraints`, and `podAntiAffinity` across availability zones.
       - Probes: `startupProbe` (`GET /healthz:8080`, `failureThreshold: 12`, `periodSeconds: 5`), `livenessProbe` (`GET /healthz:8080`), and `readinessProbe` (`GET /ready:8080`), plus `terminationGracePeriodSeconds: 35`.
       - **12-Factor Config & Secret Wiring (`configmap.yaml`, `values.yaml`)**:
         - Non-secret runtime config in `configmap.yaml` (`LOG_LEVEL`, `HTTP_ADDR`, `GRPC_ADDR`, `TEMPORAL_HOST_PORT`, `TEMPORAL_NAMESPACE`, `POSTGRES_SSLMODE`, `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS`, `AUTO_MIGRATE=false`, `REDIS_ADDRS`, `REDIS_DB`, `REDIS_TLS_ENABLED`, `S3_REGION`, `S3_BUCKET`, `USE_HASH_PREFIX_S3_LAYOUT`, `ENABLE_INSPECTION_WORKER`, `SHUTDOWN_DRAIN_DELAY`, `OPENAPI_SPEC_PATH`).
         - Explicit `existingSecret` / Secret key references for sensitive credentials (`POSTGRES_DSN`, `REDIS_PASSWORD`, `AUTH_HMAC_SECRET` / `AUTH_STATIC_TOKENS_JSON`, optional AWS keys when not using IRSA) — never plaintext secrets in `ConfigMap` or `values.yaml`, and ensuring `buildProductionTokenValidator` does not accidentally fall back to `NewFailClosedTokenValidator()` in production.
       - Configurable Argo Rollouts canary progression (`rollout.enabled`) supporting `stableService` / `canaryService` (and documenting replica-based weight fallback when no traffic router is enabled), paired with Prometheus `AnalysisTemplate` (`rollout.analysis.prometheusAddress`) evaluating `coordinator_workflow_starts_total` and child/root failure rates before promotion.
     - **High Availability (`pdb.yaml`)**: `policy/v1` `PodDisruptionBudget` with `minAvailable: 2` (paired with default `replicaCount: 3`, gated via `pdb.enabled` so single-replica dev/staging overrides do not deadlock node drains).
     - **Network Isolation (`networkpolicy.yaml`)**:
       - Ingress restricted to ports `8080` (HTTP) and `50051` (gRPC) from configurable API Gateway and Prometheus scraper `namespaceSelector`/`podSelector` peers.
       - Egress restricted to configurable peers/CIDRs for Temporal frontend (`7233`), PostgreSQL/PgBouncer (`5432`), Redis (`6379`), cluster DNS (`53` UDP/TCP), and scoped HTTPS (`443`) for AWS S3/STS VPC endpoints.
       - Covers **both** coordinator workload pods and the `PreSync` migration Job pod (`5432` + DNS `53`) without exposing the migration Job pod to `service.yaml` traffic.
     - **ArgoCD `PreSync` Migration Hook (`presync-migrate-job.yaml`)**:
       - Runs `coordinator -migrate` before canary rollout with the same hardened non-root distroless security context, `existingSecret` (`POSTGRES_DSN`), `POSTGRES_SSLMODE`, and a **distinct pod label** (`app.kubernetes.io/component: migration`) so `service.yaml` never routes live HTTP/gRPC traffic to the migration pod.
     - **Prometheus Observability & Alerting Matrix (`prometheusrule.yaml`, `servicemonitor.yaml`)**:
       - Gated by `monitoring.serviceMonitor.enabled` and `monitoring.prometheusRule.enabled` for clusters with/without Prometheus Operator CRDs.
       - *High Task Queue Backlog*: `histogram_quantile(0.95, sum(rate(temporal_activity_schedule_to_start_seconds_bucket{task_queue="doc-ai-task-queue"}[5m])) by (le)) > 60` for `> 5m` (plus fallback average `rate(..._sum)/rate(..._count)` note).
       - *Elevated Workflow Failure Rate*: `sum(rate(coordinator_child_workflows_failed_total[15m])) / clamp_min(sum(rate(coordinator_child_workflows_completed_total[15m])) + sum(rate(coordinator_child_workflows_failed_total[15m])), 1) > 0.02` for `> 5m`.
       - *Redis Publish Drops*: `sum(rate(coordinator_redis_events_errors_total[2m])) / clamp_min(sum(rate(coordinator_redis_events_published_total[2m])) + sum(rate(coordinator_redis_events_errors_total[2m])), 1) > 0.001` for `> 2m`.
     - Supporting `service.yaml` (plus optional canary/stable services), `serviceaccount.yaml` (IRSA/Workload Identity annotations), `configmap.yaml`, `_helpers.tpl`.

4. **Chaos, Scale & Memory Profiling Test Suite (`test/chaos/`)**:
   - `test/chaos/resiliency_chaos_test.go`:
     - **20,000-Page Pod Eviction & Worker Drop Chaos**: Simulates mid-slice worker crashes (`SIGKILL` / `HeartbeatTimeout` preemption) across multiple slices in a 20,000-page (400 child workflows, 4 `ContinueAsNew` generations) ingestion, verifying:
       - `HeartbeatDetails` / `ResumeCheckpoint` resumes from `LastProcessedUnit + 1` with zero duplicate work across re-presigned retry attempts.
       - Fresh 15-minute presigned URLs are re-issued on every retry.
       - Poison-pill slice isolation (Runbook 3) preserves sibling slices.
       - Exact workflow/activity event accounting verifies history stays `< 2,500` events per root generation and per child workflow (`< 10,000` hard limit) with zero dual-writes and 1 atomic terminal record.
     - **OS Subprocess Kill (`Process.Kill`) Heartbeat Recovery Test**: Spawns a real OS child worker subprocess (Go test helper subprocess, and optionally Python `rag_document_ai_worker` when available), terminates it mid-slice via `Process.Kill`, and verifies clean checkpoint recovery (`LastProcessedUnit + 1`) on replacement worker startup within the 15s heartbeat window.
     - **5,000-Concurrent-Request Load, Sustained 1,000 RPS, 10 GB Multipart & `< 256 MiB` Memory Ceiling Test**:
       - Drives `5,000` concurrent `POST /v1/ingest/upload-url` (including `10 GB` / `10,737,418,240`-byte 100-part multipart requests) and `POST /v1/ingest/workflows/start` requests plus sustained `1,000 RPS` presign load through the HTTP router/middleware stack (with non-bottlenecking multi-tenant/high-burst rate-limiter test config and `io.Discard` structured logger so stdout/429s do not skew measurements).
       - Also drives `> 10,000` unique tenants (`12,000` tenants) to verify `TokenBucketLimiter` bounded eviction (`<= 10,000` buckets) and lock contention behavior.
       - Captures `runtime.ReadMemStats` / `runtime/pprof` heap metrics (and OS RSS where supported) to assert `< 256 MiB` memory ceiling, flat memory across up to `10 GB` logical uploads, and `P99` presign latency `< 35 ms`.
       - **CI Race-Detector Separation**: Functional/concurrency chaos tests run under `-race`, while wall-clock `P99 < 35 ms` and `< 256 MiB` memory assertions run in a dedicated non-`-race` test/benchmark step (guarded when `-race` is active or invoked via dedicated `make test-load`) because `-race` adds 5–10x CPU and shadow memory overhead.
     - **Zero-Drop Canary Drain Readiness Test**: Verifies `/ready` transitions immediately to `503` on drain signal while active in-flight HTTP requests drain and complete with `200`/`202` (`0` dropped connections) during `SHUTDOWN_DRAIN_DELAY` + graceful `http.Server.Shutdown`.

5. **Developer & CI Automation (`Makefile`, `.github/workflows/ci.yml`)**:
   - `Makefile`: `fmt`, `vet`, `lint`, `test`, `test-workflow`, `test-history`, `test-chaos`, `test-load` (non-`-race` P99/memory profile gate), `test-integration`, `build`, `docker-build`, `helm-lint`, `helm-template`, `check`.
   - `.github/workflows/ci.yml`:
     - Run `-race` chaos tests (`./test/chaos/...`) and separate non-`-race` load/memory ceiling verification.
     - Validate `Dockerfile` build (`docker build -f deployments/docker/Dockerfile .`).
     - Validate Helm chart (`helm lint deployments/helm` + `helm template` across **both** `rollout.enabled=false` and `rollout.enabled=true` with `monitoring.serviceMonitor.enabled=true` and `monitoring.prometheusRule.enabled=true`).

6. **Documentation & Contract Sync (`.ai/`, `api/`)**:
   - Synchronize `api/proto/v1/types.proto`, `api/openapi/openapi.yaml`, and `.ai/doc-api.md` with `ResumeCheckpoint`, `/ready` `503` schema alignment, and child-workflow Prometheus metrics.
   - Update `.ai/progress-tracker.md` marking Phase 5 Complete with `GAP-15` pressure-test resolution and session verification notes.

---

## 2. Files to Create or Modify

| File Path | Action | Responsibility |
| :--- | :--- | :--- |
| `api/proto/v1/types.proto` & `contracts.go` | Modify | Add `ResumeCheckpoint *HeartbeatDetails` to `ExtractSegmentRequest` for cross-retry checkpoint continuity. |
| `internal/workflow/document_partition.go` | Modify | Extract `LastHeartbeatDetails` from failed/timed-out extraction errors and pass to next re-presigned attempt. |
| `internal/workflow/ingestion_root.go` | Modify | Emit `coordinator_child_workflows_completed_total` / `coordinator_child_workflows_failed_total` counters. |
| `internal/activity/polyglot.go` | Modify | Fall back to `req.ResumeCheckpoint` when `HasHeartbeatDetails` is absent across workflow-level re-presign retries. |
| `workers/python/rag_document_ai_worker/models.py` & `activity.py` | Modify | Accept optional `resume_checkpoint` on `ExtractSegmentRequest` alongside `activity.info().heartbeat_details`. |
| `internal/telemetry/metrics.go` | Modify | Register child workflow completion/failure counters and wire `temporalMetricsHandler.Counter(...)`. |
| `internal/transport/http/router.go` | Modify | Align `/ready` `503` drain JSON schema with `.ai/doc-api.md` and `api/openapi/openapi.yaml`. |
| `cmd/coordinator/main.go` | Modify | Add configurable `SHUTDOWN_DRAIN_DELAY` pre-shutdown wait for zero-drop rolling deploys on shell-less distroless. |
| `.dockerignore` | Create | Exclude `.git`, docs, PDFs, and local artifacts from Docker build context. |
| `deployments/docker/Dockerfile` | Create | Multi-stage static Go `1.26.10` build onto `gcr.io/distroless/static:nonroot` (UID 65532, `WORKDIR /`, `TMPDIR=/tmp`). |
| `deployments/helm/Chart.yaml` | Create | Helm v2 chart metadata for `rag-ingestion-coordinator`. |
| `deployments/helm/values.yaml` | Create | Production defaults (resources, probes, `existingSecret` refs, PDB, NetworkPolicy selectors, Argo Rollouts canary + services, monitoring toggles, node affinity). |
| `deployments/helm/.helmignore` | Create | Standard Helm packaging ignores. |
| `deployments/helm/templates/_helpers.tpl` | Create | Chart naming, labels, selector helpers, and migration component labels. |
| `deployments/helm/templates/serviceaccount.yaml` | Create | ServiceAccount with optional AWS IRSA / Workload Identity annotations. |
| `deployments/helm/templates/configmap.yaml` | Create | Non-secret 12-factor environment configuration (`AUTO_MIGRATE=false`, `SHUTDOWN_DRAIN_DELAY`, etc.). |
| `deployments/helm/templates/service.yaml` | Create | ClusterIP Service(s) exposing `:8080` (HTTP) and `:50051` (gRPC), including optional stable/canary services for Argo Rollouts. |
| `deployments/helm/templates/deployment.yaml` | Create | Hardened Kubernetes `Deployment` (when `rollout.enabled=false`), zero `/dev/shm`, `startupProbe`, `seccompProfile`. |
| `deployments/helm/templates/rollout.yaml` | Create | Argo Rollouts canary `Rollout` (when `rollout.enabled=true`). |
| `deployments/helm/templates/analysistemplate.yaml` | Create | Prometheus canary promotion `AnalysisTemplate` with configurable `prometheusAddress`. |
| `deployments/helm/templates/pdb.yaml` | Create | `policy/v1` `PodDisruptionBudget` (`minAvailable: 2`, gated by `pdb.enabled`). |
| `deployments/helm/templates/networkpolicy.yaml` | Create | Strict ingress/egress `NetworkPolicy` for coordinator pods and migration Job matching `.ai/security.md`. |
| `deployments/helm/templates/presync-migrate-job.yaml` | Create | ArgoCD `PreSync` / Helm pre-upgrade DB migration Job (`-migrate`) with isolated component label and Secret wiring. |
| `deployments/helm/templates/servicemonitor.yaml` | Create | Prometheus `ServiceMonitor` scraping `/metrics` (CRD-gated). |
| `deployments/helm/templates/prometheusrule.yaml` | Create | Prometheus `PrometheusRule` alerts using valid histogram/counter PromQL (CRD-gated). |
| `test/chaos/resiliency_chaos_test.go` | Create | Chaos tests: 20k-page worker drops + re-presign checkpoint recovery, subprocess `Process.Kill` recovery, 5k concurrent + 1k RPS + 10 GB multipart load & `<256 MiB` profiling, drain safety. |
| `Makefile` | Create | Standard local/CI build, lint, test, chaos, non-race load/memory, Docker, and Helm targets. |
| `.github/workflows/ci.yml` | Modify | Run `-race` chaos tests, non-`-race` load/memory checks, `Dockerfile` build, and multi-mode `helm lint`/`helm template`. |
| `api/openapi/openapi.yaml`, `.ai/doc-api.md` & `.ai/progress-tracker.md` | Modify | Synchronize `ResumeCheckpoint`, `/ready` `503` payload, child workflow metrics, and mark Phase 5 Completed (`GAP-15`). |

---

## 3. Verification Plan

1. `gofmt -s -w .`
2. `go vet ./...`
3. `golangci-lint run`
4. `go test -v -race ./test/unit/... ./test/workflow/...`
5. `go test -v -race -run "Test.*History" ./test/workflow/...`
6. `go test -v -race -run "TestChaos_(WorkerDrop|SubprocessKill|CanaryDrain|RateLimiterBound)" ./test/chaos/...`
7. `go test -v -run "TestChaos_(Load|MemoryCeiling|SustainedPresign)" ./test/chaos/...` (non-`-race` P99 `< 35 ms`, `10 GB` multipart, and `< 256 MiB` memory ceiling verification)
8. `python -m pytest workers/python/tests` (verify Python `ResumeCheckpoint` + `HeartbeatDetails` parity)
9. `go build -o bin/coordinator ./cmd/coordinator` (verify clean binary compilation)
10. Secured Logging, `gosec`, `govulncheck`, Spectral (`npx @stoplight/spectral-cli lint --ruleset .spectral.yaml api/openapi/openapi.yaml`), and OpenAPI/Doc-API sync verification.
11. Docker build check (`docker build -f deployments/docker/Dockerfile .`) and Helm validation (`helm lint deployments/helm` + `helm template` for both `rollout.enabled=false` and `rollout.enabled=true` with monitoring enabled).

---

## 4. Human-in-the-Loop Confirmation Gate

In accordance with [`.ai/ai-workflow-rules.md`](../.ai/ai-workflow-rules.md):
- This plan defines the pressure-tested specification and deliverables for **Phase 5: Resiliency, Chaos & Production Cutover** (`GAP-15`).
- Awaiting human confirmation before implementing the runtime hardening fixes, Docker/Helm manifests, CI/Makefile automation, and chaos test suites.
