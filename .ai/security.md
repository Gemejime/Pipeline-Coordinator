# Security Context - RAG Ingestion Coordinator

## Threat Model & Trust Boundaries

- **Untrusted Ingress Payloads**: Ingestion requests arrive from client applications and external tenants. File metadata, tenant identifiers, and document names must be validated against strict schemas before initiating workflows.
- **Direct-to-S3 Upload Bypass**: Raw document payloads (up to $10\text{ GB}$) bypass coordinator memory. Clients receive presigned S3 URLs directly, preventing memory exhaustion and denial-of-service vulnerabilities in the coordinator process.
- **Tenant Isolation**: Strict logical separation across all operations. Workflow IDs, database records, Redis channels, and S3 artifact namespaces are partitioned strictly by `tenant_id`.
- **Coordinator Runtime**: Isolated control-plane environment. Operates as a non-root user (`nonroot:nonroot`, UID 65532) in a scratch/distroless container image with zero shell utilities and a read-only root filesystem.
- **Internal RPC & Service Mesh**: Coordinator communicates exclusively with Temporal frontend (gRPC), PostgreSQL (TLS/PgBouncer), Redis (TLS), and telemetry collectors. Compute worker traffic remains decoupled from the coordinator.

---

## Storage Access & Presigned URL Lease Management

1. **Short-Lived Presigned S3 URLs**:
   - Presigned upload and download URLs are issued via AWS STS / S3 Presign Client with a maximum lease of **15 minutes** ($900\text{ seconds}$).
   - Permissions are restricted strictly to `s3:PutObject` (and multipart upload actions `s3:AbortMultipartUpload`, `s3:ListMultipartUploadParts`) for upload targets and `s3:GetObject` for slice reading.
2. **Deterministic Artifact Namespaces**:
   - Worker artifact paths enforce deterministic content-addressed naming conventions:
     `s3://<bucket>/artifacts/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet`
   - This ensures full idempotency upon activity retries and prevents cross-tenant or cross-document overwrites.
3. **Partition Load Distribution**:
   - For high-throughput scenarios, S3 keys include hash prefixes (`s3://<bucket>/artifacts/<hash_prefix>/...`) to distribute I/O evenly across S3 partition limits (avoiding HTTP 503 SlowDown).

---

## Secured Logging Invariants

1. **Presigned URL Redaction**:
   - Presigned S3 URLs contain cryptographic authentication tokens and signatures in query parameters (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`, `AWSAccessKeyId`).
   - **Full presigned URLs must NEVER be written to stdout or logs**.
   - Log only the bucket name and the sanitized object key:
     `s3_bucket = "my-bucket"`, `s3_key = "artifacts/tenant_1/hash/slice_0_50.parquet"`.
2. **Credentials & Auth Redaction**:
   - Client authorization headers (`Authorization: Bearer ...`), database passwords, and Redis connection strings must be stripped and masked before logging.
3. **Structured JSON Telemetry**:
   - Log entries must be structured JSON format containing only correlation IDs (`trace_id`, `tenant_id`, `document_id`, `workflow_id`, `run_id`, `status`, `duration_ms`), never raw binary buffers or tenant PII/PHI.
4. **Error Trace Sanitization**:
   - Error messages returned to clients must not disclose internal file system paths, database credentials, or internal cluster hostnames.

---

## Network & Container Hardening

1. **Distroless Non-Root Container**:
   - Base image: `gcr.io/distroless/static:nonroot`.
   - UID: `65532` (`runAsNonRoot: true`, `runAsUser: 65532`).
   - Read-only root filesystem (`readOnlyRootFilesystem: true`) with ephemeral mounts scoped to `/tmp`.
   - Drop all Linux capabilities (`capabilities: drop: ["ALL"]`).
2. **Kubernetes Network Policies**:
   - Coordinator ingress: Restriced to API Gateway on port `8080` (HTTP) and `50051` (gRPC).
   - Coordinator egress: Egress allowed only to Temporal frontend (port `7233`), PostgreSQL (port `5432`), Redis (port `6379`), and Prometheus metrics collector. Egress to external internet or compute worker pods is strictly denied.

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
