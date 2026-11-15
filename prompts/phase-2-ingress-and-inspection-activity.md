# Implementation Plan: Phase 2 — Ingress & Inspection Activity

## 1. Context & Objective

Following the architectural blueprints in [`tasks/RAG Ingestion Coordinator Implementation Plan.pdf`](<../tasks/RAG%20Ingestion%20Coordinator%20Implementation%20Plan.pdf>), [`tasks/Production Grade Backend Concepts.pdf`](<../tasks/Production%20Grade%20Backend%20Concepts.pdf>), [`tasks/Production-Grade Multimodal RAG Architectural Specification.pdf`](<../tasks/Production-Grade%20Multimodal%20RAG%20Architectural%20Specification.pdf>), and the system invariants in [`.ai/`](../.ai/), **Phase 2: Ingress & Inspection Activity** delivers the zero-heap ingress layer, deterministic S3 artifact presigning, and isolated byte-range header inspection worker:

1. **Presigned S3 Single-Part & Multipart URL Issuance with 15-Minute Lease Management**:
   - S3 presigner & range adapter (`internal/store/s3/presigner.go`) providing `PutObject`, `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `GetObject`, and bounded `ReadRange` operations with a strict 15-minute (`900s`) lease ceiling.
   - Core upload coordination service (`internal/service/upload_service.go`) enforcing:
     - Strict payload validation: size `1 B .. 10 GiB`, lowercase-normalized 64-hex SHA-256 `content_hash` (`^[a-fA-F0-9]{64}$`), supported IANA MIME types, and tenant boundary checks.
     - Filename sanitization stripping directory paths, replacing unsafe characters outside `[a-zA-Z0-9._-]`, capping length at $\le 255$ bytes, and rejecting `.` / `..` / empty names.
     - Automatic single-part ($\le 100\text{ MiB}$) vs. multipart ($> 100\text{ MiB}$ up to $10\text{ GiB}$, $100\text{ MiB}$ parts, bounded to $\le 10,000$ parts) handoffs, and deterministic S3 object key construction (`raw/<tenant_id>/<content_hash>/<sanitized_filename>`).
     - Strict `CompleteMultipartUpload` validation verifying that `s3_key` has prefix `raw/<tenant_id>/` (with zero `..` traversal), `s3_bucket` matches the configured bucket, and `parts` (`1..10,000`) are sorted in strictly ascending `part_number` order with no duplicates and non-empty `etag` values.
2. **HTTP Ingress Transport & Middleware (`internal/transport/http/`)**:
   - REST handlers (`internal/transport/http/upload_handler.go`) for `POST /v1/ingest/upload-url` and `POST /v1/ingest/upload-url/complete` with a `64 KiB` control-plane request body ceiling (`413 Content Too Large`), plus health, readiness, metrics, and OpenAPI spec endpoints (`/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, `/docs`).
   - Production middleware (`internal/transport/http/middleware/middleware.go`):
     - High-entropy request correlation (`X-Request-ID` propagated as `trace_id`).
     - Multi-tenant Bearer token & `X-Tenant-ID` authentication and scope enforcement (`ingest:write`, `ingest:read`), returning `401 Unauthorized` or `403 Forbidden`.
     - Memory-bounded per-tenant/IP token-bucket rate limiting (`MaxTrackedBuckets = 10,000` with stale-bucket eviction to protect the `<256 MiB` RSS ceiling), returning `429 Too Many Requests` with `Retry-After`.
     - Secured structured JSON request logging emitting all 4 mandatory correlation fields (`trace_id`, `tenant_id`, `document_id`, `workflow_id`) while strictly redacting `Authorization` headers and presigned S3 query strings (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`, `AWSAccessKeyId`).
3. **Isolated Byte-Range Header Inspection Worker (`internal/activity/inspection.go`)**:
   - Range-based `ByteRangeReader` with a hard `io.LimitReader` ceiling (`MaxInspectionRangeBytes = 64 KiB`) so a misbehaving S3 endpoint returning a full stream can never OOM the worker pod.
   - Multi-step `HeartbeatDetails` checkpointing (`TotalUnits = 2`: Unit `1` after the $4\text{ KiB}$ header window read, Unit `2` after the $\le 64\text{ KiB}$ tail window read) with crash recovery resumption (`activity.GetHeartbeatDetails`) and post-inspection structured logging.
   - Extracts real document/media metadata from binary headers:
     - **PDF (`application/pdf`)**: Verifies `%PDF-` magic header within the first $1,024\text{ B}$, parses linearized `/Linearized ... /N <count>` or the maximum `/Type /Pages` `/Count <N>` across head ($4\text{ KiB}$) and tail ($\le 64\text{ KiB}$) windows, with deterministic byte-size fallback when compressed `/ObjStm` object streams are used.
     - **Audio (`audio/wav`, `audio/x-wav`, `audio/flac`, `audio/mpeg`, `audio/ogg`)**: Dynamically scans RIFF subchunks for WAV `fmt ` byte rate & `data` size, parses FLAC `fLaC` `STREAMINFO` 20-bit sample rate & 36-bit total samples, Ogg `OggS` magic header, or MP3 `ID3`/MPEG frame sync bitrate to compute duration in seconds.
     - **Video (`video/mp4`, `video/quicktime`, `video/webm`)**: Parses ISO BMFF / MP4 `ftyp`/`moov`/`mvhd` timescale and duration atoms (version 0 32-bit and version 1 64-bit in head or tail window) or WebM EBML `0x1A 0x45 0xDF 0xA3` header.
     - **Tabular (`text/csv`, `application/vnd.apache.parquet`, XLSX)**: Verifies `total_bytes >= 12` and `PAR1` magic bytes in both header and footer (for Parquet), `PK\x03\x04` (for XLSX), or valid UTF-8 without NUL bytes (for CSV) and computes bounded partition counts.
4. **Deterministic S3 Artifact Key & Slice Presigning Integration (`internal/activity/presign.go`)**:
   - Wires `S3Presigner` into `PresignActivities` (`PresignSlice`) to issue 15-minute presigned `GetObject` (input raw object) and `PutObject` (content-addressed output Parquet key `artifacts/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet` or Runbook 2 hash-prefixed `artifacts/<hash_prefix>/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet`) URLs.
   - Enforces tenant isolation on `slice.S3InputKey` (`raw/<tenant_id>/...`, zero `..` traversal) and validates or auto-computes `slice.S3OutputParquetKey` (`artifacts/[<hash_prefix>/]<tenant_id>/<content_hash>/slice_<start>_<end>.parquet`).

---

## 2. Files to Create or Modify

| File Path | Action | Responsibility |
| :--- | :--- | :--- |
| `internal/store/s3/presigner.go` | Create | AWS S3 / compatible presigning, multipart upload, and bounded byte-range reader adapter (`PutObject`, `GetObject`, `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `PresignSlice`, and `ReadRange` with `64 KiB` `io.LimitReader` cap) with 15-minute (`900s`) lease enforcement. |
| `internal/service/upload_service.go` | Create | Core upload coordination service: validates requests, enforces $\le 10\text{ GiB}$ ceiling & 64-hex SHA-256 content hashes, calculates deterministic `raw/` S3 keys, orchestrates single-part ($\le 100\text{ MiB}$) and multipart ($> 100\text{ MiB}$) presigned URLs, and validates/completes multipart uploads. |
| `internal/activity/presign.go` | Modify | Enforce tenant prefix (`raw/<tenant_id>/`) and deterministic `artifacts/` path validation on `PartitionSlice` before issuing 15-minute presigned GET/PUT URLs. |
| `internal/activity/inspection.go` | Modify | Add `ByteRangeReader` interface, `LimitedByteRangeReader`, and `ByteRangeHeaderInspector` implementing bounded byte-range header/tail parsing for PDF, WAV, FLAC, MP3, Ogg, MP4/MOV, WebM, Parquet, CSV, and XLSX with 2-stage `HeartbeatDetails` checkpoint recovery. |
| `internal/transport/http/middleware/middleware.go` | Create | Request ID (`X-Request-ID`), secured JSON `slog` request logger (with mandatory `trace_id`, `tenant_id`, `document_id`, `workflow_id` fields and redaction of Bearer tokens & S3 signatures), tenant Bearer auth + `X-Tenant-ID` scope enforcement, and bounded per-tenant token-bucket rate limiter. |
| `internal/transport/http/upload_handler.go` | Create | HTTP REST handlers for `POST /v1/ingest/upload-url` and `POST /v1/ingest/upload-url/complete` with strict JSON decoding, `64 KiB` body cap, authenticated tenant cross-checks, and sanitized error responses. |
| `internal/transport/http/router.go` | Create | Standard library `net/http` (`http.ServeMux`) router wiring `/v1/ingest/upload-url`, `/v1/ingest/upload-url/complete`, `/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, and `/docs` with the middleware stack. |
| `test/unit/upload_service_test.go` | Create | Unit tests covering single-part ($\le 100\text{ MiB}$) and multipart ($> 100\text{ MiB}$ up to $10\text{ GiB}$) presigning, multipart ETag completion & part order validation, cross-tenant key rejection, 15-minute lease bounds, and deterministic `raw/` and `artifacts/` S3 keys. |
| `test/unit/inspection_test.go` | Create | Unit tests for isolated byte-range header inspection across synthetic PDF (standard, linearized, multi-node page tree, compressed fallback), WAV, FLAC, MP3, Ogg, MP4 (`mvhd` v0/v1), WebM, Parquet (`PAR1` head+tail), XLSX, CSV, corrupt/magic-mismatch payloads, `64 KiB` range cap, and `HeartbeatDetails` checkpoint recovery. |
| `test/unit/http_upload_handler_test.go` | Create | HTTP handler and middleware tests verifying `POST /v1/ingest/upload-url`, `POST /v1/ingest/upload-url/complete`, probe endpoints, tenant isolation (`X-Tenant-ID` vs Bearer claims), body size limit (`413`), rate limiting (`429`), and log redaction of `X-Amz-Signature` and `Bearer` tokens. |
| `.ai/doc-api.md` & `api/openapi/openapi.yaml` | Modify | Synchronize `X-Tenant-ID` and `X-Request-ID` headers, `/openapi.yaml` and `/docs` paths, and `401`, `403`, `413`, and `429` error responses across OpenAPI 3.1 and `.ai/doc-api.md`. |
| `.ai/progress-tracker.md` | Modify | Record Phase 2 completion and pressure-test refinement notes. |

---

## 3. Detailed Design & Invariants

### 3.1 Presigned S3 Single-Part & Multipart Handoff (`internal/service/upload_service.go`, `internal/store/s3/presigner.go`)
- **Constants & Thresholds**:
  - `SinglePartUploadMaxBytes = 100 * 1024 * 1024` ($100\text{ MiB}$): Files $\le 100\text{ MiB}$ receive a single-part `presigned_url`.
  - `DefaultMultipartPartSizeBytes = 100 * 1024 * 1024` ($100\text{ MiB}$): Files $> 100\text{ MiB}$ up to $10\text{ GiB}$ (`MaxPayloadBytes`) use multipart upload with $100\text{ MiB}$ parts (yielding at most $103$ parts for $10\text{ GiB}$, well within S3's $10,000$-part limit and $\ge 5\text{ MiB}$ minimum part size).
  - `LeaseSeconds = 900` ($15\text{ minutes}$).
- **Deterministic S3 Key Layout & Validation**:
  - Raw upload key: `raw/<tenant_id>/<content_hash>/<sanitized_filename>` (prevents path traversal `..` or slash injection in `filename` or `content_hash`).
  - Slice artifact Parquet key: `artifacts/<tenant_id>/<content_hash>/slice_<start>_<end>.parquet` (or `artifacts/<hash_prefix>/<tenant_id>/<content_hash>/slice_<start>_<end>.parquet` under Runbook 2).

### 3.2 Isolated Byte-Range Inspection (`internal/activity/inspection.go`)
- **Zero Full-File Buffering**:
  - Reads at most two bounded byte windows via `ByteRangeReader.ReadRange(ctx, bucket, key, start, end)` enforced by `io.LimitReader` (`MaxInspectionRangeBytes = 65536`):
    - Header window: `bytes=0-4095` ($4\text{ KiB}$) to verify magic bytes and parse container headers (PDF `%PDF-` / `/Linearized`, WAV `RIFF`/`WAVE`/`fmt `, FLAC `fLaC`, MP3 `ID3`/sync, Ogg `OggS`, MP4 `ftyp`/`moov`, WebM EBML, Parquet `PAR1`, XLSX `PK\x03\x04`, CSV UTF-8).
    - Tail window: `bytes=max(0, totalBytes-65536)-(totalBytes-1)` ($\le 64\text{ KiB}$) when inspecting PDF cross-reference/page-tree trailers (`/Count`), trailing MP4 `moov`/`mvhd` atoms, or Parquet footer magic `PAR1`.
  - Total heap allocation per inspection call is strictly bounded to $\le 68\text{ KiB}$ regardless of whether the S3 object is $10\text{ KiB}$ or $10\text{ GB}$.

### 3.3 Security & Logging Audit
- Every struct containing presigned URLs (`CreateUploadURLResponse`, `PresignedPart`, `PresignSliceArtifactResponse`, `ExtractSegmentRequest`) implements `slog.LogValuer` with `[REDACTED]`.
- HTTP middleware and services emit structured JSON logs containing `trace_id`, `tenant_id`, `document_id`, and `workflow_id`, while redacting `Authorization` headers and stripping query parameters containing `X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`, or `AWSAccessKeyId`.

---

## 4. Verification Plan

1. `gofmt -s -w .`
2. `go vet ./...` and `golangci-lint run`
3. `go test -v -race ./test/unit/... ./test/workflow/...`
4. Verify OpenAPI 3.1 (`api/openapi/openapi.yaml`) and [`.ai/doc-api.md`](../.ai/doc-api.md) synchronization.
5. Verify Secured Logging audit rules (`grep` check from `.github/workflows/ci.yml`).
6. Commit in logical increments using conventional commit prefixes (`feat:`, `tests:`, `docs:`).
