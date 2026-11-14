package unit_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/store/s3"
	transporthttp "rag-ingestion-coordinator/internal/transport/http"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

// setupTestServer wires an UploadService, UploadHandler, router, and static token validator.
func setupTestServer(t *testing.T, logBuf *bytes.Buffer) (http.Handler, *middleware.TokenBucketLimiter) {
	t.Helper()

	presigner, err := s3.NewPresigner(s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1"), nil, s3.PresignerConfig{})
	require.NoError(t, err)
	uploadSvc := service.NewUploadService(presigner, service.UploadServiceConfig{
		DefaultBucket: "rag-ingestion-bucket",
		LeaseSeconds:  900,
	})

	var logger *slog.Logger
	if logBuf != nil {
		logger = slog.New(slog.NewJSONHandler(logBuf, nil))
	} else {
		logger = slog.Default()
	}

	uploadHandler := transporthttp.NewUploadHandler(uploadSvc, logger)
	tokenValidator := middleware.NewStaticTokenValidator(map[string]middleware.AuthClaims{
		"token-corp-01": {
			TenantID: "tenant-corp-01",
			Scopes:   []string{middleware.ScopeIngestWrite, middleware.ScopeIngestRead},
		},
		"token-readonly": {
			TenantID: "tenant-corp-01",
			Scopes:   []string{middleware.ScopeIngestRead},
		},
		"token-tenant-02": {
			TenantID: "tenant-corp-02",
			Scopes:   []string{middleware.ScopeIngestWrite},
		},
	})

	rateLimiter := middleware.NewTokenBucketLimiter(middleware.RateLimiterConfig{
		RequestsPerSecond: 10,
		Burst:             10,
	})

	router := transporthttp.NewRouter(transporthttp.RouterConfig{
		UploadHandler:  uploadHandler,
		TokenValidator: tokenValidator,
		RateLimiter:    rateLimiter,
		Logger:         logger,
		OpenAPISpec:    []byte("openapi: 3.1.0\ninfo:\n  title: Mock\n"),
	})

	return router, rateLimiter
}

// TestHTTP_UploadURL_SuccessAndSecuredLogging verifies POST /v1/ingest/upload-url, correlation headers, and credential redaction.
func TestHTTP_UploadURL_SuccessAndSecuredLogging(t *testing.T) {
	t.Parallel()

	logBuf := new(bytes.Buffer)
	handler, _ := setupTestServer(t, logBuf)

	reqPayload := coordinatorv1.CreateUploadURLRequest{
		TenantID:           "tenant-corp-01",
		Filename:           "Quarterly Report.pdf",
		ContentType:        "application/pdf",
		ContentLengthBytes: 50 * 1024 * 1024,
		ContentHash:        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}
	body, _ := json.Marshal(reqPayload)

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-corp-01")
	req.Header.Set("X-Tenant-ID", "tenant-corp-01")
	req.Header.Set("X-Request-ID", "req-trace-test-12345")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "req-trace-test-12345", rec.Header().Get("X-Request-ID"))

	var resp coordinatorv1.CreateUploadURLResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "tenant-corp-01", resp.TenantID)
	assert.Equal(t, "rag-ingestion-bucket", resp.S3Bucket)
	assert.NotEmpty(t, resp.PresignedURL)

	// Verify Secured Logging Invariant: log contains trace_id and tenant_id, but redaction applies.
	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "req-trace-test-12345")
	assert.Contains(t, logOutput, "tenant-corp-01")
	assert.Contains(t, logOutput, "[REDACTED]")
	assert.NotContains(t, logOutput, "token-corp-01", "Bearer token must NEVER appear in cleartext in logs")
	assert.NotContains(t, logOutput, "X-Amz-Signature=", "S3 query signature must NEVER appear in logs")
}

// TestHTTP_CompleteMultipartUpload_Success verifies POST /v1/ingest/upload-url/complete.
func TestHTTP_CompleteMultipartUpload_Success(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestServer(t, nil)

	s3Key := "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/report.pdf"
	reqPayload := coordinatorv1.CompleteMultipartUploadRequest{
		UploadID: "mp_upload_abc123",
		TenantID: "tenant-corp-01",
		S3Bucket: "rag-ingestion-bucket",
		S3Key:    s3Key,
		Parts: []coordinatorv1.CompletedPart{
			{PartNumber: 1, ETag: `"etag-1"`},
			{PartNumber: 2, ETag: `"etag-2"`},
		},
	}
	body, _ := json.Marshal(reqPayload)

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url/complete", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token-corp-01")
	req.Header.Set("X-Tenant-ID", "tenant-corp-01")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp coordinatorv1.CompleteMultipartUploadResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "COMPLETED", resp.Status)
	assert.Equal(t, "s3://rag-ingestion-bucket/"+s3Key, resp.S3Location)
}

// TestHTTP_AuthAndTenantGuards verifies 401 Unauthorized, 403 Forbidden, and scope checks.
func TestHTTP_AuthAndTenantGuards(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestServer(t, nil)

	reqPayload := coordinatorv1.CreateUploadURLRequest{
		TenantID:           "tenant-corp-01",
		Filename:           "doc.pdf",
		ContentType:        "application/pdf",
		ContentLengthBytes: 1024,
		ContentHash:        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}
	body, _ := json.Marshal(reqPayload)

	t.Run("missing token returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("invalid token returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer invalid-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("missing ingest:write scope returns 403", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer token-readonly")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("cross-tenant mismatch between token and body returns 403", func(t *testing.T) {
		// token-tenant-02 represents tenant-corp-02, but body has tenant-corp-01
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer token-tenant-02")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("cross-tenant mismatch between token and X-Tenant-ID header returns 403", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer token-corp-01")
		req.Header.Set("X-Tenant-ID", "tenant-corp-02") // Mismatch!
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})
}

// TestHTTP_PayloadLimitsAndProbes verifies 413 on oversized control-plane bodies, and probes (/healthz, /ready, /metrics, /docs, /openapi.yaml).
func TestHTTP_PayloadLimitsAndProbes(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestServer(t, nil)

	t.Run("oversized control plane body returns 413", func(t *testing.T) {
		oversized := bytes.Repeat([]byte("A"), 70*1024) // 70 KiB > 64 KiB ceiling
		body := append([]byte(`{"tenant_id":"`), append(oversized, []byte(`"}`)...)...)
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer token-corp-01")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})

	t.Run("healthz liveness probe returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "healthy")
	})

	t.Run("ready probe returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "ready")
	})

	t.Run("metrics returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "coordinator_workflow_starts_total")
	})

	t.Run("openapi.yaml returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "openapi: 3.1.0")
	})

	t.Run("docs returns 200 HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/docs", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "<html")
	})

	t.Run("rate limiter returns 429 when burst exhausted", func(t *testing.T) {
		strictLimiter := middleware.NewTokenBucketLimiter(middleware.RateLimiterConfig{
			RequestsPerSecond: 1,
			Burst:             1,
		})
		p, err := s3.NewPresigner(s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1"), nil, s3.PresignerConfig{})
		require.NoError(t, err)
		strictRouter := transporthttp.NewRouter(transporthttp.RouterConfig{
			UploadHandler: transporthttp.NewUploadHandler(service.NewUploadService(p, service.UploadServiceConfig{
				DefaultBucket: "rag-ingestion-bucket",
			}), nil),
			TokenValidator: middleware.NewStaticTokenValidator(map[string]middleware.AuthClaims{
				"token-corp-01": {
					TenantID: "tenant-corp-01",
					Scopes:   []string{middleware.ScopeIngestWrite},
				},
			}),
			RateLimiter: strictLimiter,
		})

		// First request consumes the single token
		allowed, _ := strictLimiter.Allow("tenant:tenant-corp-01")
		require.True(t, allowed)

		reqPayload := coordinatorv1.CreateUploadURLRequest{
			TenantID:           "tenant-corp-01",
			Filename:           "doc.pdf",
			ContentType:        "application/pdf",
			ContentLengthBytes: 1024,
			ContentHash:        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		}
		body, _ := json.Marshal(reqPayload)
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer token-corp-01")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		strictRouter.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusTooManyRequests, rec.Code)
		assert.NotEmpty(t, rec.Header().Get("Retry-After"))
	})
}
