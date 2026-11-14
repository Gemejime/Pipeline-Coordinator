package unit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	transport "rag-ingestion-coordinator/internal/transport/http"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

type mockExecutionCoordinator struct {
	startIngestionFn    func(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error)
	getWorkflowStatusFn func(ctx context.Context, workflowID, tenantID string) (*coordinatorv1.GetIngestionStatusResponse, error)
}

func (m *mockExecutionCoordinator) StartIngestion(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error) {
	if m.startIngestionFn != nil {
		return m.startIngestionFn(ctx, req)
	}
	return nil, errors.New("StartIngestion not implemented")
}

func (m *mockExecutionCoordinator) GetWorkflowStatus(ctx context.Context, workflowID, tenantID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
	if m.getWorkflowStatusFn != nil {
		return m.getWorkflowStatusFn(ctx, workflowID, tenantID)
	}
	return nil, errors.New("GetWorkflowStatus not implemented")
}

func setupExecutionRouter(exec service.ExecutionCoordinator, limiter *middleware.TokenBucketLimiter) http.Handler {
	validator := middleware.NewStaticTokenValidator(map[string]middleware.AuthClaims{
		"valid-write-token": {
			TenantID: "tenant-corp-01",
			Scopes:   []string{middleware.ScopeIngestWrite},
		},
		"valid-read-token": {
			TenantID: "tenant-corp-01",
			Scopes:   []string{middleware.ScopeIngestRead},
		},
		"other-tenant-token": {
			TenantID: "tenant-other-02",
			Scopes:   []string{middleware.ScopeIngestWrite, middleware.ScopeIngestRead},
		},
	})

	if limiter == nil {
		limiter = middleware.NewTokenBucketLimiter(middleware.RateLimiterConfig{
			RequestsPerSecond: 1000,
			Burst:             1000,
		})
	}

	execHandler := transport.NewExecutionHandler(exec, nil)
	return transport.NewRouter(transport.RouterConfig{
		ExecutionHandler: execHandler,
		TokenValidator:   validator,
		RateLimiter:      limiter,
	})
}

func TestHTTP_StartIngestion(t *testing.T) {
	t.Parallel()

	validBody := map[string]any{
		"tenant_id":          "tenant-corp-01",
		"document_id":        "doc-01",
		"s3_bucket":          "rag-ingestion-bucket",
		"s3_key":             "raw/tenant-corp-01/hash/doc.pdf",
		"content_hash":       "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"total_bytes":        1048576,
		"mime_type":          "application/pdf",
		"pipeline_config_id": "default-multimodal-v1",
	}

	t.Run("401 Unauthorized when missing Bearer token", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		bodyBytes, _ := json.Marshal(validBody)
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("403 Forbidden on missing ingest:write scope", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		bodyBytes, _ := json.Marshal(validBody)
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer valid-read-token") // only has ingest:read
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("403 Forbidden on cross-tenant claim mismatch", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		bodyBytes, _ := json.Marshal(validBody) // tenant-corp-01
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer other-tenant-token") // tenant-other-02
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("413 Payload Too Large when request body exceeds 64 KiB", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		oversized := bytes.Repeat([]byte("A"), 70*1024)
		body := append([]byte(`{"tenant_id":"`), append(oversized, []byte(`"}`)...)...)
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer valid-write-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})

	t.Run("400 Bad Request on malformed JSON", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", strings.NewReader("{invalid-json"))
		req.Header.Set("Authorization", "Bearer valid-write-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("429 Too Many Requests when rate limited", func(t *testing.T) {
		t.Parallel()
		exhaustedLimiter := middleware.NewTokenBucketLimiter(middleware.RateLimiterConfig{
			RequestsPerSecond: 0.001,
			Burst:             1,
		})
		router := setupExecutionRouter(&mockExecutionCoordinator{}, exhaustedLimiter)

		bodyBytes, _ := json.Marshal(validBody)

		// First request consumes token
		req1 := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", bytes.NewReader(bodyBytes))
		req1.Header.Set("Authorization", "Bearer valid-write-token")
		req1.Header.Set("Content-Type", "application/json")
		rec1 := httptest.NewRecorder()
		router.ServeHTTP(rec1, req1)

		// Second request is rate limited
		req2 := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", bytes.NewReader(bodyBytes))
		req2.Header.Set("Authorization", "Bearer valid-write-token")
		req2.Header.Set("Content-Type", "application/json")
		rec2 := httptest.NewRecorder()
		router.ServeHTTP(rec2, req2)

		assert.Equal(t, http.StatusTooManyRequests, rec2.Code)
		assert.NotEmpty(t, rec2.Header().Get("Retry-After"))
	})

	t.Run("202 Accepted on successful workflow schedule", func(t *testing.T) {
		t.Parallel()
		mockExec := &mockExecutionCoordinator{
			startIngestionFn: func(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error) {
				return &coordinatorv1.StartIngestionResponse{
					WorkflowID:    "ingest-tenant-corp-01-hash",
					RunID:         "run-12345",
					Status:        "RUNNING",
					StartedAtUnix: 1794039600,
				}, nil
			},
		}

		router := setupExecutionRouter(mockExec, nil)

		bodyBytes, _ := json.Marshal(validBody)
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/workflows/start", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer valid-write-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)

		var resp coordinatorv1.StartIngestionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "ingest-tenant-corp-01-hash", resp.WorkflowID)
		assert.Equal(t, "RUNNING", resp.Status)
		assert.Equal(t, int64(1794039600), resp.StartedAtUnix)
	})
}

func TestHTTP_GetWorkflowStatus(t *testing.T) {
	t.Parallel()

	workflowID := "ingest-tenant-corp-01-hash"

	t.Run("401 Unauthorized when missing Bearer token", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		req := httptest.NewRequest(http.MethodGet, "/v1/ingest/workflows/"+workflowID+"/status?tenant_id=tenant-corp-01", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("400 Bad Request when missing tenant_id query param", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		req := httptest.NewRequest(http.MethodGet, "/v1/ingest/workflows/"+workflowID+"/status", nil)
		req.Header.Set("Authorization", "Bearer valid-read-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("403 Forbidden when token tenant does not match query tenant_id", func(t *testing.T) {
		t.Parallel()
		router := setupExecutionRouter(&mockExecutionCoordinator{}, nil)

		// valid-read-token belongs to tenant-corp-01, query requests tenant-other-02
		req := httptest.NewRequest(http.MethodGet, "/v1/ingest/workflows/"+workflowID+"/status?tenant_id=tenant-other-02", nil)
		req.Header.Set("Authorization", "Bearer valid-read-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("404 Not Found when workflow does not exist", func(t *testing.T) {
		t.Parallel()
		mockExec := &mockExecutionCoordinator{
			getWorkflowStatusFn: func(ctx context.Context, wID, tID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				return nil, service.ErrWorkflowNotFound
			},
		}

		router := setupExecutionRouter(mockExec, nil)
		req := httptest.NewRequest(http.MethodGet, "/v1/ingest/workflows/"+workflowID+"/status?tenant_id=tenant-corp-01", nil)
		req.Header.Set("Authorization", "Bearer valid-read-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("200 OK on successful workflow status query", func(t *testing.T) {
		t.Parallel()
		mockExec := &mockExecutionCoordinator{
			getWorkflowStatusFn: func(ctx context.Context, wID, tID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				return &coordinatorv1.GetIngestionStatusResponse{
					WorkflowID:      wID,
					TenantID:        tID,
					Status:          "COMPLETED",
					TotalSlices:     4,
					CompletedSlices: 4,
					FailedSlices:    0,
					TotalPages:      120,
					DurationSeconds: 15.8,
				}, nil
			},
		}

		router := setupExecutionRouter(mockExec, nil)
		req := httptest.NewRequest(http.MethodGet, "/v1/ingest/workflows/"+workflowID+"/status?tenant_id=tenant-corp-01", nil)
		req.Header.Set("Authorization", "Bearer valid-read-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp coordinatorv1.GetIngestionStatusResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, workflowID, resp.WorkflowID)
		assert.Equal(t, "COMPLETED", resp.Status)
		assert.Equal(t, int32(4), resp.CompletedSlices)
		assert.Equal(t, 15.8, resp.DurationSeconds)
	})
}
