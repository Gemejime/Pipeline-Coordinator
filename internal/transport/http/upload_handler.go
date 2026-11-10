// Package http implements the inbound HTTP REST transport handlers and routing
// for the rag-ingestion-coordinator control plane.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

const (
	// MaxControlPlaneBodyBytes (64 KiB) caps inbound JSON request bodies on control-plane endpoints.
	MaxControlPlaneBodyBytes int64 = 64 * 1024
)

// UploadCoordinator defines the upload service operations invoked by HTTP handlers.
type UploadCoordinator interface {
	CreateUploadURL(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error)
	CompleteMultipartUpload(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error)
}

// ErrorResponse is the canonical JSON error body returned by REST endpoints.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// UploadHandler serves POST /v1/ingest/upload-url and POST /v1/ingest/upload-url/complete.
type UploadHandler struct {
	uploads UploadCoordinator
	logger  *slog.Logger
}

// NewUploadHandler constructs an UploadHandler with structured JSON logging.
func NewUploadHandler(uploads UploadCoordinator, logger *slog.Logger) *UploadHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &UploadHandler{
		uploads: uploads,
		logger:  logger,
	}
}

// HandleCreateUploadURL handles POST /v1/ingest/upload-url.
func (h *UploadHandler) HandleCreateUploadURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method must be POST")
		return
	}
	if h == nil || h.uploads == nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "upload service is not initialized")
		return
	}

	var req coordinatorv1.CreateUploadURLRequest
	if !decodeBoundedJSON(w, r, &req) {
		return
	}

	if !enforceTenantConsistency(w, r, req.TenantID) {
		return
	}

	middleware.SetCorrelationFields(r.Context(), req.TenantID, req.ContentHash, "")

	resp, err := h.uploads.CreateUploadURL(r.Context(), req)
	if err != nil {
		h.writeServiceError(w, r, req.TenantID, req.ContentHash, "", err)
		return
	}

	middleware.SetCorrelationFields(r.Context(), resp.TenantID, req.ContentHash, resp.UploadID)

	// Log sanitized metadata using slog.LogValuer (presigned URLs are strictly redacted).
	h.logger.Info("issued presigned upload lease",
		slog.String("trace_id", middleware.TraceIDFromContext(r.Context())),
		slog.String("tenant_id", resp.TenantID),
		slog.String("document_id", req.ContentHash),
		slog.String("workflow_id", resp.UploadID),
		slog.String("s3_bucket", resp.S3Bucket),
		slog.String("s3_key", resp.S3Key),
		slog.Any("upload_response", *resp),
	)

	writeJSON(w, http.StatusOK, resp)
}

// HandleCompleteMultipartUpload handles POST /v1/ingest/upload-url/complete.
func (h *UploadHandler) HandleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method must be POST")
		return
	}
	if h == nil || h.uploads == nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "upload service is not initialized")
		return
	}

	var req coordinatorv1.CompleteMultipartUploadRequest
	if !decodeBoundedJSON(w, r, &req) {
		return
	}

	if !enforceTenantConsistency(w, r, req.TenantID) {
		return
	}

	middleware.SetCorrelationFields(r.Context(), req.TenantID, req.S3Key, req.UploadID)

	resp, err := h.uploads.CompleteMultipartUpload(r.Context(), req)
	if err != nil {
		h.writeServiceError(w, r, req.TenantID, req.S3Key, req.UploadID, err)
		return
	}

	h.logger.Info("completed multipart s3 upload",
		slog.String("trace_id", middleware.TraceIDFromContext(r.Context())),
		slog.String("tenant_id", req.TenantID),
		slog.String("document_id", req.S3Key),
		slog.String("workflow_id", resp.UploadID),
		slog.String("s3_bucket", req.S3Bucket),
		slog.String("s3_key", req.S3Key),
		slog.Int("parts_count", len(req.Parts)),
	)

	writeJSON(w, http.StatusOK, resp)
}

func (h *UploadHandler) writeServiceError(
	w http.ResponseWriter,
	r *http.Request,
	tenantID, documentID, workflowID string,
	err error,
) {
	statusCode := http.StatusBadRequest
	errCode := "BAD_REQUEST"

	switch {
	case errors.Is(err, service.ErrTenantMismatch):
		statusCode = http.StatusForbidden
		errCode = "FORBIDDEN"
	case errors.Is(err, service.ErrUnsupportedMimeType),
		errors.Is(err, service.ErrInvalidUploadRequest),
		errors.Is(err, service.ErrInvalidMultipartCompletion),
		errors.Is(err, service.ErrPayloadTooLarge),
		errors.Is(err, service.ErrZeroBytePayload):
		statusCode = http.StatusBadRequest
		errCode = "BAD_REQUEST"
	default:
		statusCode = http.StatusInternalServerError
		errCode = "INTERNAL_ERROR"
	}

	h.logger.Warn("upload request failed",
		slog.String("trace_id", middleware.TraceIDFromContext(r.Context())),
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.String("workflow_id", workflowID),
		slog.Int("status", statusCode),
		slog.String("error_code", errCode),
	)

	if statusCode == http.StatusInternalServerError {
		writeJSONError(w, statusCode, errCode, "internal server error processing upload request")
		return
	}
	writeJSONError(w, statusCode, errCode, err.Error())
}

func enforceTenantConsistency(w http.ResponseWriter, r *http.Request, bodyTenantID string) bool {
	trimmedBodyTenant := strings.TrimSpace(bodyTenantID)
	if trimmedBodyTenant == "" {
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "tenant_id is required in request body")
		return false
	}

	if claims, ok := middleware.ClaimsFromContext(r.Context()); ok {
		if claims.TenantID != trimmedBodyTenant {
			writeJSONError(w, http.StatusForbidden, "FORBIDDEN", "tenant_id in request body does not match authenticated tenant claims")
			return false
		}
	}

	if headerTenant := strings.TrimSpace(r.Header.Get(middleware.HeaderTenantID)); headerTenant != "" {
		if headerTenant != trimmedBodyTenant {
			writeJSONError(w, http.StatusForbidden, "FORBIDDEN", "tenant_id in request body does not match X-Tenant-ID header")
			return false
		}
	}
	return true
}

func decodeBoundedJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Body == nil {
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "request body is required")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxControlPlaneBodyBytes)
	defer func() {
		_ = r.Body.Close()
	}()

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) || strings.Contains(err.Error(), "http: request body too large") {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "control-plane request body exceeds 64 KiB limit")
			return false
		}
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "malformed or invalid JSON request body")
		return false
	}

	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "request body must contain a single JSON object")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, statusCode int, code, message string) {
	writeJSON(w, statusCode, ErrorResponse{
		Error:   code,
		Message: message,
	})
}
