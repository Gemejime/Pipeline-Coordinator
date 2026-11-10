package http

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

// ExecutionHandler serves POST /v1/ingest/workflows/start and GET /v1/ingest/workflows/{id}/status.
type ExecutionHandler struct {
	executions service.ExecutionCoordinator
	logger     *slog.Logger
}

// NewExecutionHandler constructs an ExecutionHandler with structured JSON logging.
func NewExecutionHandler(executions service.ExecutionCoordinator, logger *slog.Logger) *ExecutionHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &ExecutionHandler{
		executions: executions,
		logger:     logger,
	}
}

// HandleStartIngestion handles POST /v1/ingest/workflows/start.
func (h *ExecutionHandler) HandleStartIngestion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method must be POST")
		return
	}
	if h == nil || h.executions == nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "execution service is not initialized")
		return
	}

	var req coordinatorv1.StartIngestionRequest
	if !decodeBoundedJSON(w, r, &req) {
		return
	}

	if !enforceTenantConsistency(w, r, req.TenantID) {
		return
	}

	middleware.SetCorrelationFields(r.Context(), req.TenantID, req.DocumentID, "")

	resp, err := h.executions.StartIngestion(r.Context(), req)
	if err != nil {
		h.writeExecutionError(w, r, req.TenantID, req.DocumentID, "", err)
		return
	}

	middleware.SetCorrelationFields(r.Context(), req.TenantID, req.DocumentID, resp.WorkflowID)

	h.logger.Info("scheduled root ingestion coordinator workflow",
		slog.String("trace_id", middleware.TraceIDFromContext(r.Context())),
		slog.String("tenant_id", req.TenantID),
		slog.String("document_id", req.DocumentID),
		slog.String("workflow_id", resp.WorkflowID),
		slog.String("run_id", resp.RunID),
		slog.String("s3_bucket", req.S3Bucket),
		slog.String("s3_key", req.S3Key),
		slog.Int64("started_at_unix", resp.StartedAtUnix),
	)

	writeJSON(w, http.StatusAccepted, resp)
}

// HandleGetIngestionStatus handles GET /v1/ingest/workflows/{id}/status.
func (h *ExecutionHandler) HandleGetIngestionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method must be GET")
		return
	}
	if h == nil || h.executions == nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "execution service is not initialized")
		return
	}

	workflowID := strings.TrimSpace(r.PathValue("id"))
	if workflowID == "" {
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "workflow id path parameter is required")
		return
	}

	tenantID := strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "tenant_id query parameter is required")
		return
	}

	if !enforceTenantConsistency(w, r, tenantID) {
		return
	}

	middleware.SetCorrelationFields(r.Context(), tenantID, "", workflowID)

	resp, err := h.executions.GetWorkflowStatus(r.Context(), workflowID, tenantID)
	if err != nil {
		h.writeExecutionError(w, r, tenantID, "", workflowID, err)
		return
	}

	h.logger.Info("retrieved workflow status",
		slog.String("trace_id", middleware.TraceIDFromContext(r.Context())),
		slog.String("tenant_id", tenantID),
		slog.String("workflow_id", workflowID),
		slog.String("status", resp.Status),
		slog.Int("completed_slices", int(resp.CompletedSlices)),
		slog.Int("total_slices", int(resp.TotalSlices)),
	)

	writeJSON(w, http.StatusOK, resp)
}

func (h *ExecutionHandler) writeExecutionError(
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
	case errors.Is(err, service.ErrWorkflowNotFound):
		statusCode = http.StatusNotFound
		errCode = "NOT_FOUND"
	case errors.Is(err, service.ErrInvalidExecutionRequest),
		errors.Is(err, service.ErrUnsupportedMimeType),
		errors.Is(err, service.ErrZeroBytePayload),
		errors.Is(err, service.ErrPayloadTooLarge),
		errors.Is(err, service.ErrInvalidUploadRequest),
		errors.Is(err, service.ErrInvalidMultipartCompletion):
		statusCode = http.StatusBadRequest
		errCode = "BAD_REQUEST"
	default:
		statusCode = http.StatusInternalServerError
		errCode = "INTERNAL_ERROR"
	}

	h.logger.Warn("execution request failed",
		slog.String("trace_id", middleware.TraceIDFromContext(r.Context())),
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.String("workflow_id", workflowID),
		slog.Int("status", statusCode),
		slog.String("error_code", errCode),
	)

	if statusCode == http.StatusInternalServerError {
		writeJSONError(w, statusCode, errCode, "internal server error processing execution request")
		return
	}
	writeJSONError(w, statusCode, errCode, err.Error())
}
