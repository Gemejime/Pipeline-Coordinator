package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

const (
	// CoordinatorTaskQueue is the Temporal Task Queue for root coordinator execution.
	CoordinatorTaskQueue = "coordinator-task-queue"
	// QueryTypeIngestionStatus is the Temporal Query Type name for querying live workflow progress.
	QueryTypeIngestionStatus = "ingestion_status"
	// IngestionCoordinatorWorkflowName is the workflow function registration name on Temporal.
	IngestionCoordinatorWorkflowName = "IngestionCoordinatorWorkflow"
)

var (
	// SearchAttrTenantID maps to the typed Search Attribute "TenantID" in Temporal.
	SearchAttrTenantID = temporal.NewSearchAttributeKeyString("TenantID")
	// SearchAttrDocumentID maps to the typed Search Attribute "DocumentID" in Temporal.
	SearchAttrDocumentID = temporal.NewSearchAttributeKeyString("DocumentID")
	// SearchAttrPipelineRunID maps to the typed Search Attribute "PipelineRunID" in Temporal.
	SearchAttrPipelineRunID = temporal.NewSearchAttributeKeyString("PipelineRunID")
)

var (
	// ErrInvalidExecutionRequest indicates a non-retryable validation failure when initiating ingestion.
	ErrInvalidExecutionRequest = errors.New("INVALID_ARGUMENT: execution request validation failed")
	// ErrWorkflowNotFound indicates that the requested workflow execution does not exist.
	ErrWorkflowNotFound = errors.New("NOT_FOUND: workflow execution not found")
)

// WorkflowIngestionRequest carries parameters passed into IngestionCoordinatorWorkflow.
type WorkflowIngestionRequest struct {
	TenantID         string `json:"tenant_id"`
	DocumentID       string `json:"document_id"`
	PipelineRunID    string `json:"pipeline_run_id,omitempty"`
	S3Bucket         string `json:"s3_bucket"`
	S3Key            string `json:"s3_key"`
	ContentHash      string `json:"content_hash"`
	TotalBytes       int64  `json:"total_bytes"`
	MimeType         string `json:"mime_type"`
	PipelineConfigID string `json:"pipeline_config_id,omitempty"`
	StartedAtUnix    int64  `json:"started_at_unix,omitempty"`
}

// WorkflowIngestionStatus matches the live status payload returned by QueryTypeIngestionStatus.
type WorkflowIngestionStatus struct {
	WorkflowID        string   `json:"workflow_id"`
	TenantID          string   `json:"tenant_id"`
	DocumentID        string   `json:"document_id"`
	Status            string   `json:"status"`
	Stage             string   `json:"stage"`
	TotalPages        int32    `json:"total_pages"`
	TotalSlices       int32    `json:"total_slices"`
	CompletedSlices   int32    `json:"completed_slices"`
	FailedSlices      int32    `json:"failed_slices"`
	FailedSliceIDs    []string `json:"failed_slice_ids,omitempty"`
	CurrentGeneration int32    `json:"current_generation"`
}

// TemporalWorkflowClient abstracts the Temporal client operations used by ExecutionService.
// This interface matches go.temporal.io/sdk/client.Client methods for compile-time conformance and mocking.
type TemporalWorkflowClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error)
	QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...any) (converter.EncodedValue, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

// TerminalRunReader abstracts optional retrieval of completed workflow runs from the PostgreSQL catalog.
type TerminalRunReader interface {
	GetTerminalRun(ctx context.Context, tenantID, workflowID string) (*coordinatorv1.GetIngestionStatusResponse, error)
}

// ExecutionCoordinator defines the workflow execution orchestration interface.
type ExecutionCoordinator interface {
	StartIngestion(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error)
	GetWorkflowStatus(ctx context.Context, workflowID, tenantID string) (*coordinatorv1.GetIngestionStatusResponse, error)
}

// ExecutionServiceConfig configures dependencies and behavior of ExecutionService.
type ExecutionServiceConfig struct {
	TerminalReader TerminalRunReader
	NowFunc        func() time.Time
}

// ExecutionService coordinates workflow initiation, deduplication, and query-based status inspection.
type ExecutionService struct {
	client         TemporalWorkflowClient
	terminalReader TerminalRunReader
	nowFunc        func() time.Time
}

// NewExecutionService constructs an ExecutionService.
func NewExecutionService(client TemporalWorkflowClient, cfg ExecutionServiceConfig) *ExecutionService {
	nowFunc := cfg.NowFunc
	if nowFunc == nil {
		nowFunc = func() time.Time { return time.Now().UTC() }
	}
	return &ExecutionService{
		client:         client,
		terminalReader: cfg.TerminalReader,
		nowFunc:        nowFunc,
	}
}

// StartIngestion validates the request and idempotently schedules the root IngestionCoordinatorWorkflow.
func (s *ExecutionService) StartIngestion(
	ctx context.Context,
	req coordinatorv1.StartIngestionRequest,
) (*coordinatorv1.StartIngestionResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("%w: temporal client is not initialized", ErrInvalidExecutionRequest)
	}

	tenantID, err := ValidateTenantIdentifier(req.TenantID)
	if err != nil {
		return nil, err
	}

	documentID, err := validateDocumentIdentifier(req.DocumentID)
	if err != nil {
		return nil, err
	}

	s3Bucket := strings.TrimSpace(req.S3Bucket)
	if s3Bucket == "" {
		return nil, fmt.Errorf("%w: s3_bucket is required", ErrInvalidExecutionRequest)
	}

	if err := ValidateTenantRawS3Key(tenantID, req.S3Key); err != nil {
		return nil, err
	}

	contentHash, err := ValidateContentHash(req.ContentHash)
	if err != nil {
		return nil, err
	}

	if req.TotalBytes <= 0 {
		return nil, fmt.Errorf("%w: %w", ErrInvalidExecutionRequest, ErrZeroBytePayload)
	}
	if req.TotalBytes > MaxPayloadBytes {
		return nil, fmt.Errorf("%w: %w", ErrInvalidExecutionRequest, ErrPayloadTooLarge)
	}

	normalizedMime := strings.ToLower(strings.TrimSpace(req.MimeType))
	if !IsSupportedMimeType(normalizedMime) {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedMimeType, req.MimeType)
	}

	workflowID := BuildIngestionWorkflowID(tenantID, contentHash)
	pipelineRunID := fmt.Sprintf("run-%s-%d", contentHash[:16], s.nowFunc().UnixNano())

	ingestReq := WorkflowIngestionRequest{
		TenantID:         tenantID,
		DocumentID:       documentID,
		PipelineRunID:    pipelineRunID,
		S3Bucket:         s3Bucket,
		S3Key:            strings.TrimSpace(req.S3Key),
		ContentHash:      contentHash,
		TotalBytes:       req.TotalBytes,
		MimeType:         normalizedMime,
		PipelineConfigID: strings.TrimSpace(req.PipelineConfigID),
		StartedAtUnix:    s.nowFunc().Unix(),
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                CoordinatorTaskQueue,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			SearchAttrTenantID.ValueSet(tenantID),
			SearchAttrDocumentID.ValueSet(documentID),
			SearchAttrPipelineRunID.ValueSet(pipelineRunID),
		),
	}

	run, err := s.client.ExecuteWorkflow(ctx, workflowOptions, IngestionCoordinatorWorkflowName, ingestReq)
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return &coordinatorv1.StartIngestionResponse{
				WorkflowID:    workflowID,
				RunID:         alreadyStarted.RunId,
				Status:        "RUNNING",
				StartedAtUnix: ingestReq.StartedAtUnix,
			}, nil
		}
		return nil, fmt.Errorf("failed to schedule ingestion workflow: %w", err)
	}

	runID := run.GetRunID()
	if runID == "" {
		runID = pipelineRunID
	}

	return &coordinatorv1.StartIngestionResponse{
		WorkflowID:    workflowID,
		RunID:         runID,
		Status:        "RUNNING",
		StartedAtUnix: ingestReq.StartedAtUnix,
	}, nil
}

// GetWorkflowStatus queries in-flight workflow state via QueryTypeIngestionStatus (ADR-003: zero dual-writes)
// and falls back gracefully to TerminalRunReader or DescribeWorkflowExecution when closed.
func (s *ExecutionService) GetWorkflowStatus(
	ctx context.Context,
	workflowID, tenantID string,
) (*coordinatorv1.GetIngestionStatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("%w: temporal client is not initialized", ErrInvalidExecutionRequest)
	}

	validTenant, err := ValidateTenantIdentifier(tenantID)
	if err != nil {
		return nil, err
	}

	cleanWorkflowID := strings.TrimSpace(workflowID)
	if cleanWorkflowID == "" {
		return nil, fmt.Errorf("%w: workflow_id is required", ErrInvalidExecutionRequest)
	}

	// Step 1: Query live workflow state via QueryTypeIngestionStatus ("ingestion_status").
	queryVal, queryErr := s.client.QueryWorkflow(ctx, cleanWorkflowID, "", QueryTypeIngestionStatus)
	if queryErr == nil {
		var status WorkflowIngestionStatus
		if err := queryVal.Get(&status); err == nil {
			if status.TenantID != "" && status.TenantID != validTenant {
				return nil, fmt.Errorf("%w: workflow tenant %q does not match %q", ErrTenantMismatch, status.TenantID, validTenant)
			}
			return &coordinatorv1.GetIngestionStatusResponse{
				WorkflowID:      cleanWorkflowID,
				TenantID:        validTenant,
				Status:          status.Status,
				TotalSlices:     status.TotalSlices,
				CompletedSlices: status.CompletedSlices,
				FailedSlices:    status.FailedSlices,
				TotalPages:      status.TotalPages,
				CreatedAt:       s.nowFunc().Format(time.RFC3339),
			}, nil
		}
	}

	// Step 2: If query failed, check if TerminalRunReader provides a completed record from PostgreSQL.
	if s.terminalReader != nil {
		termResp, termErr := s.terminalReader.GetTerminalRun(ctx, validTenant, cleanWorkflowID)
		if termErr == nil && termResp != nil {
			if termResp.TenantID != "" && termResp.TenantID != validTenant {
				return nil, fmt.Errorf("%w: catalog tenant %q does not match %q", ErrTenantMismatch, termResp.TenantID, validTenant)
			}
			return termResp, nil
		}
	}

	// Step 3: Describe workflow execution to handle closed, completed, or failed runs.
	desc, descErr := s.client.DescribeWorkflowExecution(ctx, cleanWorkflowID, "")
	if descErr != nil {
		var notFound *serviceerror.NotFound
		if errors.As(descErr, &notFound) || strings.Contains(strings.ToLower(descErr.Error()), "not found") {
			return nil, fmt.Errorf("%w: workflow %q not found", ErrWorkflowNotFound, cleanWorkflowID)
		}
		return nil, fmt.Errorf("failed to describe workflow execution: %w", descErr)
	}

	if desc == nil || desc.WorkflowExecutionInfo == nil {
		return nil, fmt.Errorf("%w: workflow %q description unavailable", ErrWorkflowNotFound, cleanWorkflowID)
	}

	info := desc.WorkflowExecutionInfo

	// Enforce tenant boundary from typed search attributes if available.
	if info.SearchAttributes != nil && info.SearchAttributes.IndexedFields != nil {
		if payload, ok := info.SearchAttributes.IndexedFields[SearchAttrTenantID.GetName()]; ok {
			var searchTenant string
			if err := converter.GetDefaultDataConverter().FromPayload(payload, &searchTenant); err == nil {
				if searchTenant != "" && searchTenant != validTenant {
					return nil, fmt.Errorf("%w: search attribute tenant %q does not match %q", ErrTenantMismatch, searchTenant, validTenant)
				}
			}
		}
	}

	statusStr := mapWorkflowExecutionStatus(info.Status)
	createdAt := ""
	completedAt := ""
	var durationSec float64

	if info.StartTime != nil {
		start := info.StartTime.AsTime()
		createdAt = start.Format(time.RFC3339)
		if info.CloseTime != nil {
			closeT := info.CloseTime.AsTime()
			completedAt = closeT.Format(time.RFC3339)
			durationSec = closeT.Sub(start).Seconds()
		} else {
			durationSec = s.nowFunc().Sub(start).Seconds()
		}
	}
	if durationSec < 0 {
		durationSec = 0
	}

	runID := ""
	if info.Execution != nil {
		runID = info.Execution.GetRunId()
	}

	return &coordinatorv1.GetIngestionStatusResponse{
		WorkflowID:      cleanWorkflowID,
		RunID:           runID,
		TenantID:        validTenant,
		Status:          statusStr,
		DurationSeconds: durationSec,
		CreatedAt:       createdAt,
		CompletedAt:     completedAt,
	}, nil
}

// BuildIngestionWorkflowID computes the deterministic idempotent workflow ID: ingest-<tenant_id>-<content_hash>.
func BuildIngestionWorkflowID(tenantID, contentHash string) string {
	return fmt.Sprintf("ingest-%s-%s", tenantID, contentHash)
}

func validateDocumentIdentifier(documentID string) (string, error) {
	trimmed := strings.TrimSpace(documentID)
	if trimmed == "" {
		return "", fmt.Errorf("%w: document_id is required", ErrInvalidExecutionRequest)
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") || strings.Contains(trimmed, "..") {
		return "", fmt.Errorf("%w: document_id %q contains invalid path characters", ErrInvalidExecutionRequest, documentID)
	}
	if len(trimmed) > 255 {
		return "", fmt.Errorf("%w: document_id exceeds 255 characters", ErrInvalidExecutionRequest)
	}
	return trimmed, nil
}

func mapWorkflowExecutionStatus(status enumspb.WorkflowExecutionStatus) string {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "RUNNING"
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "COMPLETED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "FAILED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "CANCELED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "TERMINATED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "TIMED_OUT"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "CONTINUED_AS_NEW"
	default:
		return "UNSPECIFIED"
	}
}
