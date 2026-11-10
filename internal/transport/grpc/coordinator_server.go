package grpc

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
)

// UploadCoordinator abstracts upload service operations invoked by CoordinatorServer.
type UploadCoordinator interface {
	CreateUploadURL(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error)
	CompleteMultipartUpload(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error)
}

// ExecutionCoordinator abstracts execution orchestration operations invoked by CoordinatorServer.
type ExecutionCoordinator interface {
	StartIngestion(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error)
	GetWorkflowStatus(ctx context.Context, workflowID, tenantID string) (*coordinatorv1.GetIngestionStatusResponse, error)
}

// CoordinatorServer implements coordinatorv1.CoordinatorServiceServer for internal control plane RPCs.
type CoordinatorServer struct {
	uploads    UploadCoordinator
	executions ExecutionCoordinator
	logger     *slog.Logger
}

// NewCoordinatorServer constructs a CoordinatorServer with injected coordinators and structured logging.
func NewCoordinatorServer(
	uploads UploadCoordinator,
	executions ExecutionCoordinator,
	logger *slog.Logger,
) *CoordinatorServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &CoordinatorServer{
		uploads:    uploads,
		executions: executions,
		logger:     logger,
	}
}

// CreateUploadURL generates a presigned single-part or multipart S3 upload lease.
func (s *CoordinatorServer) CreateUploadURL(
	ctx context.Context,
	req *coordinatorv1.CreateUploadURLRequest,
) (*coordinatorv1.CreateUploadURLResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s == nil || s.uploads == nil {
		return nil, status.Error(codes.Internal, "upload service is not initialized")
	}

	resp, err := s.uploads.CreateUploadURL(ctx, *req)
	if err != nil {
		s.logRPCError(ctx, "CreateUploadURL", req.TenantID, req.ContentHash, "", err)
		return nil, mapServiceErrorToGRPC(err)
	}

	s.logger.Info("gRPC issued presigned upload lease",
		slog.String("tenant_id", resp.TenantID),
		slog.String("s3_bucket", resp.S3Bucket),
		slog.String("s3_key", resp.S3Key),
		slog.String("upload_id", resp.UploadID),
		slog.Any("upload_response", *resp),
	)

	return resp, nil
}

// CompleteUploadURL finalizes an S3 multipart upload with part ETags.
func (s *CoordinatorServer) CompleteUploadURL(
	ctx context.Context,
	req *coordinatorv1.CompleteMultipartUploadRequest,
) (*coordinatorv1.CompleteMultipartUploadResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s == nil || s.uploads == nil {
		return nil, status.Error(codes.Internal, "upload service is not initialized")
	}

	resp, err := s.uploads.CompleteMultipartUpload(ctx, *req)
	if err != nil {
		s.logRPCError(ctx, "CompleteUploadURL", req.TenantID, req.S3Key, req.UploadID, err)
		return nil, mapServiceErrorToGRPC(err)
	}

	s.logger.Info("gRPC completed multipart s3 upload",
		slog.String("tenant_id", req.TenantID),
		slog.String("s3_bucket", req.S3Bucket),
		slog.String("s3_key", req.S3Key),
		slog.String("upload_id", resp.UploadID),
		slog.Int("parts_count", len(req.Parts)),
	)

	return resp, nil
}

// StartIngestion triggers the root IngestionCoordinatorWorkflow on Temporal.
func (s *CoordinatorServer) StartIngestion(
	ctx context.Context,
	req *coordinatorv1.StartIngestionRequest,
) (*coordinatorv1.StartIngestionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s == nil || s.executions == nil {
		return nil, status.Error(codes.Internal, "execution service is not initialized")
	}

	resp, err := s.executions.StartIngestion(ctx, *req)
	if err != nil {
		s.logRPCError(ctx, "StartIngestion", req.TenantID, req.DocumentID, "", err)
		return nil, mapServiceErrorToGRPC(err)
	}

	s.logger.Info("gRPC scheduled ingestion workflow",
		slog.String("tenant_id", req.TenantID),
		slog.String("document_id", req.DocumentID),
		slog.String("workflow_id", resp.WorkflowID),
		slog.String("run_id", resp.RunID),
	)

	return resp, nil
}

// GetIngestionStatus queries in-flight or terminal workflow execution status.
func (s *CoordinatorServer) GetIngestionStatus(
	ctx context.Context,
	req *coordinatorv1.GetIngestionStatusRequest,
) (*coordinatorv1.GetIngestionStatusResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s == nil || s.executions == nil {
		return nil, status.Error(codes.Internal, "execution service is not initialized")
	}

	resp, err := s.executions.GetWorkflowStatus(ctx, req.WorkflowID, req.TenantID)
	if err != nil {
		s.logRPCError(ctx, "GetIngestionStatus", req.TenantID, "", req.WorkflowID, err)
		return nil, mapServiceErrorToGRPC(err)
	}

	s.logger.Info("gRPC retrieved ingestion status",
		slog.String("tenant_id", req.TenantID),
		slog.String("workflow_id", req.WorkflowID),
		slog.String("status", resp.Status),
	)

	return resp, nil
}

func (s *CoordinatorServer) logRPCError(
	ctx context.Context,
	method, tenantID, documentID, workflowID string,
	err error,
) {
	s.logger.Warn("gRPC request failed",
		slog.String("method", method),
		slog.String("tenant_id", tenantID),
		slog.String("document_id", documentID),
		slog.String("workflow_id", workflowID),
		slog.String("error", err.Error()),
	)
}

func mapServiceErrorToGRPC(err error) error {
	switch {
	case errors.Is(err, service.ErrTenantMismatch):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, service.ErrWorkflowNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrPayloadTooLarge):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, service.ErrInvalidUploadRequest),
		errors.Is(err, service.ErrInvalidExecutionRequest),
		errors.Is(err, service.ErrInvalidMultipartCompletion),
		errors.Is(err, service.ErrUnsupportedMimeType),
		errors.Is(err, service.ErrZeroBytePayload):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "context canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}

