package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

const (
	grpcMethodCreateUploadURL    = "/rag.coordinator.v1.CoordinatorService/CreateUploadURL"
	grpcMethodCompleteUploadURL  = "/rag.coordinator.v1.CoordinatorService/CompleteUploadURL"
	grpcMethodStartIngestion     = "/rag.coordinator.v1.CoordinatorService/StartIngestion"
	grpcMethodGetIngestionStatus = "/rag.coordinator.v1.CoordinatorService/GetIngestionStatus"
)

var methodRequiredScopes = map[string]string{
	grpcMethodCreateUploadURL:    middleware.ScopeIngestWrite,
	grpcMethodCompleteUploadURL:  middleware.ScopeIngestWrite,
	grpcMethodStartIngestion:     middleware.ScopeIngestWrite,
	grpcMethodGetIngestionStatus: middleware.ScopeIngestRead,
}

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

// ServerOption configures optional dependencies on CoordinatorServer.
type ServerOption func(*CoordinatorServer)

// WithTokenValidator configures the TokenValidator enforced by CoordinatorServer.
// When omitted or nil, CoordinatorServer defaults to a fail-closed validator.
func WithTokenValidator(validator middleware.TokenValidator) ServerOption {
	return func(s *CoordinatorServer) {
		if validator != nil {
			s.validator = validator
		}
	}
}

// CoordinatorServer implements coordinatorv1.CoordinatorServiceServer for internal control plane RPCs.
type CoordinatorServer struct {
	uploads    UploadCoordinator
	executions ExecutionCoordinator
	validator  middleware.TokenValidator
	logger     *slog.Logger
}

// NewCoordinatorServer constructs a CoordinatorServer with injected coordinators, fail-closed auth default, and structured logging.
func NewCoordinatorServer(
	uploads UploadCoordinator,
	executions ExecutionCoordinator,
	logger *slog.Logger,
	opts ...ServerOption,
) *CoordinatorServer {
	if logger == nil {
		logger = slog.Default()
	}
	srv := &CoordinatorServer{
		uploads:    uploads,
		executions: executions,
		validator:  middleware.NewFailClosedTokenValidator(),
		logger:     logger,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(srv)
		}
	}
	if srv.validator == nil {
		srv.validator = middleware.NewFailClosedTokenValidator()
	}
	return srv
}

// UnaryAuthInterceptor enforces Bearer authentication, optional x-tenant-id metadata consistency,
// and per-method scope authorization across all CoordinatorService unary RPCs.
func UnaryAuthInterceptor(validator middleware.TokenValidator, logger *slog.Logger) grpc.UnaryServerInterceptor {
	if validator == nil {
		validator = middleware.NewFailClosedTokenValidator()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		fullMethod := ""
		if info != nil {
			fullMethod = info.FullMethod
		}
		claims, err := authenticateAndCheckScope(ctx, validator, fullMethod, logger)
		if err != nil {
			return nil, err
		}
		return handler(middleware.WithAuthClaims(ctx, claims), req)
	}
}

// CreateUploadURL generates a presigned single-part or multipart S3 upload lease.
func (s *CoordinatorServer) CreateUploadURL(
	ctx context.Context,
	req *coordinatorv1.CreateUploadURLRequest,
) (*coordinatorv1.CreateUploadURLResponse, error) {
	if s == nil || s.uploads == nil {
		return nil, status.Error(codes.Internal, "upload service is not initialized")
	}
	claims, err := s.authorizeCaller(ctx, grpcMethodCreateUploadURL)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if err := enforceRequestTenant(claims, req.TenantID, s.logger, grpcMethodCreateUploadURL); err != nil {
		return nil, err
	}

	boundReq := *req
	boundReq.TenantID = claims.TenantID

	resp, err := s.uploads.CreateUploadURL(ctx, boundReq)
	if err != nil {
		s.logRPCError(ctx, "CreateUploadURL", claims.TenantID, req.ContentHash, "", err)
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
	if s == nil || s.uploads == nil {
		return nil, status.Error(codes.Internal, "upload service is not initialized")
	}
	claims, err := s.authorizeCaller(ctx, grpcMethodCompleteUploadURL)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if err := enforceRequestTenant(claims, req.TenantID, s.logger, grpcMethodCompleteUploadURL); err != nil {
		return nil, err
	}

	boundReq := *req
	boundReq.TenantID = claims.TenantID

	resp, err := s.uploads.CompleteMultipartUpload(ctx, boundReq)
	if err != nil {
		s.logRPCError(ctx, "CompleteUploadURL", claims.TenantID, req.S3Key, req.UploadID, err)
		return nil, mapServiceErrorToGRPC(err)
	}

	s.logger.Info("gRPC completed multipart s3 upload",
		slog.String("tenant_id", claims.TenantID),
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
	if s == nil || s.executions == nil {
		return nil, status.Error(codes.Internal, "execution service is not initialized")
	}
	claims, err := s.authorizeCaller(ctx, grpcMethodStartIngestion)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if err := enforceRequestTenant(claims, req.TenantID, s.logger, grpcMethodStartIngestion); err != nil {
		return nil, err
	}

	boundReq := *req
	boundReq.TenantID = claims.TenantID

	resp, err := s.executions.StartIngestion(ctx, boundReq)
	if err != nil {
		s.logRPCError(ctx, "StartIngestion", claims.TenantID, req.DocumentID, "", err)
		return nil, mapServiceErrorToGRPC(err)
	}

	s.logger.Info("gRPC scheduled ingestion workflow",
		slog.String("tenant_id", claims.TenantID),
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
	if s == nil || s.executions == nil {
		return nil, status.Error(codes.Internal, "execution service is not initialized")
	}
	claims, err := s.authorizeCaller(ctx, grpcMethodGetIngestionStatus)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if err := enforceRequestTenant(claims, req.TenantID, s.logger, grpcMethodGetIngestionStatus); err != nil {
		return nil, err
	}

	resp, err := s.executions.GetWorkflowStatus(ctx, req.WorkflowID, claims.TenantID)
	if err != nil {
		s.logRPCError(ctx, "GetIngestionStatus", claims.TenantID, "", req.WorkflowID, err)
		return nil, mapServiceErrorToGRPC(err)
	}

	s.logger.Info("gRPC retrieved ingestion status",
		slog.String("tenant_id", claims.TenantID),
		slog.String("workflow_id", req.WorkflowID),
		slog.String("status", resp.Status),
	)

	return resp, nil
}

func (s *CoordinatorServer) authorizeCaller(ctx context.Context, fullMethod string) (*middleware.AuthClaims, error) {
	if claims, ok := middleware.ClaimsFromContext(ctx); ok && claims != nil {
		if strings.TrimSpace(claims.TenantID) == "" {
			return nil, status.Error(codes.Unauthenticated, "invalid or expired Bearer token")
		}
		if err := checkMetadataTenantAndScope(ctx, claims, fullMethod, s.logger); err != nil {
			return nil, err
		}
		return claims, nil
	}
	validator := s.validator
	if validator == nil {
		validator = middleware.NewFailClosedTokenValidator()
	}
	return authenticateAndCheckScope(ctx, validator, fullMethod, s.logger)
}

func authenticateAndCheckScope(
	ctx context.Context,
	validator middleware.TokenValidator,
	fullMethod string,
	logger *slog.Logger,
) (*middleware.AuthClaims, error) {
	token, err := extractBearerTokenFromGRPCMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if validator == nil {
		validator = middleware.NewFailClosedTokenValidator()
	}

	claims, valErr := validator.ValidateToken(ctx, token)
	if valErr != nil || claims == nil || strings.TrimSpace(claims.TenantID) == "" {
		return nil, status.Error(codes.Unauthenticated, "invalid or expired Bearer token")
	}

	if err := checkMetadataTenantAndScope(ctx, claims, fullMethod, logger); err != nil {
		return nil, err
	}
	return claims, nil
}

func extractBearerTokenFromGRPCMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	authHeader := strings.TrimSpace(authHeaders[0])
	if authHeader == "" {
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", status.Error(codes.Unauthenticated, "invalid authorization Bearer metadata format")
	}
	return strings.TrimSpace(parts[1]), nil
}

func checkMetadataTenantAndScope(
	ctx context.Context,
	claims *middleware.AuthClaims,
	fullMethod string,
	logger *slog.Logger,
) error {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, rawTenant := range md.Get("x-tenant-id") {
			if headerTenant := strings.TrimSpace(rawTenant); headerTenant != "" && headerTenant != claims.TenantID {
				if logger != nil {
					logger.Warn("gRPC tenant metadata mismatch",
						slog.String("method", fullMethod),
						slog.String("tenant_id", claims.TenantID),
					)
				}
				return status.Error(codes.PermissionDenied, "x-tenant-id metadata does not match authenticated tenant claims")
			}
		}
	}

	requiredScope, ok := methodRequiredScopes[fullMethod]
	if !ok {
		return status.Error(codes.PermissionDenied, "method is not authorized")
	}
	if !claims.HasScope(requiredScope) {
		if logger != nil {
			logger.Warn("gRPC missing required scope",
				slog.String("method", fullMethod),
				slog.String("tenant_id", claims.TenantID),
				slog.String("required_scope", requiredScope),
			)
		}
		return status.Error(codes.PermissionDenied, fmt.Sprintf("token missing required scope %s", requiredScope))
	}
	return nil
}

func enforceRequestTenant(
	claims *middleware.AuthClaims,
	reqTenantID string,
	logger *slog.Logger,
	fullMethod string,
) error {
	trimmed := strings.TrimSpace(reqTenantID)
	if trimmed == "" {
		return status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if trimmed != claims.TenantID {
		if logger != nil {
			logger.Warn("gRPC request tenant mismatch",
				slog.String("method", fullMethod),
				slog.String("tenant_id", claims.TenantID),
			)
		}
		return status.Error(codes.PermissionDenied, "tenant_id in request does not match authenticated tenant claims")
	}
	return nil
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
