// Package coordinatorv1 provides the Go domain and RPC message contracts
// corresponding to api/proto/v1/coordinator.proto and api/proto/v1/types.proto.
package coordinatorv1

import (
	"context"
	"encoding/json"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	if encoding.GetCodec("json") == nil {
		encoding.RegisterCodec(JSONCodec{})
	}
}

// JSONCodec implements google.golang.org/grpc/encoding.Codec to serialize
// and deserialize contract types without requiring external protoc code generation.
type JSONCodec struct{}

// Marshal serializes v to JSON bytes.
func (JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal parses JSON data into v.
func (JSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Name returns the registered codec content-subtype name "json".
func (JSONCodec) Name() string {
	return "json"
}

// CoordinatorServiceServer is the server API for CoordinatorService.
type CoordinatorServiceServer interface {
	CreateUploadURL(context.Context, *CreateUploadURLRequest) (*CreateUploadURLResponse, error)
	CompleteUploadURL(context.Context, *CompleteMultipartUploadRequest) (*CompleteMultipartUploadResponse, error)
	StartIngestion(context.Context, *StartIngestionRequest) (*StartIngestionResponse, error)
	GetIngestionStatus(context.Context, *GetIngestionStatusRequest) (*GetIngestionStatusResponse, error)
}

// CoordinatorServiceClient is the client API for CoordinatorService.
type CoordinatorServiceClient interface {
	CreateUploadURL(ctx context.Context, in *CreateUploadURLRequest, opts ...grpc.CallOption) (*CreateUploadURLResponse, error)
	CompleteUploadURL(ctx context.Context, in *CompleteMultipartUploadRequest, opts ...grpc.CallOption) (*CompleteMultipartUploadResponse, error)
	StartIngestion(ctx context.Context, in *StartIngestionRequest, opts ...grpc.CallOption) (*StartIngestionResponse, error)
	GetIngestionStatus(ctx context.Context, in *GetIngestionStatusRequest, opts ...grpc.CallOption) (*GetIngestionStatusResponse, error)
}

type coordinatorServiceClient struct {
	cc grpc.ClientConnInterface
}

// NewCoordinatorServiceClient constructs a CoordinatorServiceClient over the provided gRPC connection.
func NewCoordinatorServiceClient(cc grpc.ClientConnInterface) CoordinatorServiceClient {
	return &coordinatorServiceClient{cc: cc}
}

func (c *coordinatorServiceClient) CreateUploadURL(ctx context.Context, in *CreateUploadURLRequest, opts ...grpc.CallOption) (*CreateUploadURLResponse, error) {
	out := new(CreateUploadURLResponse)
	callOpts := append([]grpc.CallOption{grpc.CallContentSubtype("json")}, opts...)
	if err := c.cc.Invoke(ctx, "/rag.coordinator.v1.CoordinatorService/CreateUploadURL", in, out, callOpts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *coordinatorServiceClient) CompleteUploadURL(ctx context.Context, in *CompleteMultipartUploadRequest, opts ...grpc.CallOption) (*CompleteMultipartUploadResponse, error) {
	out := new(CompleteMultipartUploadResponse)
	callOpts := append([]grpc.CallOption{grpc.CallContentSubtype("json")}, opts...)
	if err := c.cc.Invoke(ctx, "/rag.coordinator.v1.CoordinatorService/CompleteUploadURL", in, out, callOpts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *coordinatorServiceClient) StartIngestion(ctx context.Context, in *StartIngestionRequest, opts ...grpc.CallOption) (*StartIngestionResponse, error) {
	out := new(StartIngestionResponse)
	callOpts := append([]grpc.CallOption{grpc.CallContentSubtype("json")}, opts...)
	if err := c.cc.Invoke(ctx, "/rag.coordinator.v1.CoordinatorService/StartIngestion", in, out, callOpts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *coordinatorServiceClient) GetIngestionStatus(ctx context.Context, in *GetIngestionStatusRequest, opts ...grpc.CallOption) (*GetIngestionStatusResponse, error) {
	out := new(GetIngestionStatusResponse)
	callOpts := append([]grpc.CallOption{grpc.CallContentSubtype("json")}, opts...)
	if err := c.cc.Invoke(ctx, "/rag.coordinator.v1.CoordinatorService/GetIngestionStatus", in, out, callOpts...); err != nil {
		return nil, err
	}
	return out, nil
}

// CoordinatorService_ServiceDesc is the grpc.ServiceDesc for CoordinatorService.
var CoordinatorService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rag.coordinator.v1.CoordinatorService",
	HandlerType: (*CoordinatorServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateUploadURL",
			Handler:    _CoordinatorService_CreateUploadURL_Handler,
		},
		{
			MethodName: "CompleteUploadURL",
			Handler:    _CoordinatorService_CompleteUploadURL_Handler,
		},
		{
			MethodName: "StartIngestion",
			Handler:    _CoordinatorService_StartIngestion_Handler,
		},
		{
			MethodName: "GetIngestionStatus",
			Handler:    _CoordinatorService_GetIngestionStatus_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/proto/v1/coordinator.proto",
}

// RegisterCoordinatorServiceServer registers a CoordinatorServiceServer implementation with the grpc.ServiceRegistrar.
func RegisterCoordinatorServiceServer(s grpc.ServiceRegistrar, srv CoordinatorServiceServer) {
	s.RegisterService(&CoordinatorService_ServiceDesc, srv)
}

func _CoordinatorService_CreateUploadURL_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(CreateUploadURLRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceServer).CreateUploadURL(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rag.coordinator.v1.CoordinatorService/CreateUploadURL",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(CoordinatorServiceServer).CreateUploadURL(ctx, req.(*CreateUploadURLRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CoordinatorService_CompleteUploadURL_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(CompleteMultipartUploadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceServer).CompleteUploadURL(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rag.coordinator.v1.CoordinatorService/CompleteUploadURL",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(CoordinatorServiceServer).CompleteUploadURL(ctx, req.(*CompleteMultipartUploadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CoordinatorService_StartIngestion_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(StartIngestionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceServer).StartIngestion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rag.coordinator.v1.CoordinatorService/StartIngestion",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(CoordinatorServiceServer).StartIngestion(ctx, req.(*StartIngestionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CoordinatorService_GetIngestionStatus_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(GetIngestionStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceServer).GetIngestionStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rag.coordinator.v1.CoordinatorService/GetIngestionStatus",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(CoordinatorServiceServer).GetIngestionStatus(ctx, req.(*GetIngestionStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// MultimodalModality enumerates supported ingestion payload modalities.
type MultimodalModality int32

const (
	MultimodalModalityUnspecified MultimodalModality = 0
	MultimodalModalityDocumentPDF MultimodalModality = 1
	MultimodalModalityAudio       MultimodalModality = 2
	MultimodalModalityVideo       MultimodalModality = 3
	MultimodalModalityTabular     MultimodalModality = 4
)

// String returns the canonical Protobuf enum name for MultimodalModality.
func (m MultimodalModality) String() string {
	switch m {
	case MultimodalModalityDocumentPDF:
		return "MULTIMODAL_MODALITY_DOCUMENT_PDF"
	case MultimodalModalityAudio:
		return "MULTIMODAL_MODALITY_AUDIO"
	case MultimodalModalityVideo:
		return "MULTIMODAL_MODALITY_VIDEO"
	case MultimodalModalityTabular:
		return "MULTIMODAL_MODALITY_TABULAR"
	default:
		return "MULTIMODAL_MODALITY_UNSPECIFIED"
	}
}

// CreateUploadURLRequest represents a request to issue a presigned S3 upload lease.
type CreateUploadURLRequest struct {
	TenantID           string `json:"tenant_id"`
	Filename           string `json:"filename"`
	ContentType        string `json:"content_type"`
	ContentLengthBytes int64  `json:"content_length_bytes"`
	ContentHash        string `json:"content_hash"`
}

// PresignedPart represents a single presigned part URL in an S3 multipart upload.
type PresignedPart struct {
	PartNumber   int32  `json:"part_number"`
	PresignedURL string `json:"presigned_url"`
}

// LogValue implements slog.LogValuer to prevent accidental logging of presigned URLs.
func (p PresignedPart) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("part_number", int(p.PartNumber)),
		slog.String("presigned_url", "[REDACTED]"),
	)
}

// CreateUploadURLResponse contains either a single-part presigned URL or multipart presigned URLs.
type CreateUploadURLResponse struct {
	UploadID      string          `json:"upload_id"`
	TenantID      string          `json:"tenant_id"`
	S3Bucket      string          `json:"s3_bucket"`
	S3Key         string          `json:"s3_key"`
	PresignedURL  string          `json:"presigned_url,omitempty"`
	ExpiresAtUnix int64           `json:"expires_at_unix"`
	LeaseSeconds  int32           `json:"lease_seconds"`
	PartSizeBytes int64           `json:"part_size_bytes,omitempty"`
	Parts         []PresignedPart `json:"parts,omitempty"`
}

// LogValue implements slog.LogValuer to guarantee presigned query signatures are redacted in logs.
func (r CreateUploadURLResponse) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("upload_id", r.UploadID),
		slog.String("tenant_id", r.TenantID),
		slog.String("s3_bucket", r.S3Bucket),
		slog.String("s3_key", r.S3Key),
		slog.String("presigned_url", "[REDACTED]"),
		slog.Int64("expires_at_unix", r.ExpiresAtUnix),
		slog.Int("lease_seconds", int(r.LeaseSeconds)),
		slog.Int64("part_size_bytes", r.PartSizeBytes),
		slog.Int("parts_count", len(r.Parts)),
	)
}

// CompletedPart represents an uploaded multipart segment and its S3 ETag.
type CompletedPart struct {
	PartNumber int32  `json:"part_number"`
	ETag       string `json:"etag"`
}

// CompleteMultipartUploadRequest finalizes an S3 multipart upload.
type CompleteMultipartUploadRequest struct {
	UploadID string          `json:"upload_id"`
	TenantID string          `json:"tenant_id"`
	S3Bucket string          `json:"s3_bucket"`
	S3Key    string          `json:"s3_key"`
	Parts    []CompletedPart `json:"parts"`
}

// CompleteMultipartUploadResponse reports the finalized S3 multipart object location.
type CompleteMultipartUploadResponse struct {
	UploadID    string `json:"upload_id"`
	Status      string `json:"status"`
	S3Location  string `json:"s3_location"`
	CompletedAt string `json:"completed_at"`
}

// StartIngestionRequest triggers the root IngestionCoordinatorWorkflow.
type StartIngestionRequest struct {
	TenantID         string `json:"tenant_id"`
	DocumentID       string `json:"document_id"`
	S3Bucket         string `json:"s3_bucket"`
	S3Key            string `json:"s3_key"`
	ContentHash      string `json:"content_hash"`
	TotalBytes       int64  `json:"total_bytes"`
	MimeType         string `json:"mime_type"`
	PipelineConfigID string `json:"pipeline_config_id"`
}

// StartIngestionResponse returns the scheduled Temporal workflow metadata.
type StartIngestionResponse struct {
	WorkflowID    string `json:"workflow_id"`
	RunID         string `json:"run_id"`
	Status        string `json:"status"`
	StartedAtUnix int64  `json:"started_at_unix"`
}

// GetIngestionStatusRequest queries the status of an ingestion workflow run.
type GetIngestionStatusRequest struct {
	WorkflowID string `json:"workflow_id"`
	TenantID   string `json:"tenant_id"`
}

// GetIngestionStatusResponse returns in-flight or terminal workflow execution metrics.
type GetIngestionStatusResponse struct {
	WorkflowID      string  `json:"workflow_id"`
	RunID           string  `json:"run_id"`
	TenantID        string  `json:"tenant_id"`
	Status          string  `json:"status"`
	TotalSlices     int32   `json:"total_slices"`
	CompletedSlices int32   `json:"completed_slices"`
	FailedSlices    int32   `json:"failed_slices"`
	TotalPages      int32   `json:"total_pages"`
	DurationSeconds float64 `json:"duration_seconds"`
	CreatedAt       string  `json:"created_at"`
	CompletedAt     string  `json:"completed_at"`
}

// PartitionSlice defines a deterministic document or media partition assigned to a child workflow.
type PartitionSlice struct {
	SliceID            string             `json:"slice_id"`
	SliceIndex         int32              `json:"slice_index"`
	TotalSlices        int32              `json:"total_slices"`
	TenantID           string             `json:"tenant_id"`
	DocumentID         string             `json:"document_id"`
	ContentHash        string             `json:"content_hash"`
	S3InputBucket      string             `json:"s3_input_bucket"`
	S3InputKey         string             `json:"s3_input_key"`
	S3OutputParquetKey string             `json:"s3_output_parquet_key"`
	Modality           MultimodalModality `json:"modality"`
	StartPage          int32              `json:"start_page"`
	EndPage            int32              `json:"end_page"`
	StartTimeSeconds   float64            `json:"start_time_seconds"`
	EndTimeSeconds     float64            `json:"end_time_seconds"`
	OverlapSeconds     float64            `json:"overlap_seconds"`
	PipelineConfigID   string             `json:"pipeline_config_id,omitempty"`
	PipelineConfig     *PipelineConfig    `json:"pipeline_config,omitempty"`
}

// HeartbeatDetails represents the native Temporal activity heartbeat checkpoint payload (ADR-003).
type HeartbeatDetails struct {
	SliceID           string `json:"slice_id"`
	SliceIndex        int32  `json:"slice_index"`
	LastProcessedUnit int32  `json:"last_processed_unit"`
	ProcessedUnits    int32  `json:"processed_units"`
	TotalUnits        int32  `json:"total_units"`
	IntermediateS3Key string `json:"intermediate_s3_key"`
	UpdatedAtUnix     int64  `json:"updated_at_unix"`
}

// PipelineConfig defines tenant-scoped chunking and extraction parameters.
type PipelineConfig struct {
	ID                 string `json:"id"`
	TenantID           string `json:"tenant_id"`
	Name               string `json:"name"`
	ChunkSizeTokens    int32  `json:"chunk_size_tokens"`
	ChunkOverlapTokens int32  `json:"chunk_overlap_tokens"`
	OCREnabled         bool   `json:"ocr_enabled"`
}

// ProgressMilestoneEvent is the real-time telemetry frame broadcast over Redis Pub/Sub.
type ProgressMilestoneEvent struct {
	EventID         string  `json:"event_id"`
	SequenceNumber  int64   `json:"sequence_number"`
	TenantID        string  `json:"tenant_id"`
	DocumentID      string  `json:"document_id"`
	WorkflowID      string  `json:"workflow_id"`
	Stage           string  `json:"stage"`
	Status          string  `json:"status"`
	ProgressPct     float64 `json:"progress_pct"`
	SlicesCompleted int32   `json:"slices_completed"`
	TotalSlices     int32   `json:"total_slices"`
	TimestampUnix   int64   `json:"timestamp_unix"`
}

// InspectDocumentRequest is the input contract for InspectDocumentHeaderActivity on inspection-queue.
type InspectDocumentRequest struct {
	TenantID    string `json:"tenant_id"`
	DocumentID  string `json:"document_id"`
	S3Bucket    string `json:"s3_bucket"`
	S3Key       string `json:"s3_key"`
	ContentHash string `json:"content_hash"`
	TotalBytes  int64  `json:"total_bytes"`
	MimeType    string `json:"mime_type"`
}

// InspectDocumentResponse is the output contract for InspectDocumentHeaderActivity.
type InspectDocumentResponse struct {
	Modality                 MultimodalModality `json:"modality"`
	TotalPages               int32              `json:"total_pages"`
	DurationSeconds          float64            `json:"duration_seconds"`
	RecommendedSlicePages    int32              `json:"recommended_slice_pages"`
	RecommendedWindowSeconds float64            `json:"recommended_window_seconds"`
}

// PresignSliceArtifactRequest requests short-lived GET/PUT URLs for a partition slice.
type PresignSliceArtifactRequest struct {
	Slice        PartitionSlice `json:"slice"`
	LeaseSeconds int32          `json:"lease_seconds"`
}

// PresignSliceArtifactResponse contains the presigned URLs for slice extraction.
type PresignSliceArtifactResponse struct {
	S3Bucket              string `json:"s3_bucket"`
	S3InputKey            string `json:"s3_input_key"`
	S3OutputParquetKey    string `json:"s3_output_parquet_key"`
	PresignedInputGetURL  string `json:"presigned_input_get_url"`
	PresignedOutputPutURL string `json:"presigned_output_put_url"`
	ExpiresAtUnix         int64  `json:"expires_at_unix"`
}

// LogValue implements slog.LogValuer to redact presigned URLs from structured logs.
func (r PresignSliceArtifactResponse) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("s3_bucket", r.S3Bucket),
		slog.String("s3_input_key", r.S3InputKey),
		slog.String("s3_output_parquet_key", r.S3OutputParquetKey),
		slog.String("presigned_input_get_url", "[REDACTED]"),
		slog.String("presigned_output_put_url", "[REDACTED]"),
		slog.Int64("expires_at_unix", r.ExpiresAtUnix),
	)
}

// ExtractSegmentRequest is the polyglot activity input on doc-ai-task-queue.
type ExtractSegmentRequest struct {
	Slice                 PartitionSlice    `json:"slice"`
	PresignedInputGetURL  string            `json:"presigned_input_get_url"`
	PresignedOutputPutURL string            `json:"presigned_output_put_url"`
	PipelineConfig        PipelineConfig    `json:"pipeline_config"`
	ResumeCheckpoint      *HeartbeatDetails `json:"resume_checkpoint,omitempty"`
}

// LogValue implements slog.LogValuer to redact presigned URLs from structured logs.
func (r ExtractSegmentRequest) LogValue() slog.Value {
	var resumeUnit int32
	if r.ResumeCheckpoint != nil {
		resumeUnit = r.ResumeCheckpoint.LastProcessedUnit
	}
	return slog.GroupValue(
		slog.String("slice_id", r.Slice.SliceID),
		slog.Int("slice_index", int(r.Slice.SliceIndex)),
		slog.String("s3_input_bucket", r.Slice.S3InputBucket),
		slog.String("s3_input_key", r.Slice.S3InputKey),
		slog.String("s3_output_parquet_key", r.Slice.S3OutputParquetKey),
		slog.Int("resume_from_unit", int(resumeUnit)),
		slog.String("presigned_input_get_url", "[REDACTED]"),
		slog.String("presigned_output_put_url", "[REDACTED]"),
	)
}

// ExtractSegmentResponse is the polyglot activity output from doc-ai-task-queue.
type ExtractSegmentResponse struct {
	SliceID            string `json:"slice_id"`
	SliceIndex         int32  `json:"slice_index"`
	S3OutputBucket     string `json:"s3_output_bucket"`
	S3OutputParquetKey string `json:"s3_output_parquet_key"`
	ExtractedPages     int32  `json:"extracted_pages"`
	ExtractedSegments  int32  `json:"extracted_segments"`
	ParquetBytes       int64  `json:"parquet_bytes"`
}

// CompileChunksRequest is the polyglot activity input on chunk-lifecycle-queue.
type CompileChunksRequest struct {
	Slice              PartitionSlice `json:"slice"`
	S3OutputBucket     string         `json:"s3_output_bucket"`
	S3OutputParquetKey string         `json:"s3_output_parquet_key"`
	PipelineConfig     PipelineConfig `json:"pipeline_config"`
}

// CompileChunksResponse is the polyglot activity output from chunk-lifecycle-queue.
type CompileChunksResponse struct {
	SliceID        string `json:"slice_id"`
	SliceIndex     int32  `json:"slice_index"`
	CompiledChunks int32  `json:"compiled_chunks"`
	TotalTokens    int32  `json:"total_tokens"`
}

// PublishMilestoneRequest wraps a ProgressMilestoneEvent for PublishMilestoneActivity.
type PublishMilestoneRequest struct {
	Event ProgressMilestoneEvent `json:"event"`
}

// PublishMilestoneResponse reports Redis Pub/Sub publication results.
type PublishMilestoneResponse struct {
	Channel        string `json:"channel"`
	SequenceNumber int64  `json:"sequence_number"`
	Published      bool   `json:"published"`
}

// CatalogTerminalRunRequest persists the single terminal execution record to PostgreSQL (ADR-003).
type CatalogTerminalRunRequest struct {
	WorkflowID      string  `json:"workflow_id"`
	RunID           string  `json:"run_id"`
	TenantID        string  `json:"tenant_id"`
	DocumentID      string  `json:"document_id"`
	ContentHash     string  `json:"content_hash"`
	Status          string  `json:"status"`
	TotalPages      int32   `json:"total_pages"`
	TotalSlices     int32   `json:"total_slices"`
	CompletedSlices int32   `json:"completed_slices"`
	FailedSlices    int32   `json:"failed_slices"`
	DurationSeconds float64 `json:"duration_seconds"`
	ErrorMessage    string  `json:"error_message,omitempty"`
}

// CatalogTerminalRunResponse reports PostgreSQL terminal persistence results.
type CatalogTerminalRunResponse struct {
	RecordID  string `json:"record_id"`
	Persisted bool   `json:"persisted"`
}
