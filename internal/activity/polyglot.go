package activity

import (
	"context"
	"math"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

// UnitProcessorHook is an optional callback invoked for each processed unit (page or media segment)
// before recording a Temporal activity heartbeat checkpoint.
type UnitProcessorHook func(ctx context.Context, unit int32) error

// PolyglotWorkerActivities provides reference implementations and test harnesses for
// cross-language activities polled on doc-ai-task-queue and chunk-lifecycle-queue,
// demonstrating native Temporal HeartbeatDetails checkpoint recovery (ADR-003).
type PolyglotWorkerActivities struct {
	OnExtractUnit UnitProcessorHook
	OnCompileUnit UnitProcessorHook
}

// NewPolyglotWorkerActivities creates a PolyglotWorkerActivities instance.
func NewPolyglotWorkerActivities(onExtract, onCompile UnitProcessorHook) *PolyglotWorkerActivities {
	return &PolyglotWorkerActivities{
		OnExtractUnit: onExtract,
		OnCompileUnit: onCompile,
	}
}

// DispatchDocAIExtractActivity is the activity contract scheduled on doc-ai-task-queue
// (15s heartbeat timeout). In production, Python Document AI workers (Docling/OCR/ASR)
// poll this activity name directly via the Temporal Python SDK; this handler also provides
// deterministic HeartbeatDetails checkpoint resumption for simulation and verification.
func DispatchDocAIExtractActivity(
	ctx context.Context,
	req coordinatorv1.ExtractSegmentRequest,
) (*coordinatorv1.ExtractSegmentResponse, error) {
	a := &PolyglotWorkerActivities{}
	return a.DispatchDocAIExtract(ctx, req)
}

// DispatchDocAIExtract executes slice extraction with HeartbeatDetails checkpoint recovery (ADR-003).
// If an activity worker crashes midway through a slice, Temporal passes the last recorded
// HeartbeatDetails on retry, allowing the worker to resume from LastProcessedUnit + 1
// without repeating already-processed pages or sub-segments.
func (a *PolyglotWorkerActivities) DispatchDocAIExtract(
	ctx context.Context,
	req coordinatorv1.ExtractSegmentRequest,
) (*coordinatorv1.ExtractSegmentResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	slice := req.Slice
	if strings.TrimSpace(slice.SliceID) == "" || strings.TrimSpace(slice.TenantID) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"slice_id and tenant_id are required for extraction",
			ErrCodeInvalidArgument,
			nil,
		)
	}
	if strings.TrimSpace(req.PresignedInputGetURL) == "" || strings.TrimSpace(req.PresignedOutputPutURL) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"presigned input and output URLs are required",
			ErrCodeInvalidArgument,
			nil,
		)
	}

	totalUnits := computeSliceTotalUnits(slice)
	var checkpoint coordinatorv1.HeartbeatDetails
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &checkpoint)
	}

	startUnit := checkpoint.LastProcessedUnit + 1
	if startUnit < 1 {
		startUnit = 1
	}

	logger := activity.GetLogger(ctx)
	logger.Info("starting DocAI segment extraction",
		"tenant_id", slice.TenantID,
		"document_id", slice.DocumentID,
		"slice_id", slice.SliceID,
		"slice_index", slice.SliceIndex,
		"s3_bucket", slice.S3InputBucket,
		"s3_key", slice.S3OutputParquetKey,
		"resumed_from_unit", checkpoint.LastProcessedUnit,
		"total_units", totalUnits,
	)

	for unit := startUnit; unit <= totalUnits; unit++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if a != nil && a.OnExtractUnit != nil {
			if err := a.OnExtractUnit(ctx, unit); err != nil {
				return nil, err
			}
		}

		checkpoint = coordinatorv1.HeartbeatDetails{
			SliceID:           slice.SliceID,
			SliceIndex:        slice.SliceIndex,
			LastProcessedUnit: unit,
			ProcessedUnits:    unit,
			TotalUnits:        totalUnits,
			IntermediateS3Key: slice.S3OutputParquetKey,
			UpdatedAtUnix:     time.Now().UTC().Unix(),
		}
		activity.RecordHeartbeat(ctx, checkpoint)
	}

	extractedPages := int32(0)
	if slice.EndPage >= slice.StartPage && slice.StartPage > 0 {
		extractedPages = slice.EndPage - slice.StartPage + 1
	}

	return &coordinatorv1.ExtractSegmentResponse{
		SliceID:            slice.SliceID,
		SliceIndex:         slice.SliceIndex,
		S3OutputBucket:     slice.S3InputBucket,
		S3OutputParquetKey: slice.S3OutputParquetKey,
		ExtractedPages:     extractedPages,
		ExtractedSegments:  totalUnits,
		ParquetBytes:       int64(totalUnits) * 4096,
	}, nil
}

// CompileChunksActivity is the activity contract scheduled on chunk-lifecycle-queue
// (10s heartbeat timeout), polled by rag-chunk-lifecycle-service to compile parent-child
// chunk trees from content-addressed S3 Parquet slices.
func CompileChunksActivity(
	ctx context.Context,
	req coordinatorv1.CompileChunksRequest,
) (*coordinatorv1.CompileChunksResponse, error) {
	a := &PolyglotWorkerActivities{}
	return a.CompileChunks(ctx, req)
}

// CompileChunks executes parent-child chunk compilation with HeartbeatDetails resumption.
func (a *PolyglotWorkerActivities) CompileChunks(
	ctx context.Context,
	req coordinatorv1.CompileChunksRequest,
) (*coordinatorv1.CompileChunksResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	slice := req.Slice
	if strings.TrimSpace(slice.SliceID) == "" || strings.TrimSpace(req.S3OutputBucket) == "" || strings.TrimSpace(req.S3OutputParquetKey) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"slice_id, s3_output_bucket, and s3_output_parquet_key are required",
			ErrCodeInvalidArgument,
			nil,
		)
	}

	totalUnits := computeSliceTotalUnits(slice)
	var checkpoint coordinatorv1.HeartbeatDetails
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &checkpoint)
	}

	startUnit := checkpoint.LastProcessedUnit + 1
	if startUnit < 1 {
		startUnit = 1
	}

	for unit := startUnit; unit <= totalUnits; unit++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if a != nil && a.OnCompileUnit != nil {
			if err := a.OnCompileUnit(ctx, unit); err != nil {
				return nil, err
			}
		}

		checkpoint = coordinatorv1.HeartbeatDetails{
			SliceID:           slice.SliceID,
			SliceIndex:        slice.SliceIndex,
			LastProcessedUnit: unit,
			ProcessedUnits:    unit,
			TotalUnits:        totalUnits,
			IntermediateS3Key: req.S3OutputParquetKey,
			UpdatedAtUnix:     time.Now().UTC().Unix(),
		}
		activity.RecordHeartbeat(ctx, checkpoint)
	}

	chunkSize := req.PipelineConfig.ChunkSizeTokens
	if chunkSize <= 0 {
		chunkSize = 300
	}
	compiledChunks := totalUnits * 2
	totalTokens := compiledChunks * chunkSize

	logger := activity.GetLogger(ctx)
	logger.Info("compiled chunks from Parquet slice",
		"tenant_id", slice.TenantID,
		"document_id", slice.DocumentID,
		"slice_id", slice.SliceID,
		"slice_index", slice.SliceIndex,
		"s3_bucket", req.S3OutputBucket,
		"s3_key", req.S3OutputParquetKey,
		"compiled_chunks", compiledChunks,
		"total_tokens", totalTokens,
	)

	return &coordinatorv1.CompileChunksResponse{
		SliceID:        slice.SliceID,
		SliceIndex:     slice.SliceIndex,
		CompiledChunks: compiledChunks,
		TotalTokens:    totalTokens,
	}, nil
}

func computeSliceTotalUnits(slice coordinatorv1.PartitionSlice) int32 {
	if slice.EndPage >= slice.StartPage && slice.StartPage > 0 {
		return slice.EndPage - slice.StartPage + 1
	}
	if slice.EndTimeSeconds > slice.StartTimeSeconds {
		units := int32(math.Ceil((slice.EndTimeSeconds - slice.StartTimeSeconds) / 10.0))
		if units < 1 {
			return 1
		}
		return units
	}
	return 1
}
