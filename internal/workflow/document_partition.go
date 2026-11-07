package workflow

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
)

// PartitionSlice aliases the shared Protobuf PartitionSlice message.
type PartitionSlice = coordinatorv1.PartitionSlice

// PartitionResult summarizes the completed execution of a single DocumentPartitionWorkflow slice.
type PartitionResult struct {
	SliceID            string `json:"slice_id"`
	SliceIndex         int32  `json:"slice_index"`
	S3OutputBucket     string `json:"s3_output_bucket"`
	S3OutputParquetKey string `json:"s3_output_parquet_key"`
	ExtractedPages     int32  `json:"extracted_pages"`
	ExtractedSegments  int32  `json:"extracted_segments"`
	ParquetBytes       int64  `json:"parquet_bytes"`
	CompiledChunks     int32  `json:"compiled_chunks"`
	TotalTokens        int32  `json:"total_tokens"`
}

// DocumentPartitionWorkflow executes the per-slice ingestion pipeline in an isolated
// child workflow execution to prevent Temporal event history exhaustion (ADR-002):
//  1. PresignS3ArtifactActivity on coordinator-task-queue (15-min lease)
//  2. DispatchDocAIExtractActivity on doc-ai-task-queue (15s heartbeat timeout)
//  3. CompileChunksActivity on chunk-lifecycle-queue (10s heartbeat timeout)
func DocumentPartitionWorkflow(ctx workflow.Context, slice PartitionSlice) (*PartitionResult, error) {
	info := workflow.GetInfo(ctx)
	logger := workflow.GetLogger(ctx)

	logger.Info("starting child document partition workflow",
		"tenant_id", slice.TenantID,
		"document_id", slice.DocumentID,
		"workflow_id", info.WorkflowExecution.ID,
		"slice_id", slice.SliceID,
		"slice_index", slice.SliceIndex,
		"s3_bucket", slice.S3InputBucket,
		"s3_key", slice.S3OutputParquetKey,
	)

	// Step 1: Issue 15-minute presigned GET/PUT URLs on coordinator-task-queue.
	presignCtx := workflow.WithActivityOptions(ctx, CoordinatorActivityOptions())
	var presignResp coordinatorv1.PresignSliceArtifactResponse
	err := workflow.ExecuteActivity(
		presignCtx,
		activity.PresignS3ArtifactActivity,
		coordinatorv1.PresignSliceArtifactRequest{
			Slice:        slice,
			LeaseSeconds: activity.DefaultPresignLeaseSeconds,
		},
	).Get(presignCtx, &presignResp)
	if err != nil {
		logger.Error("failed to presign S3 slice artifacts",
			"tenant_id", slice.TenantID,
			"document_id", slice.DocumentID,
			"workflow_id", info.WorkflowExecution.ID,
			"slice_id", slice.SliceID,
			"s3_bucket", slice.S3InputBucket,
			"s3_key", slice.S3OutputParquetKey,
			"error", err,
		)
		return nil, fmt.Errorf("presign slice %s failed: %w", slice.SliceID, err)
	}

	// Step 2: Dispatch extraction to Python Document AI workers on doc-ai-task-queue.
	// Note: Raw presigned URLs in presignResp are never logged.
	extractCtx := workflow.WithActivityOptions(ctx, DocAIActivityOptions())
	var extractResp coordinatorv1.ExtractSegmentResponse
	err = workflow.ExecuteActivity(
		extractCtx,
		activity.DispatchDocAIExtractActivity,
		coordinatorv1.ExtractSegmentRequest{
			Slice:                 slice,
			PresignedInputGetURL:  presignResp.PresignedInputGetURL,
			PresignedOutputPutURL: presignResp.PresignedOutputPutURL,
			PipelineConfig: coordinatorv1.PipelineConfig{
				ID:                 "default-multimodal-v1",
				TenantID:           slice.TenantID,
				ChunkSizeTokens:    300,
				ChunkOverlapTokens: 50,
				OCREnabled:         true,
			},
		},
	).Get(extractCtx, &extractResp)
	if err != nil {
		logger.Error("failed DocAI segment extraction",
			"tenant_id", slice.TenantID,
			"document_id", slice.DocumentID,
			"workflow_id", info.WorkflowExecution.ID,
			"slice_id", slice.SliceID,
			"s3_bucket", slice.S3InputBucket,
			"s3_key", slice.S3OutputParquetKey,
			"error", err,
		)
		return nil, fmt.Errorf("extract slice %s failed: %w", slice.SliceID, err)
	}

	// Step 3: Compile parent-child chunks on chunk-lifecycle-queue.
	compileCtx := workflow.WithActivityOptions(ctx, ChunkLifecycleActivityOptions())
	var compileResp coordinatorv1.CompileChunksResponse
	err = workflow.ExecuteActivity(
		compileCtx,
		activity.CompileChunksActivity,
		coordinatorv1.CompileChunksRequest{
			Slice:              slice,
			S3OutputBucket:     extractResp.S3OutputBucket,
			S3OutputParquetKey: extractResp.S3OutputParquetKey,
			PipelineConfig: coordinatorv1.PipelineConfig{
				ID:                 "default-multimodal-v1",
				TenantID:           slice.TenantID,
				ChunkSizeTokens:    300,
				ChunkOverlapTokens: 50,
				OCREnabled:         true,
			},
		},
	).Get(compileCtx, &compileResp)
	if err != nil {
		logger.Error("failed chunk compilation",
			"tenant_id", slice.TenantID,
			"document_id", slice.DocumentID,
			"workflow_id", info.WorkflowExecution.ID,
			"slice_id", slice.SliceID,
			"s3_bucket", extractResp.S3OutputBucket,
			"s3_key", extractResp.S3OutputParquetKey,
			"error", err,
		)
		return nil, fmt.Errorf("compile chunks for slice %s failed: %w", slice.SliceID, err)
	}

	logger.Info("completed child document partition workflow",
		"tenant_id", slice.TenantID,
		"document_id", slice.DocumentID,
		"workflow_id", info.WorkflowExecution.ID,
		"slice_id", slice.SliceID,
		"s3_bucket", extractResp.S3OutputBucket,
		"s3_key", extractResp.S3OutputParquetKey,
		"compiled_chunks", compileResp.CompiledChunks,
		"total_tokens", compileResp.TotalTokens,
	)

	return &PartitionResult{
		SliceID:            slice.SliceID,
		SliceIndex:         slice.SliceIndex,
		S3OutputBucket:     extractResp.S3OutputBucket,
		S3OutputParquetKey: extractResp.S3OutputParquetKey,
		ExtractedPages:     extractResp.ExtractedPages,
		ExtractedSegments:  extractResp.ExtractedSegments,
		ParquetBytes:       extractResp.ParquetBytes,
		CompiledChunks:     compileResp.CompiledChunks,
		TotalTokens:        compileResp.TotalTokens,
	}, nil
}
