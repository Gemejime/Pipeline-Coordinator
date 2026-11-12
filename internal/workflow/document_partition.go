package workflow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
)

const (
	// DefaultPipelineConfigID is the fallback pipeline configuration identifier when none is specified.
	DefaultPipelineConfigID = "default-multimodal-v1"
	// DefaultChunkSizeTokens is the fallback token target per chunk when unset.
	DefaultChunkSizeTokens int32 = 300
	// DefaultChunkOverlapTokens is the fallback token overlap between adjacent chunks when unset.
	DefaultChunkOverlapTokens int32 = 50
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

// ResolvePipelineConfig resolves the effective PipelineConfig for a slice from the caller-supplied
// PipelineConfig or PipelineConfigID, applying defaults only for omitted fields.
func ResolvePipelineConfig(
	tenantID string,
	pipelineConfigID string,
	cfg *coordinatorv1.PipelineConfig,
) coordinatorv1.PipelineConfig {
	resolved := coordinatorv1.PipelineConfig{
		ID:                 DefaultPipelineConfigID,
		TenantID:           tenantID,
		ChunkSizeTokens:    DefaultChunkSizeTokens,
		ChunkOverlapTokens: DefaultChunkOverlapTokens,
		OCREnabled:         true,
	}

	if trimmedID := strings.TrimSpace(pipelineConfigID); trimmedID != "" {
		resolved.ID = trimmedID
	}

	if cfg != nil {
		resolved.OCREnabled = cfg.OCREnabled
		if trimmedID := strings.TrimSpace(cfg.ID); trimmedID != "" {
			resolved.ID = trimmedID
		}
		if trimmedTenant := strings.TrimSpace(cfg.TenantID); trimmedTenant != "" {
			resolved.TenantID = trimmedTenant
		}
		if trimmedName := strings.TrimSpace(cfg.Name); trimmedName != "" {
			resolved.Name = trimmedName
		}
		if cfg.ChunkSizeTokens > 0 {
			resolved.ChunkSizeTokens = cfg.ChunkSizeTokens
		}
		if cfg.ChunkOverlapTokens >= 0 && (cfg.ChunkOverlapTokens > 0 || cfg.ChunkSizeTokens > 0) {
			resolved.ChunkOverlapTokens = cfg.ChunkOverlapTokens
		}
	}

	return resolved
}

// DocumentPartitionWorkflow executes the per-slice ingestion pipeline in an isolated
// child workflow execution to prevent Temporal event history exhaustion (ADR-002):
//  1. PresignS3ArtifactActivity on coordinator-task-queue (15-min lease)
//  2. DispatchDocAIExtractActivity on doc-ai-task-queue (15s heartbeat timeout), re-presigning on retry
//  3. CompileChunksActivity on chunk-lifecycle-queue (10s heartbeat timeout)
func DocumentPartitionWorkflow(ctx workflow.Context, slice PartitionSlice) (*PartitionResult, error) {
	info := workflow.GetInfo(ctx)
	logger := workflow.GetLogger(ctx)
	pipelineConfig := ResolvePipelineConfig(slice.TenantID, slice.PipelineConfigID, slice.PipelineConfig)

	logger.Info("starting child document partition workflow",
		"tenant_id", slice.TenantID,
		"document_id", slice.DocumentID,
		"workflow_id", info.WorkflowExecution.ID,
		"slice_id", slice.SliceID,
		"slice_index", slice.SliceIndex,
		"s3_bucket", slice.S3InputBucket,
		"s3_key", slice.S3OutputParquetKey,
		"pipeline_config_id", pipelineConfig.ID,
	)

	presignCtx := workflow.WithActivityOptions(ctx, CoordinatorActivityOptions())
	extractCtx := workflow.WithActivityOptions(ctx, DocAIActivityOptions())
	computeRetry := DefaultComputeRetryPolicy()
	maxAttempts := computeRetry.MaximumAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var extractResp coordinatorv1.ExtractSegmentResponse
	var resumeCheckpoint *coordinatorv1.HeartbeatDetails
	for attempt := int32(1); attempt <= maxAttempts; attempt++ {
		// Step 1: Issue fresh 15-minute presigned GET/PUT URLs before every extraction attempt
		// so retries never reuse expired S3 URLs.
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
				"attempt", attempt,
				"error", err,
			)
			return nil, fmt.Errorf("presign slice %s failed: %w", slice.SliceID, err)
		}

		// Step 2: Dispatch extraction to Python Document AI workers on doc-ai-task-queue.
		// Carry forward any recorded heartbeat checkpoint from a previous timed-out/crashed attempt.
		// Note: Raw presigned URLs in presignResp are never logged.
		err = workflow.ExecuteActivity(
			extractCtx,
			activity.DispatchDocAIExtractActivity,
			coordinatorv1.ExtractSegmentRequest{
				Slice:                 slice,
				PresignedInputGetURL:  presignResp.PresignedInputGetURL,
				PresignedOutputPutURL: presignResp.PresignedOutputPutURL,
				PipelineConfig:        pipelineConfig,
				ResumeCheckpoint:      resumeCheckpoint,
			},
		).Get(extractCtx, &extractResp)
		if err == nil {
			break
		}

		if extractedHB := extractHeartbeatCheckpoint(err); extractedHB != nil {
			if resumeCheckpoint == nil || extractedHB.LastProcessedUnit > resumeCheckpoint.LastProcessedUnit {
				resumeCheckpoint = extractedHB
			}
		}

		if attempt >= maxAttempts || isNonRetryableExtractError(ctx, err, computeRetry.NonRetryableErrorTypes) {
			logger.Error("failed DocAI segment extraction",
				"tenant_id", slice.TenantID,
				"document_id", slice.DocumentID,
				"workflow_id", info.WorkflowExecution.ID,
				"slice_id", slice.SliceID,
				"s3_bucket", slice.S3InputBucket,
				"s3_key", slice.S3OutputParquetKey,
				"attempt", attempt,
				"error", err,
			)
			return nil, fmt.Errorf("extract slice %s failed: %w", slice.SliceID, err)
		}

		backoff := computeRetryBackoff(attempt, computeRetry)
		logger.Warn("retrying DocAI segment extraction with fresh presigned URLs",
			"tenant_id", slice.TenantID,
			"document_id", slice.DocumentID,
			"workflow_id", info.WorkflowExecution.ID,
			"slice_id", slice.SliceID,
			"s3_bucket", slice.S3InputBucket,
			"s3_key", slice.S3OutputParquetKey,
			"attempt", attempt,
			"backoff_ms", backoff.Milliseconds(),
			"error", err,
		)
		if sleepErr := workflow.Sleep(ctx, backoff); sleepErr != nil {
			return nil, fmt.Errorf("extract slice %s canceled during retry backoff: %w", slice.SliceID, sleepErr)
		}
	}

	// Step 3: Compile parent-child chunks on chunk-lifecycle-queue.
	compileCtx := workflow.WithActivityOptions(ctx, ChunkLifecycleActivityOptions())
	var compileResp coordinatorv1.CompileChunksResponse
	err := workflow.ExecuteActivity(
		compileCtx,
		activity.CompileChunksActivity,
		coordinatorv1.CompileChunksRequest{
			Slice:              slice,
			S3OutputBucket:     extractResp.S3OutputBucket,
			S3OutputParquetKey: extractResp.S3OutputParquetKey,
			PipelineConfig:     pipelineConfig,
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

// isNonRetryableExtractError determines whether an extraction error should abort workflow-level retries immediately.
func isNonRetryableExtractError(ctx workflow.Context, err error, nonRetryableTypes []string) bool {
	if temporal.IsCanceledError(err) || errors.Is(ctx.Err(), workflow.ErrCanceled) {
		return true
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		if appErr.NonRetryable() {
			return true
		}
		errType := appErr.Type()
		for _, nonRetryable := range nonRetryableTypes {
			if errType == nonRetryable {
				return true
			}
		}
	}
	errMsg := err.Error()
	for _, nonRetryable := range nonRetryableTypes {
		if strings.Contains(errMsg, nonRetryable) {
			return true
		}
	}
	return false
}

// computeRetryBackoff calculates the bounded exponential backoff interval for the given 1-based attempt number.
func computeRetryBackoff(attempt int32, policy *temporal.RetryPolicy) time.Duration {
	if policy == nil || policy.InitialInterval <= 0 {
		return 2 * time.Second
	}
	backoff := float64(policy.InitialInterval)
	coeff := policy.BackoffCoefficient
	if coeff < 1.0 {
		coeff = 2.0
	}
	for i := int32(1); i < attempt; i++ {
		backoff *= coeff
		if policy.MaximumInterval > 0 && time.Duration(backoff) >= policy.MaximumInterval {
			return policy.MaximumInterval
		}
	}
	d := time.Duration(backoff)
	if policy.MaximumInterval > 0 && d > policy.MaximumInterval {
		return policy.MaximumInterval
	}
	return d
}

// extractHeartbeatCheckpoint extracts the latest HeartbeatDetails carried on a failed or timed-out
// Temporal activity error so workflow-level re-presign retries can resume without duplicate work.
func extractHeartbeatCheckpoint(err error) *coordinatorv1.HeartbeatDetails {
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) && timeoutErr.HasLastHeartbeatDetails() {
		var hb coordinatorv1.HeartbeatDetails
		if decodeErr := timeoutErr.LastHeartbeatDetails(&hb); decodeErr == nil && hb.LastProcessedUnit > 0 {
			return &hb
		}
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.HasDetails() {
		var hb coordinatorv1.HeartbeatDetails
		if decodeErr := appErr.Details(&hb); decodeErr == nil && hb.LastProcessedUnit > 0 {
			return &hb
		}
	}
	return nil
}
