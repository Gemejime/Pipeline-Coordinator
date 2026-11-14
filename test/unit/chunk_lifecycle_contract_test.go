package unit_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
)

// TestChunkLifecycleContract_DocAIParquetToCompileChunks verifies the downstream
// rag-chunk-lifecycle-service contract: ExtractSegmentResponse + custom PipelineConfig
// handoff into CompileChunksActivity on chunk-lifecycle-queue with HeartbeatDetails recovery.
func TestChunkLifecycleContract_DocAIParquetToCompileChunks(t *testing.T) {
	t.Parallel()

	// Standardized Parquet columns produced by workers/python/rag_document_ai_worker/activity.py (PARQUET_SCHEMA).
	expectedDocAIParquetColumns := []string{
		"slice_id",
		"slice_index",
		"unit_index",
		"content",
		"metadata_json",
		"tokens_estimate",
	}
	assert.Len(t, expectedDocAIParquetColumns, 6)

	customPipelineCfg := coordinatorv1.PipelineConfig{
		ID:                 "custom-chunk-512",
		TenantID:           "tenant-corp-01",
		Name:               "Custom Dense Chunking",
		ChunkSizeTokens:    512,
		ChunkOverlapTokens: 64,
		OCREnabled:         true,
	}

	slice := coordinatorv1.PartitionSlice{
		SliceID:            "slice-contract-01",
		SliceIndex:         0,
		TotalSlices:        1,
		TenantID:           "tenant-corp-01",
		DocumentID:         "doc-contract-01",
		ContentHash:        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		S3InputBucket:      "rag-ingestion-bucket",
		S3InputKey:         "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/doc.pdf",
		S3OutputParquetKey: "artifacts/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/slice_1_10.parquet",
		Modality:           coordinatorv1.MultimodalModalityDocumentPDF,
		StartPage:          1,
		EndPage:            10,
		PipelineConfigID:   customPipelineCfg.ID,
		PipelineConfig:     &customPipelineCfg,
	}

	extractOut := coordinatorv1.ExtractSegmentResponse{
		SliceID:            slice.SliceID,
		SliceIndex:         slice.SliceIndex,
		S3OutputBucket:     slice.S3InputBucket,
		S3OutputParquetKey: slice.S3OutputParquetKey,
		ExtractedPages:     10,
		ExtractedSegments:  10,
		ParquetBytes:       40960,
	}

	compileReq := coordinatorv1.CompileChunksRequest{
		Slice:              slice,
		S3OutputBucket:     extractOut.S3OutputBucket,
		S3OutputParquetKey: extractOut.S3OutputParquetKey,
		PipelineConfig:     customPipelineCfg,
	}

	var suite testsuite.WorkflowTestSuite

	// Attempt 1: crash at unit 4 after checkpointing units 1..3.
	var attempt1Units []int32
	poly1 := activity.NewPolyglotWorkerActivities(nil, func(ctx context.Context, unit int32) error {
		if unit == 4 {
			return errors.New("simulated chunk-lifecycle worker preemption at unit 4")
		}
		attempt1Units = append(attempt1Units, unit)
		return nil
	})

	env1 := suite.NewTestActivityEnvironment()
	env1.RegisterActivity(poly1.CompileChunks)
	_, err := env1.ExecuteActivity(poly1.CompileChunks, compileReq)
	require.Error(t, err)
	assert.Equal(t, []int32{1, 2, 3}, attempt1Units)

	// Attempt 2: resume from HeartbeatDetails checkpoint unit 3 -> processes units 4..10 only.
	var attempt2Units []int32
	poly2 := activity.NewPolyglotWorkerActivities(nil, func(ctx context.Context, unit int32) error {
		attempt2Units = append(attempt2Units, unit)
		return nil
	})

	env2 := suite.NewTestActivityEnvironment()
	env2.SetHeartbeatDetails(coordinatorv1.HeartbeatDetails{
		SliceID:           slice.SliceID,
		SliceIndex:        slice.SliceIndex,
		LastProcessedUnit: 3,
		ProcessedUnits:    3,
		TotalUnits:        10,
		IntermediateS3Key: compileReq.S3OutputParquetKey,
	})
	env2.RegisterActivity(poly2.CompileChunks)

	encoded, err := env2.ExecuteActivity(poly2.CompileChunks, compileReq)
	require.NoError(t, err)

	var compileResp coordinatorv1.CompileChunksResponse
	require.NoError(t, encoded.Get(&compileResp))
	assert.Equal(t, []int32{4, 5, 6, 7, 8, 9, 10}, attempt2Units)
	assert.Equal(t, int32(20), compileResp.CompiledChunks)
	assert.Equal(t, int32(20*512), compileResp.TotalTokens, "must propagate custom PipelineConfig.ChunkSizeTokens")
}
