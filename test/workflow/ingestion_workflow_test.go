package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	temporalactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	temporalworkflow "go.temporal.io/sdk/workflow"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/store/s3"
	"rag-ingestion-coordinator/internal/workflow"
)

func registerTestPresignActivity(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
	t.Helper()
	p, err := s3.NewPresigner(s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1"), nil, s3.PresignerConfig{})
	require.NoError(t, err)
	presignActs := activity.NewPresignActivities(p)
	require.NoError(t, presignActs.Register(env))
}

// TestDocumentPartitionWorkflow_Success verifies the per-slice child workflow activity sequence and custom PipelineConfig propagation.
func TestDocumentPartitionWorkflow_Success(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	registerTestPresignActivity(t, env)
	env.RegisterActivity(activity.DispatchDocAIExtractActivity)
	env.RegisterActivity(activity.CompileChunksActivity)

	customCfg := &coordinatorv1.PipelineConfig{
		ID:                 "custom-finance-v2",
		TenantID:           "tenant-corp-01",
		ChunkSizeTokens:    512,
		ChunkOverlapTokens: 64,
		OCREnabled:         false,
	}

	slice := coordinatorv1.PartitionSlice{
		SliceID:            "slice-hash-001",
		SliceIndex:         0,
		TotalSlices:        2,
		TenantID:           "tenant-corp-01",
		DocumentID:         "doc-001",
		ContentHash:        "hash-001",
		S3InputBucket:      "rag-ingestion-bucket",
		S3InputKey:         "raw/tenant-corp-01/hash-001/report.pdf",
		S3OutputParquetKey: "artifacts/tenant-corp-01/hash-001/slice_1_50.parquet",
		Modality:           coordinatorv1.MultimodalModalityDocumentPDF,
		StartPage:          1,
		EndPage:            50,
		PipelineConfigID:   customCfg.ID,
		PipelineConfig:     customCfg,
	}

	env.ExecuteWorkflow(workflow.DocumentPartitionWorkflow, slice)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result workflow.PartitionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "slice-hash-001", result.SliceID)
	assert.Equal(t, int32(0), result.SliceIndex)
	assert.Equal(t, "rag-ingestion-bucket", result.S3OutputBucket)
	assert.Equal(t, "artifacts/tenant-corp-01/hash-001/slice_1_50.parquet", result.S3OutputParquetKey)
	assert.Equal(t, int32(50), result.ExtractedPages)
	assert.Equal(t, int32(100), result.CompiledChunks)
	assert.Equal(t, int32(51200), result.TotalTokens)
}

// TestDocumentPartitionWorkflow_RePresignsOnExtractionRetry verifies that each extraction retry receives freshly presigned URLs.
func TestDocumentPartitionWorkflow_RePresignsOnExtractionRetry(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var presignAttempts int
	env.RegisterActivityWithOptions(
		func(ctx context.Context, req coordinatorv1.PresignSliceArtifactRequest) (*coordinatorv1.PresignSliceArtifactResponse, error) {
			presignAttempts++
			return &coordinatorv1.PresignSliceArtifactResponse{
				S3Bucket:              req.Slice.S3InputBucket,
				S3InputKey:            req.Slice.S3InputKey,
				S3OutputParquetKey:    req.Slice.S3OutputParquetKey,
				PresignedInputGetURL:  fmt.Sprintf("https://s3.example.com/get?attempt=%d", presignAttempts),
				PresignedOutputPutURL: fmt.Sprintf("https://s3.example.com/put?attempt=%d", presignAttempts),
				ExpiresAtUnix:         1794040500 + int64(presignAttempts),
			}, nil
		},
		temporalactivity.RegisterOptions{Name: "PresignS3ArtifactActivity"},
	)

	var seenGetURLs []string
	env.RegisterActivityWithOptions(
		func(ctx context.Context, req coordinatorv1.ExtractSegmentRequest) (*coordinatorv1.ExtractSegmentResponse, error) {
			seenGetURLs = append(seenGetURLs, req.PresignedInputGetURL)
			if len(seenGetURLs) == 1 {
				return nil, errors.New("transient GPU worker timeout")
			}
			return &coordinatorv1.ExtractSegmentResponse{
				SliceID:            req.Slice.SliceID,
				SliceIndex:         req.Slice.SliceIndex,
				S3OutputBucket:     req.Slice.S3InputBucket,
				S3OutputParquetKey: req.Slice.S3OutputParquetKey,
				ExtractedPages:     10,
				ExtractedSegments:  10,
				ParquetBytes:       40960,
			}, nil
		},
		temporalactivity.RegisterOptions{Name: "DispatchDocAIExtractActivity"},
	)
	env.RegisterActivity(activity.CompileChunksActivity)

	slice := coordinatorv1.PartitionSlice{
		SliceID:            "slice-represign-01",
		SliceIndex:         0,
		TotalSlices:        1,
		TenantID:           "tenant-corp-01",
		DocumentID:         "doc-001",
		ContentHash:        "hash-001",
		S3InputBucket:      "rag-ingestion-bucket",
		S3InputKey:         "raw/tenant-corp-01/hash-001/report.pdf",
		S3OutputParquetKey: "artifacts/tenant-corp-01/hash-001/slice_1_10.parquet",
		Modality:           coordinatorv1.MultimodalModalityDocumentPDF,
		StartPage:          1,
		EndPage:            10,
	}

	env.ExecuteWorkflow(workflow.DocumentPartitionWorkflow, slice)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	assert.Equal(t, 2, presignAttempts, "must re-issue presigned URLs before retrying extraction")
	require.Equal(t, []string{
		"https://s3.example.com/get?attempt=1",
		"https://s3.example.com/get?attempt=2",
	}, seenGetURLs)
}

// TestActivity_HeartbeatRecovery_ResumesFromLastCheckpoint verifies ADR-003 HeartbeatDetails checkpoint resumption.
func TestActivity_HeartbeatRecovery_ResumesFromLastCheckpoint(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	var processedAttempt1 []int32
	var lastRecordedHeartbeat coordinatorv1.HeartbeatDetails

	polyglotAttempt1 := activity.NewPolyglotWorkerActivities(
		func(ctx context.Context, unit int32) error {
			if unit == 26 {
				return errors.New("simulated GPU worker preemption after page 25")
			}
			processedAttempt1 = append(processedAttempt1, unit)
			return nil
		},
		nil,
	)

	actEnv1 := suite.NewTestActivityEnvironment()
	actEnv1.RegisterActivity(polyglotAttempt1.DispatchDocAIExtract)

	req := coordinatorv1.ExtractSegmentRequest{
		Slice: coordinatorv1.PartitionSlice{
			SliceID:            "slice-crash-recovery-01",
			SliceIndex:         0,
			TotalSlices:        1,
			TenantID:           "tenant-corp-01",
			DocumentID:         "doc-crash-01",
			ContentHash:        "hash-crash-01",
			S3InputBucket:      "rag-ingestion-bucket",
			S3InputKey:         "raw/tenant-corp-01/hash-crash-01/report.pdf",
			S3OutputParquetKey: "artifacts/tenant-corp-01/hash-crash-01/slice_1_50.parquet",
			Modality:           coordinatorv1.MultimodalModalityDocumentPDF,
			StartPage:          1,
			EndPage:            50,
		},
		PresignedInputGetURL:  "https://rag-ingestion-bucket.s3.amazonaws.com/raw/doc.pdf?X-Amz-Expires=900",
		PresignedOutputPutURL: "https://rag-ingestion-bucket.s3.amazonaws.com/artifacts/slice_1_50.parquet?X-Amz-Expires=900",
		PipelineConfig: coordinatorv1.PipelineConfig{
			ID:              "default-multimodal-v1",
			TenantID:        "tenant-corp-01",
			ChunkSizeTokens: 300,
		},
	}

	_, err := actEnv1.ExecuteActivity(polyglotAttempt1.DispatchDocAIExtract, req)
	require.Error(t, err)
	assert.Len(t, processedAttempt1, 25)
	assert.Equal(t, int32(1), processedAttempt1[0])
	assert.Equal(t, int32(25), processedAttempt1[len(processedAttempt1)-1])

	lastRecordedHeartbeat = coordinatorv1.HeartbeatDetails{
		SliceID:           req.Slice.SliceID,
		SliceIndex:        req.Slice.SliceIndex,
		LastProcessedUnit: 25,
		ProcessedUnits:    25,
		TotalUnits:        50,
		IntermediateS3Key: req.Slice.S3OutputParquetKey,
	}

	// Attempt 2: Restart activity with recorded HeartbeatDetails (LastProcessedUnit = 25).
	var processedAttempt2 []int32
	polyglotAttempt2 := activity.NewPolyglotWorkerActivities(
		func(ctx context.Context, unit int32) error {
			processedAttempt2 = append(processedAttempt2, unit)
			return nil
		},
		nil,
	)

	actEnv2 := suite.NewTestActivityEnvironment()
	actEnv2.SetHeartbeatDetails(lastRecordedHeartbeat)
	actEnv2.RegisterActivity(polyglotAttempt2.DispatchDocAIExtract)

	encodedVal, err := actEnv2.ExecuteActivity(polyglotAttempt2.DispatchDocAIExtract, req)
	require.NoError(t, err)

	var resp coordinatorv1.ExtractSegmentResponse
	require.NoError(t, encodedVal.Get(&resp))
	assert.Equal(t, int32(50), resp.ExtractedPages)

	// Verify zero duplicate work: Attempt 2 must start strictly at unit 26 through 50.
	require.Len(t, processedAttempt2, 25)
	assert.Equal(t, int32(26), processedAttempt2[0], "must resume at unit 26 after checkpoint 25")
	assert.Equal(t, int32(50), processedAttempt2[len(processedAttempt2)-1])
}

// TestIngestionCoordinatorWorkflow_EndToEnd_And_QueryStatus verifies end-to-end root workflow orchestration, status query handler, and terminal cataloging.
func TestIngestionCoordinatorWorkflow_EndToEnd_And_QueryStatus(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(workflow.DocumentPartitionWorkflow)
	env.RegisterActivity(activity.InspectDocumentHeaderActivity)
	registerTestPresignActivity(t, env)
	env.RegisterActivity(activity.DispatchDocAIExtractActivity)
	env.RegisterActivity(activity.CompileChunksActivity)

	var publishedEvents []coordinatorv1.ProgressMilestoneEvent
	notifActs := activity.NewNotificationActivities(&inMemoryMilestonePublisher{
		onPublish: func(ev coordinatorv1.ProgressMilestoneEvent) {
			publishedEvents = append(publishedEvents, ev)
		},
	})
	env.RegisterActivityWithOptions(
		notifActs.PublishMilestone,
		temporalactivity.RegisterOptions{Name: "PublishMilestoneActivity"},
	)

	var catalogedRuns []coordinatorv1.CatalogTerminalRunRequest
	catActs := activity.NewCatalogActivities(&inMemoryCatalogStore{
		onRecord: func(req coordinatorv1.CatalogTerminalRunRequest) {
			catalogedRuns = append(catalogedRuns, req)
		},
	})
	require.NoError(t, catActs.Register(env))

	req := workflow.IngestionRequest{
		TenantID:         "tenant-corp-01",
		DocumentID:       "doc-e2e-01",
		S3Bucket:         "rag-ingestion-bucket",
		S3Key:            "raw/tenant-corp-01/hash/report.pdf",
		ContentHash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		TotalBytes:       10 * 1024 * 1024, // ~160 pages -> 4 slices of 50 pages
		MimeType:         "application/pdf",
		PipelineConfigID: "default-multimodal-v1",
	}

	env.ExecuteWorkflow(workflow.IngestionCoordinatorWorkflow, req)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result workflow.IngestionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, activity.TerminalStatusCompleted, result.Status)
	assert.Equal(t, int32(160), result.TotalPages)
	assert.Equal(t, int32(4), result.TotalSlices)
	assert.Equal(t, int32(4), result.CompletedSlices)
	assert.Equal(t, int32(0), result.FailedSlices)
	assert.Equal(t, int32(1), result.Generations)

	// Verify Temporal Query Handler (ADR-003)
	queryVal, err := env.QueryWorkflow(workflow.QueryTypeIngestionStatus)
	require.NoError(t, err)
	var queriedStatus workflow.IngestionStatus
	require.NoError(t, queryVal.Get(&queriedStatus))
	assert.Equal(t, activity.TerminalStatusCompleted, queriedStatus.Status)
	assert.Equal(t, int32(160), queriedStatus.TotalPages)
	assert.Equal(t, int32(4), queriedStatus.TotalSlices)
	assert.Equal(t, int32(4), queriedStatus.CompletedSlices)
	assert.Equal(t, int32(0), queriedStatus.FailedSlices)

	// Verify Redis milestone sequence numbers are strictly monotonic (1, 2, 3...)
	require.Len(t, publishedEvents, 3)
	for i, ev := range publishedEvents {
		assert.Equal(t, int64(i+1), ev.SequenceNumber)
		assert.Equal(t, "tenant-corp-01", ev.TenantID)
	}
	assert.Equal(t, workflow.StageInspecting, publishedEvents[0].Stage)
	assert.Equal(t, workflow.StageExtractSegments, publishedEvents[1].Stage)
	assert.Equal(t, workflow.StageCompleted, publishedEvents[2].Stage)

	// Verify ADR-003 Zero Dual-Writes: Exactly 1 terminal record written at pipeline completion.
	require.Len(t, catalogedRuns, 1)
	assert.Equal(t, activity.TerminalStatusCompleted, catalogedRuns[0].Status)
	assert.Equal(t, int32(4), catalogedRuns[0].CompletedSlices)
	assert.Equal(t, int32(0), catalogedRuns[0].FailedSlices)
}

// TestIngestionCoordinatorWorkflow_HistoryLimit_20000Pages verifies bounded ContinueAsNew generations (<2,500 history events) for a 20,000-page document.
func TestIngestionCoordinatorWorkflow_HistoryLimit_20000Pages(t *testing.T) {
	t.Parallel()

	// 20,000 pages decomposed into 50-page slices = 400 child workflows.
	// With MaxSlicesPerGeneration = 100, the root workflow executes across 4 bounded
	// ContinueAsNew generations (100 slices per generation).
	const (
		totalPages              = 20000
		expectedSlices          = 400
		expectedGenerations     = 4
		operationalEventCeiling = 2500
		hardEventCeiling        = 10000
	)

	currentReq := workflow.IngestionRequest{
		TenantID:              "tenant-scale-20k",
		DocumentID:            "doc-20000-pages",
		S3Bucket:              "rag-ingestion-bucket",
		S3Key:                 "raw/tenant-scale-20k/hash/massive-corpus.pdf",
		ContentHash:           "99887766554433221100ffeeddccbbaa99887766554433221100ffeeddccbbaa",
		TotalBytes:            int64(totalPages) * 64 * 1024,
		MimeType:              "application/pdf",
		PipelineConfigID:      "custom-20k-config-v1",
		UseHashPrefixS3Layout: true,
	}

	var catalogedRuns []coordinatorv1.CatalogTerminalRunRequest
	var finalResult workflow.IngestionResult

	for gen := 0; gen < expectedGenerations; gen++ {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()

		env.RegisterWorkflow(workflow.DocumentPartitionWorkflow)
		env.RegisterActivity(activity.InspectDocumentHeaderActivity)
		registerTestPresignActivity(t, env)
		env.RegisterActivity(activity.DispatchDocAIExtractActivity)
		env.RegisterActivity(activity.CompileChunksActivity)
		env.RegisterActivity(activity.PublishMilestoneActivity)

		catActs := activity.NewCatalogActivities(&inMemoryCatalogStore{
			onRecord: func(r coordinatorv1.CatalogTerminalRunRequest) {
				catalogedRuns = append(catalogedRuns, r)
			},
		})
		require.NoError(t, catActs.Register(env))

		// Estimate history events for this generation:
		// Each child workflow spawn & completion in a root generation records ~6 events
		// (StartChildWorkflowExecutionInitiated, ChildWorkflowExecutionStarted, ChildWorkflowExecutionCompleted,
		// plus WorkflowTaskScheduled/Started/Completed).
		// 100 slices * 6 events + ~25 activity/workflow overhead events = ~625 events per generation.
		env.ExecuteWorkflow(workflow.IngestionCoordinatorWorkflow, currentReq)
		require.True(t, env.IsWorkflowCompleted())

		wfErr := env.GetWorkflowError()
		if gen < expectedGenerations-1 {
			var canErr *temporalworkflow.ContinueAsNewError
			require.ErrorAs(t, wfErr, &canErr, "generation %d must transition via ContinueAsNewError", gen)
			require.NotNil(t, canErr.Input)

			var nextReq workflow.IngestionRequest
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &nextReq))

			completedSoFar := int32((gen + 1) * workflow.MaxSlicesPerGeneration)
			assert.Equal(t, int32(gen+1), nextReq.Generation)
			assert.Equal(t, completedSoFar, nextReq.CompletedSlices)
			assert.Equal(t, completedSoFar, nextReq.NextSliceIndex)
			assert.Equal(t, coordinatorv1.MultimodalModalityDocumentPDF, nextReq.Modality)
			assert.Equal(t, int32(totalPages), nextReq.TotalPages)
			assert.True(t, nextReq.UseHashPrefixS3Layout)
			assert.Equal(t, "custom-20k-config-v1", nextReq.PipelineConfigID)
			require.NotNil(t, nextReq.PipelineConfig)
			assert.Equal(t, "custom-20k-config-v1", nextReq.PipelineConfig.ID)

			queryVal, qErr := env.QueryWorkflow(workflow.QueryTypeIngestionStatus)
			require.NoError(t, qErr)
			var genStatus workflow.IngestionStatus
			require.NoError(t, queryVal.Get(&genStatus))
			assert.Equal(t, completedSoFar, genStatus.CompletedSlices)
			assert.Equal(t, int32(gen), genStatus.CurrentGeneration)

			estimatedRootEvents := int(workflow.MaxSlicesPerGeneration)*6 + 30
			assert.Less(t, estimatedRootEvents, operationalEventCeiling, "root generation %d exceeded 2,500 event operational target", gen)
			assert.Less(t, estimatedRootEvents, hardEventCeiling, "root generation %d exceeded 10,000 event hard ceiling", gen)

			currentReq = nextReq
		} else {
			require.NoError(t, wfErr, "final generation must complete without error")
			require.NoError(t, env.GetWorkflowResult(&finalResult))
		}
	}

	assert.Equal(t, activity.TerminalStatusCompleted, finalResult.Status)
	assert.Equal(t, int32(totalPages), finalResult.TotalPages)
	assert.Equal(t, int32(expectedSlices), finalResult.TotalSlices)
	assert.Equal(t, int32(expectedSlices), finalResult.CompletedSlices)
	assert.Equal(t, int32(0), finalResult.FailedSlices)
	assert.Equal(t, int32(expectedGenerations), finalResult.Generations)
	assert.Equal(t, int32(expectedSlices*100), finalResult.CompiledChunks)

	// Verify ADR-003: Zero database writes during the first 3 generations; exactly 1 write at the final boundary.
	require.Len(t, catalogedRuns, 1)
	assert.Equal(t, int32(expectedSlices), catalogedRuns[0].CompletedSlices)
}

// TestIngestionCoordinatorWorkflow_PoisonPillSliceIsolation verifies Runbook 3 poison-pill partition isolation.
func TestIngestionCoordinatorWorkflow_PoisonPillSliceIsolation(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(workflow.DocumentPartitionWorkflow)
	env.RegisterActivity(activity.InspectDocumentHeaderActivity)
	registerTestPresignActivity(t, env)
	env.RegisterActivity(activity.DispatchDocAIExtractActivity)
	env.RegisterActivity(activity.CompileChunksActivity)
	env.RegisterActivity(activity.PublishMilestoneActivity)

	var catalogedRuns []coordinatorv1.CatalogTerminalRunRequest
	catActs := activity.NewCatalogActivities(&inMemoryCatalogStore{
		onRecord: func(r coordinatorv1.CatalogTerminalRunRequest) {
			catalogedRuns = append(catalogedRuns, r)
		},
	})
	require.NoError(t, catActs.Register(env))

	// Simulate poison-pill failure on slice_index == 1 while sibling slices (0, 2, 3) succeed (Runbook 3).
	env.OnActivity(
		activity.DispatchDocAIExtractActivity,
		mock.Anything,
		mock.MatchedBy(func(req coordinatorv1.ExtractSegmentRequest) bool {
			return req.Slice.SliceIndex == 1
		}),
	).Return(nil, errors.New("CORRUPT_INPUT: malformed PDF xref stream in slice 1"))

	env.OnActivity(
		activity.DispatchDocAIExtractActivity,
		mock.Anything,
		mock.MatchedBy(func(req coordinatorv1.ExtractSegmentRequest) bool {
			return req.Slice.SliceIndex != 1
		}),
	).Return(func(ctx context.Context, req coordinatorv1.ExtractSegmentRequest) (*coordinatorv1.ExtractSegmentResponse, error) {
		return &coordinatorv1.ExtractSegmentResponse{
			SliceID:            req.Slice.SliceID,
			SliceIndex:         req.Slice.SliceIndex,
			S3OutputBucket:     req.Slice.S3InputBucket,
			S3OutputParquetKey: req.Slice.S3OutputParquetKey,
			ExtractedPages:     50,
			ExtractedSegments:  50,
			ParquetBytes:       204800,
		}, nil
	})

	req := workflow.IngestionRequest{
		TenantID:    "tenant-corp-01",
		DocumentID:  "doc-poison-pill-01",
		S3Bucket:    "rag-ingestion-bucket",
		S3Key:       "raw/tenant-corp-01/hash/partial-corrupt.pdf",
		ContentHash: "cafebabe0123456789abcdef0123456789abcdef0123456789abcdef01234567",
		TotalBytes:  10 * 1024 * 1024, // 160 pages -> 4 slices
		MimeType:    "application/pdf",
	}

	env.ExecuteWorkflow(workflow.IngestionCoordinatorWorkflow, req)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result workflow.IngestionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, activity.TerminalStatusCompleted, result.Status)
	assert.Equal(t, int32(4), result.TotalSlices)
	assert.Equal(t, int32(3), result.CompletedSlices)
	assert.Equal(t, int32(1), result.FailedSlices)
	require.Len(t, result.FailedSliceIDs, 1)

	require.Len(t, catalogedRuns, 1)
	assert.Equal(t, activity.TerminalStatusCompleted, catalogedRuns[0].Status)
	assert.Equal(t, int32(3), catalogedRuns[0].CompletedSlices)
	assert.Equal(t, int32(1), catalogedRuns[0].FailedSlices)
	assert.Contains(t, catalogedRuns[0].ErrorMessage, "poison-pill")
}

// TestIngestionCoordinatorWorkflow_FailureCataloging verifies terminal FAILED cataloging on non-retryable inspection failure.
func TestIngestionCoordinatorWorkflow_FailureCataloging(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(workflow.DocumentPartitionWorkflow)
	env.RegisterActivity(activity.InspectDocumentHeaderActivity)
	env.RegisterActivity(activity.PublishMilestoneActivity)

	var catalogedRuns []coordinatorv1.CatalogTerminalRunRequest
	catActs := activity.NewCatalogActivities(&inMemoryCatalogStore{
		onRecord: func(r coordinatorv1.CatalogTerminalRunRequest) {
			catalogedRuns = append(catalogedRuns, r)
		},
	})
	require.NoError(t, catActs.Register(env))

	// Zero-byte payload triggers non-retryable CORRUPT_INPUT during header inspection.
	req := workflow.IngestionRequest{
		TenantID:    "tenant-corp-01",
		DocumentID:  "doc-corrupt-zero-bytes",
		S3Bucket:    "rag-ingestion-bucket",
		S3Key:       "raw/tenant-corp-01/hash/empty.pdf",
		ContentHash: "0000000000000000000000000000000000000000000000000000000000000000",
		TotalBytes:  0,
		MimeType:    "application/pdf",
	}

	env.ExecuteWorkflow(workflow.IngestionCoordinatorWorkflow, req)
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())

	require.Len(t, catalogedRuns, 1)
	assert.Equal(t, activity.TerminalStatusFailed, catalogedRuns[0].Status)
	assert.Equal(t, "doc-corrupt-zero-bytes", catalogedRuns[0].DocumentID)
}

// inMemoryMilestonePublisher captures published ProgressMilestoneEvent payloads in memory for test assertions.
type inMemoryMilestonePublisher struct {
	onPublish func(ev coordinatorv1.ProgressMilestoneEvent)
}

// PublishMilestone records the milestone event in memory.
func (p *inMemoryMilestonePublisher) PublishMilestone(
	_ context.Context,
	_ string,
	ev coordinatorv1.ProgressMilestoneEvent,
) error {
	if p.onPublish != nil {
		p.onPublish(ev)
	}
	return nil
}

// inMemoryCatalogStore captures terminal workflow catalog requests in memory for test assertions.
type inMemoryCatalogStore struct {
	onRecord func(req coordinatorv1.CatalogTerminalRunRequest)
}

// RecordTerminalRun records the terminal workflow run in memory and returns a deterministic record ID.
func (s *inMemoryCatalogStore) RecordTerminalRun(
	_ context.Context,
	req coordinatorv1.CatalogTerminalRunRequest,
) (string, error) {
	if s.onRecord != nil {
		s.onRecord(req)
	}
	return fmt.Sprintf("rec_%s", req.WorkflowID), nil
}
