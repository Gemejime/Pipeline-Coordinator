package unit_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/workflow"
)

// TestTaskQueueAndSearchAttributeRegistry verifies task queue constants, search attribute keys, and activity retry policies.
func TestTaskQueueAndSearchAttributeRegistry(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "coordinator-task-queue", workflow.CoordinatorTaskQueue)
	assert.Equal(t, "inspection-queue", workflow.InspectionTaskQueue)
	assert.Equal(t, "doc-ai-task-queue", workflow.DocAITaskQueue)
	assert.Equal(t, "chunk-lifecycle-queue", workflow.ChunkLifecycleTaskQueue)
	assert.Equal(t, "ingestion_status", workflow.QueryTypeIngestionStatus)

	assert.Equal(t, "TenantID", workflow.SearchAttrTenantID.GetName())
	assert.Equal(t, "DocumentID", workflow.SearchAttrDocumentID.GetName())
	assert.Equal(t, "PipelineRunID", workflow.SearchAttrPipelineRunID.GetName())

	nonRetryable := workflow.NonRetryableErrorTypes()
	assert.Contains(t, nonRetryable, activity.ErrCodeInvalidArgument)
	assert.Contains(t, nonRetryable, activity.ErrCodeCorruptInput)
	assert.Contains(t, nonRetryable, activity.ErrCodeUnsupportedMimeType)
	assert.Contains(t, nonRetryable, activity.ErrCodeTenantMismatch)

	inspectOpts := workflow.InspectionActivityOptions()
	assert.Equal(t, workflow.InspectionTaskQueue, inspectOpts.TaskQueue)
	assert.Equal(t, workflow.InspectionHeartbeatTimeout, inspectOpts.HeartbeatTimeout)

	docAIOpts := workflow.DocAIActivityOptions()
	assert.Equal(t, workflow.DocAITaskQueue, docAIOpts.TaskQueue)
	assert.Equal(t, workflow.DocAIHeartbeatTimeout, docAIOpts.HeartbeatTimeout)
	require.NotNil(t, docAIOpts.RetryPolicy)
	assert.Equal(t, int32(1), docAIOpts.RetryPolicy.MaximumAttempts)

	chunkOpts := workflow.ChunkLifecycleActivityOptions()
	assert.Equal(t, workflow.ChunkLifecycleTaskQueue, chunkOpts.TaskQueue)
	assert.Equal(t, workflow.ChunkLifecycleHeartbeatTimeout, chunkOpts.HeartbeatTimeout)
}

// TestCatalogTerminalRun_RequiresConfiguredStore verifies that terminal cataloging fails when Store is nil.
func TestCatalogTerminalRun_RequiresConfiguredStore(t *testing.T) {
	t.Parallel()

	req := coordinatorv1.CatalogTerminalRunRequest{
		WorkflowID: "wf-01",
		RunID:      "run-01",
		TenantID:   "tenant-01",
		DocumentID: "doc-01",
		Status:     activity.TerminalStatusCompleted,
	}

	_, err := activity.CatalogTerminalRunActivity(context.Background(), req)
	require.ErrorIs(t, err, activity.ErrNilCatalogStore)

	nilStoreActivities := activity.NewCatalogActivities(nil)
	_, err = nilStoreActivities.CatalogTerminalRun(context.Background(), req)
	require.ErrorIs(t, err, activity.ErrNilCatalogStore)
	require.ErrorIs(t, nilStoreActivities.Register(nil), activity.ErrNilCatalogStore)
}

// TestSecuredLogging_RedactsPresignedURLsInSlog verifies that slog.LogValuer implementations redact presigned URLs.
func TestSecuredLogging_RedactsPresignedURLsInSlog(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	jsonLogger := slog.New(slog.NewJSONHandler(&buf, nil))

	secretPresignResp := coordinatorv1.PresignSliceArtifactResponse{
		S3Bucket:              "rag-ingestion-bucket",
		S3InputKey:            "raw/tenant-01/hash/doc.pdf",
		S3OutputParquetKey:    "artifacts/tenant-01/hash/slice_1_50.parquet",
		PresignedInputGetURL:  "https://rag-ingestion-bucket.s3.amazonaws.com/raw/doc.pdf?X-Amz-Signature=SUPER_SECRET_SIG_1",
		PresignedOutputPutURL: "https://rag-ingestion-bucket.s3.amazonaws.com/artifacts/slice_1_50.parquet?X-Amz-Signature=SUPER_SECRET_SIG_2",
		ExpiresAtUnix:         1794039630,
	}

	jsonLogger.Info("test presign response redaction", "response", secretPresignResp)
	output := buf.String()

	require.Contains(t, output, "rag-ingestion-bucket")
	require.Contains(t, output, "artifacts/tenant-01/hash/slice_1_50.parquet")
	require.Contains(t, output, "[REDACTED]")
	require.NotContains(t, output, "SUPER_SECRET_SIG_1")
	require.NotContains(t, output, "SUPER_SECRET_SIG_2")
	require.NotContains(t, output, "X-Amz-Signature")
}
