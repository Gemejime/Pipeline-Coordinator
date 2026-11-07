package unit_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/workflow"
)

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

	chunkOpts := workflow.ChunkLifecycleActivityOptions()
	assert.Equal(t, workflow.ChunkLifecycleTaskQueue, chunkOpts.TaskQueue)
	assert.Equal(t, workflow.ChunkLifecycleHeartbeatTimeout, chunkOpts.HeartbeatTimeout)
}

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
