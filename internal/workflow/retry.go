// Package workflow implements deterministic Temporal workflow orchestrations
// for the rag-ingestion-coordinator control plane.
package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/service"
)

const (
	// CoordinatorTaskQueue is polled by Go coordinator workers for presigning,
	// Redis Pub/Sub milestone publishing, and terminal PostgreSQL cataloging.
	CoordinatorTaskQueue = "coordinator-task-queue"

	// InspectionTaskQueue is polled by isolated inspection workers performing
	// random-access byte range header parsing on S3 objects.
	InspectionTaskQueue = "inspection-queue"

	// DocAITaskQueue is polled by autoscaling GPU Python workers (Temporal Python SDK)
	// executing Docling layout analysis, OCR, and ASR.
	DocAITaskQueue = "doc-ai-task-queue"

	// ChunkLifecycleTaskQueue is polled by rag-chunk-lifecycle-service workers
	// compiling parent-child chunk trees from Parquet slices.
	ChunkLifecycleTaskQueue = "chunk-lifecycle-queue"

	// QueryTypeIngestionStatus is the Temporal query handler name for live in-flight
	// workflow status inspection without relational database dual-writes (ADR-003).
	QueryTypeIngestionStatus = "ingestion_status"

	// MaxSlicesPerGeneration caps the number of child workflows spawned in a single
	// root workflow execution before triggering ContinueAsNew, keeping event history
	// strictly below 2,500 events (and <10,000 hard limit) per ADR-002.
	MaxSlicesPerGeneration = 100

	// InspectionHeartbeatTimeout is the maximum allowed gap between heartbeats on inspection-queue.
	InspectionHeartbeatTimeout = 10 * time.Second
	// DocAIHeartbeatTimeout is the maximum allowed gap between heartbeats on doc-ai-task-queue.
	DocAIHeartbeatTimeout = 15 * time.Second
	// ChunkLifecycleHeartbeatTimeout is the maximum allowed gap between heartbeats on chunk-lifecycle-queue.
	ChunkLifecycleHeartbeatTimeout = 10 * time.Second
)

var (
	// SearchAttrTenantID indexes workflow executions by TenantID.
	SearchAttrTenantID = service.SearchAttrTenantID
	// SearchAttrDocumentID indexes workflow executions by DocumentID.
	SearchAttrDocumentID = service.SearchAttrDocumentID
	// SearchAttrPipelineRunID indexes workflow executions by PipelineRunID.
	SearchAttrPipelineRunID = service.SearchAttrPipelineRunID
)

// NonRetryableErrorTypes returns the canonical error codes that must fail-fast
// without activity retries.
func NonRetryableErrorTypes() []string {
	return []string{
		activity.ErrCodeInvalidArgument,
		activity.ErrCodeCorruptInput,
		activity.ErrCodeUnsupportedMimeType,
		activity.ErrCodeTenantMismatch,
	}
}

// DefaultControlPlaneRetryPolicy defines fast exponential backoff for control-plane
// operations (S3 presigning, Redis publishing, PostgreSQL terminal cataloging).
func DefaultControlPlaneRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:        500 * time.Millisecond,
		BackoffCoefficient:     2.0,
		MaximumInterval:        10 * time.Second,
		MaximumAttempts:        5,
		NonRetryableErrorTypes: NonRetryableErrorTypes(),
	}
}

// DefaultComputeRetryPolicy defines resilient exponential backoff for heavy data-plane
// worker activities (header byte inspection, GPU DocAI extraction, chunk compilation).
func DefaultComputeRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:        2 * time.Second,
		BackoffCoefficient:     2.0,
		MaximumInterval:        60 * time.Second,
		MaximumAttempts:        4,
		NonRetryableErrorTypes: NonRetryableErrorTypes(),
	}
}

// CoordinatorActivityOptions returns deterministic ActivityOptions for coordinator-task-queue.
func CoordinatorActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           CoordinatorTaskQueue,
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         DefaultControlPlaneRetryPolicy(),
	}
}

// InspectionActivityOptions returns deterministic ActivityOptions for inspection-queue.
func InspectionActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           InspectionTaskQueue,
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    InspectionHeartbeatTimeout,
		RetryPolicy:         DefaultComputeRetryPolicy(),
	}
}

// DocAIActivityOptions returns deterministic ActivityOptions for doc-ai-task-queue.
// Single-attempt execution (MaximumAttempts = 1) is enforced at the activity level so
// DocumentPartitionWorkflow can re-issue fresh 15-minute presigned S3 URLs before every retry.
func DocAIActivityOptions() workflow.ActivityOptions {
	retryPolicy := DefaultComputeRetryPolicy()
	retryPolicy.MaximumAttempts = 1
	return workflow.ActivityOptions{
		TaskQueue:           DocAITaskQueue,
		StartToCloseTimeout: time.Duration(activity.DefaultPresignLeaseSeconds) * time.Second,
		HeartbeatTimeout:    DocAIHeartbeatTimeout,
		RetryPolicy:         retryPolicy,
	}
}

// ChunkLifecycleActivityOptions returns deterministic ActivityOptions for chunk-lifecycle-queue.
func ChunkLifecycleActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           ChunkLifecycleTaskQueue,
		StartToCloseTimeout: 15 * time.Minute,
		HeartbeatTimeout:    ChunkLifecycleHeartbeatTimeout,
		RetryPolicy:         DefaultComputeRetryPolicy(),
	}
}
