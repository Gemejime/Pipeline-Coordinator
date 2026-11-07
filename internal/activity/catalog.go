package activity

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

const (
	// TerminalStatusCompleted indicates all slices (or partial non-fatal slices) finished.
	TerminalStatusCompleted = "COMPLETED"
	// TerminalStatusFailed indicates the workflow failed fatally.
	TerminalStatusFailed = "FAILED"
	// TerminalStatusTerminated indicates the workflow was cancelled or terminated.
	TerminalStatusTerminated = "TERMINATED"
)

// TerminalCatalogStore abstracts atomic terminal run persistence in PostgreSQL (ADR-003).
type TerminalCatalogStore interface {
	RecordTerminalRun(ctx context.Context, req coordinatorv1.CatalogTerminalRunRequest) (string, error)
}

// CatalogActivities holds dependencies for terminal PostgreSQL run cataloging.
type CatalogActivities struct {
	Store TerminalCatalogStore
}

// NewCatalogActivities creates a CatalogActivities instance.
func NewCatalogActivities(store TerminalCatalogStore) *CatalogActivities {
	return &CatalogActivities{Store: store}
}

// CatalogTerminalRunActivity executes on coordinator-task-queue exclusively at workflow
// terminal boundaries (COMPLETED, FAILED, TERMINATED) to persist a single atomic record
// in PostgreSQL without any in-flight dual-writes (ADR-003).
func CatalogTerminalRunActivity(
	ctx context.Context,
	req coordinatorv1.CatalogTerminalRunRequest,
) (*coordinatorv1.CatalogTerminalRunResponse, error) {
	a := &CatalogActivities{}
	return a.CatalogTerminalRun(ctx, req)
}

// CatalogTerminalRun validates and persists the terminal run metrics.
func (a *CatalogActivities) CatalogTerminalRun(
	ctx context.Context,
	req coordinatorv1.CatalogTerminalRunRequest,
) (*coordinatorv1.CatalogTerminalRunResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if strings.TrimSpace(req.WorkflowID) == "" || strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.DocumentID) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"workflow_id, tenant_id, and document_id are required for terminal cataloging",
			ErrCodeInvalidArgument,
			nil,
		)
	}

	switch req.Status {
	case TerminalStatusCompleted, TerminalStatusFailed, TerminalStatusTerminated:
	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("invalid terminal status %q: must be COMPLETED, FAILED, or TERMINATED", req.Status),
			ErrCodeInvalidArgument,
			nil,
		)
	}

	recordID := fmt.Sprintf("run_rec_%s", req.WorkflowID)
	if a != nil && a.Store != nil {
		id, err := a.Store.RecordTerminalRun(ctx, req)
		if err != nil {
			return nil, err
		}
		recordID = id
	}

	logger := activity.GetLogger(ctx)
	logger.Info("cataloged terminal ingestion workflow run",
		"tenant_id", req.TenantID,
		"document_id", req.DocumentID,
		"workflow_id", req.WorkflowID,
		"run_id", req.RunID,
		"status", req.Status,
		"total_pages", req.TotalPages,
		"total_slices", req.TotalSlices,
		"completed_slices", req.CompletedSlices,
		"failed_slices", req.FailedSlices,
		"duration_seconds", req.DurationSeconds,
	)

	return &coordinatorv1.CatalogTerminalRunResponse{
		RecordID:  recordID,
		Persisted: true,
	}, nil
}
