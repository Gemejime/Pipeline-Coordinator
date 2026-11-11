package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
)

const upsertTerminalRunSQL = `
INSERT INTO pipeline_runs (
	workflow_id,
	run_id,
	tenant_id,
	document_id,
	content_hash,
	status,
	total_pages,
	total_slices,
	completed_slices,
	failed_slices,
	duration_seconds,
	error_message,
	created_at,
	completed_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10,
	$11, NULLIF($12, ''), NOW(), NOW()
)
ON CONFLICT (workflow_id) DO UPDATE SET
	run_id = EXCLUDED.run_id,
	status = EXCLUDED.status,
	total_pages = EXCLUDED.total_pages,
	total_slices = EXCLUDED.total_slices,
	completed_slices = EXCLUDED.completed_slices,
	failed_slices = EXCLUDED.failed_slices,
	duration_seconds = EXCLUDED.duration_seconds,
	error_message = EXCLUDED.error_message,
	completed_at = NOW()
WHERE pipeline_runs.tenant_id = EXCLUDED.tenant_id
RETURNING id::text;
`

const selectTerminalRunSQL = `
SELECT
	workflow_id,
	run_id,
	tenant_id,
	status,
	total_slices,
	completed_slices,
	failed_slices,
	total_pages,
	duration_seconds,
	error_message,
	created_at,
	completed_at
FROM pipeline_runs
WHERE workflow_id = $1;
`

// RunRepository implements activity.TerminalCatalogStore and service.TerminalRunReader
// for atomic terminal workflow cataloging in PostgreSQL (ADR-003).
type RunRepository struct {
	db DBTX
}

// NewRunRepository constructs a RunRepository backed by a pgxpool.Pool or DBTX implementation.
func NewRunRepository(db DBTX) *RunRepository {
	return &RunRepository{db: db}
}

// RecordTerminalRun performs an atomic, tenant-scoped upsert of terminal workflow metrics into pipeline_runs.
func (r *RunRepository) RecordTerminalRun(
	ctx context.Context,
	req coordinatorv1.CatalogTerminalRunRequest,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r == nil || r.db == nil {
		return "", errors.New("postgresql run repository is not initialized")
	}

	workflowID := strings.TrimSpace(req.WorkflowID)
	tenantID := strings.TrimSpace(req.TenantID)
	documentID := strings.TrimSpace(req.DocumentID)
	if workflowID == "" || tenantID == "" || documentID == "" {
		return "", fmt.Errorf("%w: workflow_id, tenant_id, and document_id are required", service.ErrInvalidExecutionRequest)
	}

	switch req.Status {
	case "COMPLETED", "FAILED", "TERMINATED":
	default:
		return "", fmt.Errorf("%w: invalid terminal status %q", service.ErrInvalidExecutionRequest, req.Status)
	}

	var recordID string
	err := r.db.QueryRow(
		ctx,
		upsertTerminalRunSQL,
		workflowID,
		strings.TrimSpace(req.RunID),
		tenantID,
		documentID,
		strings.TrimSpace(req.ContentHash),
		req.Status,
		req.TotalPages,
		req.TotalSlices,
		req.CompletedSlices,
		req.FailedSlices,
		req.DurationSeconds,
		req.ErrorMessage,
	).Scan(&recordID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("%w: workflow_id %q is owned by another tenant", service.ErrTenantMismatch, workflowID)
		}
		return "", fmt.Errorf("failed to catalog terminal run for workflow %q: %w", workflowID, err)
	}

	return recordID, nil
}

// GetTerminalRun retrieves a terminal workflow execution record while enforcing strict tenant isolation.
func (r *RunRepository) GetTerminalRun(
	ctx context.Context,
	tenantID, workflowID string,
) (*coordinatorv1.GetIngestionStatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, errors.New("postgresql run repository is not initialized")
	}

	cleanTenant := strings.TrimSpace(tenantID)
	cleanWorkflow := strings.TrimSpace(workflowID)
	if cleanTenant == "" || cleanWorkflow == "" {
		return nil, fmt.Errorf("%w: tenant_id and workflow_id are required", service.ErrInvalidExecutionRequest)
	}

	var (
		rowWorkflowID   string
		rowRunID        string
		rowTenantID     string
		status          string
		totalSlices     int32
		completedSlices int32
		failedSlices    int32
		totalPages      int32
		durationSeconds float64
		errMessage      *string
		createdAt       time.Time
		completedAt     time.Time
	)

	err := r.db.QueryRow(ctx, selectTerminalRunSQL, cleanWorkflow).Scan(
		&rowWorkflowID,
		&rowRunID,
		&rowTenantID,
		&status,
		&totalSlices,
		&completedSlices,
		&failedSlices,
		&totalPages,
		&durationSeconds,
		&errMessage,
		&createdAt,
		&completedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: workflow %q not found in catalog", service.ErrWorkflowNotFound, cleanWorkflow)
		}
		return nil, fmt.Errorf("failed to query terminal run %q: %w", cleanWorkflow, err)
	}

	if rowTenantID != cleanTenant {
		return nil, fmt.Errorf("%w: workflow %q does not belong to tenant %q", service.ErrTenantMismatch, cleanWorkflow, cleanTenant)
	}

	// errMessage is intentionally scanned safely via *string to support NULL values without leaking internal DB details.
	_ = errMessage

	return &coordinatorv1.GetIngestionStatusResponse{
		WorkflowID:      rowWorkflowID,
		RunID:           rowRunID,
		TenantID:        rowTenantID,
		Status:          status,
		TotalSlices:     totalSlices,
		CompletedSlices: completedSlices,
		FailedSlices:    failedSlices,
		TotalPages:      totalPages,
		DurationSeconds: durationSeconds,
		CreatedAt:       createdAt.UTC().Format(time.RFC3339),
		CompletedAt:     completedAt.UTC().Format(time.RFC3339),
	}, nil
}
