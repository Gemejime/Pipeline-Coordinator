package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
)

// ErrPipelineConfigNotFound indicates that the requested pipeline_config_id does not exist.
var ErrPipelineConfigNotFound = service.ErrPipelineConfigNotFound

const selectPipelineConfigSQL = `
SELECT
	id,
	tenant_id,
	name,
	chunk_size_tokens,
	chunk_overlap_tokens,
	ocr_enabled
FROM pipeline_configs
WHERE id = $1;
`

const upsertPipelineConfigSQL = `
INSERT INTO pipeline_configs (
	id,
	tenant_id,
	name,
	chunk_size_tokens,
	chunk_overlap_tokens,
	ocr_enabled,
	created_at,
	updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, NOW(), NOW()
)
ON CONFLICT (id) DO UPDATE SET
	name = EXCLUDED.name,
	chunk_size_tokens = EXCLUDED.chunk_size_tokens,
	chunk_overlap_tokens = EXCLUDED.chunk_overlap_tokens,
	ocr_enabled = EXCLUDED.ocr_enabled,
	updated_at = NOW()
WHERE pipeline_configs.tenant_id = EXCLUDED.tenant_id;
`

// PipelineConfigRepository defines tenant-isolated CRUD operations for pipeline configurations.
type PipelineConfigRepository interface {
	GetPipelineConfig(ctx context.Context, tenantID, configID string) (*coordinatorv1.PipelineConfig, error)
	SavePipelineConfig(ctx context.Context, cfg coordinatorv1.PipelineConfig) error
}

// PipelineRepo persists and retrieves tenant-scoped pipeline configurations in PostgreSQL.
type PipelineRepo struct {
	db DBTX
}

// NewPipelineRepo constructs a PipelineRepo backed by a pgxpool.Pool or DBTX implementation.
func NewPipelineRepo(db DBTX) *PipelineRepo {
	return &PipelineRepo{db: db}
}

// GetPipelineConfig retrieves a pipeline configuration by id and enforces strict tenant ownership.
func (r *PipelineRepo) GetPipelineConfig(
	ctx context.Context,
	tenantID, configID string,
) (*coordinatorv1.PipelineConfig, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, errors.New("postgresql pipeline repository is not initialized")
	}

	cleanTenant := strings.TrimSpace(tenantID)
	cleanID := strings.TrimSpace(configID)
	if cleanTenant == "" || cleanID == "" {
		return nil, fmt.Errorf("%w: tenant_id and config_id are required", service.ErrInvalidExecutionRequest)
	}

	var cfg coordinatorv1.PipelineConfig
	err := r.db.QueryRow(ctx, selectPipelineConfigSQL, cleanID).Scan(
		&cfg.ID,
		&cfg.TenantID,
		&cfg.Name,
		&cfg.ChunkSizeTokens,
		&cfg.ChunkOverlapTokens,
		&cfg.OCREnabled,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: config_id %q", ErrPipelineConfigNotFound, cleanID)
		}
		return nil, fmt.Errorf("failed to query pipeline config %q: %w", cleanID, err)
	}

	if cfg.TenantID != cleanTenant {
		return nil, fmt.Errorf("%w: pipeline config %q does not belong to tenant %q", service.ErrTenantMismatch, cleanID, cleanTenant)
	}

	return &cfg, nil
}

// SavePipelineConfig atomically upserts a pipeline configuration, preventing cross-tenant overwrites.
func (r *PipelineRepo) SavePipelineConfig(ctx context.Context, cfg coordinatorv1.PipelineConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.db == nil {
		return errors.New("postgresql pipeline repository is not initialized")
	}

	cleanID := strings.TrimSpace(cfg.ID)
	cleanTenant := strings.TrimSpace(cfg.TenantID)
	cleanName := strings.TrimSpace(cfg.Name)
	if cleanID == "" || cleanTenant == "" || cleanName == "" {
		return fmt.Errorf("%w: pipeline config id, tenant_id, and name are required", service.ErrInvalidExecutionRequest)
	}
	if cfg.ChunkSizeTokens <= 0 {
		return fmt.Errorf("%w: chunk_size_tokens must be > 0", service.ErrInvalidExecutionRequest)
	}
	if cfg.ChunkOverlapTokens < 0 || cfg.ChunkOverlapTokens >= cfg.ChunkSizeTokens {
		return fmt.Errorf("%w: chunk_overlap_tokens must be >= 0 and < chunk_size_tokens", service.ErrInvalidExecutionRequest)
	}

	tag, err := r.db.Exec(
		ctx,
		upsertPipelineConfigSQL,
		cleanID,
		cleanTenant,
		cleanName,
		cfg.ChunkSizeTokens,
		cfg.ChunkOverlapTokens,
		cfg.OCREnabled,
	)
	if err != nil {
		return fmt.Errorf("failed to save pipeline config %q: %w", cleanID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: pipeline config %q belongs to another tenant", service.ErrTenantMismatch, cleanID)
	}
	return nil
}
