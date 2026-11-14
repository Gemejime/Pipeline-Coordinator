-- Phase 4: Idempotent PostgreSQL schema initialization for rag-ingestion-coordinator.
-- Supports ArgoCD PreSync hook execution without dual-write checkpoint tables (ADR-003).

CREATE TABLE IF NOT EXISTS pipeline_configs (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    chunk_size_tokens INT NOT NULL DEFAULT 300 CHECK (chunk_size_tokens > 0),
    chunk_overlap_tokens INT NOT NULL DEFAULT 50 CHECK (chunk_overlap_tokens >= 0),
    ocr_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id VARCHAR(255) NOT NULL UNIQUE,
    run_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    document_id VARCHAR(255) NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('COMPLETED', 'FAILED', 'TERMINATED')),
    total_pages INT NOT NULL DEFAULT 0,
    total_slices INT NOT NULL DEFAULT 0,
    completed_slices INT NOT NULL DEFAULT 0,
    failed_slices INT NOT NULL DEFAULT 0,
    duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_configs_tenant
    ON pipeline_configs (tenant_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_tenant_created
    ON pipeline_runs (tenant_id, created_at DESC);

