-- Reversible rollback migration for rag-ingestion-coordinator initial schema.

DROP INDEX IF EXISTS idx_pipeline_runs_tenant_created;
DROP INDEX IF EXISTS idx_pipeline_configs_tenant;
DROP TABLE IF EXISTS pipeline_runs;
DROP TABLE IF EXISTS pipeline_configs;

