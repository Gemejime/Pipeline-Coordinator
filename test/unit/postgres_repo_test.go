package unit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/store/postgres"
)

type mockPGDB struct {
	execFn     func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
}

func (m *mockPGDB) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if m.execFn != nil {
		return m.execFn(ctx, sql, arguments...)
	}
	return pgconn.CommandTag{}, errors.New("Exec not implemented")
}

func (m *mockPGDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, sql, args...)
	}
	return mockPGRow{err: errors.New("QueryRow not implemented")}
}

type mockPGRow struct {
	scanFn func(dest ...any) error
	err    error
}

func (r mockPGRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

func TestPostgresClient_PgBouncerConfigAndDSNRedaction(t *testing.T) {
	t.Parallel()

	t.Run("missing DSN fails", func(t *testing.T) {
		t.Parallel()
		_, err := postgres.ParsePoolConfig(postgres.Config{})
		require.ErrorIs(t, err, postgres.ErrMissingDSN)
	})

	t.Run("defaults to QueryExecModeExec for PgBouncer transaction pooling", func(t *testing.T) {
		t.Parallel()
		poolCfg, err := postgres.ParsePoolConfig(postgres.Config{
			DSN:     "postgres://coord_user:supersecret@pgbouncer.internal:6432/rag_db",
			SSLMode: "require",
		})
		require.NoError(t, err)
		assert.Equal(t, pgx.QueryExecModeExec, poolCfg.ConnConfig.DefaultQueryExecMode)
		assert.Equal(t, postgres.DefaultMaxConns, poolCfg.MaxConns)
		assert.Equal(t, postgres.DefaultMinConns, poolCfg.MinConns)
		assert.Equal(t, postgres.DefaultMaxConnLifetime, poolCfg.MaxConnLifetime)
		assert.Equal(t, postgres.DefaultMaxConnIdleTime, poolCfg.MaxConnIdleTime)
		assert.Equal(t, postgres.DefaultHealthCheckPeriod, poolCfg.HealthCheckPeriod)
		require.NotNil(t, poolCfg.ConnConfig.TLSConfig)
	})

	t.Run("respects explicit QueryExecModeSimpleProtocol", func(t *testing.T) {
		t.Parallel()
		poolCfg, err := postgres.ParsePoolConfig(postgres.Config{
			DSN:           "postgres://coord_user:supersecret@localhost:5432/rag_db?sslmode=disable",
			QueryExecMode: pgx.QueryExecModeSimpleProtocol,
			MaxConns:      10,
			MinConns:      1,
		})
		require.NoError(t, err)
		assert.Equal(t, pgx.QueryExecModeSimpleProtocol, poolCfg.ConnConfig.DefaultQueryExecMode)
		assert.Equal(t, int32(10), poolCfg.MaxConns)
		assert.Equal(t, int32(1), poolCfg.MinConns)
	})

	t.Run("SanitizeDSN redacts passwords from URL and key-value DSNs", func(t *testing.T) {
		t.Parallel()
		urlDSN := "postgres://admin:S3cr3tPass!@db.internal:5432/rag?sslmode=require"
		redactedURL := postgres.SanitizeDSN(urlDSN)
		assert.NotContains(t, redactedURL, "S3cr3tPass!")
		assert.Contains(t, redactedURL, "REDACTED")

		kvDSN := "host=db.internal user=admin password=MySecretPassword dbname=rag sslmode=require"
		redactedKV := postgres.SanitizeDSN(kvDSN)
		assert.NotContains(t, redactedKV, "MySecretPassword")
		assert.Contains(t, redactedKV, "password=REDACTED")
	})
}

func TestRunRepository_RecordAndGetTerminalRun(t *testing.T) {
	t.Parallel()

	t.Run("atomic upsert returns generated record UUID", func(t *testing.T) {
		t.Parallel()
		var capturedArgs []any
		repo := postgres.NewRunRepository(&mockPGDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				assert.Contains(t, sql, "ON CONFLICT (workflow_id) DO UPDATE")
				assert.Contains(t, sql, "WHERE pipeline_runs.tenant_id = EXCLUDED.tenant_id")
				capturedArgs = args
				return mockPGRow{
					scanFn: func(dest ...any) error {
						*(dest[0].(*string)) = "550e8400-e29b-41d4-a716-446655440000"
						return nil
					},
				}
			},
		})

		recID, err := repo.RecordTerminalRun(context.Background(), coordinatorv1.CatalogTerminalRunRequest{
			WorkflowID:      "ingest-tenant-01-hash01",
			RunID:           "run-01",
			TenantID:        "tenant-01",
			DocumentID:      "doc-01",
			ContentHash:     "abc123",
			Status:          "COMPLETED",
			TotalPages:      200,
			TotalSlices:     4,
			CompletedSlices: 4,
			FailedSlices:    0,
			DurationSeconds: 18.4,
		})
		require.NoError(t, err)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", recID)
		require.Len(t, capturedArgs, 12)
		assert.Equal(t, "ingest-tenant-01-hash01", capturedArgs[0])
		assert.Equal(t, "COMPLETED", capturedArgs[5])
	})

	t.Run("cross-tenant conflict on upsert returns ErrTenantMismatch", func(t *testing.T) {
		t.Parallel()
		repo := postgres.NewRunRepository(&mockPGDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockPGRow{err: pgx.ErrNoRows}
			},
		})

		_, err := repo.RecordTerminalRun(context.Background(), coordinatorv1.CatalogTerminalRunRequest{
			WorkflowID: "ingest-tenant-01-hash01",
			RunID:      "run-02",
			TenantID:   "tenant-02",
			DocumentID: "doc-02",
			Status:     "FAILED",
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, service.ErrTenantMismatch)
	})

	t.Run("GetTerminalRun formats RFC3339 timestamps and handles nullable error_message", func(t *testing.T) {
		t.Parallel()
		created := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
		completed := time.Date(2026, 11, 10, 8, 1, 15, 0, time.UTC)

		repo := postgres.NewRunRepository(&mockPGDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockPGRow{
					scanFn: func(dest ...any) error {
						*(dest[0].(*string)) = "ingest-tenant-01-hash01"
						*(dest[1].(*string)) = "run-01"
						*(dest[2].(*string)) = "tenant-01"
						*(dest[3].(*string)) = "COMPLETED"
						*(dest[4].(*int32)) = 8
						*(dest[5].(*int32)) = 8
						*(dest[6].(*int32)) = 0
						*(dest[7].(*int32)) = 400
						*(dest[8].(*float64)) = 75.0
						*(dest[9].(**string)) = nil // NULL error_message
						*(dest[10].(*time.Time)) = created
						*(dest[11].(*time.Time)) = completed
						return nil
					},
				}
			},
		})

		resp, err := repo.GetTerminalRun(context.Background(), "tenant-01", "ingest-tenant-01-hash01")
		require.NoError(t, err)
		assert.Equal(t, "ingest-tenant-01-hash01", resp.WorkflowID)
		assert.Equal(t, "run-01", resp.RunID)
		assert.Equal(t, "COMPLETED", resp.Status)
		assert.Equal(t, int32(400), resp.TotalPages)
		assert.Equal(t, "2026-11-10T08:00:00Z", resp.CreatedAt)
		assert.Equal(t, "2026-11-10T08:01:15Z", resp.CompletedAt)
	})

	t.Run("GetTerminalRun rejects cross-tenant read without leaking row", func(t *testing.T) {
		t.Parallel()
		repo := postgres.NewRunRepository(&mockPGDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockPGRow{
					scanFn: func(dest ...any) error {
						*(dest[0].(*string)) = "ingest-tenant-01-hash01"
						*(dest[1].(*string)) = "run-01"
						*(dest[2].(*string)) = "tenant-01"
						*(dest[3].(*string)) = "COMPLETED"
						return nil
					},
				}
			},
		})

		resp, err := repo.GetTerminalRun(context.Background(), "tenant-other", "ingest-tenant-01-hash01")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.ErrorIs(t, err, service.ErrTenantMismatch)
	})

	t.Run("GetTerminalRun returns ErrWorkflowNotFound on pgx.ErrNoRows", func(t *testing.T) {
		t.Parallel()
		repo := postgres.NewRunRepository(&mockPGDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockPGRow{err: pgx.ErrNoRows}
			},
		})

		_, err := repo.GetTerminalRun(context.Background(), "tenant-01", "ingest-tenant-01-missing")
		require.Error(t, err)
		assert.ErrorIs(t, err, service.ErrWorkflowNotFound)
	})
}

func TestPipelineRepo_TenantIsolation(t *testing.T) {
	t.Parallel()

	t.Run("GetPipelineConfig returns config for matching tenant", func(t *testing.T) {
		t.Parallel()
		repo := postgres.NewPipelineRepo(&mockPGDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockPGRow{
					scanFn: func(dest ...any) error {
						*(dest[0].(*string)) = "cfg-finance-v1"
						*(dest[1].(*string)) = "tenant-01"
						*(dest[2].(*string)) = "Finance Dense OCR"
						*(dest[3].(*int32)) = 512
						*(dest[4].(*int32)) = 64
						*(dest[5].(*bool)) = true
						return nil
					},
				}
			},
		})

		cfg, err := repo.GetPipelineConfig(context.Background(), "tenant-01", "cfg-finance-v1")
		require.NoError(t, err)
		assert.Equal(t, "cfg-finance-v1", cfg.ID)
		assert.Equal(t, int32(512), cfg.ChunkSizeTokens)
		assert.Equal(t, int32(64), cfg.ChunkOverlapTokens)
		assert.True(t, cfg.OCREnabled)
	})

	t.Run("GetPipelineConfig rejects cross-tenant read", func(t *testing.T) {
		t.Parallel()
		repo := postgres.NewPipelineRepo(&mockPGDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return mockPGRow{
					scanFn: func(dest ...any) error {
						*(dest[0].(*string)) = "cfg-finance-v1"
						*(dest[1].(*string)) = "tenant-01"
						*(dest[2].(*string)) = "Finance Dense OCR"
						*(dest[3].(*int32)) = 512
						*(dest[4].(*int32)) = 64
						*(dest[5].(*bool)) = true
						return nil
					},
				}
			},
		})

		_, err := repo.GetPipelineConfig(context.Background(), "tenant-02", "cfg-finance-v1")
		require.Error(t, err)
		assert.ErrorIs(t, err, service.ErrTenantMismatch)
	})

	t.Run("SavePipelineConfig rejects cross-tenant overwrite when 0 rows affected", func(t *testing.T) {
		t.Parallel()
		repo := postgres.NewPipelineRepo(&mockPGDB{
			execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				assert.Contains(t, sql, "WHERE pipeline_configs.tenant_id = EXCLUDED.tenant_id")
				return pgconn.NewCommandTag("INSERT 0 0"), nil
			},
		})

		err := repo.SavePipelineConfig(context.Background(), coordinatorv1.PipelineConfig{
			ID:                 "cfg-finance-v1",
			TenantID:           "tenant-attacker",
			Name:               "Hijacked",
			ChunkSizeTokens:    300,
			ChunkOverlapTokens: 50,
			OCREnabled:         false,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, service.ErrTenantMismatch)
	})

	t.Run("SavePipelineConfig succeeds when 1 row affected", func(t *testing.T) {
		t.Parallel()
		repo := postgres.NewPipelineRepo(&mockPGDB{
			execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				return pgconn.NewCommandTag("INSERT 0 1"), nil
			},
		})

		err := repo.SavePipelineConfig(context.Background(), coordinatorv1.PipelineConfig{
			ID:                 "cfg-finance-v1",
			TenantID:           "tenant-01",
			Name:               "Finance Dense OCR",
			ChunkSizeTokens:    400,
			ChunkOverlapTokens: 40,
			OCREnabled:         true,
		})
		require.NoError(t, err)
	})
}
