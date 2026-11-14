package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	pgstore "rag-ingestion-coordinator/internal/store/postgres"
	redisstore "rag-ingestion-coordinator/internal/store/redis"
	"rag-ingestion-coordinator/internal/telemetry"
	"rag-ingestion-coordinator/migrations"
)

func requireDockerDaemon(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		t.Skipf("skipping integration test: Docker daemon unavailable (%v)", err)
	}
}

// TestPhase4_PostgresMigrationsAndTerminalCatalog exercises embedded SQL migrations,
// PgBouncer-safe pgx queries, tenant isolation, and atomic terminal run persistence against PostgreSQL 16.
func TestPhase4_PostgresMigrationsAndTerminalCatalog(t *testing.T) {
	requireDockerDaemon(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "coordinator",
				"POSTGRES_PASSWORD": "secret",
				"POSTGRES_DB":       "coordinator_test",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(45 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pgContainer.Terminate(context.Background())
	})

	host, err := pgContainer.Host(ctx)
	require.NoError(t, err)
	port, err := pgContainer.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	dsn := fmt.Sprintf("postgres://coordinator:secret@%s:%s/coordinator_test?sslmode=disable", host, port.Port())
	pgClient, err := pgstore.NewClient(ctx, pgstore.Config{
		DSN:     dsn,
		SSLMode: "disable",
	})
	require.NoError(t, err)
	defer pgClient.Close()

	// Verify idempotent embedded migration execution.
	require.NoError(t, pgClient.RunMigrations(ctx, migrations.FS))
	require.NoError(t, pgClient.RunMigrations(ctx, migrations.FS))

	pipelineRepo := pgstore.NewPipelineRepo(pgClient.Pool())
	cfg := coordinatorv1.PipelineConfig{
		ID:                 "cfg-integration-01",
		TenantID:           "tenant-alpha",
		Name:               "Financial OCR",
		ChunkSizeTokens:    512,
		ChunkOverlapTokens: 64,
		OCREnabled:         true,
	}
	require.NoError(t, pipelineRepo.SavePipelineConfig(ctx, cfg))

	gotCfg, err := pipelineRepo.GetPipelineConfig(ctx, "tenant-alpha", "cfg-integration-01")
	require.NoError(t, err)
	assert.Equal(t, int32(512), gotCfg.ChunkSizeTokens)
	assert.Equal(t, int32(64), gotCfg.ChunkOverlapTokens)
	assert.True(t, gotCfg.OCREnabled)

	// Cross-tenant read and overwrite must be denied.
	_, err = pipelineRepo.GetPipelineConfig(ctx, "tenant-beta", "cfg-integration-01")
	assert.ErrorIs(t, err, service.ErrPipelineConfigNotFound)

	crossCfg := cfg
	crossCfg.TenantID = "tenant-beta"
	err = pipelineRepo.SavePipelineConfig(ctx, crossCfg)
	assert.ErrorIs(t, err, service.ErrTenantMismatch)

	runRepo := pgstore.NewRunRepository(pgClient.Pool())
	recID, err := runRepo.RecordTerminalRun(ctx, coordinatorv1.CatalogTerminalRunRequest{
		WorkflowID:      "ingest-tenant-alpha-hash01",
		RunID:           "run-001",
		TenantID:        "tenant-alpha",
		DocumentID:      "doc-001",
		ContentHash:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:          "COMPLETED",
		TotalPages:      40,
		TotalSlices:     4,
		CompletedSlices: 4,
		FailedSlices:    0,
		DurationSeconds: 18.25,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, recID)

	// Idempotent upsert with same tenant updates status/duration without error.
	recID2, err := runRepo.RecordTerminalRun(ctx, coordinatorv1.CatalogTerminalRunRequest{
		WorkflowID:      "ingest-tenant-alpha-hash01",
		RunID:           "run-002",
		TenantID:        "tenant-alpha",
		DocumentID:      "doc-001",
		ContentHash:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:          "COMPLETED",
		TotalPages:      40,
		TotalSlices:     4,
		CompletedSlices: 4,
		FailedSlices:    0,
		DurationSeconds: 19.50,
	})
	require.NoError(t, err)
	assert.Equal(t, recID, recID2)

	// Cross-tenant overwrite on existing workflow_id must be rejected.
	_, err = runRepo.RecordTerminalRun(ctx, coordinatorv1.CatalogTerminalRunRequest{
		WorkflowID:      "ingest-tenant-alpha-hash01",
		RunID:           "run-003",
		TenantID:        "tenant-beta",
		DocumentID:      "doc-001",
		ContentHash:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:          "FAILED",
		DurationSeconds: 1.0,
	})
	assert.ErrorIs(t, err, service.ErrTenantMismatch)

	statusResp, err := runRepo.GetTerminalRun(ctx, "tenant-alpha", "ingest-tenant-alpha-hash01")
	require.NoError(t, err)
	assert.Equal(t, "run-002", statusResp.RunID)
	assert.Equal(t, int32(4), statusResp.CompletedSlices)
	assert.InDelta(t, 19.50, statusResp.DurationSeconds, 0.001)

	_, err = runRepo.GetTerminalRun(ctx, "tenant-beta", "ingest-tenant-alpha-hash01")
	assert.ErrorIs(t, err, service.ErrTenantMismatch)
}

// TestPhase4_RedisPubSubMilestoneBroadcast verifies live Pub/Sub delivery on
// ingestion:events:<tenant_id>, sequence monotonicity, and schema alignment against Redis 7.
func TestPhase4_RedisPubSubMilestoneBroadcast(t *testing.T) {
	requireDockerDaemon(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = redisContainer.Terminate(context.Background())
	})

	host, err := redisContainer.Host(ctx)
	require.NoError(t, err)
	port, err := redisContainer.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	metrics := telemetry.NewMetrics()
	pub, err := redisstore.NewPublisher(redisstore.Config{
		Addrs:   []string{addr},
		Metrics: metrics,
	})
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	require.NoError(t, pub.Ping(ctx))

	subClient := goredis.NewClient(&goredis.Options{Addr: addr})
	defer func() { _ = subClient.Close() }()

	channel := "ingestion:events:tenant-corp-01"
	pubsub := subClient.Subscribe(ctx, channel)
	defer func() { _ = pubsub.Close() }()
	_, err = pubsub.Receive(ctx)
	require.NoError(t, err)

	ev1 := coordinatorv1.ProgressMilestoneEvent{
		EventID:         "evt_live_01",
		SequenceNumber:  1,
		TenantID:        "tenant-corp-01",
		DocumentID:      "doc-live-01",
		WorkflowID:      "ingest-tenant-corp-01-live",
		Stage:           "EXTRACT_SEGMENTS",
		Status:          "PROGRESS",
		ProgressPct:     50.0,
		SlicesCompleted: 1,
		TotalSlices:     2,
		TimestampUnix:   1794039630,
	}
	require.NoError(t, pub.PublishMilestone(ctx, channel, ev1))

	msgCtx, msgCancel := context.WithTimeout(ctx, 5*time.Second)
	defer msgCancel()
	msg, err := pubsub.ReceiveMessage(msgCtx)
	require.NoError(t, err)
	assert.Equal(t, channel, msg.Channel)

	var decoded coordinatorv1.ProgressMilestoneEvent
	require.NoError(t, json.Unmarshal([]byte(msg.Payload), &decoded))
	assert.Equal(t, ev1, decoded)

	// Out-of-order replay should return an error without blocking or corrupting channel state.
	evStale := ev1
	evStale.EventID = "evt_stale"
	evStale.SequenceNumber = 1
	err = pub.PublishMilestone(ctx, channel, evStale)
	assert.ErrorIs(t, err, redisstore.ErrOutOfOrderSequence)
}
