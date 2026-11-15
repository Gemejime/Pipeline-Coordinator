package unit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	redisstore "rag-ingestion-coordinator/internal/store/redis"
	"rag-ingestion-coordinator/internal/telemetry"
)

type publishedFrame struct {
	channel string
	payload []byte
}

type mockRedisClient struct {
	publishFn func(ctx context.Context, channel string, message any) *redis.IntCmd
	pingFn    func(ctx context.Context) *redis.StatusCmd
	closeFn   func() error
	frames    []publishedFrame
}

func (m *mockRedisClient) Publish(ctx context.Context, channel string, message any) *redis.IntCmd {
	if m.publishFn != nil {
		return m.publishFn(ctx, channel, message)
	}
	raw, _ := message.([]byte)
	m.frames = append(m.frames, publishedFrame{channel: channel, payload: raw})
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(1)
	return cmd
}

func (m *mockRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	if m.pingFn != nil {
		return m.pingFn(ctx)
	}
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("PONG")
	return cmd
}

func (m *mockRedisClient) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func TestRedisUniversalOptions_Topologies(t *testing.T) {
	t.Parallel()

	t.Run("standalone topology", func(t *testing.T) {
		t.Parallel()
		opts, err := redisstore.BuildUniversalOptions(redisstore.Config{
			Addrs:      []string{"redis.internal:6379"},
			DB:         2,
			TLSEnabled: true,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"redis.internal:6379"}, opts.Addrs)
		assert.Empty(t, opts.MasterName)
		assert.Equal(t, 2, opts.DB)
		require.NotNil(t, opts.TLSConfig)
	})

	t.Run("sentinel topology", func(t *testing.T) {
		t.Parallel()
		opts, err := redisstore.BuildUniversalOptions(redisstore.Config{
			Addrs:      []string{"sentinel-1:26379", "sentinel-2:26379"},
			MasterName: "mymaster",
		})
		require.NoError(t, err)
		assert.Equal(t, "mymaster", opts.MasterName)
		assert.Len(t, opts.Addrs, 2)
	})

	t.Run("cluster topology", func(t *testing.T) {
		t.Parallel()
		opts, err := redisstore.BuildUniversalOptions(redisstore.Config{
			Addrs: []string{"node-1:6379", "node-2:6379", "node-3:6379"},
		})
		require.NoError(t, err)
		assert.Empty(t, opts.MasterName)
		assert.Len(t, opts.Addrs, 3)
	})

	t.Run("empty addrs fails", func(t *testing.T) {
		t.Parallel()
		_, err := redisstore.BuildUniversalOptions(redisstore.Config{})
		require.ErrorIs(t, err, redisstore.ErrNoRedisAddrs)
	})
}

func TestRedisPublisher_PublishMilestone_MonotonicAndMetrics(t *testing.T) {
	t.Parallel()

	metrics := telemetry.NewMetrics()
	mockClient := &mockRedisClient{}
	pub, err := redisstore.NewPublisherWithClient(mockClient, redisstore.Config{
		Metrics: metrics,
	})
	require.NoError(t, err)

	baseEvent := coordinatorv1.ProgressMilestoneEvent{
		EventID:         "evt-1",
		SequenceNumber:  1,
		TenantID:        "tenant-corp-01",
		DocumentID:      "doc-01",
		WorkflowID:      "ingest-tenant-corp-01-hash",
		Stage:           "INSPECTING",
		Status:          "PROGRESS",
		ProgressPct:     10.0,
		SlicesCompleted: 0,
		TotalSlices:     4,
		TimestampUnix:   1794039630,
	}

	require.NoError(t, pub.PublishMilestone(context.Background(), "ingestion:events:tenant-corp-01", baseEvent))
	require.Len(t, mockClient.frames, 1)
	assert.Equal(t, "ingestion:events:tenant-corp-01", mockClient.frames[0].channel)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(mockClient.frames[0].payload, &decoded))
	assert.Equal(t, "evt-1", decoded["event_id"])
	assert.Equal(t, float64(1), decoded["sequence_number"])
	assert.Equal(t, "tenant-corp-01", decoded["tenant_id"])
	assert.Equal(t, "INSPECTING", decoded["stage"])

	// Idempotent Temporal retry with identical sequence_number (1 == 1) is allowed.
	require.NoError(t, pub.PublishMilestone(context.Background(), "ingestion:events:tenant-corp-01", baseEvent))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.RedisEventsPublishedTotal))

	// Advancing to sequence 2 succeeds.
	ev2 := baseEvent
	ev2.EventID = "evt-2"
	ev2.SequenceNumber = 2
	ev2.Stage = "EXTRACT_SEGMENTS"
	require.NoError(t, pub.PublishMilestone(context.Background(), "ingestion:events:tenant-corp-01", ev2))
	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.RedisEventsPublishedTotal))

	// Regression to sequence 1 is rejected and increments error counter.
	err = pub.PublishMilestone(context.Background(), "ingestion:events:tenant-corp-01", baseEvent)
	require.ErrorIs(t, err, redisstore.ErrOutOfOrderSequence)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.RedisEventsErrorsTotal))

	// Cross-tenant channel mismatch is rejected.
	err = pub.PublishMilestone(context.Background(), "ingestion:events:other-tenant", ev2)
	require.ErrorIs(t, err, service.ErrTenantMismatch)
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.RedisEventsErrorsTotal))
}

func TestRedisPublisher_ErrorCountingAndBoundedMemory(t *testing.T) {
	t.Parallel()

	metrics := telemetry.NewMetrics()
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

	mockClient := &mockRedisClient{
		publishFn: func(ctx context.Context, channel string, message any) *redis.IntCmd {
			cmd := redis.NewIntCmd(ctx)
			cmd.SetErr(errors.New("redis connection reset"))
			return cmd
		},
	}

	pub, err := redisstore.NewPublisherWithClient(mockClient, redisstore.Config{
		Metrics:             metrics,
		MaxTrackedSequences: 5,
		SequenceTTL:         5 * time.Minute,
		NowFunc:             func() time.Time { return now },
	})
	require.NoError(t, err)

	ev := coordinatorv1.ProgressMilestoneEvent{
		EventID:        "evt-fail",
		SequenceNumber: 1,
		TenantID:       "tenant-01",
		DocumentID:     "doc-01",
		WorkflowID:     "wf-fail",
		Stage:          "INSPECTING",
		Status:         "PROGRESS",
	}

	err = pub.PublishMilestone(context.Background(), "", ev)
	require.Error(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.RedisEventsErrorsTotal))
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.RedisEventsPublishedTotal))

	// Switch to healthy client and verify bounded sequence eviction across > MaxTrackedSequences workflows.
	mockClient.publishFn = nil
	for i := 0; i < 12; i++ {
		now = now.Add(time.Second)
		wEv := ev
		wEv.EventID = fmt.Sprintf("evt-%d", i)
		wEv.WorkflowID = fmt.Sprintf("wf-%d", i)
		require.NoError(t, pub.PublishMilestone(context.Background(), "", wEv))
	}
	assert.LessOrEqual(t, pub.TrackedWorkflowCount(), 5, "sequence tracker must stay strictly bounded")
	require.NoError(t, pub.Ping(context.Background()))
	require.NoError(t, pub.Close())
}
