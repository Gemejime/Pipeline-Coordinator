// Package redis provides a production go-redis UniversalClient telemetry publisher
// supporting Standalone, Sentinel, and Cluster topologies with memory-bounded
// monotonic sequence tracking (ADR-004).
package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/telemetry"
)

const (
	// MaxTrackedWorkflowSequences caps in-memory sequence state to preserve the <256 MiB RSS ceiling.
	MaxTrackedWorkflowSequences = 10000
	// DefaultSequenceTTL defines when idle workflow sequence entries become eligible for eviction.
	DefaultSequenceTTL = 30 * time.Minute
)

var (
	// ErrInvalidTelemetryEvent indicates missing required fields or non-positive sequence number.
	ErrInvalidTelemetryEvent = errors.New("INVALID_ARGUMENT: invalid progress milestone event")
	// ErrOutOfOrderSequence indicates a monotonic sequence regression for a workflow.
	ErrOutOfOrderSequence = errors.New("OUT_OF_ORDER_SEQUENCE: milestone sequence_number regressed")
	// ErrNoRedisAddrs indicates that no Redis addresses were configured.
	ErrNoRedisAddrs = errors.New("redis addresses must not be empty")
)

// PubSubClient abstracts go-redis UniversalClient operations for unit testing and production execution.
type PubSubClient interface {
	Publish(ctx context.Context, channel string, message any) *redis.IntCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

// Config configures Redis Standalone, Sentinel, or Cluster connections and sequence guard bounds.
type Config struct {
	Addrs               []string
	MasterName          string
	Username            string
	Password            string
	DB                  int
	TLSEnabled          bool
	TLSConfig           *tls.Config
	DialTimeout         time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	MaxTrackedSequences int
	SequenceTTL         time.Duration
	Metrics             *telemetry.Metrics
	NowFunc             func() time.Time
}

type sequenceEntry struct {
	seq      int64
	lastSeen time.Time
}

// Publisher broadcasts ProgressMilestoneEvent JSON frames to ingestion:events:<tenant_id>.
type Publisher struct {
	client       PubSubClient
	metrics      *telemetry.Metrics
	maxSequences int
	sequenceTTL  time.Duration
	nowFunc      func() time.Time

	mu        sync.Mutex
	sequences map[string]sequenceEntry
}

// BuildUniversalOptions constructs go-redis UniversalOptions supporting Standalone, Sentinel, and Cluster topologies.
func BuildUniversalOptions(cfg Config) (*redis.UniversalOptions, error) {
	addrs := make([]string, 0, len(cfg.Addrs))
	for _, addr := range cfg.Addrs {
		if trimmed := strings.TrimSpace(addr); trimmed != "" {
			addrs = append(addrs, trimmed)
		}
	}
	if len(addrs) == 0 {
		return nil, ErrNoRedisAddrs
	}

	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = 3 * time.Second
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 3 * time.Second
	}

	tlsCfg := cfg.TLSConfig
	if tlsCfg == nil && cfg.TLSEnabled {
		tlsCfg = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	return &redis.UniversalOptions{
		Addrs:        addrs,
		MasterName:   strings.TrimSpace(cfg.MasterName),
		Username:     strings.TrimSpace(cfg.Username),
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		TLSConfig:    tlsCfg,
	}, nil
}

// NewPublisher constructs a Publisher backed by a go-redis UniversalClient.
func NewPublisher(cfg Config) (*Publisher, error) {
	opts, err := BuildUniversalOptions(cfg)
	if err != nil {
		return nil, err
	}
	client := redis.NewUniversalClient(opts)
	return NewPublisherWithClient(client, cfg)
}

// NewPublisherWithClient constructs a Publisher wrapping an existing PubSubClient.
func NewPublisherWithClient(client PubSubClient, cfg Config) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("redis client must not be nil")
	}
	maxSeq := cfg.MaxTrackedSequences
	if maxSeq <= 0 || maxSeq > MaxTrackedWorkflowSequences {
		maxSeq = MaxTrackedWorkflowSequences
	}
	seqTTL := cfg.SequenceTTL
	if seqTTL <= 0 {
		seqTTL = DefaultSequenceTTL
	}
	nowFunc := cfg.NowFunc
	if nowFunc == nil {
		nowFunc = func() time.Time { return time.Now().UTC() }
	}
	return &Publisher{
		client:       client,
		metrics:      cfg.Metrics,
		maxSequences: maxSeq,
		sequenceTTL:  seqTTL,
		nowFunc:      nowFunc,
		sequences:    make(map[string]sequenceEntry),
	}, nil
}

// PublishMilestone validates, sequence-checks, serializes, and publishes a ProgressMilestoneEvent to Redis Pub/Sub.
func (p *Publisher) PublishMilestone(
	ctx context.Context,
	channel string,
	event coordinatorv1.ProgressMilestoneEvent,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || p.client == nil {
		return errors.New("redis milestone publisher is not initialized")
	}

	if err := validateMilestoneEvent(event); err != nil {
		p.metrics.IncRedisEventError()
		return err
	}

	expectedChannel := activity.RedisIngestionChannel(event.TenantID)
	targetChannel := strings.TrimSpace(channel)
	if targetChannel == "" {
		targetChannel = expectedChannel
	}
	if targetChannel != expectedChannel {
		p.metrics.IncRedisEventError()
		return fmt.Errorf("%w: channel %q does not match tenant channel %q", service.ErrTenantMismatch, targetChannel, expectedChannel)
	}

	wfKey := event.TenantID + ":" + event.WorkflowID
	if err := p.checkSequence(wfKey, event.SequenceNumber); err != nil {
		p.metrics.IncRedisEventError()
		return err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		p.metrics.IncRedisEventError()
		return fmt.Errorf("failed to marshal milestone event: %w", err)
	}

	if err := p.client.Publish(ctx, targetChannel, payload).Err(); err != nil {
		p.metrics.IncRedisEventError()
		return fmt.Errorf("failed to publish milestone to %s: %w", targetChannel, err)
	}

	p.recordSequence(wfKey, event.SequenceNumber)
	p.metrics.IncRedisEventPublished()
	return nil
}

// Ping verifies active Redis broker connectivity for /ready probing.
func (p *Publisher) Ping(ctx context.Context) error {
	if p == nil || p.client == nil {
		return errors.New("redis milestone publisher is not initialized")
	}
	return p.client.Ping(ctx).Err()
}

// Close terminates the underlying Redis client connections cleanly.
func (p *Publisher) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.Close()
}

// TrackedWorkflowCount returns the number of workflow sequence entries currently held in memory.
func (p *Publisher) TrackedWorkflowCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sequences)
}

func (p *Publisher) checkSequence(wfKey string, seq int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, exists := p.sequences[wfKey]; exists {
		if seq < entry.seq {
			return fmt.Errorf("%w: got %d after %d", ErrOutOfOrderSequence, seq, entry.seq)
		}
	}
	return nil
}

func (p *Publisher) recordSequence(wfKey string, seq int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.nowFunc()
	if _, exists := p.sequences[wfKey]; !exists && len(p.sequences) >= p.maxSequences {
		p.evictStaleOrOldestLocked(now)
	}
	p.sequences[wfKey] = sequenceEntry{
		seq:      seq,
		lastSeen: now,
	}
}

func (p *Publisher) evictStaleOrOldestLocked(now time.Time) {
	var oldestKey string
	var oldestTime time.Time
	evicted := false

	for k, entry := range p.sequences {
		if now.Sub(entry.lastSeen) >= p.sequenceTTL {
			delete(p.sequences, k)
			evicted = true
		} else if oldestKey == "" || entry.lastSeen.Before(oldestTime) {
			oldestKey = k
			oldestTime = entry.lastSeen
		}
	}
	if !evicted && oldestKey != "" {
		delete(p.sequences, oldestKey)
	}
}

func validateMilestoneEvent(ev coordinatorv1.ProgressMilestoneEvent) error {
	if strings.TrimSpace(ev.EventID) == "" {
		return fmt.Errorf("%w: event_id is required", ErrInvalidTelemetryEvent)
	}
	if strings.TrimSpace(ev.TenantID) == "" || strings.TrimSpace(ev.DocumentID) == "" || strings.TrimSpace(ev.WorkflowID) == "" {
		return fmt.Errorf("%w: tenant_id, document_id, and workflow_id are required", ErrInvalidTelemetryEvent)
	}
	if strings.TrimSpace(ev.Stage) == "" || strings.TrimSpace(ev.Status) == "" {
		return fmt.Errorf("%w: stage and status are required", ErrInvalidTelemetryEvent)
	}
	if ev.SequenceNumber < 1 {
		return fmt.Errorf("%w: sequence_number must be >= 1", ErrInvalidTelemetryEvent)
	}
	return nil
}
