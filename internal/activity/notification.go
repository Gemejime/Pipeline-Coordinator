package activity

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

// PublishMilestoneActivityName is the canonical Temporal activity registration name
// for Redis Pub/Sub milestone emission on coordinator-task-queue.
const PublishMilestoneActivityName = "PublishMilestoneActivity"

// ErrNilMilestonePublisher indicates that NotificationActivities was registered or invoked without a MilestonePublisher.
var ErrNilMilestonePublisher = errors.New("milestone publisher is not configured")

// MilestonePublisher abstracts Redis Pub/Sub milestone event broadcasting.
type MilestonePublisher interface {
	PublishMilestone(ctx context.Context, channel string, event coordinatorv1.ProgressMilestoneEvent) error
}

// NotificationActivities holds dependencies for Redis Pub/Sub progress event emission.
type NotificationActivities struct {
	Publisher MilestonePublisher
}

// NewNotificationActivities creates a NotificationActivities instance.
func NewNotificationActivities(publisher MilestonePublisher) *NotificationActivities {
	return &NotificationActivities{Publisher: publisher}
}

// Register binds the dependency-backed PublishMilestone activity method under
// PublishMilestoneActivityName on the provided Temporal ActivityRegistry.
// It returns ErrNilMilestonePublisher if the receiver or its Publisher is nil.
func (a *NotificationActivities) Register(reg worker.ActivityRegistry) error {
	if a == nil || a.Publisher == nil {
		return ErrNilMilestonePublisher
	}
	if reg == nil {
		return errors.New("activity registry is nil")
	}
	reg.RegisterActivityWithOptions(
		a.PublishMilestone,
		activity.RegisterOptions{Name: PublishMilestoneActivityName},
	)
	return nil
}

// RedisIngestionChannel returns the tenant-scoped Redis Pub/Sub channel name:
// ingestion:events:<tenant_id>.
func RedisIngestionChannel(tenantID string) string {
	return fmt.Sprintf("ingestion:events:%s", strings.TrimSpace(tenantID))
}

// PublishMilestoneActivity executes on coordinator-task-queue to broadcast a
// ProgressMilestoneEvent with a monotonic sequence_number to Redis Pub/Sub (ADR-004).
func PublishMilestoneActivity(
	ctx context.Context,
	req coordinatorv1.PublishMilestoneRequest,
) (*coordinatorv1.PublishMilestoneResponse, error) {
	a := &NotificationActivities{}
	return a.PublishMilestone(ctx, req)
}

// PublishMilestone validates and publishes the milestone frame to ingestion:events:<tenant_id>.
func (a *NotificationActivities) PublishMilestone(
	ctx context.Context,
	req coordinatorv1.PublishMilestoneRequest,
) (*coordinatorv1.PublishMilestoneResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ev := req.Event
	if strings.TrimSpace(ev.TenantID) == "" || strings.TrimSpace(ev.DocumentID) == "" || strings.TrimSpace(ev.WorkflowID) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"milestone event tenant_id, document_id, and workflow_id are required",
			ErrCodeInvalidArgument,
			nil,
		)
	}
	if ev.SequenceNumber <= 0 {
		return nil, temporal.NewNonRetryableApplicationError(
			"milestone event sequence_number must be >= 1",
			ErrCodeInvalidArgument,
			nil,
		)
	}

	channel := RedisIngestionChannel(ev.TenantID)
	if a != nil && a.Publisher != nil {
		if err := a.Publisher.PublishMilestone(ctx, channel, ev); err != nil {
			return nil, err
		}
	}

	logger := activity.GetLogger(ctx)
	logger.Info("published ingestion milestone event",
		"tenant_id", ev.TenantID,
		"document_id", ev.DocumentID,
		"workflow_id", ev.WorkflowID,
		"channel", channel,
		"stage", ev.Stage,
		"status", ev.Status,
		"sequence_number", ev.SequenceNumber,
		"slices_completed", ev.SlicesCompleted,
		"total_slices", ev.TotalSlices,
	)

	return &coordinatorv1.PublishMilestoneResponse{
		Channel:        channel,
		SequenceNumber: ev.SequenceNumber,
		Published:      true,
	}, nil
}
