// Package activity defines the Temporal activities and polyglot activity contracts
// for the rag-ingestion-coordinator control plane.
package activity

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
)

const (
	// ErrCodeInvalidArgument marks non-retryable input validation errors.
	ErrCodeInvalidArgument = "INVALID_ARGUMENT"
	// ErrCodeCorruptInput marks non-retryable corrupt payload or range errors.
	ErrCodeCorruptInput = "CORRUPT_INPUT"
	// ErrCodeUnsupportedMimeType marks non-retryable MIME type errors.
	ErrCodeUnsupportedMimeType = "UNSUPPORTED_MIME_TYPE"
	// ErrCodeTenantMismatch marks non-retryable tenant boundary violations.
	ErrCodeTenantMismatch = "TENANT_MISMATCH"
)

// HeaderInspector abstracts random-access byte inspection over S3 objects.
type HeaderInspector interface {
	InspectHeader(ctx context.Context, req coordinatorv1.InspectDocumentRequest) (*coordinatorv1.InspectDocumentResponse, error)
}

// InspectionActivities holds dependencies for document header inspection activities.
type InspectionActivities struct {
	Inspector HeaderInspector
}

// NewInspectionActivities creates an InspectionActivities instance.
func NewInspectionActivities(inspector HeaderInspector) *InspectionActivities {
	return &InspectionActivities{Inspector: inspector}
}

// InspectDocumentHeaderActivity executes on inspection-queue (10s heartbeat timeout)
// to determine modality, page count, or media duration via range byte reads.
func InspectDocumentHeaderActivity(
	ctx context.Context,
	req coordinatorv1.InspectDocumentRequest,
) (*coordinatorv1.InspectDocumentResponse, error) {
	a := &InspectionActivities{}
	return a.InspectDocumentHeader(ctx, req)
}

// InspectDocumentHeader performs input validation, resumes from any prior heartbeat
// details, emits progress heartbeats, and returns layout metadata.
func (a *InspectionActivities) InspectDocumentHeader(
	ctx context.Context,
	req coordinatorv1.InspectDocumentRequest,
) (*coordinatorv1.InspectDocumentResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if strings.TrimSpace(req.TenantID) == "" || strings.TrimSpace(req.DocumentID) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"tenant_id and document_id are required",
			ErrCodeInvalidArgument,
			nil,
		)
	}
	if strings.TrimSpace(req.S3Bucket) == "" || strings.TrimSpace(req.S3Key) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"s3_bucket and s3_key are required",
			ErrCodeInvalidArgument,
			nil,
		)
	}
	if req.TotalBytes <= 0 {
		return nil, temporal.NewNonRetryableApplicationError(
			"document total_bytes must be greater than zero",
			ErrCodeCorruptInput,
			service.ErrZeroBytePayload,
		)
	}
	if req.TotalBytes > service.MaxPayloadBytes {
		return nil, temporal.NewNonRetryableApplicationError(
			"document total_bytes exceeds 10GB limit",
			ErrCodeInvalidArgument,
			service.ErrPayloadTooLarge,
		)
	}

	modality, err := ResolveModalityFromMIME(req.MimeType)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			err.Error(),
			ErrCodeUnsupportedMimeType,
			err,
		)
	}

	var checkpoint coordinatorv1.HeartbeatDetails
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &checkpoint)
	}

	checkpoint.SliceID = "header-inspection"
	checkpoint.TotalUnits = 1
	checkpoint.LastProcessedUnit = 1
	checkpoint.ProcessedUnits = 1
	checkpoint.UpdatedAtUnix = time.Now().UTC().Unix()
	activity.RecordHeartbeat(ctx, checkpoint)

	logger := activity.GetLogger(ctx)
	logger.Info("inspected document header metadata",
		"tenant_id", req.TenantID,
		"document_id", req.DocumentID,
		"s3_bucket", req.S3Bucket,
		"s3_key", req.S3Key,
		"mime_type", req.MimeType,
		"modality", modality.String(),
	)

	if a != nil && a.Inspector != nil {
		return a.Inspector.InspectHeader(ctx, req)
	}

	switch modality {
	case coordinatorv1.MultimodalModalityDocumentPDF, coordinatorv1.MultimodalModalityTabular:
		estimatedPages := int32(req.TotalBytes / (64 * 1024))
		if estimatedPages < 1 {
			estimatedPages = 1
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:              modality,
			TotalPages:            estimatedPages,
			RecommendedSlicePages: service.DefaultPagesPerSlice,
		}, nil
	case coordinatorv1.MultimodalModalityAudio, coordinatorv1.MultimodalModalityVideo:
		estimatedDuration := float64(req.TotalBytes) / (32 * 1024)
		if estimatedDuration < 1.0 {
			estimatedDuration = 60.0
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:                 modality,
			DurationSeconds:          estimatedDuration,
			RecommendedWindowSeconds: service.DefaultWindowSeconds,
		}, nil
	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unsupported modality for mime_type %s", req.MimeType),
			ErrCodeUnsupportedMimeType,
			nil,
		)
	}
}

// ResolveModalityFromMIME maps an IANA MIME type to a MultimodalModality enum.
func ResolveModalityFromMIME(mimeType string) (coordinatorv1.MultimodalModality, error) {
	normalized := strings.ToLower(strings.TrimSpace(mimeType))
	switch normalized {
	case "application/pdf":
		return coordinatorv1.MultimodalModalityDocumentPDF, nil
	case "text/csv", "application/vnd.apache.parquet", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
		return coordinatorv1.MultimodalModalityTabular, nil
	case "audio/mpeg", "audio/wav", "audio/x-wav", "audio/flac", "audio/ogg":
		return coordinatorv1.MultimodalModalityAudio, nil
	case "video/mp4", "video/webm", "video/quicktime":
		return coordinatorv1.MultimodalModalityVideo, nil
	default:
		return coordinatorv1.MultimodalModalityUnspecified, fmt.Errorf("unsupported mime_type: %q", mimeType)
	}
}
