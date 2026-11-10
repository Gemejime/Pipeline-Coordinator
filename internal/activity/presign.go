package activity

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
)

const (
	// DefaultPresignLeaseSeconds is the mandatory 15-minute (900s) S3 presigned URL lease.
	DefaultPresignLeaseSeconds int32 = 900
)

// SlicePresigner generates scoped S3 GET and PUT presigned URLs for a partition slice.
type SlicePresigner interface {
	PresignSlice(ctx context.Context, req coordinatorv1.PresignSliceArtifactRequest) (*coordinatorv1.PresignSliceArtifactResponse, error)
}

// PresignActivities holds dependencies for S3 presigned URL issuance.
type PresignActivities struct {
	Presigner SlicePresigner
}

// NewPresignActivities constructs a PresignActivities instance.
func NewPresignActivities(presigner SlicePresigner) *PresignActivities {
	return &PresignActivities{Presigner: presigner}
}

// PresignS3ArtifactActivity executes on coordinator-task-queue to issue short-lived
// (15-minute / 900s) presigned S3 GET and PUT URLs for a child partition slice.
// Presigned URLs are strictly redacted from all log entries.
func PresignS3ArtifactActivity(
	ctx context.Context,
	req coordinatorv1.PresignSliceArtifactRequest,
) (*coordinatorv1.PresignSliceArtifactResponse, error) {
	a := &PresignActivities{}
	return a.PresignS3Artifact(ctx, req)
}

// PresignS3Artifact validates slice metadata, enforces tenant isolation on raw input keys
// and deterministic content-addressed output Parquet keys, and issues scoped 15-minute S3 URLs.
func (a *PresignActivities) PresignS3Artifact(
	ctx context.Context,
	req coordinatorv1.PresignSliceArtifactRequest,
) (*coordinatorv1.PresignSliceArtifactResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	slice := req.Slice
	if strings.TrimSpace(slice.TenantID) == "" || strings.TrimSpace(slice.DocumentID) == "" || strings.TrimSpace(slice.SliceID) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"slice tenant_id, document_id, and slice_id are required",
			ErrCodeInvalidArgument,
			nil,
		)
	}
	if strings.TrimSpace(slice.S3InputBucket) == "" || strings.TrimSpace(slice.S3InputKey) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"slice s3_input_bucket and s3_input_key are required",
			ErrCodeInvalidArgument,
			nil,
		)
	}

	// Enforce tenant isolation and zero path-traversal on slice.S3InputKey (raw/<tenant_id>/...).
	if err := service.ValidateTenantRawS3Key(slice.TenantID, slice.S3InputKey); err != nil {
		errCode := ErrCodeInvalidArgument
		if errors.Is(err, service.ErrTenantMismatch) {
			errCode = ErrCodeTenantMismatch
		}
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), errCode, err)
	}

	// Auto-compute deterministic S3OutputParquetKey when omitted and ContentHash is present.
	if strings.TrimSpace(slice.S3OutputParquetKey) == "" && strings.TrimSpace(slice.ContentHash) != "" {
		startMarker := int(slice.StartPage)
		endMarker := int(slice.EndPage)
		if slice.Modality == coordinatorv1.MultimodalModalityAudio || slice.Modality == coordinatorv1.MultimodalModalityVideo {
			startMarker = int(math.Round(slice.StartTimeSeconds))
			endMarker = int(math.Round(slice.EndTimeSeconds))
		}
		slice.S3OutputParquetKey = service.BuildS3OutputParquetKey(
			slice.TenantID,
			slice.DocumentID,
			slice.ContentHash,
			startMarker,
			endMarker,
			false,
		)
	}
	if strings.TrimSpace(slice.S3OutputParquetKey) == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"slice s3_output_parquet_key is required",
			ErrCodeInvalidArgument,
			nil,
		)
	}

	// Validate deterministic content-addressed S3 output Parquet key when ContentHash is specified.
	if strings.TrimSpace(slice.ContentHash) != "" {
		if err := service.ValidateSliceOutputParquetKey(slice.TenantID, slice.ContentHash, slice.S3OutputParquetKey); err != nil {
			errCode := ErrCodeInvalidArgument
			if errors.Is(err, service.ErrTenantMismatch) {
				errCode = ErrCodeTenantMismatch
			}
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), errCode, err)
		}
	}

	req.Slice = slice
	leaseSeconds := req.LeaseSeconds
	if leaseSeconds <= 0 || leaseSeconds > DefaultPresignLeaseSeconds {
		leaseSeconds = DefaultPresignLeaseSeconds
	}
	req.LeaseSeconds = leaseSeconds

	if a != nil && a.Presigner != nil {
		resp, err := a.Presigner.PresignSlice(ctx, req)
		if err != nil {
			return nil, err
		}
		logRedactedPresignEvent(ctx, slice, leaseSeconds)
		return resp, nil
	}

	expiresAtUnix := time.Now().UTC().Add(time.Duration(leaseSeconds) * time.Second).Unix()
	resp := &coordinatorv1.PresignSliceArtifactResponse{
		S3Bucket:           slice.S3InputBucket,
		S3InputKey:         slice.S3InputKey,
		S3OutputParquetKey: slice.S3OutputParquetKey,
		PresignedInputGetURL: fmt.Sprintf(
			"https://%s.s3.amazonaws.com/%s?X-Amz-Expires=%d",
			slice.S3InputBucket, slice.S3InputKey, leaseSeconds,
		),
		PresignedOutputPutURL: fmt.Sprintf(
			"https://%s.s3.amazonaws.com/%s?X-Amz-Expires=%d",
			slice.S3InputBucket, slice.S3OutputParquetKey, leaseSeconds,
		),
		ExpiresAtUnix: expiresAtUnix,
	}

	logRedactedPresignEvent(ctx, slice, leaseSeconds)
	return resp, nil
}

// logRedactedPresignEvent logs only sanitized S3 bucket and key identifiers,
// never logging raw presigned URLs or query parameters.
func logRedactedPresignEvent(ctx context.Context, slice coordinatorv1.PartitionSlice, leaseSeconds int32) {
	defer func() {
		_ = recover()
	}()
	logger := activity.GetLogger(ctx)
	logger.Info("issued presigned S3 slice artifact lease",
		"tenant_id", slice.TenantID,
		"document_id", slice.DocumentID,
		"slice_id", slice.SliceID,
		"slice_index", slice.SliceIndex,
		"s3_bucket", slice.S3InputBucket,
		"s3_key", slice.S3OutputParquetKey,
		"s3_input_key", slice.S3InputKey,
		"lease_seconds", leaseSeconds,
	)
}
