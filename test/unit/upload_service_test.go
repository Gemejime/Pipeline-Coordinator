package unit_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/store/s3"
)

const validSHA256Hash = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"

// TestUploadService_SinglePartAndMultipartHandoff verifies single-part (<=100 MiB) vs multipart (>100 MiB up to 10 GiB) URL issuance and 15-minute lease bounds.
func TestUploadService_SinglePartAndMultipartHandoff(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	fakeAPI := s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1")
	presigner, err := s3.NewPresigner(fakeAPI, nil, s3.PresignerConfig{
		Endpoint:        "https://s3.amazonaws.com",
		Region:          "us-east-1",
		MaxLeaseSeconds: 1800, // Should clamp to 900s (15m)
		NowFunc:         func() time.Time { return fixedNow },
	})
	require.NoError(t, err)
	uploadSvc := service.NewUploadService(presigner, service.UploadServiceConfig{
		DefaultBucket: "rag-ingestion-bucket",
		LeaseSeconds:  3600, // Should clamp to 900s (15m)
		NowFunc:       func() time.Time { return fixedNow },
	})

	// 1. Single-part upload at exact 100 MiB boundary.
	singleReq := coordinatorv1.CreateUploadURLRequest{
		TenantID:           "tenant-corp-01",
		Filename:           "../dir/Quarterly Report (2026).pdf",
		ContentType:        "application/pdf",
		ContentLengthBytes: service.SinglePartUploadMaxBytes, // 100 MiB
		ContentHash:        validSHA256Hash,
	}
	singleResp, err := uploadSvc.CreateUploadURL(context.Background(), singleReq)
	require.NoError(t, err)
	assert.Equal(t, "tenant-corp-01", singleResp.TenantID)
	assert.Equal(t, "rag-ingestion-bucket", singleResp.S3Bucket)
	assert.Equal(t, "raw/tenant-corp-01/"+strings.ToLower(validSHA256Hash)+"/Quarterly_Report__2026_.pdf", singleResp.S3Key)
	assert.NotEmpty(t, singleResp.PresignedURL)
	assert.Contains(t, singleResp.PresignedURL, "X-Amz-Expires=900")
	assert.Contains(t, singleResp.PresignedURL, "X-Amz-Signature=")
	assert.Equal(t, int32(900), singleResp.LeaseSeconds)
	assert.Equal(t, fixedNow.Add(15*time.Minute).Unix(), singleResp.ExpiresAtUnix)
	assert.Empty(t, singleResp.Parts)
	assert.Zero(t, singleResp.PartSizeBytes)

	// 2. Multipart upload just above 100 MiB (250 MiB -> 3 parts of 100 MiB).
	multiReq := coordinatorv1.CreateUploadURLRequest{
		TenantID:           "tenant-corp-01",
		Filename:           "dataset-2026.parquet",
		ContentType:        "application/vnd.apache.parquet",
		ContentLengthBytes: 250 * 1024 * 1024,
		ContentHash:        validSHA256Hash,
	}
	multiResp, err := uploadSvc.CreateUploadURL(context.Background(), multiReq)
	require.NoError(t, err)
	assert.Empty(t, multiResp.PresignedURL, "multipart response must populate parts instead of single presigned_url")
	assert.Equal(t, int32(900), multiResp.LeaseSeconds)
	assert.Equal(t, service.DefaultMultipartPartSizeBytes, multiResp.PartSizeBytes)
	require.Len(t, multiResp.Parts, 3)
	for i, part := range multiResp.Parts {
		assert.Equal(t, int32(i+1), part.PartNumber)
		assert.Contains(t, part.PresignedURL, "partNumber=")
		assert.Contains(t, part.PresignedURL, "uploadId=")
		assert.Contains(t, part.PresignedURL, "X-Amz-Expires=900")
	}

	// 3. Maximum 10 GiB upload (10,240 MiB -> 103 parts of 100 MiB).
	tenGBReq := coordinatorv1.CreateUploadURLRequest{
		TenantID:           "tenant-corp-01",
		Filename:           "monolith-video.mp4",
		ContentType:        "video/mp4",
		ContentLengthBytes: service.MaxPayloadBytes,
		ContentHash:        validSHA256Hash,
	}
	tenGBResp, err := uploadSvc.CreateUploadURL(context.Background(), tenGBReq)
	require.NoError(t, err)
	require.Len(t, tenGBResp.Parts, 103)
	assert.Equal(t, int32(900), tenGBResp.LeaseSeconds)
}

// TestUploadService_ValidationGuards verifies rejection of >10 GiB payloads, 0-byte payloads, invalid SHA-256 hashes, unsupported MIME types, and unsafe filenames.
func TestUploadService_ValidationGuards(t *testing.T) {
	t.Parallel()

	presigner, err := s3.NewPresigner(s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1"), nil, s3.PresignerConfig{})
	require.NoError(t, err)
	uploadSvc := service.NewUploadService(presigner, service.UploadServiceConfig{
		DefaultBucket: "rag-ingestion-bucket",
	})

	baseReq := coordinatorv1.CreateUploadURLRequest{
		TenantID:           "tenant-corp-01",
		Filename:           "doc.pdf",
		ContentType:        "application/pdf",
		ContentLengthBytes: 1024,
		ContentHash:        validSHA256Hash,
	}

	t.Run("zero byte payload rejected", func(t *testing.T) {
		req := baseReq
		req.ContentLengthBytes = 0
		_, err := uploadSvc.CreateUploadURL(context.Background(), req)
		require.ErrorIs(t, err, service.ErrZeroBytePayload)
	})

	t.Run("over 10 GiB payload rejected", func(t *testing.T) {
		req := baseReq
		req.ContentLengthBytes = service.MaxPayloadBytes + 1
		_, err := uploadSvc.CreateUploadURL(context.Background(), req)
		require.ErrorIs(t, err, service.ErrPayloadTooLarge)
	})

	t.Run("unsupported MIME type rejected", func(t *testing.T) {
		req := baseReq
		req.ContentType = "application/x-msdownload"
		_, err := uploadSvc.CreateUploadURL(context.Background(), req)
		require.ErrorIs(t, err, service.ErrUnsupportedMimeType)
	})

	t.Run("invalid SHA-256 hash rejected", func(t *testing.T) {
		req := baseReq
		req.ContentHash = "not-a-valid-sha256"
		_, err := uploadSvc.CreateUploadURL(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidUploadRequest)
	})

	t.Run("dot-dot filename traversal rejected", func(t *testing.T) {
		req := baseReq
		req.Filename = ".."
		_, err := uploadSvc.CreateUploadURL(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidUploadRequest)
	})

	t.Run("tenant_id path traversal rejected", func(t *testing.T) {
		req := baseReq
		req.TenantID = "../evil-tenant"
		_, err := uploadSvc.CreateUploadURL(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidUploadRequest)
	})
}

// TestUploadService_CompleteMultipartUpload_Validation verifies ETag completion, strictly ascending part numbers, bucket matching, and cross-tenant S3 key rejection.
func TestUploadService_CompleteMultipartUpload_Validation(t *testing.T) {
	t.Parallel()

	presigner, err := s3.NewPresigner(s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1"), nil, s3.PresignerConfig{})
	require.NoError(t, err)
	uploadSvc := service.NewUploadService(presigner, service.UploadServiceConfig{
		DefaultBucket: "rag-ingestion-bucket",
	})

	validKey := "raw/tenant-corp-01/" + strings.ToLower(validSHA256Hash) + "/report.pdf"
	validCompleteReq := coordinatorv1.CompleteMultipartUploadRequest{
		UploadID: "mp_upload_01HXYZ",
		TenantID: "tenant-corp-01",
		S3Bucket: "rag-ingestion-bucket",
		S3Key:    validKey,
		Parts: []coordinatorv1.CompletedPart{
			{PartNumber: 1, ETag: `"etag-1"`},
			{PartNumber: 2, ETag: `"etag-2"`},
		},
	}

	resp, err := uploadSvc.CompleteMultipartUpload(context.Background(), validCompleteReq)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", resp.Status)
	assert.Equal(t, "s3://rag-ingestion-bucket/"+validKey, resp.S3Location)
	assert.NotEmpty(t, resp.CompletedAt)

	t.Run("rejects cross-tenant s3_key", func(t *testing.T) {
		req := validCompleteReq
		req.S3Key = "raw/other-tenant/" + strings.ToLower(validSHA256Hash) + "/report.pdf"
		_, err := uploadSvc.CompleteMultipartUpload(context.Background(), req)
		require.ErrorIs(t, err, service.ErrTenantMismatch)
	})

	t.Run("rejects path traversal in s3_key", func(t *testing.T) {
		req := validCompleteReq
		req.S3Key = "raw/tenant-corp-01/../other-tenant/report.pdf"
		_, err := uploadSvc.CompleteMultipartUpload(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidMultipartCompletion)
	})

	t.Run("rejects unsorted or duplicate part numbers", func(t *testing.T) {
		req := validCompleteReq
		req.Parts = []coordinatorv1.CompletedPart{
			{PartNumber: 2, ETag: `"etag-2"`},
			{PartNumber: 1, ETag: `"etag-1"`},
		}
		_, err := uploadSvc.CompleteMultipartUpload(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidMultipartCompletion)

		req.Parts = []coordinatorv1.CompletedPart{
			{PartNumber: 1, ETag: `"etag-1"`},
			{PartNumber: 1, ETag: `"etag-1-dup"`},
		}
		_, err = uploadSvc.CompleteMultipartUpload(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidMultipartCompletion)
	})

	t.Run("rejects empty part ETag", func(t *testing.T) {
		req := validCompleteReq
		req.Parts = []coordinatorv1.CompletedPart{
			{PartNumber: 1, ETag: `""`},
		}
		_, err := uploadSvc.CompleteMultipartUpload(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidMultipartCompletion)
	})

	t.Run("rejects mismatched S3 bucket", func(t *testing.T) {
		req := validCompleteReq
		req.S3Bucket = "untrusted-bucket"
		_, err := uploadSvc.CompleteMultipartUpload(context.Background(), req)
		require.ErrorIs(t, err, service.ErrInvalidMultipartCompletion)
	})
}

// TestPresignActivities_EnforcesTenantAndDeterministicArtifactKeys verifies slice presigning validation and S3 range reader 64 KiB cap.
func TestPresignActivities_EnforcesTenantAndDeterministicArtifactKeys(t *testing.T) {
	t.Parallel()

	// Configure S3 Presigner with a RangeStreamGetter returning a 256 KiB stream to verify the 64 KiB LimitReader cap.
	presigner, err := s3.NewPresigner(s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1"), rangeStreamFunc(func(_ context.Context, _, _, _ string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("A"), 256*1024))), nil
	}), s3.PresignerConfig{})
	require.NoError(t, err)

	data, err := presigner.ReadRange(context.Background(), "rag-ingestion-bucket", "raw/tenant-corp-01/hash/doc.pdf", 0, 128*1024)
	require.NoError(t, err)
	assert.Len(t, data, int(s3.MaxInspectionRangeBytes), "Presigner.ReadRange must cap reads at 64 KiB")

	presignActs := activity.NewPresignActivities(presigner)

	// Valid standard and Runbook 2 hash-prefixed slice presigning.
	validSlice := coordinatorv1.PartitionSlice{
		SliceID:       "slice-01",
		SliceIndex:    0,
		TotalSlices:   1,
		TenantID:      "tenant-corp-01",
		DocumentID:    "doc-01",
		ContentHash:   strings.ToLower(validSHA256Hash),
		S3InputBucket: "rag-ingestion-bucket",
		S3InputKey:    "raw/tenant-corp-01/" + strings.ToLower(validSHA256Hash) + "/doc.pdf",
		Modality:      coordinatorv1.MultimodalModalityDocumentPDF,
		StartPage:     1,
		EndPage:       50,
	}

	// Auto-computes S3OutputParquetKey when omitted.
	resp, err := presignActs.PresignS3Artifact(context.Background(), coordinatorv1.PresignSliceArtifactRequest{
		Slice:        validSlice,
		LeaseSeconds: 1200, // Clamped to 900
	})
	require.NoError(t, err)
	assert.Equal(t, "artifacts/tenant-corp-01/"+strings.ToLower(validSHA256Hash)+"/slice_1_50.parquet", resp.S3OutputParquetKey)
	assert.Contains(t, resp.PresignedInputGetURL, "X-Amz-Expires=900")
	assert.Contains(t, resp.PresignedOutputPutURL, "X-Amz-Expires=900")

	// Reject cross-tenant S3InputKey.
	crossTenantSlice := validSlice
	crossTenantSlice.S3InputKey = "raw/other-tenant/" + strings.ToLower(validSHA256Hash) + "/doc.pdf"
	_, err = presignActs.PresignS3Artifact(context.Background(), coordinatorv1.PresignSliceArtifactRequest{
		Slice: crossTenantSlice,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TENANT_MISMATCH")

	// Reject cross-tenant S3OutputParquetKey.
	badOutputSlice := validSlice
	badOutputSlice.S3OutputParquetKey = "artifacts/other-tenant/" + strings.ToLower(validSHA256Hash) + "/slice_1_50.parquet"
	_, err = presignActs.PresignS3Artifact(context.Background(), coordinatorv1.PresignSliceArtifactRequest{
		Slice: badOutputSlice,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TENANT_MISMATCH")
}

func TestPresigner_RequiresConfiguredAPI(t *testing.T) {
	t.Parallel()

	// NewPresigner fails when api is nil
	_, err := s3.NewPresigner(nil, nil, s3.PresignerConfig{})
	require.ErrorIs(t, err, s3.ErrPresignAPINotConfigured)

	// PresignS3Artifact fails when presigner is nil
	unconfiguredActs := activity.NewPresignActivities(nil)
	_, err = unconfiguredActs.PresignS3Artifact(context.Background(), coordinatorv1.PresignSliceArtifactRequest{
		Slice: coordinatorv1.PartitionSlice{
			SliceID:       "slice-01",
			TenantID:      "tenant-01",
			DocumentID:    "doc-01",
			S3InputBucket: "bucket",
			S3InputKey:    "raw/tenant-01/hash/file.pdf",
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "slice presigner is not configured")
}

type rangeStreamFunc func(ctx context.Context, bucket, key, rangeHeader string) (io.ReadCloser, error)

func (f rangeStreamFunc) GetObjectRange(ctx context.Context, bucket, key, rangeHeader string) (io.ReadCloser, error) {
	return f(ctx, bucket, key, rangeHeader)
}
