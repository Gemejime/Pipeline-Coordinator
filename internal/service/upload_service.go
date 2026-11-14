package service

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

const (
	// SinglePartUploadMaxBytes (100 MiB) is the maximum payload size eligible for a single-part presigned PUT URL.
	// Payloads > 100 MiB up to 10 GiB use multipart upload URLs.
	SinglePartUploadMaxBytes int64 = 100 * 1024 * 1024
	// DefaultMultipartPartSizeBytes (100 MiB) is the default part size for multipart S3 uploads.
	DefaultMultipartPartSizeBytes int64 = 100 * 1024 * 1024
	// MinMultipartPartSizeBytes (5 MiB) is the S3 minimum part size for multipart uploads.
	MinMultipartPartSizeBytes int64 = 5 * 1024 * 1024
	// MaxMultipartParts (10,000) is the S3 maximum part count per multipart upload.
	MaxMultipartParts int32 = 10000
	// DefaultUploadLeaseSeconds (900s / 15 minutes) is the mandatory presigned URL lease ceiling.
	DefaultUploadLeaseSeconds int32 = 900
	// MaxFilenameBytes is the maximum sanitized filename byte length.
	MaxFilenameBytes = 255
	// DefaultIngestionBucket is the fallback S3 bucket name when unconfigured.
	DefaultIngestionBucket = "rag-ingestion-bucket"
)

var (
	// ErrInvalidUploadRequest indicates a non-retryable validation failure on upload URL creation.
	ErrInvalidUploadRequest = errors.New("INVALID_ARGUMENT: upload request validation failed")
	// ErrUnsupportedMimeType indicates an unsupported IANA MIME type.
	ErrUnsupportedMimeType = errors.New("UNSUPPORTED_MIME_TYPE: mime_type is not supported")
	// ErrTenantMismatch indicates a tenant boundary violation (e.g. cross-tenant S3 key or claim mismatch).
	ErrTenantMismatch = errors.New("TENANT_MISMATCH: tenant_id does not match target resource")
	// ErrInvalidMultipartCompletion indicates an invalid multipart completion manifest.
	ErrInvalidMultipartCompletion = errors.New("INVALID_ARGUMENT: multipart completion validation failed")
)

// UploadPresigner abstracts S3 presigned URL generation and multipart lifecycle calls.
type UploadPresigner interface {
	PutObject(ctx context.Context, bucket, key, contentType string, contentLength int64, leaseSeconds int32) (presignedURL string, expiresAtUnix int64, effectiveLease int32, err error)
	CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (uploadID string, err error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int32, leaseSeconds int32) (presignedURL string, err error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []coordinatorv1.CompletedPart) (s3Location string, err error)
}

// UploadServiceConfig holds configuration parameters for UploadService.
type UploadServiceConfig struct {
	DefaultBucket string
	LeaseSeconds  int32
	PartSizeBytes int64
	NowFunc       func() time.Time
}

// UploadService coordinates presigned S3 single-part and multipart upload leases.
type UploadService struct {
	presigner     UploadPresigner
	defaultBucket string
	leaseSeconds  int32
	partSizeBytes int64
	nowFunc       func() time.Time
}

// NewUploadService constructs an UploadService enforcing the 15-minute lease ceiling and 10 GiB size limit.
func NewUploadService(presigner UploadPresigner, cfg UploadServiceConfig) *UploadService {
	bucket := strings.TrimSpace(cfg.DefaultBucket)
	if bucket == "" {
		bucket = DefaultIngestionBucket
	}
	lease := cfg.LeaseSeconds
	if lease <= 0 || lease > DefaultUploadLeaseSeconds {
		lease = DefaultUploadLeaseSeconds
	}
	partSize := cfg.PartSizeBytes
	if partSize <= 0 {
		partSize = DefaultMultipartPartSizeBytes
	}
	if partSize < MinMultipartPartSizeBytes {
		partSize = MinMultipartPartSizeBytes
	}
	nowFunc := cfg.NowFunc
	if nowFunc == nil {
		nowFunc = func() time.Time { return time.Now().UTC() }
	}
	return &UploadService{
		presigner:     presigner,
		defaultBucket: bucket,
		leaseSeconds:  lease,
		partSizeBytes: partSize,
		nowFunc:       nowFunc,
	}
}

// CreateUploadURL validates the upload request and issues either a single-part presigned PUT URL
// (for payloads <= 100 MiB) or a multipart upload manifest (for payloads > 100 MiB up to 10 GiB)
// with a strict 15-minute (900s) lease.
func (s *UploadService) CreateUploadURL(
	ctx context.Context,
	req coordinatorv1.CreateUploadURLRequest,
) (*coordinatorv1.CreateUploadURLResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.presigner == nil {
		return nil, fmt.Errorf("%w: upload presigner is not configured", ErrInvalidUploadRequest)
	}

	tenantID, err := ValidateTenantIdentifier(req.TenantID)
	if err != nil {
		return nil, err
	}

	sanitizedFilename, err := SanitizeFilename(req.Filename)
	if err != nil {
		return nil, err
	}

	normalizedMIME := strings.ToLower(strings.TrimSpace(req.ContentType))
	if !IsSupportedMimeType(normalizedMIME) {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedMimeType, req.ContentType)
	}

	if req.ContentLengthBytes <= 0 {
		return nil, fmt.Errorf("%w: %w", ErrInvalidUploadRequest, ErrZeroBytePayload)
	}
	if req.ContentLengthBytes > MaxPayloadBytes {
		return nil, fmt.Errorf("%w: %w", ErrInvalidUploadRequest, ErrPayloadTooLarge)
	}

	normalizedHash, err := ValidateContentHash(req.ContentHash)
	if err != nil {
		return nil, err
	}

	s3Key, err := BuildRawUploadS3Key(tenantID, normalizedHash, sanitizedFilename)
	if err != nil {
		return nil, err
	}

	// Single-part upload handoff for payloads <= 100 MiB.
	if req.ContentLengthBytes <= SinglePartUploadMaxBytes {
		presignedURL, expiresAtUnix, effectiveLease, err := s.presigner.PutObject(
			ctx,
			s.defaultBucket,
			s3Key,
			normalizedMIME,
			req.ContentLengthBytes,
			s.leaseSeconds,
		)
		if err != nil {
			return nil, err
		}

		return &coordinatorv1.CreateUploadURLResponse{
			UploadID:      "upl_" + normalizedHash[:24],
			TenantID:      tenantID,
			S3Bucket:      s.defaultBucket,
			S3Key:         s3Key,
			PresignedURL:  presignedURL,
			ExpiresAtUnix: expiresAtUnix,
			LeaseSeconds:  effectiveLease,
		}, nil
	}

	// Multipart upload handoff for payloads > 100 MiB up to 10 GiB.
	partSize := s.partSizeBytes
	if partSize < MinMultipartPartSizeBytes {
		partSize = DefaultMultipartPartSizeBytes
	}
	partCount64 := (req.ContentLengthBytes + partSize - 1) / partSize
	if partCount64 > int64(MaxMultipartParts) {
		partSize = (req.ContentLengthBytes + int64(MaxMultipartParts) - 1) / int64(MaxMultipartParts)
		if partSize < MinMultipartPartSizeBytes {
			partSize = MinMultipartPartSizeBytes
		}
		partCount64 = (req.ContentLengthBytes + partSize - 1) / partSize
	}
	if partCount64 < 1 || partCount64 > int64(MaxMultipartParts) {
		return nil, fmt.Errorf("%w: calculated multipart count %d out of bounds", ErrInvalidUploadRequest, partCount64)
	}

	uploadID, err := s.presigner.CreateMultipartUpload(ctx, s.defaultBucket, s3Key, normalizedMIME)
	if err != nil {
		return nil, err
	}

	partCount := int32(partCount64)
	parts := make([]coordinatorv1.PresignedPart, 0, partCount)
	for partNum := int32(1); partNum <= partCount; partNum++ {
		partURL, err := s.presigner.UploadPart(ctx, s.defaultBucket, s3Key, uploadID, partNum, s.leaseSeconds)
		if err != nil {
			return nil, err
		}
		parts = append(parts, coordinatorv1.PresignedPart{
			PartNumber:   partNum,
			PresignedURL: partURL,
		})
	}

	expiresAtUnix := s.now().Add(time.Duration(s.leaseSeconds) * time.Second).Unix()
	return &coordinatorv1.CreateUploadURLResponse{
		UploadID:      uploadID,
		TenantID:      tenantID,
		S3Bucket:      s.defaultBucket,
		S3Key:         s3Key,
		ExpiresAtUnix: expiresAtUnix,
		LeaseSeconds:  s.leaseSeconds,
		PartSizeBytes: partSize,
		Parts:         parts,
	}, nil
}

// CompleteMultipartUpload validates the client-submitted ETag manifest and tenant S3 key prefix
// before finalizing the multipart upload in S3.
func (s *UploadService) CompleteMultipartUpload(
	ctx context.Context,
	req coordinatorv1.CompleteMultipartUploadRequest,
) (*coordinatorv1.CompleteMultipartUploadResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.presigner == nil {
		return nil, fmt.Errorf("%w: upload presigner is not configured", ErrInvalidMultipartCompletion)
	}

	uploadID := strings.TrimSpace(req.UploadID)
	if uploadID == "" {
		return nil, fmt.Errorf("%w: upload_id is required", ErrInvalidMultipartCompletion)
	}

	tenantID, err := ValidateTenantIdentifier(req.TenantID)
	if err != nil {
		return nil, err
	}

	bucket := strings.TrimSpace(req.S3Bucket)
	if bucket == "" || bucket != s.defaultBucket {
		return nil, fmt.Errorf("%w: s3_bucket %q does not match coordinator bucket %q", ErrInvalidMultipartCompletion, bucket, s.defaultBucket)
	}

	if err := ValidateTenantRawS3Key(tenantID, req.S3Key); err != nil {
		return nil, err
	}

	if len(req.Parts) == 0 || len(req.Parts) > int(MaxMultipartParts) {
		return nil, fmt.Errorf(
			"%w: parts count must be between 1 and %d, got %d",
			ErrInvalidMultipartCompletion, MaxMultipartParts, len(req.Parts),
		)
	}

	normalizedParts := make([]coordinatorv1.CompletedPart, len(req.Parts))
	var prevPartNumber int32
	for i, part := range req.Parts {
		if part.PartNumber < 1 || part.PartNumber > MaxMultipartParts {
			return nil, fmt.Errorf(
				"%w: part_number %d out of range [1, %d]",
				ErrInvalidMultipartCompletion, part.PartNumber, MaxMultipartParts,
			)
		}
		if part.PartNumber <= prevPartNumber {
			return nil, fmt.Errorf(
				"%w: parts must be in strictly ascending part_number order without duplicates (saw %d after %d)",
				ErrInvalidMultipartCompletion, part.PartNumber, prevPartNumber,
			)
		}
		etag := strings.TrimSpace(part.ETag)
		if etag == "" || etag == `""` {
			return nil, fmt.Errorf(
				"%w: part %d has empty etag",
				ErrInvalidMultipartCompletion, part.PartNumber,
			)
		}
		prevPartNumber = part.PartNumber
		normalizedParts[i] = coordinatorv1.CompletedPart{
			PartNumber: part.PartNumber,
			ETag:       etag,
		}
	}

	s3Location, err := s.presigner.CompleteMultipartUpload(ctx, bucket, req.S3Key, uploadID, normalizedParts)
	if err != nil {
		return nil, err
	}

	return &coordinatorv1.CompleteMultipartUploadResponse{
		UploadID:    uploadID,
		Status:      "COMPLETED",
		S3Location:  s3Location,
		CompletedAt: s.now().Format(time.RFC3339),
	}, nil
}

func (s *UploadService) now() time.Time {
	if s != nil && s.nowFunc != nil {
		return s.nowFunc().UTC()
	}
	return time.Now().UTC()
}

// ValidateTenantIdentifier verifies that tenantID is non-empty and free of path-traversal characters.
func ValidateTenantIdentifier(tenantID string) (string, error) {
	trimmed := strings.TrimSpace(tenantID)
	if trimmed == "" {
		return "", fmt.Errorf("%w: tenant_id is required", ErrInvalidUploadRequest)
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") || strings.Contains(trimmed, "..") || trimmed == "." {
		return "", fmt.Errorf("%w: tenant_id contains invalid path characters", ErrInvalidUploadRequest)
	}
	return trimmed, nil
}

// ValidateContentHash validates a 64-character hexadecimal SHA-256 digest and returns its lowercase form.
func ValidateContentHash(contentHash string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(contentHash))
	if len(trimmed) != 64 {
		return "", fmt.Errorf("%w: content_hash must be a 64-character hex SHA-256 digest", ErrInvalidUploadRequest)
	}
	if _, err := hex.DecodeString(trimmed); err != nil {
		return "", fmt.Errorf("%w: content_hash must contain only hexadecimal characters [0-9a-f]", ErrInvalidUploadRequest)
	}
	return trimmed, nil
}

// SanitizeFilename strips directory paths, replaces unsafe characters outside [a-zA-Z0-9._-] with '_',
// caps length at <= 255 bytes, and rejects '.', '..', or empty filenames.
func SanitizeFilename(filename string) (string, error) {
	trimmed := strings.TrimSpace(filename)
	if trimmed == "" {
		return "", fmt.Errorf("%w: filename is required", ErrInvalidUploadRequest)
	}

	normalized := strings.ReplaceAll(trimmed, "\\", "/")
	if idx := strings.LastIndex(normalized, "/"); idx >= 0 {
		normalized = normalized[idx+1:]
	}
	normalized = strings.TrimSpace(normalized)
	if normalized == "" || normalized == "." || normalized == ".." {
		return "", fmt.Errorf("%w: invalid filename %q", ErrInvalidUploadRequest, filename)
	}

	var b strings.Builder
	b.Grow(len(normalized))
	hasAlnum := false
	for i := 0; i < len(normalized); i++ {
		ch := normalized[i]
		switch {
		case (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9'):
			hasAlnum = true
			b.WriteByte(ch)
		case ch == '.' || ch == '_' || ch == '-':
			b.WriteByte(ch)
		default:
			b.WriteByte('_')
		}
	}

	sanitized := b.String()
	if len(sanitized) > MaxFilenameBytes {
		sanitized = sanitized[:MaxFilenameBytes]
	}
	if sanitized == "" || sanitized == "." || sanitized == ".." || !hasAlnum {
		return "", fmt.Errorf("%w: filename %q has no valid alphanumeric characters", ErrInvalidUploadRequest, filename)
	}
	return sanitized, nil
}

// IsSupportedMimeType returns true if the IANA MIME type is supported by the multimodal ingestion pipeline.
func IsSupportedMimeType(mimeType string) bool {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "application/pdf",
		"text/csv",
		"application/vnd.apache.parquet",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"audio/mpeg",
		"audio/wav",
		"audio/x-wav",
		"audio/flac",
		"audio/ogg",
		"video/mp4",
		"video/webm",
		"video/quicktime":
		return true
	default:
		return false
	}
}

// BuildRawUploadS3Key constructs the deterministic S3 key for a raw tenant upload:
// raw/<tenant_id>/<content_hash>/<sanitized_filename>
func BuildRawUploadS3Key(tenantID, contentHash, filename string) (string, error) {
	validTenant, err := ValidateTenantIdentifier(tenantID)
	if err != nil {
		return "", err
	}
	validHash, err := ValidateContentHash(contentHash)
	if err != nil {
		return "", err
	}
	validFilename, err := SanitizeFilename(filename)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("raw/%s/%s/%s", validTenant, validHash, validFilename), nil
}

// ValidateTenantRawS3Key verifies that s3Key has zero path-traversal segments and belongs
// strictly to raw/<tenant_id>/<hash>/<filename>.
func ValidateTenantRawS3Key(tenantID, s3Key string) error {
	validTenant, err := ValidateTenantIdentifier(tenantID)
	if err != nil {
		return err
	}
	trimmedKey := strings.TrimSpace(s3Key)
	if trimmedKey == "" {
		return fmt.Errorf("%w: s3_key is required", ErrInvalidMultipartCompletion)
	}
	if strings.HasPrefix(trimmedKey, "/") || strings.HasSuffix(trimmedKey, "/") || strings.Contains(trimmedKey, "\\") {
		return fmt.Errorf("%w: malformed s3_key %q", ErrInvalidMultipartCompletion, s3Key)
	}
	segments := strings.Split(trimmedKey, "/")
	for _, seg := range segments {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: s3_key %q contains path traversal or empty segment", ErrInvalidMultipartCompletion, s3Key)
		}
	}
	if len(segments) < 4 || segments[0] != "raw" {
		return fmt.Errorf("%w: s3_key %q must start with raw/<tenant_id>/", ErrInvalidMultipartCompletion, s3Key)
	}
	if segments[1] != validTenant {
		return fmt.Errorf("%w: s3_key tenant %q does not match tenant %q", ErrTenantMismatch, segments[1], validTenant)
	}
	return nil
}

// ValidateSliceOutputParquetKey verifies that a slice's S3OutputParquetKey follows the deterministic
// content-addressed layout:
//   - Standard: artifacts/<tenant_id>/<content_hash>/slice_<start>_<end>.parquet
//   - Runbook 2 hash-prefixed: artifacts/<hash_prefix>/<tenant_id>/<content_hash>/slice_<start>_<end>.parquet
func ValidateSliceOutputParquetKey(tenantID, contentHash, s3OutputKey string) error {
	validTenant, err := ValidateTenantIdentifier(tenantID)
	if err != nil {
		return err
	}
	trimmedHash := strings.TrimSpace(contentHash)
	if trimmedHash == "" || strings.Contains(trimmedHash, "/") || strings.Contains(trimmedHash, "..") {
		return fmt.Errorf("%w: invalid slice content_hash", ErrInvalidUploadRequest)
	}
	trimmedKey := strings.TrimSpace(s3OutputKey)
	if trimmedKey == "" {
		return fmt.Errorf("%w: s3_output_parquet_key is required", ErrInvalidUploadRequest)
	}
	if strings.HasPrefix(trimmedKey, "/") || strings.HasSuffix(trimmedKey, "/") || strings.Contains(trimmedKey, "\\") {
		return fmt.Errorf("%w: malformed s3_output_parquet_key %q", ErrInvalidUploadRequest, s3OutputKey)
	}
	segments := strings.Split(trimmedKey, "/")
	for _, seg := range segments {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: s3_output_parquet_key %q contains path traversal", ErrInvalidUploadRequest, s3OutputKey)
		}
	}
	if segments[0] != "artifacts" {
		return fmt.Errorf("%w: s3_output_parquet_key %q must start with artifacts/", ErrInvalidUploadRequest, s3OutputKey)
	}

	var keyTenant, keyHash, sliceFile string
	switch len(segments) {
	case 4:
		keyTenant = segments[1]
		keyHash = segments[2]
		sliceFile = segments[3]
	case 5:
		// Runbook 2 hash-prefixed layout: artifacts/<4-hex-prefix>/<tenant_id>/<content_hash>/slice_<start>_<end>.parquet
		prefix := segments[1]
		if len(prefix) != 4 {
			return fmt.Errorf("%w: invalid S3 hash prefix %q in %q", ErrInvalidUploadRequest, prefix, s3OutputKey)
		}
		if _, err := hex.DecodeString(prefix); err != nil {
			return fmt.Errorf("%w: S3 hash prefix %q is not valid hex", ErrInvalidUploadRequest, prefix)
		}
		keyTenant = segments[2]
		keyHash = segments[3]
		sliceFile = segments[4]
	default:
		return fmt.Errorf("%w: s3_output_parquet_key %q does not match deterministic artifacts layout", ErrInvalidUploadRequest, s3OutputKey)
	}

	if keyTenant != validTenant {
		return fmt.Errorf("%w: s3_output_parquet_key tenant %q does not match slice tenant %q", ErrTenantMismatch, keyTenant, validTenant)
	}
	if !strings.EqualFold(keyHash, trimmedHash) {
		return fmt.Errorf("%w: s3_output_parquet_key hash %q does not match slice content_hash %q", ErrInvalidUploadRequest, keyHash, trimmedHash)
	}
	if !strings.HasPrefix(sliceFile, "slice_") || !strings.HasSuffix(sliceFile, ".parquet") {
		return fmt.Errorf("%w: s3_output_parquet_key file %q must match slice_<start>_<end>.parquet", ErrInvalidUploadRequest, sliceFile)
	}
	return nil
}
