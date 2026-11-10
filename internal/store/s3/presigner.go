// Package s3 provides an AWS S3 / compatible presigning, multipart upload, and
// bounded byte-range reader adapter enforcing a 15-minute (900s) lease ceiling
// and a 64 KiB inspection range read cap.
package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

const (
	// DefaultPresignLeaseSeconds is the mandatory 15-minute (900s) S3 presigned URL lease ceiling.
	DefaultPresignLeaseSeconds int32 = 900
	// MaxInspectionRangeBytes is the hard 64 KiB byte ceiling enforced via io.LimitReader on range reads.
	MaxInspectionRangeBytes int64 = 64 * 1024
	// DefaultS3Endpoint is the default S3 base endpoint when none is configured.
	DefaultS3Endpoint = "https://s3.amazonaws.com"
	// DefaultS3Region is the default AWS region used for signing when none is configured.
	DefaultS3Region = "us-east-1"
)

var (
	// ErrInvalidS3Target indicates an empty bucket, key, or invalid S3 parameter.
	ErrInvalidS3Target = errors.New("INVALID_ARGUMENT: s3 bucket and key are required")
	// ErrInvalidByteRange indicates a negative or inverted byte range.
	ErrInvalidByteRange = errors.New("INVALID_ARGUMENT: invalid byte range bounds")
	// ErrRangeReaderNotConfigured indicates that no RangeStreamGetter was wired into the Presigner.
	ErrRangeReaderNotConfigured = errors.New("UNAVAILABLE: S3 range stream getter is not configured")
	// ErrPresignAPINotConfigured indicates that no PresignAPI was configured for the presigner.
	ErrPresignAPINotConfigured = errors.New("UNAVAILABLE: S3 presign API is not configured")
)

// PresignAPI abstracts the underlying AWS SDK v2 S3 presign and multipart operations.
type PresignAPI interface {
	PresignPutObject(ctx context.Context, bucket, key, contentType string, contentLength int64, lease time.Duration) (string, error)
	PresignGetObject(ctx context.Context, bucket, key string, lease time.Duration) (string, error)
	CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (string, error)
	PresignUploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int32, lease time.Duration) (string, error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []coordinatorv1.CompletedPart) (string, error)
}

// RangeStreamGetter abstracts S3 GetObject requests with an HTTP Range header (`bytes=start-end`).
type RangeStreamGetter interface {
	GetObjectRange(ctx context.Context, bucket, key, rangeHeader string) (io.ReadCloser, error)
}

// PresignerConfig configures the S3 Presigner adapter.
type PresignerConfig struct {
	Endpoint        string
	Region          string
	MaxLeaseSeconds int32
	SigningKey      string
	NowFunc         func() time.Time
}

// Presigner issues short-lived (<= 15-minute) presigned S3 single-part, multipart, and slice
// artifact URLs, and performs bounded byte-range reads capped at 64 KiB.
type Presigner struct {
	api             PresignAPI
	rangeGetter     RangeStreamGetter
	endpoint        string
	region          string
	maxLeaseSeconds int32
	nowFunc         func() time.Time
}

// NewPresigner constructs a Presigner with 15-minute lease enforcement. It requires a configured PresignAPI.
func NewPresigner(api PresignAPI, rangeGetter RangeStreamGetter, cfg PresignerConfig) (*Presigner, error) {
	if api == nil {
		return nil, ErrPresignAPINotConfigured
	}
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		endpoint = DefaultS3Endpoint
	}
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		region = DefaultS3Region
	}
	maxLease := cfg.MaxLeaseSeconds
	if maxLease <= 0 || maxLease > DefaultPresignLeaseSeconds {
		maxLease = DefaultPresignLeaseSeconds
	}
	nowFunc := cfg.NowFunc
	if nowFunc == nil {
		nowFunc = func() time.Time { return time.Now().UTC() }
	}
	return &Presigner{
		api:             api,
		rangeGetter:     rangeGetter,
		endpoint:        endpoint,
		region:          region,
		maxLeaseSeconds: maxLease,
		nowFunc:         nowFunc,
	}, nil
}

// ClampLeaseSeconds enforces the 15-minute (900s) maximum S3 presigned URL lease.
func (p *Presigner) ClampLeaseSeconds(requested int32) int32 {
	maxLease := DefaultPresignLeaseSeconds
	if p != nil && p.maxLeaseSeconds > 0 && p.maxLeaseSeconds <= DefaultPresignLeaseSeconds {
		maxLease = p.maxLeaseSeconds
	}
	if requested <= 0 || requested > maxLease {
		return maxLease
	}
	return requested
}

// PutObject issues a presigned S3 PutObject URL bounded to at most 15 minutes (900s).
func (p *Presigner) PutObject(
	ctx context.Context,
	bucket, key, contentType string,
	contentLength int64,
	leaseSeconds int32,
) (string, int64, int32, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, 0, err
	}
	if p == nil || p.api == nil {
		return "", 0, 0, ErrPresignAPINotConfigured
	}
	if err := validateBucketAndKey(bucket, key); err != nil {
		return "", 0, 0, err
	}
	effectiveLease := p.ClampLeaseSeconds(leaseSeconds)
	leaseDur := time.Duration(effectiveLease) * time.Second
	now := p.now()
	expiresAtUnix := now.Add(leaseDur).Unix()

	u, err := p.api.PresignPutObject(ctx, bucket, key, contentType, contentLength, leaseDur)
	if err != nil {
		return "", 0, 0, err
	}
	return u, expiresAtUnix, effectiveLease, nil
}

// GetObject issues a presigned S3 GetObject URL bounded to at most 15 minutes (900s).
func (p *Presigner) GetObject(
	ctx context.Context,
	bucket, key string,
	leaseSeconds int32,
) (string, int64, int32, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, 0, err
	}
	if p == nil || p.api == nil {
		return "", 0, 0, ErrPresignAPINotConfigured
	}
	if err := validateBucketAndKey(bucket, key); err != nil {
		return "", 0, 0, err
	}
	effectiveLease := p.ClampLeaseSeconds(leaseSeconds)
	leaseDur := time.Duration(effectiveLease) * time.Second
	now := p.now()
	expiresAtUnix := now.Add(leaseDur).Unix()

	u, err := p.api.PresignGetObject(ctx, bucket, key, leaseDur)
	if err != nil {
		return "", 0, 0, err
	}
	return u, expiresAtUnix, effectiveLease, nil
}

// CreateMultipartUpload initiates an S3 multipart upload and returns its upload_id.
func (p *Presigner) CreateMultipartUpload(
	ctx context.Context,
	bucket, key, contentType string,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p == nil || p.api == nil {
		return "", ErrPresignAPINotConfigured
	}
	if err := validateBucketAndKey(bucket, key); err != nil {
		return "", err
	}

	return p.api.CreateMultipartUpload(ctx, bucket, key, contentType)
}

// UploadPart issues a 15-minute presigned S3 UploadPart URL for a specific 1-based part number.
func (p *Presigner) UploadPart(
	ctx context.Context,
	bucket, key, uploadID string,
	partNumber int32,
	leaseSeconds int32,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p == nil || p.api == nil {
		return "", ErrPresignAPINotConfigured
	}
	if err := validateBucketAndKey(bucket, key); err != nil {
		return "", err
	}
	if strings.TrimSpace(uploadID) == "" {
		return "", fmt.Errorf("%w: upload_id is required", ErrInvalidS3Target)
	}
	if partNumber < 1 || partNumber > 10000 {
		return "", fmt.Errorf("%w: part_number must be between 1 and 10000, got %d", ErrInvalidS3Target, partNumber)
	}

	effectiveLease := p.ClampLeaseSeconds(leaseSeconds)
	leaseDur := time.Duration(effectiveLease) * time.Second

	return p.api.PresignUploadPart(ctx, bucket, key, uploadID, partNumber, leaseDur)
}

// CompleteMultipartUpload finalizes an S3 multipart upload with the ordered list of part ETags.
func (p *Presigner) CompleteMultipartUpload(
	ctx context.Context,
	bucket, key, uploadID string,
	parts []coordinatorv1.CompletedPart,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p == nil || p.api == nil {
		return "", ErrPresignAPINotConfigured
	}
	if err := validateBucketAndKey(bucket, key); err != nil {
		return "", err
	}
	if strings.TrimSpace(uploadID) == "" {
		return "", fmt.Errorf("%w: upload_id is required", ErrInvalidS3Target)
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("%w: parts must not be empty", ErrInvalidS3Target)
	}

	return p.api.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
}

// PresignSlice issues 15-minute presigned GET (raw input) and PUT (Parquet slice output) URLs
// for a child partition workflow.
func (p *Presigner) PresignSlice(
	ctx context.Context,
	req coordinatorv1.PresignSliceArtifactRequest,
) (*coordinatorv1.PresignSliceArtifactResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slice := req.Slice
	effectiveLease := p.ClampLeaseSeconds(req.LeaseSeconds)

	getURL, expiresAtUnix, _, err := p.GetObject(ctx, slice.S3InputBucket, slice.S3InputKey, effectiveLease)
	if err != nil {
		return nil, err
	}
	putURL, _, _, err := p.PutObject(ctx, slice.S3InputBucket, slice.S3OutputParquetKey, "application/vnd.apache.parquet", 0, effectiveLease)
	if err != nil {
		return nil, err
	}

	return &coordinatorv1.PresignSliceArtifactResponse{
		S3Bucket:              slice.S3InputBucket,
		S3InputKey:            slice.S3InputKey,
		S3OutputParquetKey:    slice.S3OutputParquetKey,
		PresignedInputGetURL:  getURL,
		PresignedOutputPutURL: putURL,
		ExpiresAtUnix:         expiresAtUnix,
	}, nil
}

// ReadRange fetches a bounded byte window [start, end] (inclusive) from S3 and enforces
// a hard MaxInspectionRangeBytes (64 KiB) ceiling via io.LimitReader so a misbehaving
// storage endpoint can never exhaust worker pod memory.
func (p *Presigner) ReadRange(
	ctx context.Context,
	bucket, key string,
	start, end int64,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateBucketAndKey(bucket, key); err != nil {
		return nil, err
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("%w: start=%d end=%d", ErrInvalidByteRange, start, end)
	}
	if p == nil || p.rangeGetter == nil {
		return nil, ErrRangeReaderNotConfigured
	}

	if end-start+1 > MaxInspectionRangeBytes {
		end = start + MaxInspectionRangeBytes - 1
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)

	rc, err := p.rangeGetter.GetObjectRange(ctx, bucket, key, rangeHeader)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rc.Close()
	}()

	limited := io.LimitReader(rc, MaxInspectionRangeBytes)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (p *Presigner) now() time.Time {
	if p != nil && p.nowFunc != nil {
		return p.nowFunc().UTC()
	}
	return time.Now().UTC()
}

// FakePresignAPI provides a test-friendly in-memory PresignAPI implementation.
type FakePresignAPI struct {
	BaseEndpoint string
	Region       string
}

// NewFakePresignAPI constructs a FakePresignAPI with default or custom endpoint.
func NewFakePresignAPI(endpoint, region string) *FakePresignAPI {
	trimmed := strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if trimmed == "" {
		trimmed = DefaultS3Endpoint
	}
	if region == "" {
		region = DefaultS3Region
	}
	return &FakePresignAPI{BaseEndpoint: trimmed, Region: region}
}

func (f *FakePresignAPI) PresignPutObject(_ context.Context, bucket, key, contentType string, contentLength int64, lease time.Duration) (string, error) {
	return fmt.Sprintf("%s/%s/%s?X-Amz-Expires=%d&X-Amz-Signature=fake-sig", f.BaseEndpoint, bucket, escapeS3KeyPath(key), int(lease.Seconds())), nil
}

func (f *FakePresignAPI) PresignGetObject(_ context.Context, bucket, key string, lease time.Duration) (string, error) {
	return fmt.Sprintf("%s/%s/%s?X-Amz-Expires=%d&X-Amz-Signature=fake-sig", f.BaseEndpoint, bucket, escapeS3KeyPath(key), int(lease.Seconds())), nil
}

func (f *FakePresignAPI) CreateMultipartUpload(_ context.Context, bucket, key, contentType string) (string, error) {
	return fmt.Sprintf("mp_upload_%s_%s", bucket, hex.EncodeToString([]byte(key))[:16]), nil
}

func (f *FakePresignAPI) PresignUploadPart(_ context.Context, bucket, key, uploadID string, partNumber int32, lease time.Duration) (string, error) {
	return fmt.Sprintf("%s/%s/%s?partNumber=%d&uploadId=%s&X-Amz-Expires=%d&X-Amz-Signature=fake-sig", f.BaseEndpoint, bucket, escapeS3KeyPath(key), partNumber, uploadID, int(lease.Seconds())), nil
}

func (f *FakePresignAPI) CompleteMultipartUpload(_ context.Context, bucket, key, uploadID string, parts []coordinatorv1.CompletedPart) (string, error) {
	return fmt.Sprintf("s3://%s/%s", bucket, key), nil
}

func escapeS3KeyPath(key string) string {
	segments := strings.Split(key, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.Join(segments, "/")
}

func validateBucketAndKey(bucket, key string) error {
	trimmedBucket := strings.TrimSpace(bucket)
	trimmedKey := strings.TrimSpace(key)
	if trimmedBucket == "" || trimmedKey == "" {
		return ErrInvalidS3Target
	}
	if strings.Contains(trimmedBucket, "/") || strings.Contains(trimmedBucket, "..") {
		return fmt.Errorf("%w: invalid bucket name", ErrInvalidS3Target)
	}
	if strings.HasPrefix(trimmedKey, "/") || strings.Contains(trimmedKey, "\\") {
		return fmt.Errorf("%w: invalid object key format", ErrInvalidS3Target)
	}
	for _, seg := range strings.Split(trimmedKey, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: path traversal or empty segment in object key", ErrInvalidS3Target)
		}
	}
	return nil
}
