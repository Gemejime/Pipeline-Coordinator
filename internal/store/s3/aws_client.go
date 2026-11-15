package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

// AWSClientConfig configures the AWS SDK v2 S3 and Presign clients.
type AWSClientConfig struct {
	Region          string
	Endpoint        string
	UsePathStyle    bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// AWSClient implements PresignAPI and RangeStreamGetter using AWS SDK v2.
type AWSClient struct {
	client        *awss3.Client
	presignClient *awss3.PresignClient
}

// NewAWSClient constructs an AWS SDK v2 S3 adapter supporting standard IAM/STS and custom endpoints (LocalStack/MinIO).
func NewAWSClient(ctx context.Context, cfg AWSClientConfig) (*AWSClient, error) {
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		region = DefaultS3Region
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}

	if strings.TrimSpace(cfg.AccessKeyID) != "" && strings.TrimSpace(cfg.SecretAccessKey) != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	s3Client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	return &AWSClient{
		client:        s3Client,
		presignClient: awss3.NewPresignClient(s3Client),
	}, nil
}

// PresignPutObject issues a time-bounded presigned PutObject URL.
func (c *AWSClient) PresignPutObject(
	ctx context.Context,
	bucket, key, contentType string,
	contentLength int64,
	lease time.Duration,
) (string, error) {
	if c == nil || c.presignClient == nil {
		return "", errors.New("aws s3 presign client is not initialized")
	}

	input := &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if strings.TrimSpace(contentType) != "" {
		input.ContentType = aws.String(contentType)
	}
	if contentLength > 0 {
		input.ContentLength = aws.Int64(contentLength)
	}

	out, err := c.presignClient.PresignPutObject(ctx, input, awss3.WithPresignExpires(lease))
	if err != nil {
		return "", fmt.Errorf("failed to presign PutObject: %w", err)
	}
	return out.URL, nil
}

// PresignGetObject issues a time-bounded presigned GetObject URL.
func (c *AWSClient) PresignGetObject(
	ctx context.Context,
	bucket, key string,
	lease time.Duration,
) (string, error) {
	if c == nil || c.presignClient == nil {
		return "", errors.New("aws s3 presign client is not initialized")
	}

	out, err := c.presignClient.PresignGetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, awss3.WithPresignExpires(lease))
	if err != nil {
		return "", fmt.Errorf("failed to presign GetObject: %w", err)
	}
	return out.URL, nil
}

// CreateMultipartUpload initiates an S3 multipart upload session and returns its upload ID.
func (c *AWSClient) CreateMultipartUpload(
	ctx context.Context,
	bucket, key, contentType string,
) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("aws s3 client is not initialized")
	}

	input := &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if strings.TrimSpace(contentType) != "" {
		input.ContentType = aws.String(contentType)
	}

	out, err := c.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to create multipart upload: %w", err)
	}
	return aws.ToString(out.UploadId), nil
}

// PresignUploadPart issues a time-bounded presigned UploadPart URL for the given 1-based part number.
func (c *AWSClient) PresignUploadPart(
	ctx context.Context,
	bucket, key, uploadID string,
	partNumber int32,
	lease time.Duration,
) (string, error) {
	if c == nil || c.presignClient == nil {
		return "", errors.New("aws s3 presign client is not initialized")
	}

	out, err := c.presignClient.PresignUploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNumber),
	}, awss3.WithPresignExpires(lease))
	if err != nil {
		return "", fmt.Errorf("failed to presign UploadPart %d: %w", partNumber, err)
	}
	return out.URL, nil
}

// CompleteMultipartUpload finalizes an S3 multipart upload with part ETags and returns the s3:// location.
func (c *AWSClient) CompleteMultipartUpload(
	ctx context.Context,
	bucket, key, uploadID string,
	parts []coordinatorv1.CompletedPart,
) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("aws s3 client is not initialized")
	}

	completed := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		pn := part.PartNumber
		etag := part.ETag
		completed[i] = types.CompletedPart{
			PartNumber: aws.Int32(pn),
			ETag:       aws.String(etag),
		}
	}
	sort.Slice(completed, func(i, j int) bool {
		return aws.ToInt32(completed[i].PartNumber) < aws.ToInt32(completed[j].PartNumber)
	})

	_, err := c.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	return fmt.Sprintf("s3://%s/%s", bucket, key), nil
}

// GetObjectRange opens a bounded byte-range stream from S3 using the HTTP Range header.
func (c *AWSClient) GetObjectRange(
	ctx context.Context,
	bucket, key, rangeHeader string,
) (io.ReadCloser, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("aws s3 client is not initialized")
	}

	out, err := c.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 byte range: %w", err)
	}
	return out.Body, nil
}
