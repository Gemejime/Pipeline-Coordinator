// Package service implements core control-plane domain logic including deterministic
// slice manifest planning and S3 content-addressed key calculation.
package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
)

const (
	// DefaultPagesPerSlice is the default page interval size for PDF/tabular documents.
	DefaultPagesPerSlice int32 = 50
	// MinPagesPerSlice is the minimum valid page interval size.
	MinPagesPerSlice int32 = 1
	// MaxPagesPerSlice is the maximum valid page interval size per child workflow slice.
	MaxPagesPerSlice int32 = 100

	// DefaultWindowSeconds is the default 10-minute (600s) window for audio/video media.
	DefaultWindowSeconds float64 = 600.0
	// DefaultOverlapSeconds is the default sliding window overlap for continuous media.
	DefaultOverlapSeconds float64 = 15.0
	// MaxPayloadBytes is the 10 GiB hard ceiling for a single monolithic upload.
	MaxPayloadBytes int64 = 10 * 1024 * 1024 * 1024
)

var (
	// ErrInvalidManifestInput indicates a non-retryable validation failure in manifest planning.
	ErrInvalidManifestInput = errors.New("INVALID_ARGUMENT: manifest planning input validation failed")
	// ErrZeroBytePayload indicates a zero or negative file byte size.
	ErrZeroBytePayload = errors.New("CORRUPT_INPUT: payload size must be greater than zero bytes")
	// ErrCorruptedRange indicates a non-positive page count, duration, or corrupted slice range.
	ErrCorruptedRange = errors.New("CORRUPT_INPUT: invalid or corrupted page/media range bounds")
	// ErrPayloadTooLarge indicates an upload exceeding the 10 GB maximum limit.
	ErrPayloadTooLarge = errors.New("INVALID_ARGUMENT: payload exceeds 10GB maximum limit")
)

// PartitionSlice aliases the canonical Protobuf PartitionSlice contract.
type PartitionSlice = coordinatorv1.PartitionSlice

// ManifestPlanInput encapsulates all parameters required to deterministically plan
// partition slices without performing any I/O.
type ManifestPlanInput struct {
	TenantID              string
	DocumentID            string
	ContentHash           string
	S3InputBucket         string
	S3InputKey            string
	TotalBytes            int64
	Modality              coordinatorv1.MultimodalModality
	TotalPages            int32
	DurationSeconds       float64
	PagesPerSlice         int32
	WindowSeconds         float64
	OverlapSeconds        float64
	HasOverlapConfig      bool
	UseHashPrefixS3Layout bool // Runbook 2 mitigation for S3 HTTP 503 SlowDown
}

// PlanSlices decomposes a document or continuous media asset into a deterministic,
// non-overlapping (for pages) or sliding-window (for audio/video) slice manifest.
// This function is completely pure and performs zero I/O, making it safe to call
// directly inside deterministic Temporal workflows or unit tests.
func PlanSlices(input ManifestPlanInput) ([]PartitionSlice, error) {
	if err := validateManifestInput(input); err != nil {
		return nil, err
	}

	switch input.Modality {
	case coordinatorv1.MultimodalModalityDocumentPDF, coordinatorv1.MultimodalModalityTabular:
		return planDocumentPageSlices(input)
	case coordinatorv1.MultimodalModalityAudio, coordinatorv1.MultimodalModalityVideo:
		return planMediaWindowSlices(input)
	default:
		return nil, fmt.Errorf("%w: unsupported modality %v", ErrInvalidManifestInput, input.Modality)
	}
}

func validateManifestInput(input ManifestPlanInput) error {
	if strings.TrimSpace(input.TenantID) == "" {
		return fmt.Errorf("%w: tenant_id is required", ErrInvalidManifestInput)
	}
	if strings.TrimSpace(input.DocumentID) == "" {
		return fmt.Errorf("%w: document_id is required", ErrInvalidManifestInput)
	}
	if strings.TrimSpace(input.ContentHash) == "" {
		return fmt.Errorf("%w: content_hash is required", ErrInvalidManifestInput)
	}
	if strings.TrimSpace(input.S3InputBucket) == "" {
		return fmt.Errorf("%w: s3_input_bucket is required", ErrInvalidManifestInput)
	}
	if strings.TrimSpace(input.S3InputKey) == "" {
		return fmt.Errorf("%w: s3_input_key is required", ErrInvalidManifestInput)
	}
	if input.TotalBytes <= 0 {
		return ErrZeroBytePayload
	}
	if input.TotalBytes > MaxPayloadBytes {
		return ErrPayloadTooLarge
	}
	return nil
}

func planDocumentPageSlices(input ManifestPlanInput) ([]PartitionSlice, error) {
	if input.TotalPages <= 0 {
		return nil, fmt.Errorf("%w: total_pages must be >= 1, got %d", ErrCorruptedRange, input.TotalPages)
	}

	pagesPerSlice := input.PagesPerSlice
	if pagesPerSlice == 0 {
		pagesPerSlice = DefaultPagesPerSlice
	}
	if pagesPerSlice < MinPagesPerSlice || pagesPerSlice > MaxPagesPerSlice {
		return nil, fmt.Errorf(
			"%w: pages_per_slice must be between %d and %d, got %d",
			ErrCorruptedRange, MinPagesPerSlice, MaxPagesPerSlice, pagesPerSlice,
		)
	}

	totalSlices := (input.TotalPages + pagesPerSlice - 1) / pagesPerSlice
	slices := make([]PartitionSlice, 0, totalSlices)

	for idx := int32(0); idx < totalSlices; idx++ {
		startPage := idx*pagesPerSlice + 1
		endPage := startPage + pagesPerSlice - 1
		if endPage > input.TotalPages {
			endPage = input.TotalPages
		}

		rangeSpec := fmt.Sprintf("pages_%d_%d", startPage, endPage)
		sliceID := ComputeDeterministicSliceID(input.DocumentID, input.ContentHash, rangeSpec)
		s3OutputKey := BuildS3OutputParquetKey(
			input.TenantID,
			input.DocumentID,
			input.ContentHash,
			int(startPage),
			int(endPage),
			input.UseHashPrefixS3Layout,
		)

		slices = append(slices, PartitionSlice{
			SliceID:            sliceID,
			SliceIndex:         idx,
			TotalSlices:        totalSlices,
			TenantID:           input.TenantID,
			DocumentID:         input.DocumentID,
			ContentHash:        input.ContentHash,
			S3InputBucket:      input.S3InputBucket,
			S3InputKey:         input.S3InputKey,
			S3OutputParquetKey: s3OutputKey,
			Modality:           input.Modality,
			StartPage:          startPage,
			EndPage:            endPage,
		})
	}

	return slices, nil
}

func planMediaWindowSlices(input ManifestPlanInput) ([]PartitionSlice, error) {
	if math.IsNaN(input.DurationSeconds) || math.IsInf(input.DurationSeconds, 0) || input.DurationSeconds <= 0 {
		return nil, fmt.Errorf("%w: duration_seconds must be > 0, got %v", ErrCorruptedRange, input.DurationSeconds)
	}

	windowSec := input.WindowSeconds
	if windowSec == 0 {
		windowSec = DefaultWindowSeconds
	}
	if math.IsNaN(windowSec) || math.IsInf(windowSec, 0) || windowSec <= 0 {
		return nil, fmt.Errorf("%w: window_seconds must be > 0, got %v", ErrCorruptedRange, windowSec)
	}

	overlapSec := input.OverlapSeconds
	if !input.HasOverlapConfig && overlapSec == 0 {
		overlapSec = DefaultOverlapSeconds
		if overlapSec >= windowSec {
			overlapSec = 0
		}
	}
	if math.IsNaN(overlapSec) || math.IsInf(overlapSec, 0) || overlapSec < 0 || overlapSec >= windowSec {
		return nil, fmt.Errorf(
			"%w: overlap_seconds (%v) must be >= 0 and strictly less than window_seconds (%v)",
			ErrCorruptedRange, overlapSec, windowSec,
		)
	}

	stride := windowSec - overlapSec
	type timeWindow struct {
		startSec float64
		endSec   float64
	}

	var windows []timeWindow
	for start := 0.0; start < input.DurationSeconds; start += stride {
		end := start + windowSec
		if end >= input.DurationSeconds {
			end = input.DurationSeconds
			windows = append(windows, timeWindow{startSec: start, endSec: end})
			break
		}
		windows = append(windows, timeWindow{startSec: start, endSec: end})
	}

	totalSlices := int32(len(windows))
	slices := make([]PartitionSlice, 0, totalSlices)
	for i, w := range windows {
		startMarker := int(math.Round(w.startSec))
		endMarker := int(math.Round(w.endSec))
		rangeSpec := fmt.Sprintf("sec_%d_%d", startMarker, endMarker)
		sliceID := ComputeDeterministicSliceID(input.DocumentID, input.ContentHash, rangeSpec)
		s3OutputKey := BuildS3OutputParquetKey(
			input.TenantID,
			input.DocumentID,
			input.ContentHash,
			startMarker,
			endMarker,
			input.UseHashPrefixS3Layout,
		)

		slices = append(slices, PartitionSlice{
			SliceID:            sliceID,
			SliceIndex:         int32(i),
			TotalSlices:        totalSlices,
			TenantID:           input.TenantID,
			DocumentID:         input.DocumentID,
			ContentHash:        input.ContentHash,
			S3InputBucket:      input.S3InputBucket,
			S3InputKey:         input.S3InputKey,
			S3OutputParquetKey: s3OutputKey,
			Modality:           input.Modality,
			StartTimeSeconds:   w.startSec,
			EndTimeSeconds:     w.endSec,
			OverlapSeconds:     overlapSec,
		})
	}

	return slices, nil
}

// ComputeDeterministicSliceID computes a deterministic hex-encoded SHA-256 identifier
// from document_id, content_hash, and range_spec.
func ComputeDeterministicSliceID(documentID, contentHash, rangeSpec string) string {
	sum := sha256.Sum256([]byte(documentID + ":" + contentHash + ":" + rangeSpec))
	return hex.EncodeToString(sum[:])
}

// ComputeS3PartitionHashPrefix returns the 4-hex-character prefix of SHA-256(documentID + pageRange)
// to distribute S3 I/O across partition prefixes (Runbook 2 mitigation for HTTP 503 SlowDown).
func ComputeS3PartitionHashPrefix(documentID, pageRange string) string {
	sum := sha256.Sum256([]byte(documentID + pageRange))
	return hex.EncodeToString(sum[:2])
}

// BuildS3OutputParquetKey builds the deterministic, content-addressed S3 object key
// for a partition slice Parquet output:
//   - Standard: artifacts/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet
//   - Hash-prefixed (Runbook 2): artifacts/<hash_prefix>/<tenant_id>/<doc_hash>/slice_<start>_<end>.parquet
func BuildS3OutputParquetKey(tenantID, documentID, docHash string, start, end int, useHashPrefix bool) string {
	pageRange := fmt.Sprintf("%d_%d", start, end)
	if useHashPrefix {
		prefix := ComputeS3PartitionHashPrefix(documentID, pageRange)
		return fmt.Sprintf("artifacts/%s/%s/%s/slice_%d_%d.parquet", prefix, tenantID, docHash, start, end)
	}
	return fmt.Sprintf("artifacts/%s/%s/slice_%d_%d.parquet", tenantID, docHash, start, end)
}
