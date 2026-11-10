// Package activity defines the Temporal activities and polyglot activity contracts
// for the rag-ingestion-coordinator control plane.
package activity

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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

	// HeaderInspectionWindowBytes (4 KiB) is the initial byte-range window read from S3 (bytes=0-4095).
	HeaderInspectionWindowBytes int64 = 4 * 1024
	// TailInspectionWindowBytes (64 KiB) is the maximum trailing byte-range window read from S3.
	TailInspectionWindowBytes int64 = 64 * 1024
	// MaxInspectionRangeBytes (64 KiB) is the hard memory ceiling enforced on any single range read.
	MaxInspectionRangeBytes int64 = 64 * 1024
)

var (
	pdfLinearizedCountRegex = regexp.MustCompile(`(?s)/Linearized\b.*?/N\s+(\d+)`)
	pdfPagesNodeRegex       = regexp.MustCompile(`(?s)<<.*?>>`)
	pdfTypePagesRegex       = regexp.MustCompile(`/Type\s*/Pages\b`)
	pdfCountEntryRegex      = regexp.MustCompile(`/Count\s+(\d+)`)
)

// ByteRangeReader reads a bounded byte range [start, end] (inclusive) from an S3 object without
// buffering or streaming the full file into memory.
type ByteRangeReader interface {
	ReadRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}

// RangeReadCloserFunc opens an io.ReadCloser for an S3 byte range [start, end].
type RangeReadCloserFunc func(ctx context.Context, bucket, key string, start, end int64) (io.ReadCloser, error)

// LimitedByteRangeReader wraps a streaming RangeReadCloserFunc or underlying ByteRangeReader
// and enforces a strict io.LimitReader cap of MaxInspectionRangeBytes (64 KiB).
type LimitedByteRangeReader struct {
	streamFunc RangeReadCloserFunc
	underlying ByteRangeReader
	maxBytes   int64
}

// NewLimitedByteRangeReader constructs a LimitedByteRangeReader from a streaming RangeReadCloserFunc.
func NewLimitedByteRangeReader(streamFunc RangeReadCloserFunc) *LimitedByteRangeReader {
	return &LimitedByteRangeReader{
		streamFunc: streamFunc,
		maxBytes:   MaxInspectionRangeBytes,
	}
}

// NewLimitedByteRangeReaderFromReader wraps an existing ByteRangeReader with a 64 KiB cap.
func NewLimitedByteRangeReaderFromReader(reader ByteRangeReader) *LimitedByteRangeReader {
	return &LimitedByteRangeReader{
		underlying: reader,
		maxBytes:   MaxInspectionRangeBytes,
	}
}

// ReadRange reads at most MaxInspectionRangeBytes (64 KiB) for [start, end] using io.LimitReader.
func (r *LimitedByteRangeReader) ReadRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid byte range [%d, %d]", start, end)
	}
	limit := r.maxBytes
	if limit <= 0 || limit > MaxInspectionRangeBytes {
		limit = MaxInspectionRangeBytes
	}
	if end-start+1 > limit {
		end = start + limit - 1
	}

	if r.streamFunc != nil {
		rc, err := r.streamFunc(ctx, bucket, key, start, end)
		if err != nil {
			return nil, err
		}
		defer func() {
			_ = rc.Close()
		}()
		return io.ReadAll(io.LimitReader(rc, limit))
	}

	if r.underlying != nil {
		data, err := r.underlying.ReadRange(ctx, bucket, key, start, end)
		if err != nil {
			return nil, err
		}
		if int64(len(data)) > limit {
			data = data[:limit]
		}
		return data, nil
	}

	return nil, errors.New("byte range reader is not configured")
}

// HeaderInspector abstracts random-access byte inspection over S3 objects.
type HeaderInspector interface {
	InspectHeader(ctx context.Context, req coordinatorv1.InspectDocumentRequest) (*coordinatorv1.InspectDocumentResponse, error)
}

// ByteRangeHeaderInspector inspects document and media container headers/footers via bounded
// S3 byte-range reads (4 KiB head window + <= 64 KiB tail window) with 2-unit HeartbeatDetails
// checkpoint recovery.
type ByteRangeHeaderInspector struct {
	reader *LimitedByteRangeReader
}

// NewByteRangeHeaderInspector constructs a ByteRangeHeaderInspector wrapping reader in a 64 KiB cap.
func NewByteRangeHeaderInspector(reader ByteRangeReader) *ByteRangeHeaderInspector {
	if limited, ok := reader.(*LimitedByteRangeReader); ok && limited != nil {
		return &ByteRangeHeaderInspector{reader: limited}
	}
	return &ByteRangeHeaderInspector{
		reader: NewLimitedByteRangeReaderFromReader(reader),
	}
}

// InspectHeader performs 2-stage byte-range inspection with HeartbeatDetails checkpoint resumption.
//   - Unit 1: Reads the 4 KiB header window (bytes=0-4095), verifies format magic bytes, and extracts header metadata.
//   - Unit 2: Reads the <= 64 KiB tail window (bytes=max(0,totalBytes-65536)-(totalBytes-1)) when needed for trailer/atom parsing.
func (i *ByteRangeHeaderInspector) InspectHeader(
	ctx context.Context,
	req coordinatorv1.InspectDocumentRequest,
) (*coordinatorv1.InspectDocumentResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i == nil || i.reader == nil {
		return nil, errors.New("byte range header inspector reader is nil")
	}

	modality, err := ResolveModalityFromMIME(req.MimeType)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrCodeUnsupportedMimeType, err)
	}
	normalizedMIME := strings.ToLower(strings.TrimSpace(req.MimeType))

	var checkpoint coordinatorv1.HeartbeatDetails
	hasPriorCheckpoint := getHeartbeatDetailsSafe(ctx, &checkpoint)

	var headerBytes []byte
	var checkpointPages int32
	var checkpointDuration float64
	resumedFromUnit1 := false

	if hasPriorCheckpoint && checkpoint.LastProcessedUnit >= 1 {
		if pages, dur, ok := parseHeaderCheckpointState(checkpoint.IntermediateS3Key); ok {
			checkpointPages = pages
			checkpointDuration = dur
			resumedFromUnit1 = true
		}
	}

	// Stage / Unit 1: Read 4 KiB header window (bytes=0-4095) unless already checkpointed.
	if !resumedFromUnit1 {
		headerEnd := HeaderInspectionWindowBytes - 1
		if req.TotalBytes-1 < headerEnd {
			headerEnd = req.TotalBytes - 1
		}
		headerBytes, err = i.reader.ReadRange(ctx, req.S3Bucket, req.S3Key, 0, headerEnd)
		if err != nil {
			return nil, err
		}
		if len(headerBytes) == 0 {
			return nil, temporal.NewNonRetryableApplicationError(
				"empty byte range returned for document header",
				ErrCodeCorruptInput,
				service.ErrZeroBytePayload,
			)
		}

		// Validate magic bytes & parse any metadata available in the 4 KiB header window.
		checkpointPages, checkpointDuration, err = inspectHeaderWindow(normalizedMIME, headerBytes, req.TotalBytes)
		if err != nil {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrCodeCorruptInput, err)
		}

		checkpoint = coordinatorv1.HeartbeatDetails{
			SliceID:           "header-inspection",
			SliceIndex:        0,
			LastProcessedUnit: 1,
			ProcessedUnits:    1,
			TotalUnits:        2,
			IntermediateS3Key: formatHeaderCheckpointState(req.S3Key, checkpointPages, checkpointDuration),
			UpdatedAtUnix:     time.Now().UTC().Unix(),
		}
		recordHeartbeatSafe(ctx, checkpoint)
	}

	// Stage / Unit 2: Read <= 64 KiB tail window when inspecting trailers/footers or when header did not resolve full metadata.
	var tailBytes []byte
	needsTailRead := requiresTailWindow(normalizedMIME, req.TotalBytes, checkpointPages, checkpointDuration)
	if needsTailRead {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tailStart := req.TotalBytes - TailInspectionWindowBytes
		if tailStart < 0 {
			tailStart = 0
		}
		tailEnd := req.TotalBytes - 1
		// Avoid duplicate S3 read if the entire file already fit inside the 4 KiB header window we just read in this attempt.
		if !resumedFromUnit1 && req.TotalBytes <= HeaderInspectionWindowBytes && len(headerBytes) > 0 {
			tailBytes = headerBytes
		} else {
			tailBytes, err = i.reader.ReadRange(ctx, req.S3Bucket, req.S3Key, tailStart, tailEnd)
			if err != nil {
				return nil, err
			}
		}
	}

	resp, err := finalizeInspectionResponse(
		modality,
		normalizedMIME,
		headerBytes,
		tailBytes,
		req.TotalBytes,
		checkpointPages,
		checkpointDuration,
	)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrCodeCorruptInput, err)
	}

	checkpoint = coordinatorv1.HeartbeatDetails{
		SliceID:           "header-inspection",
		SliceIndex:        0,
		LastProcessedUnit: 2,
		ProcessedUnits:    2,
		TotalUnits:        2,
		IntermediateS3Key: req.S3Key,
		UpdatedAtUnix:     time.Now().UTC().Unix(),
	}
	recordHeartbeatSafe(ctx, checkpoint)

	return resp, nil
}

// InspectionActivities holds dependencies for document header inspection activities.
type InspectionActivities struct {
	Inspector HeaderInspector
}

// NewInspectionActivities creates an InspectionActivities instance.
func NewInspectionActivities(inspector HeaderInspector) *InspectionActivities {
	return &InspectionActivities{Inspector: inspector}
}

// NewInspectionActivitiesWithRangeReader constructs InspectionActivities backed by a ByteRangeHeaderInspector.
func NewInspectionActivitiesWithRangeReader(reader ByteRangeReader) *InspectionActivities {
	return &InspectionActivities{
		Inspector: NewByteRangeHeaderInspector(reader),
	}
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

// InspectDocumentHeader performs input validation, enforces tenant S3 key boundaries,
// resumes from any prior heartbeat details, emits progress heartbeats, and returns layout metadata.
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
	if err := service.ValidateTenantRawS3Key(req.TenantID, req.S3Key); err != nil {
		errCode := ErrCodeInvalidArgument
		if errors.Is(err, service.ErrTenantMismatch) {
			errCode = ErrCodeTenantMismatch
		}
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), errCode, err)
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

	if a != nil && a.Inspector != nil {
		resp, err := a.Inspector.InspectHeader(ctx, req)
		if err != nil {
			return nil, err
		}
		logInspectionResultSafe(ctx, req, resp)
		return resp, nil
	}

	var checkpoint coordinatorv1.HeartbeatDetails
	_ = getHeartbeatDetailsSafe(ctx, &checkpoint)

	checkpoint.SliceID = "header-inspection"
	checkpoint.TotalUnits = 2
	checkpoint.LastProcessedUnit = 2
	checkpoint.ProcessedUnits = 2
	checkpoint.IntermediateS3Key = req.S3Key
	checkpoint.UpdatedAtUnix = time.Now().UTC().Unix()
	recordHeartbeatSafe(ctx, checkpoint)

	var resp *coordinatorv1.InspectDocumentResponse
	switch modality {
	case coordinatorv1.MultimodalModalityDocumentPDF, coordinatorv1.MultimodalModalityTabular:
		estimatedPages := estimateDocumentPagesFromBytes(req.TotalBytes)
		resp = &coordinatorv1.InspectDocumentResponse{
			Modality:              modality,
			TotalPages:            estimatedPages,
			RecommendedSlicePages: service.DefaultPagesPerSlice,
		}
	case coordinatorv1.MultimodalModalityAudio, coordinatorv1.MultimodalModalityVideo:
		estimatedDuration := estimateMediaDurationFromBytes(req.TotalBytes)
		resp = &coordinatorv1.InspectDocumentResponse{
			Modality:                 modality,
			DurationSeconds:          estimatedDuration,
			RecommendedWindowSeconds: service.DefaultWindowSeconds,
		}
	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unsupported modality for mime_type %s", req.MimeType),
			ErrCodeUnsupportedMimeType,
			nil,
		)
	}

	logInspectionResultSafe(ctx, req, resp)
	return resp, nil
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

func inspectHeaderWindow(mimeType string, header []byte, totalBytes int64) (int32, float64, error) {
	switch mimeType {
	case "application/pdf":
		searchLen := len(header)
		if searchLen > 1024 {
			searchLen = 1024
		}
		if !bytes.Contains(header[:searchLen], []byte("%PDF-")) {
			return 0, 0, fmt.Errorf("%w: missing %%PDF- magic header in first 1024 bytes", service.ErrCorruptedRange)
		}
		if linPages := parsePDFLinearizedPages(header); linPages > 0 {
			return linPages, 0, nil
		}
		if treePages := parsePDFPageTreeCount(header); treePages > 0 {
			return treePages, 0, nil
		}
		return 0, 0, nil

	case "application/vnd.apache.parquet":
		if totalBytes < 12 {
			return 0, 0, fmt.Errorf("%w: parquet file total_bytes (%d) must be >= 12", service.ErrCorruptedRange, totalBytes)
		}
		if len(header) < 4 || string(header[:4]) != "PAR1" {
			return 0, 0, fmt.Errorf("%w: missing PAR1 header magic bytes", service.ErrCorruptedRange)
		}
		return estimateDocumentPagesFromBytes(totalBytes), 0, nil

	case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
		if len(header) < 4 || !bytes.Equal(header[:4], []byte{'P', 'K', 0x03, 0x04}) {
			return 0, 0, fmt.Errorf("%w: missing XLSX PK\\x03\\x04 zip header magic", service.ErrCorruptedRange)
		}
		return estimateDocumentPagesFromBytes(totalBytes), 0, nil

	case "text/csv":
		if err := validateCSVHeaderSample(header); err != nil {
			return 0, 0, err
		}
		return estimateDocumentPagesFromBytes(totalBytes), 0, nil

	case "audio/wav", "audio/x-wav":
		dur, err := parseWAVHeaderDuration(header, totalBytes)
		if err != nil {
			return 0, 0, err
		}
		return 0, dur, nil

	case "audio/flac":
		dur, err := parseFLACHeaderDuration(header, totalBytes)
		if err != nil {
			return 0, 0, err
		}
		return 0, dur, nil

	case "audio/mpeg":
		dur, err := parseMP3HeaderDuration(header, totalBytes)
		if err != nil {
			return 0, 0, err
		}
		return 0, dur, nil

	case "audio/ogg":
		dur, err := parseOggHeaderDuration(header)
		if err != nil {
			return 0, 0, err
		}
		return 0, dur, nil

	case "video/mp4", "video/quicktime":
		dur, err := parseMP4HeaderDuration(header)
		if err != nil {
			return 0, 0, err
		}
		return 0, dur, nil

	case "video/webm":
		if len(header) < 4 || !bytes.Equal(header[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
			return 0, 0, fmt.Errorf("%w: missing WebM EBML 0x1A45DFA3 magic header", service.ErrCorruptedRange)
		}
		return 0, estimateMediaDurationFromBytes(totalBytes), nil

	default:
		return 0, 0, fmt.Errorf("unsupported mime_type %q", mimeType)
	}
}

func requiresTailWindow(mimeType string, totalBytes int64, headerPages int32, headerDuration float64) bool {
	switch mimeType {
	case "application/pdf":
		// Read tail window if PDF is not linearized or if trailer/page-tree is in the tail.
		return headerPages <= 0 || totalBytes > HeaderInspectionWindowBytes
	case "application/vnd.apache.parquet":
		// Always verify trailing PAR1 magic footer on Parquet payloads.
		return true
	case "video/mp4", "video/quicktime":
		// Read tail window when moov/mvhd atom was not placed at the front of the file.
		return headerDuration <= 0
	case "audio/ogg":
		// Read tail window to inspect final OggS page granule position when possible.
		return headerDuration <= 0 && totalBytes > HeaderInspectionWindowBytes
	default:
		return false
	}
}

func finalizeInspectionResponse(
	modality coordinatorv1.MultimodalModality,
	mimeType string,
	header []byte,
	tail []byte,
	totalBytes int64,
	headerPages int32,
	headerDuration float64,
) (*coordinatorv1.InspectDocumentResponse, error) {
	switch mimeType {
	case "application/pdf":
		pages := headerPages
		if len(tail) > 0 {
			if tailCount := parsePDFPageTreeCount(tail); tailCount > pages {
				pages = tailCount
			}
		}
		if pages <= 0 {
			// Deterministic byte-size fallback when PDF 1.5+ compressed /ObjStm streams hide /Count.
			pages = estimateDocumentPagesFromBytes(totalBytes)
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:              modality,
			TotalPages:            pages,
			RecommendedSlicePages: service.DefaultPagesPerSlice,
		}, nil

	case "application/vnd.apache.parquet":
		if len(tail) < 4 || string(tail[len(tail)-4:]) != "PAR1" {
			return nil, fmt.Errorf("%w: missing PAR1 footer magic bytes in parquet trailer", service.ErrCorruptedRange)
		}
		pages := headerPages
		if pages <= 0 {
			pages = estimateDocumentPagesFromBytes(totalBytes)
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:              modality,
			TotalPages:            pages,
			RecommendedSlicePages: service.DefaultPagesPerSlice,
		}, nil

	case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "text/csv":
		pages := headerPages
		if pages <= 0 {
			pages = estimateDocumentPagesFromBytes(totalBytes)
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:              modality,
			TotalPages:            pages,
			RecommendedSlicePages: service.DefaultPagesPerSlice,
		}, nil

	case "video/mp4", "video/quicktime":
		dur := headerDuration
		if dur <= 0 && len(tail) > 0 {
			dur = parseMP4MvhdDuration(tail)
		}
		if dur <= 0 {
			dur = estimateMediaDurationFromBytes(totalBytes)
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:                 modality,
			DurationSeconds:          dur,
			RecommendedWindowSeconds: service.DefaultWindowSeconds,
		}, nil

	case "audio/ogg":
		dur := headerDuration
		if dur <= 0 && len(tail) > 0 {
			dur = parseOggTailDuration(header, tail)
		}
		if dur <= 0 {
			dur = estimateMediaDurationFromBytes(totalBytes)
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:                 modality,
			DurationSeconds:          dur,
			RecommendedWindowSeconds: service.DefaultWindowSeconds,
		}, nil

	case "audio/wav", "audio/x-wav", "audio/flac", "audio/mpeg", "video/webm":
		dur := headerDuration
		if dur <= 0 {
			dur = estimateMediaDurationFromBytes(totalBytes)
		}
		return &coordinatorv1.InspectDocumentResponse{
			Modality:                 modality,
			DurationSeconds:          dur,
			RecommendedWindowSeconds: service.DefaultWindowSeconds,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported mime_type %q", mimeType)
	}
}

func parsePDFLinearizedPages(buf []byte) int32 {
	m := pdfLinearizedCountRegex.FindSubmatch(buf)
	if len(m) == 2 {
		if n, err := strconv.ParseInt(string(m[1]), 10, 32); err == nil && n > 0 {
			return int32(n)
		}
	}
	return 0
}

func parsePDFPageTreeCount(buf []byte) int32 {
	var maxCount int32
	nodes := pdfPagesNodeRegex.FindAll(buf, -1)
	for _, node := range nodes {
		if !pdfTypePagesRegex.Match(node) {
			continue
		}
		matches := pdfCountEntryRegex.FindAllSubmatch(node, -1)
		for _, m := range matches {
			if len(m) == 2 {
				if n, err := strconv.ParseInt(string(m[1]), 10, 32); err == nil && int32(n) > maxCount {
					maxCount = int32(n)
				}
			}
		}
	}
	return maxCount
}

func parseWAVHeaderDuration(header []byte, totalBytes int64) (float64, error) {
	if len(header) < 12 || string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return 0, fmt.Errorf("%w: invalid WAV header (missing RIFF/WAVE magic bytes)", service.ErrCorruptedRange)
	}

	var byteRate uint32
	var dataSize uint32
	offset := 12
	for offset+8 <= len(header) {
		chunkID := string(header[offset : offset+4])
		chunkSize := binary.LittleEndian.Uint32(header[offset+4 : offset+8])
		chunkDataStart := offset + 8

		switch chunkID {
		case "fmt ":
			if chunkSize >= 12 && chunkDataStart+12 <= len(header) {
				byteRate = binary.LittleEndian.Uint32(header[chunkDataStart+8 : chunkDataStart+12])
			}
		case "data":
			dataSize = chunkSize
		}

		if byteRate > 0 && dataSize > 0 {
			break
		}
		step := int(chunkSize)
		if step < 0 {
			break
		}
		if step%2 == 1 {
			step++
		}
		nextOffset := chunkDataStart + step
		if nextOffset <= offset {
			break
		}
		offset = nextOffset
	}

	if byteRate == 0 {
		return 0, fmt.Errorf("%w: WAV header missing valid fmt byte rate", service.ErrCorruptedRange)
	}

	payloadBytes := float64(dataSize)
	if payloadBytes <= 0 && totalBytes > 44 {
		payloadBytes = float64(totalBytes - 44)
	}
	if payloadBytes <= 0 {
		return 0, fmt.Errorf("%w: WAV data size must be > 0", service.ErrCorruptedRange)
	}
	return payloadBytes / float64(byteRate), nil
}

func parseFLACHeaderDuration(header []byte, totalBytes int64) (float64, error) {
	if len(header) < 4 || string(header[0:4]) != "fLaC" {
		return 0, fmt.Errorf("%w: invalid FLAC header (missing fLaC magic bytes)", service.ErrCorruptedRange)
	}
	if len(header) >= 26 {
		blockType := header[4] & 0x7F
		if blockType == 0 {
			// STREAMINFO block starts at byte 8; bytes 18..25 contain sampleRate (20 bits),
			// channels-1 (3 bits), bitsPerSample-1 (5 bits), and totalSamples (36 bits).
			// sampleRate is in bytes 18, 19, and the upper 4 bits of 20: (b[18]<<12) | (b[19]<<4) | (b[20]>>4)
			// totalSamples is 36 bits: lower 4 bits of 21, then bytes 22, 23, 24, 25.
			sampleRate := (uint32(header[18]) << 12) | (uint32(header[19]) << 4) | uint32(header[20]>>4)
			totalSamples := (uint64(header[21]&0x0F) << 32) | uint64(binary.BigEndian.Uint32(header[22:26]))
			if sampleRate > 0 && totalSamples > 0 {
				return float64(totalSamples) / float64(sampleRate), nil
			}
		}
	}
	return estimateMediaDurationFromBytes(totalBytes), nil
}

func parseMP3HeaderDuration(header []byte, totalBytes int64) (float64, error) {
	if len(header) < 4 {
		return 0, fmt.Errorf("%w: MP3 header too short", service.ErrCorruptedRange)
	}

	hasID3 := len(header) >= 3 && string(header[0:3]) == "ID3"
	searchOffset := 0
	if hasID3 && len(header) >= 10 {
		id3Size := (int(header[6]&0x7F) << 21) | (int(header[7]&0x7F) << 14) | (int(header[8]&0x7F) << 7) | int(header[9]&0x7F)
		if 10+id3Size < len(header) {
			searchOffset = 10 + id3Size
		}
	}

	// MPEG-1 Layer III bitrate lookup table (in kbps).
	mpeg1Layer3Bitrates := [16]int{0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0}
	foundSync := false
	var bitrateKbps int

	for idx := searchOffset; idx+4 <= len(header); idx++ {
		if header[idx] == 0xFF && (header[idx+1]&0xE0) == 0xE0 {
			layerBits := (header[idx+1] >> 1) & 0x03
			bitrateIdx := (header[idx+2] >> 4) & 0x0F
			sampleRateIdx := (header[idx+2] >> 2) & 0x03
			if layerBits != 0 && bitrateIdx > 0 && bitrateIdx < 15 && sampleRateIdx < 3 {
				foundSync = true
				bitrateKbps = mpeg1Layer3Bitrates[bitrateIdx]
				break
			}
		}
	}

	if !hasID3 && !foundSync {
		return 0, fmt.Errorf("%w: invalid MP3 header (missing ID3 tag or MPEG frame sync)", service.ErrCorruptedRange)
	}
	if bitrateKbps > 0 {
		return float64(totalBytes*8) / float64(bitrateKbps*1000), nil
	}
	return estimateMediaDurationFromBytes(totalBytes), nil
}

func parseOggHeaderDuration(header []byte) (float64, error) {
	if len(header) < 4 || string(header[0:4]) != "OggS" {
		return 0, fmt.Errorf("%w: invalid Ogg header (missing OggS capture pattern)", service.ErrCorruptedRange)
	}
	return 0, nil
}

func parseOggTailDuration(header, tail []byte) float64 {
	sampleRate := uint32(48000) // default Opus sample rate
	if idx := bytes.Index(header, []byte("\x01vorbis")); idx >= 0 && idx+16 <= len(header) {
		sr := binary.LittleEndian.Uint32(header[idx+11 : idx+15])
		if sr > 0 {
			sampleRate = sr
		}
	}

	lastIdx := bytes.LastIndex(tail, []byte("OggS"))
	if lastIdx >= 0 && lastIdx+14 <= len(tail) {
		granulePos := binary.LittleEndian.Uint64(tail[lastIdx+6 : lastIdx+14])
		if granulePos > 0 && granulePos != ^uint64(0) && sampleRate > 0 {
			return float64(granulePos) / float64(sampleRate)
		}
	}
	return 0
}

func parseMP4HeaderDuration(header []byte) (float64, error) {
	if len(header) < 8 {
		return 0, fmt.Errorf("%w: MP4/MOV header too short", service.ErrCorruptedRange)
	}
	boxType := string(header[4:8])
	switch boxType {
	case "ftyp", "moov", "mdat", "wide", "free", "skip", "styp", "pnot":
	default:
		return 0, fmt.Errorf("%w: invalid ISO BMFF / MP4 top-level box type %q", service.ErrCorruptedRange, boxType)
	}
	return parseMP4MvhdDuration(header), nil
}

func parseMP4MvhdDuration(buf []byte) float64 {
	search := buf
	for {
		idx := bytes.Index(search, []byte("mvhd"))
		if idx < 0 {
			return 0
		}
		atom := search[idx:]
		if len(atom) >= 24 {
			version := atom[4]
			switch version {
			case 0:
				timescale := binary.BigEndian.Uint32(atom[16:20])
				duration := binary.BigEndian.Uint32(atom[20:24])
				if timescale > 0 && duration > 0 {
					return float64(duration) / float64(timescale)
				}
			case 1:
				if len(atom) >= 36 {
					timescale := binary.BigEndian.Uint32(atom[24:28])
					duration := binary.BigEndian.Uint64(atom[28:36])
					if timescale > 0 && duration > 0 {
						return float64(duration) / float64(timescale)
					}
				}
			}
		}
		if len(atom) <= 4 {
			return 0
		}
		search = atom[4:]
	}
}

func validateCSVHeaderSample(header []byte) error {
	if len(header) == 0 {
		return fmt.Errorf("%w: empty CSV payload", service.ErrCorruptedRange)
	}
	if bytes.IndexByte(header, 0x00) >= 0 {
		return fmt.Errorf("%w: binary NUL byte found in CSV header", service.ErrCorruptedRange)
	}
	// Trim any trailing incomplete multi-byte UTF-8 sequence caused by the 4 KiB range boundary.
	sample := header
	for i := 1; i <= 3 && len(sample) > 0 && !utf8.Valid(sample); i++ {
		sample = sample[:len(sample)-1]
	}
	if !utf8.Valid(sample) {
		return fmt.Errorf("%w: CSV header is not valid UTF-8", service.ErrCorruptedRange)
	}
	return nil
}

func estimateDocumentPagesFromBytes(totalBytes int64) int32 {
	estimatedPages := int32(totalBytes / (64 * 1024))
	if estimatedPages < 1 {
		estimatedPages = 1
	}
	return estimatedPages
}

func estimateMediaDurationFromBytes(totalBytes int64) float64 {
	estimatedDuration := float64(totalBytes) / (32 * 1024)
	if estimatedDuration < 1.0 {
		estimatedDuration = 60.0
	}
	return estimatedDuration
}

func formatHeaderCheckpointState(s3Key string, pages int32, duration float64) string {
	return fmt.Sprintf("%s#hdr:pages=%d,dur=%.6f", s3Key, pages, duration)
}

func parseHeaderCheckpointState(intermediateKey string) (int32, float64, bool) {
	idx := strings.LastIndex(intermediateKey, "#hdr:")
	if idx < 0 {
		return 0, 0, false
	}
	meta := intermediateKey[idx+len("#hdr:"):]
	parts := strings.Split(meta, ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	pagesStr := strings.TrimPrefix(parts[0], "pages=")
	durStr := strings.TrimPrefix(parts[1], "dur=")
	pages, err1 := strconv.ParseInt(pagesStr, 10, 32)
	dur, err2 := strconv.ParseFloat(durStr, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return int32(pages), dur, true
}

func getHeartbeatDetailsSafe(ctx context.Context, dst *coordinatorv1.HeartbeatDetails) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, dst); err == nil {
			return true
		}
	}
	return false
}

func recordHeartbeatSafe(ctx context.Context, details coordinatorv1.HeartbeatDetails) {
	defer func() {
		_ = recover()
	}()
	activity.RecordHeartbeat(ctx, details)
}

func logInspectionResultSafe(
	ctx context.Context,
	req coordinatorv1.InspectDocumentRequest,
	resp *coordinatorv1.InspectDocumentResponse,
) {
	defer func() {
		if recover() != nil {
			slog.Default().Info("inspected document header metadata",
				"tenant_id", req.TenantID,
				"document_id", req.DocumentID,
				"s3_bucket", req.S3Bucket,
				"s3_key", req.S3Key,
				"mime_type", req.MimeType,
				"modality", resp.Modality.String(),
				"total_pages", resp.TotalPages,
				"duration_seconds", resp.DurationSeconds,
			)
		}
	}()
	logger := activity.GetLogger(ctx)
	logger.Info("inspected document header metadata",
		"tenant_id", req.TenantID,
		"document_id", req.DocumentID,
		"s3_bucket", req.S3Bucket,
		"s3_key", req.S3Key,
		"mime_type", req.MimeType,
		"modality", resp.Modality.String(),
		"total_pages", resp.TotalPages,
		"duration_seconds", resp.DurationSeconds,
	)
}
