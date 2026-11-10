package unit_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
)

// syntheticStorage implements activity.ByteRangeReader using an in-memory byte slice.
type syntheticStorage struct {
	data []byte
}

func (s *syntheticStorage) ReadRange(_ context.Context, _, _ string, start, end int64) ([]byte, error) {
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid range [%d, %d]", start, end)
	}
	if start >= int64(len(s.data)) {
		return []byte{}, nil
	}
	actualEnd := end + 1
	if actualEnd > int64(len(s.data)) {
		actualEnd = int64(len(s.data))
	}
	return s.data[start:actualEnd], nil
}

// TestInspection_SyntheticPDF verifies standard, linearized, page tree, and compressed stream fallbacks.
func TestInspection_SyntheticPDF(t *testing.T) {
	t.Parallel()

	t.Run("standard linearized PDF", func(t *testing.T) {
		pdf := []byte("%PDF-1.7\n1 0 obj\n<< /Linearized 1.0 /L 10240 /O 12 /E 8192 /N 42 /T 9800 >>\nendobj\n")
		// Pad to > 4 KiB
		pdf = append(pdf, bytes.Repeat([]byte(" "), 5000)...)

		storage := &syntheticStorage{data: pdf}
		inspector := activity.NewByteRangeHeaderInspector(storage)
		act := activity.NewInspectionActivities(inspector)

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-pdf-01",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/linear.pdf",
			TotalBytes: int64(len(pdf)),
			MimeType:   "application/pdf",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, coordinatorv1.MultimodalModalityDocumentPDF, resp.Modality)
		assert.Equal(t, int32(42), resp.TotalPages)
		assert.Equal(t, int32(50), resp.RecommendedSlicePages)
	})

	t.Run("non-linearized PDF with trailer page tree count", func(t *testing.T) {
		head := []byte("%PDF-1.4\n%âãÏÓ\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
		head = append(head, bytes.Repeat([]byte("x"), 10000)...)
		tail := []byte("2 0 obj\n<< /Type /Pages /Count 120 /Kids [ 3 0 R ] >>\nendobj\nxref\ntrailer\n<< /Root 1 0 R >>\nstartxref\n10100\n%%EOF\n")
		fullPDF := append(head, tail...)

		storage := &syntheticStorage{data: fullPDF}
		inspector := activity.NewByteRangeHeaderInspector(storage)
		act := activity.NewInspectionActivities(inspector)

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-pdf-02",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/tree.pdf",
			TotalBytes: int64(len(fullPDF)),
			MimeType:   "application/pdf",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(120), resp.TotalPages)
	})

	t.Run("compressed PDF 1.5 objstm fallback to byte size estimate", func(t *testing.T) {
		// Valid PDF header without cleartext /Count
		pdf := []byte("%PDF-1.5\n%stream-compressed-object-data\n")
		pdf = append(pdf, bytes.Repeat([]byte{0x1F, 0x8B}, 128*1024)...) // 256 KiB -> ~4 pages

		storage := &syntheticStorage{data: pdf}
		inspector := activity.NewByteRangeHeaderInspector(storage)
		act := activity.NewInspectionActivities(inspector)

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-pdf-03",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/compressed.pdf",
			TotalBytes: int64(len(pdf)),
			MimeType:   "application/pdf",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(len(pdf)/(64*1024)), resp.TotalPages)
	})

	t.Run("invalid PDF magic header rejected", func(t *testing.T) {
		badPDF := []byte("NOT-A-PDF-FILE\nheader stuff\n")
		storage := &syntheticStorage{data: badPDF}
		inspector := activity.NewByteRangeHeaderInspector(storage)
		act := activity.NewInspectionActivities(inspector)

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-pdf-04",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/bad.pdf",
			TotalBytes: int64(len(badPDF)),
			MimeType:   "application/pdf",
		}

		_, err := act.InspectDocumentHeader(context.Background(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CORRUPT_INPUT")
	})
}

// TestInspection_SyntheticAudio verifies WAV, FLAC, and MP3 duration parsing.
func TestInspection_SyntheticAudio(t *testing.T) {
	t.Parallel()

	t.Run("WAV duration calculation", func(t *testing.T) {
		// 44-byte WAV header: 44.1kHz 16-bit stereo = 176,400 bytes/sec
		byteRate := uint32(176400)
		dataSize := uint32(176400 * 10) // 10 seconds of audio

		buf := new(bytes.Buffer)
		buf.WriteString("RIFF")
		_ = binary.Write(buf, binary.LittleEndian, uint32(36+dataSize))
		buf.WriteString("WAVE")
		buf.WriteString("fmt ")
		_ = binary.Write(buf, binary.LittleEndian, uint32(16))    // chunk size
		_ = binary.Write(buf, binary.LittleEndian, uint16(1))     // PCM format
		_ = binary.Write(buf, binary.LittleEndian, uint16(2))     // channels
		_ = binary.Write(buf, binary.LittleEndian, uint32(44100)) // sample rate
		_ = binary.Write(buf, binary.LittleEndian, byteRate)      // byte rate
		_ = binary.Write(buf, binary.LittleEndian, uint16(4))     // block align
		_ = binary.Write(buf, binary.LittleEndian, uint16(16))    // bits per sample
		buf.WriteString("data")
		_ = binary.Write(buf, binary.LittleEndian, dataSize)
		buf.Write(make([]byte, 100)) // placeholder data

		wavBytes := buf.Bytes()
		storage := &syntheticStorage{data: wavBytes}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-audio-01",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/test.wav",
			TotalBytes: int64(len(wavBytes)),
			MimeType:   "audio/wav",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, coordinatorv1.MultimodalModalityAudio, resp.Modality)
		assert.InDelta(t, 10.0, resp.DurationSeconds, 0.01)
	})

	t.Run("FLAC duration calculation", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("fLaC")
		buf.WriteByte(0x00)                 // metadata block header: last=false, type=STREAMINFO (0)
		buf.Write([]byte{0x00, 0x00, 0x22}) // block length 34 bytes
		// STREAMINFO block
		buf.Write(make([]byte, 10)) // min/max block size & frame size (bytes 8..17)
		// bytes 18..25:
		// sampleRate (20 bits): 44100 = 0x0AC44
		// channels-1 (3 bits): 2 channels - 1 = 1 = 0b001
		// bitsPerSample-1 (5 bits): 16 bits - 1 = 15 = 0b01111
		// totalSamples (36 bits): 441000 = 0x00006BA48
		// byte 18: 0x0A
		// byte 19: 0xC4
		// byte 20: (sampleRate & 0xF)<<4 | (channels-1 & 0x7)<<1 | (bitsPerSample-1 >> 4)
		//          = 0x40 | 0x02 | 0x00 = 0x42
		// byte 21: (bitsPerSample-1 & 0xF)<<4 | (totalSamples >> 32 & 0xF)
		//          = 0xF0 | 0x00 = 0xF0
		// bytes 22..25: totalSamples lower 32 bits = 0x0006BA48
		b18 := byte(0x0A)
		b19 := byte(0xC4)
		b20 := byte(0x42)
		b21 := byte(0xF0)
		buf.Write([]byte{b18, b19, b20, b21, 0x00, 0x06, 0xBA, 0x48})
		buf.Write(make([]byte, 16)) // MD5 signature

		flacBytes := buf.Bytes()
		storage := &syntheticStorage{data: flacBytes}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-audio-02",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/test.flac",
			TotalBytes: int64(len(flacBytes)),
			MimeType:   "audio/flac",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.InDelta(t, 10.0, resp.DurationSeconds, 0.1)
	})

	t.Run("MP3 duration calculation via frame sync", func(t *testing.T) {
		buf := new(bytes.Buffer)
		// MPEG-1 Layer 3 sync header: 0xFF, 0xFB, 0x90, 0x64 (128 kbps, 44100 Hz)
		buf.Write([]byte{0xFF, 0xFB, 0x90, 0x64})
		// 128 kbps = 16,000 bytes/sec. For 10 seconds = 160,000 bytes
		totalBytes := int64(160000)
		buf.Write(bytes.Repeat([]byte{0xAA}, 1024))

		storage := &syntheticStorage{data: buf.Bytes()}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-audio-03",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/test.mp3",
			TotalBytes: totalBytes,
			MimeType:   "audio/mpeg",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.InDelta(t, 10.0, resp.DurationSeconds, 0.1)
	})
}

// TestInspection_SyntheticVideoMP4 verifies ISO BMFF / MP4 mvhd duration parsing (v0 and v1).
func TestInspection_SyntheticVideoMP4(t *testing.T) {
	t.Parallel()

	t.Run("MP4 mvhd version 0 32-bit duration", func(t *testing.T) {
		buf := new(bytes.Buffer)
		// ftyp atom
		buf.Write([]byte{0x00, 0x00, 0x00, 0x14, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm', 0x00, 0x00, 0x02, 0x00, 'm', 'p', '4', '1'})
		// moov atom
		moovStart := buf.Len()
		buf.Write([]byte{0x00, 0x00, 0x00, 0x00, 'm', 'o', 'o', 'v'})
		// mvhd atom
		mvhdStart := buf.Len()
		buf.Write([]byte{0x00, 0x00, 0x00, 0x6C, 'm', 'v', 'h', 'd'})
		buf.WriteByte(0x00)                       // version 0
		buf.Write([]byte{0x00, 0x00, 0x00})       // flags
		buf.Write([]byte{0x00, 0x00, 0x00, 0x00}) // creation time
		buf.Write([]byte{0x00, 0x00, 0x00, 0x00}) // modification time
		// timescale = 1000, duration = 15000 -> 15.0 seconds
		_ = binary.Write(buf, binary.BigEndian, uint32(1000))
		_ = binary.Write(buf, binary.BigEndian, uint32(15000))
		buf.Write(make([]byte, 80)) // remaining mvhd fields

		data := buf.Bytes()
		// fix moov length
		binary.BigEndian.PutUint32(data[moovStart:moovStart+4], uint32(len(data)-moovStart))
		_ = mvhdStart

		storage := &syntheticStorage{data: data}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-video-01",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/test.mp4",
			TotalBytes: int64(len(data)),
			MimeType:   "video/mp4",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, coordinatorv1.MultimodalModalityVideo, resp.Modality)
		assert.InDelta(t, 15.0, resp.DurationSeconds, 0.01)
	})

	t.Run("MP4 mvhd version 1 64-bit duration in tail window", func(t *testing.T) {
		// Place ftyp and mdat in head, moov/mvhd at the end (tail window)
		head := []byte{0x00, 0x00, 0x00, 0x14, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm', 0x00, 0x00, 0x02, 0x00, 'm', 'p', '4', '1'}
		head = append(head, []byte{0x00, 0x00, 0x10, 0x00, 'm', 'd', 'a', 't'}...)
		head = append(head, bytes.Repeat([]byte{0x00}, 20000)...) // padding > 16 KiB

		tailBuf := new(bytes.Buffer)
		tailBuf.Write([]byte{0x00, 0x00, 0x00, 0x00, 'm', 'o', 'o', 'v'})
		tailBuf.Write([]byte{0x00, 0x00, 0x00, 0x78, 'm', 'v', 'h', 'd'})
		tailBuf.WriteByte(0x01)                 // version 1
		tailBuf.Write([]byte{0x00, 0x00, 0x00}) // flags
		tailBuf.Write(make([]byte, 16))         // 64-bit creation & modification times
		// timescale = 600, duration = 36000 -> 60.0 seconds
		_ = binary.Write(tailBuf, binary.BigEndian, uint32(600))
		_ = binary.Write(tailBuf, binary.BigEndian, uint64(36000))
		tailBuf.Write(make([]byte, 80))

		tail := tailBuf.Bytes()
		binary.BigEndian.PutUint32(tail[0:4], uint32(len(tail)))
		fullVideo := append(head, tail...)

		storage := &syntheticStorage{data: fullVideo}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-video-02",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/tail_moov.mp4",
			TotalBytes: int64(len(fullVideo)),
			MimeType:   "video/mp4",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.InDelta(t, 60.0, resp.DurationSeconds, 0.01)
	})
}

// TestInspection_SyntheticTabular verifies Parquet (PAR1 magic head & tail), XLSX, and CSV.
func TestInspection_SyntheticTabular(t *testing.T) {
	t.Parallel()

	t.Run("Parquet head and tail PAR1 verification", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("PAR1")
		buf.Write(bytes.Repeat([]byte{0x01}, 1000))
		buf.WriteString("PAR1")
		data := buf.Bytes()

		storage := &syntheticStorage{data: data}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-tab-01",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/data.parquet",
			TotalBytes: int64(len(data)),
			MimeType:   "application/vnd.apache.parquet",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, coordinatorv1.MultimodalModalityTabular, resp.Modality)
		assert.GreaterOrEqual(t, resp.TotalPages, int32(1))
	})

	t.Run("Parquet missing footer PAR1 rejected", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteString("PAR1")
		buf.Write(bytes.Repeat([]byte{0x01}, 1000))
		buf.WriteString("BAD1")
		data := buf.Bytes()

		storage := &syntheticStorage{data: data}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-tab-02",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/bad_parquet.parquet",
			TotalBytes: int64(len(data)),
			MimeType:   "application/vnd.apache.parquet",
		}

		_, err := act.InspectDocumentHeader(context.Background(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CORRUPT_INPUT")
	})

	t.Run("CSV UTF-8 validation and NUL byte rejection", func(t *testing.T) {
		csvData := []byte("id,name,value\n1,Alice,100\n2,Bob,200\n")
		storage := &syntheticStorage{data: csvData}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-csv-01",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/test.csv",
			TotalBytes: int64(len(csvData)),
			MimeType:   "text/csv",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, coordinatorv1.MultimodalModalityTabular, resp.Modality)

		// NUL byte injection
		corruptCSV := []byte("id,name\n1,A\x00lice\n")
		storage.data = corruptCSV
		req.TotalBytes = int64(len(corruptCSV))
		_, err = act.InspectDocumentHeader(context.Background(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CORRUPT_INPUT")
	})

	t.Run("XLSX zip magic verification", func(t *testing.T) {
		xlsxData := []byte{'P', 'K', 0x03, 0x04, 0x14, 0x00, 0x06, 0x00}
		xlsxData = append(xlsxData, bytes.Repeat([]byte{0x00}, 500)...)
		storage := &syntheticStorage{data: xlsxData}
		act := activity.NewInspectionActivities(activity.NewByteRangeHeaderInspector(storage))

		req := coordinatorv1.InspectDocumentRequest{
			TenantID:   "tenant-corp-01",
			DocumentID: "doc-xlsx-01",
			S3Bucket:   "rag-ingestion-bucket",
			S3Key:      "raw/tenant-corp-01/hash/sheet.xlsx",
			TotalBytes: int64(len(xlsxData)),
			MimeType:   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		}

		resp, err := act.InspectDocumentHeader(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, coordinatorv1.MultimodalModalityTabular, resp.Modality)
	})
}

// TestInspection_LimitedByteRangeReader_MemoryCap verifies that reading >64 KiB is clamped via LimitReader.
func TestInspection_LimitedByteRangeReader_MemoryCap(t *testing.T) {
	t.Parallel()

	bigData := bytes.Repeat([]byte("B"), 256*1024) // 256 KiB
	storage := &syntheticStorage{data: bigData}
	limited := activity.NewLimitedByteRangeReaderFromReader(storage)

	data, err := limited.ReadRange(context.Background(), "bucket", "key", 0, 200*1024)
	require.NoError(t, err)
	assert.Equal(t, int(activity.MaxInspectionRangeBytes), len(data), "must enforce 64 KiB ceiling")
}
