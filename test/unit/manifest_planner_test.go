package unit_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
)

func TestPlanSlices_SinglePagePDF(t *testing.T) {
	t.Parallel()

	input := service.ManifestPlanInput{
		TenantID:      "tenant-corp-01",
		DocumentID:    "doc-single-01",
		ContentHash:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		S3InputBucket: "rag-ingestion-bucket",
		S3InputKey:    "raw/tenant-corp-01/hash/single.pdf",
		TotalBytes:    32768,
		Modality:      coordinatorv1.MultimodalModalityDocumentPDF,
		TotalPages:    1,
		PagesPerSlice: 50,
	}

	slices, err := service.PlanSlices(input)
	require.NoError(t, err)
	require.Len(t, slices, 1)

	s := slices[0]
	assert.Equal(t, int32(0), s.SliceIndex)
	assert.Equal(t, int32(1), s.TotalSlices)
	assert.Equal(t, int32(1), s.StartPage)
	assert.Equal(t, int32(1), s.EndPage)
	assert.Equal(t, "artifacts/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/slice_1_1.parquet", s.S3OutputParquetKey)
	assert.Len(t, s.SliceID, 64)
}

func TestPlanSlices_20000PageDocument_NonOverlapping(t *testing.T) {
	t.Parallel()

	input := service.ManifestPlanInput{
		TenantID:      "tenant-enterprise-99",
		DocumentID:    "doc-20k-pages",
		ContentHash:   "a1b2c3d4e5f6789012345678abcdef01a1b2c3d4e5f6789012345678abcdef01",
		S3InputBucket: "rag-ingestion-bucket",
		S3InputKey:    "raw/tenant-enterprise-99/hash/encyclopedia.pdf",
		TotalBytes:    2 * 1024 * 1024 * 1024, // 2 GiB
		Modality:      coordinatorv1.MultimodalModalityDocumentPDF,
		TotalPages:    20000,
		PagesPerSlice: 50,
	}

	slices, err := service.PlanSlices(input)
	require.NoError(t, err)
	require.Len(t, slices, 400)

	seenSliceIDs := make(map[string]struct{}, 400)
	seenOutputKeys := make(map[string]struct{}, 400)

	expectedStart := int32(1)
	for i, s := range slices {
		assert.Equal(t, int32(i), s.SliceIndex)
		assert.Equal(t, int32(400), s.TotalSlices)
		assert.Equal(t, expectedStart, s.StartPage)
		expectedEnd := expectedStart + 49
		assert.Equal(t, expectedEnd, s.EndPage)

		_, idDup := seenSliceIDs[s.SliceID]
		assert.False(t, idDup, "duplicate slice_id at index %d", i)
		seenSliceIDs[s.SliceID] = struct{}{}

		_, keyDup := seenOutputKeys[s.S3OutputParquetKey]
		assert.False(t, keyDup, "duplicate s3_output_parquet_key at index %d", i)
		seenOutputKeys[s.S3OutputParquetKey] = struct{}{}

		expectedStart = expectedEnd + 1
	}
	assert.Equal(t, int32(20000), slices[len(slices)-1].EndPage)
}

func TestPlanSlices_DeterministicIdempotencyAndHashPrefixRunbook2(t *testing.T) {
	t.Parallel()

	baseInput := service.ManifestPlanInput{
		TenantID:      "tenant-corp-01",
		DocumentID:    "doc-idempotent-01",
		ContentHash:   "deadbeefcafe0123456789abcdef0123456789abcdef0123456789abcdef0123",
		S3InputBucket: "rag-ingestion-bucket",
		S3InputKey:    "raw/tenant-corp-01/deadbeef/report.pdf",
		TotalBytes:    10 * 1024 * 1024,
		Modality:      coordinatorv1.MultimodalModalityDocumentPDF,
		TotalPages:    125,
		PagesPerSlice: 50,
	}

	firstRun, err := service.PlanSlices(baseInput)
	require.NoError(t, err)
	secondRun, err := service.PlanSlices(baseInput)
	require.NoError(t, err)
	require.Equal(t, firstRun, secondRun, "manifest planning must be 100%% deterministic")
	require.Len(t, firstRun, 3)

	assert.Equal(t, int32(1), firstRun[0].StartPage)
	assert.Equal(t, int32(50), firstRun[0].EndPage)
	assert.Equal(t, int32(51), firstRun[1].StartPage)
	assert.Equal(t, int32(100), firstRun[1].EndPage)
	assert.Equal(t, int32(101), firstRun[2].StartPage)
	assert.Equal(t, int32(125), firstRun[2].EndPage)

	// Enable Runbook 2 hash-prefixed S3 layout to mitigate HTTP 503 SlowDown.
	hashPrefixInput := baseInput
	hashPrefixInput.UseHashPrefixS3Layout = true
	hashedSlices, err := service.PlanSlices(hashPrefixInput)
	require.NoError(t, err)
	require.Len(t, hashedSlices, 3)

	for i, s := range hashedSlices {
		expectedPrefix := service.ComputeS3PartitionHashPrefix(
			s.DocumentID,
			fmt.Sprintf("%d_%d", s.StartPage, s.EndPage),
		)
		assert.Len(t, expectedPrefix, 4)
		expectedKey := fmt.Sprintf(
			"artifacts/%s/%s/%s/slice_%d_%d.parquet",
			expectedPrefix, s.TenantID, s.ContentHash, s.StartPage, s.EndPage,
		)
		assert.Equal(t, expectedKey, s.S3OutputParquetKey, "slice %d hash-prefixed key mismatch", i)
		assert.True(t, strings.HasPrefix(s.S3OutputParquetKey, "artifacts/"+expectedPrefix+"/"))
	}
}

func TestPlanSlices_MediaSlidingWindows(t *testing.T) {
	t.Parallel()

	input := service.ManifestPlanInput{
		TenantID:         "tenant-media-01",
		DocumentID:       "doc-video-01",
		ContentHash:      "11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff",
		S3InputBucket:    "rag-ingestion-bucket",
		S3InputKey:       "raw/tenant-media-01/hash/keynote.mp4",
		TotalBytes:       500 * 1024 * 1024,
		Modality:         coordinatorv1.MultimodalModalityVideo,
		DurationSeconds:  1500.0,
		WindowSeconds:    600.0,
		OverlapSeconds:   30.0,
		HasOverlapConfig: true,
	}

	slices, err := service.PlanSlices(input)
	require.NoError(t, err)
	require.Len(t, slices, 3)

	// Window 0: [0, 600], stride = 570
	assert.InDelta(t, 0.0, slices[0].StartTimeSeconds, 0.001)
	assert.InDelta(t, 600.0, slices[0].EndTimeSeconds, 0.001)
	assert.InDelta(t, 30.0, slices[0].OverlapSeconds, 0.001)

	// Window 1: [570, 1170]
	assert.InDelta(t, 570.0, slices[1].StartTimeSeconds, 0.001)
	assert.InDelta(t, 1170.0, slices[1].EndTimeSeconds, 0.001)

	// Window 2: [1140, 1500]
	assert.InDelta(t, 1140.0, slices[2].StartTimeSeconds, 0.001)
	assert.InDelta(t, 1500.0, slices[2].EndTimeSeconds, 0.001)
}

func TestPlanSlices_RejectsInvalidAndCorruptedInputs(t *testing.T) {
	t.Parallel()

	validBase := service.ManifestPlanInput{
		TenantID:      "tenant-01",
		DocumentID:    "doc-01",
		ContentHash:   "hash-01",
		S3InputBucket: "bucket-01",
		S3InputKey:    "raw/tenant-01/hash-01/doc.pdf",
		TotalBytes:    1024,
		Modality:      coordinatorv1.MultimodalModalityDocumentPDF,
		TotalPages:    10,
		PagesPerSlice: 50,
	}

	tests := []struct {
		name        string
		mutate      func(*service.ManifestPlanInput)
		expectedErr error
	}{
		{
			name:        "zero byte upload",
			mutate:      func(in *service.ManifestPlanInput) { in.TotalBytes = 0 },
			expectedErr: service.ErrZeroBytePayload,
		},
		{
			name:        "negative byte upload",
			mutate:      func(in *service.ManifestPlanInput) { in.TotalBytes = -512 },
			expectedErr: service.ErrZeroBytePayload,
		},
		{
			name:        "exceeds 10GB limit",
			mutate:      func(in *service.ManifestPlanInput) { in.TotalBytes = service.MaxPayloadBytes + 1 },
			expectedErr: service.ErrPayloadTooLarge,
		},
		{
			name:        "zero pages",
			mutate:      func(in *service.ManifestPlanInput) { in.TotalPages = 0 },
			expectedErr: service.ErrCorruptedRange,
		},
		{
			name:        "negative pages",
			mutate:      func(in *service.ManifestPlanInput) { in.TotalPages = -5 },
			expectedErr: service.ErrCorruptedRange,
		},
		{
			name:        "pages per slice exceeds max 100",
			mutate:      func(in *service.ManifestPlanInput) { in.PagesPerSlice = 150 },
			expectedErr: service.ErrCorruptedRange,
		},
		{
			name:        "negative pages per slice",
			mutate:      func(in *service.ManifestPlanInput) { in.PagesPerSlice = -10 },
			expectedErr: service.ErrCorruptedRange,
		},
		{
			name:        "missing tenant_id",
			mutate:      func(in *service.ManifestPlanInput) { in.TenantID = "   " },
			expectedErr: service.ErrInvalidManifestInput,
		},
		{
			name:        "unsupported modality",
			mutate:      func(in *service.ManifestPlanInput) { in.Modality = coordinatorv1.MultimodalModalityUnspecified },
			expectedErr: service.ErrInvalidManifestInput,
		},
		{
			name: "media with non-positive duration",
			mutate: func(in *service.ManifestPlanInput) {
				in.Modality = coordinatorv1.MultimodalModalityAudio
				in.DurationSeconds = 0
			},
			expectedErr: service.ErrCorruptedRange,
		},
		{
			name: "media with overlap >= window",
			mutate: func(in *service.ManifestPlanInput) {
				in.Modality = coordinatorv1.MultimodalModalityAudio
				in.DurationSeconds = 300
				in.WindowSeconds = 60
				in.OverlapSeconds = 60
				in.HasOverlapConfig = true
			},
			expectedErr: service.ErrCorruptedRange,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := validBase
			tc.mutate(&candidate)
			_, err := service.PlanSlices(candidate)
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}
}
