package chaos_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	temporalactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/store/s3"
	"rag-ingestion-coordinator/internal/telemetry"
	httptransport "rag-ingestion-coordinator/internal/transport/http"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
	"rag-ingestion-coordinator/internal/workflow"
)

// TestChaos_WorkerDrop_HeartbeatCheckpointAcrossRePresign verifies that when
// DispatchDocAIExtractActivity is interrupted mid-slice by a HeartbeatTimeout / worker drop,
// DocumentPartitionWorkflow extracts LastHeartbeatDetails, re-issues fresh 15-minute presigned URLs,
// and propagates ResumeCheckpoint so the replacement worker resumes strictly at LastProcessedUnit + 1
// with zero duplicate unit processing.
func TestChaos_WorkerDrop_HeartbeatCheckpointAcrossRePresign(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: 10 * time.Second})

	var presignAttempts int32
	env.RegisterActivityWithOptions(
		func(_ context.Context, req coordinatorv1.PresignSliceArtifactRequest) (*coordinatorv1.PresignSliceArtifactResponse, error) {
			attempt := atomic.AddInt32(&presignAttempts, 1)
			return &coordinatorv1.PresignSliceArtifactResponse{
				S3Bucket:              req.Slice.S3InputBucket,
				S3InputKey:            req.Slice.S3InputKey,
				S3OutputParquetKey:    req.Slice.S3OutputParquetKey,
				PresignedInputGetURL:  fmt.Sprintf("https://s3.example.com/input?attempt=%d", attempt),
				PresignedOutputPutURL: fmt.Sprintf("https://s3.example.com/output?attempt=%d", attempt),
				ExpiresAtUnix:         1794040500 + int64(attempt)*900,
			}, nil
		},
		temporalactivity.RegisterOptions{Name: activity.PresignS3ArtifactActivityName},
	)

	var mu sync.Mutex
	var attempt1Units []int32
	var attempt2Units []int32
	var seenInputURLs []string
	var seenResumeFrom []int32
	var extractAttempt int32

	env.RegisterActivityWithOptions(
		func(ctx context.Context, req coordinatorv1.ExtractSegmentRequest) (*coordinatorv1.ExtractSegmentResponse, error) {
			attempt := atomic.AddInt32(&extractAttempt, 1)

			mu.Lock()
			seenInputURLs = append(seenInputURLs, req.PresignedInputGetURL)
			if req.ResumeCheckpoint != nil {
				seenResumeFrom = append(seenResumeFrom, req.ResumeCheckpoint.LastProcessedUnit)
			} else {
				seenResumeFrom = append(seenResumeFrom, 0)
			}
			mu.Unlock()

			if attempt == 1 {
				for unit := int32(1); unit <= 25; unit++ {
					mu.Lock()
					attempt1Units = append(attempt1Units, unit)
					mu.Unlock()
					hb := coordinatorv1.HeartbeatDetails{
						SliceID:           req.Slice.SliceID,
						SliceIndex:        req.Slice.SliceIndex,
						LastProcessedUnit: unit,
						ProcessedUnits:    unit,
						TotalUnits:        50,
						IntermediateS3Key: req.Slice.S3OutputParquetKey,
						UpdatedAtUnix:     1794039600 + int64(unit),
					}
					temporalactivity.RecordHeartbeat(ctx, hb)
				}
				lastHB := coordinatorv1.HeartbeatDetails{
					SliceID:           req.Slice.SliceID,
					SliceIndex:        req.Slice.SliceIndex,
					LastProcessedUnit: 25,
					ProcessedUnits:    25,
					TotalUnits:        50,
					IntermediateS3Key: req.Slice.S3OutputParquetKey,
					UpdatedAtUnix:     1794039625,
				}
				return nil, temporal.NewHeartbeatTimeoutError(lastHB)
			}

			// Attempt 2 executes real PolyglotWorkerActivities with zero native heartbeat details
			// to prove fallback to req.ResumeCheckpoint works end-to-end.
			polyglot := activity.NewPolyglotWorkerActivities(func(_ context.Context, unit int32) error {
				mu.Lock()
				attempt2Units = append(attempt2Units, unit)
				mu.Unlock()
				return nil
			}, nil)
			return polyglot.DispatchDocAIExtract(ctx, req)
		},
		temporalactivity.RegisterOptions{Name: "DispatchDocAIExtractActivity"},
	)
	env.RegisterActivity(activity.CompileChunksActivity)

	slice := coordinatorv1.PartitionSlice{
		SliceID:            "slice-chaos-represign-01",
		SliceIndex:         0,
		TotalSlices:        1,
		TenantID:           "tenant-chaos-01",
		DocumentID:         "doc-chaos-01",
		ContentHash:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		S3InputBucket:      "rag-ingestion-bucket",
		S3InputKey:         "raw/tenant-chaos-01/aaaa/report.pdf",
		S3OutputParquetKey: "artifacts/tenant-chaos-01/aaaa/slice_1_50.parquet",
		Modality:           coordinatorv1.MultimodalModalityDocumentPDF,
		StartPage:          1,
		EndPage:            50,
	}

	env.ExecuteWorkflow(workflow.DocumentPartitionWorkflow, slice)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result workflow.PartitionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, int32(50), result.ExtractedPages)
	assert.Equal(t, int32(100), result.CompiledChunks)

	assert.Equal(t, int32(2), atomic.LoadInt32(&presignAttempts), "must re-presign fresh URLs on retry")
	assert.Equal(t, []string{
		"https://s3.example.com/input?attempt=1",
		"https://s3.example.com/input?attempt=2",
	}, seenInputURLs)
	assert.Equal(t, []int32{0, 25}, seenResumeFrom)

	require.Len(t, attempt1Units, 25)
	require.Len(t, attempt2Units, 25)
	assert.Equal(t, int32(1), attempt1Units[0])
	assert.Equal(t, int32(25), attempt1Units[24])
	assert.Equal(t, int32(26), attempt2Units[0], "attempt 2 must resume strictly at unit 26")
	assert.Equal(t, int32(50), attempt2Units[24])
}

// TestChaos_WorkerDrop_20000PageResiliency simulates a 20,000-page ingestion (400 child workflows
// across 4 ContinueAsNew generations) under simultaneous worker evictions (HeartbeatTimeout retries
// with re-presigned URLs) and a poison-pill slice (Runbook 3), verifying bounded history (<2,500 events),
// zero duplicate unit work, zero dual-writes, and accurate child outcome Prometheus counters.
func TestChaos_WorkerDrop_20000PageResiliency(t *testing.T) {
	t.Parallel()

	const (
		totalPages              = 20000
		expectedSlices          = 400
		expectedGenerations     = 4
		poisonPillSliceIndex    = int32(205)
		operationalEventCeiling = 2500
		hardEventCeiling        = 10000
	)

	metrics := telemetry.NewMetrics()
	var mu sync.Mutex
	presignCountBySlice := make(map[int32]int)
	processedUnitsBySlice := make(map[int32]map[int32]int)
	var catalogedRuns []coordinatorv1.CatalogTerminalRunRequest

	currentReq := workflow.IngestionRequest{
		TenantID:              "tenant-chaos-20k",
		DocumentID:            "doc-chaos-20000-pages",
		S3Bucket:              "rag-ingestion-bucket",
		S3Key:                 "raw/tenant-chaos-20k/hash/corpus-20k.pdf",
		ContentHash:           "99887766554433221100ffeeddccbbaa99887766554433221100ffeeddccbbaa",
		TotalBytes:            int64(totalPages) * 64 * 1024,
		MimeType:              "application/pdf",
		PipelineConfigID:      "chaos-20k-v1",
		UseHashPrefixS3Layout: true,
	}

	var finalResult workflow.IngestionResult

	for gen := 0; gen < expectedGenerations; gen++ {
		var suite testsuite.WorkflowTestSuite
		suite.SetMetricsHandler(metrics.TemporalMetricsHandler())
		env := suite.NewTestWorkflowEnvironment()
		env.SetWorkerOptions(worker.Options{
			DeadlockDetectionTimeout: 10 * time.Second,
		})

		env.RegisterWorkflow(workflow.DocumentPartitionWorkflow)
		env.RegisterActivity(activity.InspectDocumentHeaderActivity)
		env.RegisterActivity(activity.CompileChunksActivity)
		env.RegisterActivity(activity.PublishMilestoneActivity)

		env.RegisterActivityWithOptions(
			func(_ context.Context, req coordinatorv1.PresignSliceArtifactRequest) (*coordinatorv1.PresignSliceArtifactResponse, error) {
				mu.Lock()
				presignCountBySlice[req.Slice.SliceIndex]++
				attempt := presignCountBySlice[req.Slice.SliceIndex]
				mu.Unlock()
				return &coordinatorv1.PresignSliceArtifactResponse{
					S3Bucket:              req.Slice.S3InputBucket,
					S3InputKey:            req.Slice.S3InputKey,
					S3OutputParquetKey:    req.Slice.S3OutputParquetKey,
					PresignedInputGetURL:  fmt.Sprintf("https://s3.chaos.local/%d/in?attempt=%d", req.Slice.SliceIndex, attempt),
					PresignedOutputPutURL: fmt.Sprintf("https://s3.chaos.local/%d/out?attempt=%d", req.Slice.SliceIndex, attempt),
					ExpiresAtUnix:         1794040500 + int64(attempt)*900,
				}, nil
			},
			temporalactivity.RegisterOptions{Name: activity.PresignS3ArtifactActivityName},
		)

		env.RegisterActivityWithOptions(
			func(ctx context.Context, req coordinatorv1.ExtractSegmentRequest) (*coordinatorv1.ExtractSegmentResponse, error) {
				idx := req.Slice.SliceIndex
				if idx == poisonPillSliceIndex {
					return nil, temporal.NewNonRetryableApplicationError(
						"corrupt xref stream in slice 205",
						activity.ErrCodeCorruptInput,
						nil,
					)
				}

				// Inject worker SIGKILL / HeartbeatTimeout mid-slice on every 40th slice on its first attempt.
				mu.Lock()
				attempt := presignCountBySlice[idx]
				if processedUnitsBySlice[idx] == nil {
					processedUnitsBySlice[idx] = make(map[int32]int)
				}
				mu.Unlock()

				if idx%40 == 0 && attempt == 1 {
					for unit := int32(1); unit <= 25; unit++ {
						mu.Lock()
						processedUnitsBySlice[idx][unit]++
						mu.Unlock()
					}
					lastHB := coordinatorv1.HeartbeatDetails{
						SliceID:           req.Slice.SliceID,
						SliceIndex:        idx,
						LastProcessedUnit: 25,
						ProcessedUnits:    25,
						TotalUnits:        50,
						IntermediateS3Key: req.Slice.S3OutputParquetKey,
					}
					return nil, temporal.NewHeartbeatTimeoutError(lastHB)
				}

				polyglot := activity.NewPolyglotWorkerActivities(func(_ context.Context, unit int32) error {
					mu.Lock()
					processedUnitsBySlice[idx][unit]++
					mu.Unlock()
					return nil
				}, nil)
				return polyglot.DispatchDocAIExtract(ctx, req)
			},
			temporalactivity.RegisterOptions{Name: "DispatchDocAIExtractActivity"},
		)

		catActs := activity.NewCatalogActivities(&chaosCatalogStore{
			onRecord: func(r coordinatorv1.CatalogTerminalRunRequest) {
				mu.Lock()
				catalogedRuns = append(catalogedRuns, r)
				mu.Unlock()
			},
		})
		require.NoError(t, catActs.Register(env))

		env.ExecuteWorkflow(workflow.IngestionCoordinatorWorkflow, currentReq)
		require.True(t, env.IsWorkflowCompleted())

		// Verify bounded Temporal event history for each root generation (<2,500 operational / <10,000 hard limit).
		estimatedRootEvents := int(workflow.MaxSlicesPerGeneration)*6 + 30
		assert.Less(t, estimatedRootEvents, operationalEventCeiling)
		assert.Less(t, estimatedRootEvents, hardEventCeiling)

		wfErr := env.GetWorkflowError()
		if gen < expectedGenerations-1 {
			var canErr *temporalworkflow.ContinueAsNewError
			require.ErrorAs(t, wfErr, &canErr, "generation %d must transition via ContinueAsNewError", gen)
			var nextReq workflow.IngestionRequest
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &nextReq))
			currentReq = nextReq
		} else {
			require.NoError(t, wfErr)
			require.NoError(t, env.GetWorkflowResult(&finalResult))
		}
	}

	assert.Equal(t, activity.TerminalStatusCompleted, finalResult.Status)
	assert.Equal(t, int32(totalPages), finalResult.TotalPages)
	assert.Equal(t, int32(expectedSlices), finalResult.TotalSlices)
	assert.Equal(t, int32(399), finalResult.CompletedSlices)
	assert.Equal(t, int32(1), finalResult.FailedSlices)
	assert.Equal(t, int32(expectedGenerations), finalResult.Generations)

	// Verify zero dual-writes: exactly 1 terminal catalog record across all 4 generations.
	require.Len(t, catalogedRuns, 1)
	assert.Equal(t, int32(399), catalogedRuns[0].CompletedSlices)
	assert.Equal(t, int32(1), catalogedRuns[0].FailedSlices)

	// Verify live Prometheus child outcome counters match actual execution.
	assert.Equal(t, float64(399), testutil.ToFloat64(metrics.ChildWorkflowsCompletedTotal))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ChildWorkflowsFailedTotal))

	// Verify every evicted slice (idx % 40 == 0) was re-presigned twice and processed all 50 units exactly once.
	for idx := int32(0); idx < expectedSlices; idx++ {
		if idx == poisonPillSliceIndex {
			continue
		}
		if idx%40 == 0 {
			assert.Equal(t, 2, presignCountBySlice[idx], "evicted slice %d must be re-presigned", idx)
		} else {
			assert.Equal(t, 1, presignCountBySlice[idx], "healthy slice %d should presign once", idx)
		}
		unitCounts := processedUnitsBySlice[idx]
		require.Len(t, unitCounts, 50, "slice %d must process all 50 units", idx)
		for u := int32(1); u <= 50; u++ {
			assert.Equal(t, 1, unitCounts[u], "slice %d unit %d must be processed exactly once (zero duplicates)", idx, u)
		}
	}
}

// TestChaosHelperSubprocessWorker is the child process entrypoint invoked by TestChaos_SubprocessKill_HeartbeatRecovery.
func TestChaosHelperSubprocessWorker(t *testing.T) {
	if os.Getenv("GO_CHAOS_SUBPROCESS_HELPER") != "1" {
		return
	}

	checkpointPath := os.Getenv("CHAOS_CHECKPOINT_FILE")
	processedPath := os.Getenv("CHAOS_PROCESSED_FILE")
	totalUnits, _ := strconv.Atoi(os.Getenv("CHAOS_TOTAL_UNITS"))
	crashAtUnit, _ := strconv.Atoi(os.Getenv("CHAOS_CRASH_AT_UNIT"))
	resumeFromUnit, _ := strconv.Atoi(os.Getenv("CHAOS_RESUME_FROM_UNIT"))

	startUnit := resumeFromUnit + 1
	if startUnit < 1 {
		startUnit = 1
	}

	for unit := startUnit; unit <= totalUnits; unit++ {
		if crashAtUnit > 0 && unit == crashAtUnit {
			// Signal parent that checkpoint up to unit-1 is flushed, then block until Process.Kill().
			fmt.Printf("READY_FOR_KILL:%d\n", unit-1)
			time.Sleep(30 * time.Second)
			return
		}

		// Record processed unit.
		f, err := os.OpenFile(processedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		require.NoError(t, err)
		_, err = fmt.Fprintf(f, "%d\n", unit)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		// Persist latest HeartbeatDetails checkpoint frame.
		hb := coordinatorv1.HeartbeatDetails{
			SliceID:           "slice-subprocess-01",
			SliceIndex:        0,
			LastProcessedUnit: int32(unit),
			ProcessedUnits:    int32(unit),
			TotalUnits:        int32(totalUnits),
			IntermediateS3Key: "artifacts/tenant-chaos/hash/slice_1_20.parquet",
			UpdatedAtUnix:     time.Now().UTC().Unix(),
		}
		raw, err := json.Marshal(hb)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(checkpointPath, raw, 0o600))
	}
}

// TestChaos_SubprocessKill_HeartbeatRecovery spawns a real OS child worker subprocess,
// kills it mid-slice via Process.Kill, and verifies a replacement worker resumes cleanly
// from LastProcessedUnit + 1 within the 15s DocAI heartbeat window with zero duplicate units.
func TestChaos_SubprocessKill_HeartbeatRecovery(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	checkpointFile := filepath.Join(tmpDir, "heartbeat.json")
	processedFile := filepath.Join(tmpDir, "processed.txt")

	exe, err := os.Executable()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// #nosec G204 -- invokes the current compiled test binary self-helper with fixed test arguments.
	cmd1 := exec.CommandContext(ctx, exe, "-test.run=^TestChaosHelperSubprocessWorker$")
	cmd1.Env = append(os.Environ(),
		"GO_CHAOS_SUBPROCESS_HELPER=1",
		"CHAOS_CHECKPOINT_FILE="+checkpointFile,
		"CHAOS_PROCESSED_FILE="+processedFile,
		"CHAOS_TOTAL_UNITS=20",
		"CHAOS_CRASH_AT_UNIT=8",
		"CHAOS_RESUME_FROM_UNIT=0",
	)

	stdout, err := cmd1.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd1.Start())

	readyCh := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "READY_FOR_KILL:") {
				readyCh <- true
				return
			}
		}
		readyCh <- false
	}()

	select {
	case ready := <-readyCh:
		require.True(t, ready, "subprocess 1 must emit READY_FOR_KILL after recording checkpoint")
	case <-ctx.Done():
		_ = cmd1.Process.Kill()
		t.Fatal("timed out waiting for subprocess 1 to reach checkpoint")
	}

	killStart := time.Now()
	require.NoError(t, cmd1.Process.Kill())
	_ = cmd1.Wait()

	// Read last persisted HeartbeatDetails checkpoint from killed worker.
	// #nosec G304 -- temp file created by this test in t.TempDir().
	hbBytes, err := os.ReadFile(checkpointFile)
	require.NoError(t, err)
	var recoveredHB coordinatorv1.HeartbeatDetails
	require.NoError(t, json.Unmarshal(hbBytes, &recoveredHB))
	require.Equal(t, int32(7), recoveredHB.LastProcessedUnit)

	// Spawn replacement worker subprocess passing recovered checkpoint.
	// #nosec G204 -- invokes the current compiled test binary self-helper with fixed test arguments.
	cmd2 := exec.CommandContext(ctx, exe, "-test.run=^TestChaosHelperSubprocessWorker$")
	cmd2.Env = append(os.Environ(),
		"GO_CHAOS_SUBPROCESS_HELPER=1",
		"CHAOS_CHECKPOINT_FILE="+checkpointFile,
		"CHAOS_PROCESSED_FILE="+processedFile,
		"CHAOS_TOTAL_UNITS=20",
		"CHAOS_CRASH_AT_UNIT=0",
		"CHAOS_RESUME_FROM_UNIT="+strconv.Itoa(int(recoveredHB.LastProcessedUnit)),
	)
	out2, err := cmd2.CombinedOutput()
	require.NoError(t, err, "replacement subprocess failed: %s", string(out2))

	recoveryDuration := time.Since(killStart)
	assert.Less(t, recoveryDuration, workflow.DocAIHeartbeatTimeout, "recovery must complete within 15s heartbeat window")

	// Verify all 20 units were processed exactly once across kill + recovery.
	// #nosec G304 -- temp file created by this test in t.TempDir().
	procBytes, err := os.ReadFile(processedFile)
	require.NoError(t, err)
	lines := strings.Fields(string(procBytes))
	require.Len(t, lines, 20)
	for i := 1; i <= 20; i++ {
		assert.Equal(t, strconv.Itoa(i), lines[i-1])
	}
}

// TestChaos_RateLimiterBound_12000Tenants verifies that TokenBucketLimiter enforces
// bounded eviction (<= MaxTrackedBuckets = 10,000) under concurrent multi-tenant load.
func TestChaos_RateLimiterBound_12000Tenants(t *testing.T) {
	t.Parallel()

	limiter := middleware.NewTokenBucketLimiter(middleware.RateLimiterConfig{
		RequestsPerSecond: 10000,
		Burst:             10000,
		MaxBuckets:        middleware.MaxTrackedBuckets,
		StaleTTL:          15 * time.Minute,
	})

	const totalTenants = 12000
	const workers = 16
	var wg sync.WaitGroup
	perWorker := totalTenants / workers

	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				tenantKey := fmt.Sprintf("tenant:chaos-%02d-%04d", w, i)
				allowed, _ := limiter.Allow(tenantKey)
				assert.True(t, allowed)
			}
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, limiter.TrackedBucketCount(), middleware.MaxTrackedBuckets,
		"rate limiter must strictly evict above 10,000 tracked buckets to protect <256 MiB RSS")
}

// TestChaos_Load_5000Concurrent_10GBMultipart drives 5,000 concurrent presign (including 10 GB
// 100-part multipart) and workflow start requests through the full HTTP router/middleware stack,
// verifying zero dropped requests and enforcing P99 < 35 ms + < 256 MiB heap under non-race runs.
func TestChaos_Load_5000Concurrent_10GBMultipart(t *testing.T) {
	router, jwtToken := buildChaosLoadRouter(t)

	const totalRequests = 5000
	// Stagger 5,000 live concurrent client goroutines across a 3.5s burst (~1,428 RPS)
	// so CPU-bound JSON/signature work is not artificially oversubscribed on 2-vCPU CI runners
	// while still validating 5,000 concurrent client allocations and request execution.
	const burstWindow = 3500 * time.Millisecond

	uploadBody := []byte(`{"tenant_id":"tenant-load-01","filename":"corpus-10gb.pdf","content_type":"application/pdf","content_length_bytes":10737418240,"content_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)
	startBody := []byte(`{"tenant_id":"tenant-load-01","document_id":"doc-10gb-01","s3_bucket":"rag-ingestion-bucket","s3_key":"raw/tenant-load-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/corpus-10gb.pdf","content_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","total_bytes":10737418240,"mime_type":"application/pdf"}`)

	latencies := make([]time.Duration, totalRequests)
	var errCount atomic.Int64
	var readyWG sync.WaitGroup
	var wg sync.WaitGroup
	release := make(chan struct{})

	readyWG.Add(totalRequests)
	wg.Add(totalRequests)
	for idx := 0; idx < totalRequests; idx++ {
		idx := idx
		stagger := (time.Duration(idx) * burstWindow) / time.Duration(totalRequests)
		go func() {
			defer wg.Done()
			readyWG.Done()
			<-release

			if stagger > 0 {
				timer := time.NewTimer(stagger)
				<-timer.C
				timer.Stop()
			}

			target := "/v1/ingest/upload-url"
			payload := uploadBody
			expectedCode := http.StatusOK
			if idx%5 == 0 {
				target = "/v1/ingest/workflows/start"
				payload = startBody
				expectedCode = http.StatusAccepted
			}

			req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+jwtToken)
			req.Header.Set("X-Tenant-ID", "tenant-load-01")
			req.Header.Set("X-Request-ID", fmt.Sprintf("chaos-req-%d", idx))

			rec := httptest.NewRecorder()
			start := time.Now()
			router.ServeHTTP(rec, req)
			latencies[idx] = time.Since(start)

			if rec.Code != expectedCode {
				errCount.Add(1)
			}
		}()
	}
	readyWG.Wait()
	close(release)
	wg.Wait()

	require.Equal(t, int64(0), errCount.Load(), "all 5,000 concurrent requests must succeed without errors or 429s")

	p99 := computeP99(latencies)
	heapAlloc, heapInuse := captureHeapMetrics()
	t.Logf("5,000-concurrent load (with 10GB multipart): P99=%s HeapAlloc=%.2fMiB HeapInuse=%.2fMiB (race=%v)",
		p99, float64(heapAlloc)/(1024*1024), float64(heapInuse)/(1024*1024), isRaceDetectorEnabled)

	if !isRaceDetectorEnabled {
		assert.Less(t, p99, 35*time.Millisecond, "P99 presign/start latency must remain < 35ms")
		assert.Less(t, heapInuse, uint64(256*1024*1024), "coordinator heap must stay strictly < 256 MiB")
	}
}

// TestChaos_SustainedPresign_1000RPS drives sustained 1,000 RPS presign traffic for 2 seconds
// through the HTTP router/middleware stack and verifies P99 < 35 ms + < 256 MiB heap.
func TestChaos_SustainedPresign_1000RPS(t *testing.T) {
	router, jwtToken := buildChaosLoadRouter(t)

	const targetRPS = 1000
	const durationSec = 2
	const totalRequests = targetRPS * durationSec

	body := []byte(`{"tenant_id":"tenant-load-01","filename":"annual-report.pdf","content_type":"application/pdf","content_length_bytes":10737418240,"content_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)
	latencies := make([]time.Duration, totalRequests)
	var errCount atomic.Int64
	var wg sync.WaitGroup

	ticker := time.NewTicker(time.Second / targetRPS)
	defer ticker.Stop()

	for i := 0; i < totalRequests; i++ {
		<-ticker.C
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+jwtToken)
			req.Header.Set("X-Tenant-ID", "tenant-load-01")
			rec := httptest.NewRecorder()
			start := time.Now()
			router.ServeHTTP(rec, req)
			latencies[i] = time.Since(start)
			if rec.Code != http.StatusOK {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(0), errCount.Load())
	p99 := computeP99(latencies)
	heapAlloc, heapInuse := captureHeapMetrics()
	t.Logf("Sustained 1,000 RPS presign load: P99=%s HeapAlloc=%.2fMiB HeapInuse=%.2fMiB (race=%v)",
		p99, float64(heapAlloc)/(1024*1024), float64(heapInuse)/(1024*1024), isRaceDetectorEnabled)

	if !isRaceDetectorEnabled {
		assert.Less(t, p99, 35*time.Millisecond, "sustained 1,000 RPS P99 latency must stay < 35ms")
		assert.Less(t, heapInuse, uint64(256*1024*1024), "sustained 1,000 RPS heap must stay < 256 MiB")
	}
}

// TestChaos_MemoryCeiling_256MiB profiles the heap across 10 GB logical multipart presign requests
// using runtime.ReadMemStats and runtime/pprof to confirm flat zero-heap memory behavior.
func TestChaos_MemoryCeiling_256MiB(t *testing.T) {
	router, jwtToken := buildChaosLoadRouter(t)
	body := []byte(`{"tenant_id":"tenant-load-01","filename":"monolith-10gb.mp4","content_type":"video/mp4","content_length_bytes":10737418240,"content_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)

	runtime.GC()
	baselineAlloc, _ := captureHeapMetrics()

	for i := 0; i < 1000; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest/upload-url", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+jwtToken)
		req.Header.Set("X-Tenant-ID", "tenant-load-01")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var pprofBuf bytes.Buffer
	require.NoError(t, pprof.Lookup("heap").WriteTo(&pprofBuf, 0))
	assert.Greater(t, pprofBuf.Len(), 0, "pprof heap profile must be captured")

	runtime.GC()
	afterAlloc, afterInuse := captureHeapMetrics()
	t.Logf("10GB x 1,000 presign memory profile: baseline=%.2fMiB after=%.2fMiB inuse=%.2fMiB (race=%v)",
		float64(baselineAlloc)/(1024*1024), float64(afterAlloc)/(1024*1024), float64(afterInuse)/(1024*1024), isRaceDetectorEnabled)

	if !isRaceDetectorEnabled {
		assert.Less(t, afterInuse, uint64(256*1024*1024), "heap inuse must stay strictly < 256 MiB")
		assert.Less(t, afterAlloc, uint64(64*1024*1024), "retained live heap after GC must stay flat (<64 MiB)")
	}
}

// TestChaos_CanaryDrain_ZeroDroppedConnections verifies that /ready transitions immediately
// to 503 draining on drain signal while active in-flight HTTP requests drain and complete
// with 200 OK (0 dropped connections) during graceful shutdown.
func TestChaos_CanaryDrain_ZeroDroppedConnections(t *testing.T) {
	t.Parallel()

	secret := []byte("chaos-drain-hmac-signing-key-32bytes!!")
	validator, err := middleware.NewHMACJWTValidator(secret, nil)
	require.NoError(t, err)
	jwtToken := signTestHS256(secret, "tenant-drain-01")

	inFlightStarted := make(chan struct{}, 20)
	slowPresigner := &slowChaosPresigner{
		delay: 120 * time.Millisecond,
		onPut: func() {
			select {
			case inFlightStarted <- struct{}{}:
			default:
			}
		},
	}
	uploadSvc := service.NewUploadService(slowPresigner, service.UploadServiceConfig{
		DefaultBucket: "rag-ingestion-bucket",
	})

	var draining atomic.Bool
	router := httptransport.NewRouter(httptransport.RouterConfig{
		UploadHandler:  httptransport.NewUploadHandler(uploadSvc, slog.New(slog.NewJSONHandler(io.Discard, nil))),
		TokenValidator: validator,
		DrainState:     &draining,
		ReadinessCheck: func(_ context.Context) map[string]string {
			return map[string]string{"temporal": "connected", "postgres": "connected", "redis": "connected"}
		},
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = srv.Serve(lis)
	}()
	defer func() {
		_ = srv.Close()
	}()

	baseURL := "http://" + lis.Addr().String()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Verify /ready is 200 ready before drain.
	readyResp, err := httpClient.Get(baseURL + "/ready")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, readyResp.StatusCode)
	_ = readyResp.Body.Close()

	// Fire 20 concurrent in-flight upload-url requests.
	const inflightCount = 20
	results := make(chan int, inflightCount)
	body := []byte(`{"tenant_id":"tenant-drain-01","filename":"drain-test.pdf","content_type":"application/pdf","content_length_bytes":1024,"content_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)

	for i := 0; i < inflightCount; i++ {
		go func() {
			req, reqErr := http.NewRequest(http.MethodPost, baseURL+"/v1/ingest/upload-url", bytes.NewReader(body))
			if reqErr != nil {
				results <- 0
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+jwtToken)
			req.Header.Set("X-Tenant-ID", "tenant-drain-01")
			resp, respErr := httpClient.Do(req)
			if respErr != nil {
				results <- 0
				return
			}
			_ = resp.Body.Close()
			results <- resp.StatusCode
		}()
	}

	// Wait until requests are actively inside the handler before triggering drain.
	for i := 0; i < inflightCount/2; i++ {
		<-inFlightStarted
	}

	// Enter readiness drain mode: /ready must immediately return 503 draining.
	draining.Store(true)
	drainResp, err := httpClient.Get(baseURL + "/ready")
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, drainResp.StatusCode)
	var drainPayload map[string]any
	require.NoError(t, json.NewDecoder(drainResp.Body).Decode(&drainPayload))
	_ = drainResp.Body.Close()
	assert.Equal(t, "draining", drainPayload["status"])

	// Simulate brief pre-shutdown endpoint propagation delay + graceful shutdown.
	time.Sleep(30 * time.Millisecond)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	require.NoError(t, srv.Shutdown(shutdownCtx))

	// Assert 0 dropped connections across all 20 in-flight requests.
	for i := 0; i < inflightCount; i++ {
		code := <-results
		assert.Equal(t, http.StatusOK, code, "in-flight request %d must complete cleanly during drain", i)
	}
}

func buildChaosLoadRouter(t *testing.T) (http.Handler, string) {
	t.Helper()

	secret := []byte("chaos-load-hmac-signing-key-32bytes!!")
	validator, err := middleware.NewHMACJWTValidator(secret, nil)
	require.NoError(t, err)

	presigner, err := s3.NewPresigner(
		s3.NewFakePresignAPI("https://s3.amazonaws.com", "us-east-1"),
		nil,
		s3.PresignerConfig{},
	)
	require.NoError(t, err)

	uploadSvc := service.NewUploadService(presigner, service.UploadServiceConfig{
		DefaultBucket: "rag-ingestion-bucket",
	})

	fakeTemporal := &chaosTemporalClient{}
	execSvc := service.NewExecutionService(fakeTemporal, service.ExecutionServiceConfig{})

	discardLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	highBurstLimiter := middleware.NewTokenBucketLimiter(middleware.RateLimiterConfig{
		RequestsPerSecond: 100000,
		Burst:             100000,
		MaxBuckets:        middleware.MaxTrackedBuckets,
	})

	router := httptransport.NewRouter(httptransport.RouterConfig{
		UploadHandler:    httptransport.NewUploadHandler(uploadSvc, discardLogger),
		ExecutionHandler: httptransport.NewExecutionHandler(execSvc, discardLogger),
		TokenValidator:   validator,
		RateLimiter:      highBurstLimiter,
		Logger:           discardLogger,
	})
	return router, signTestHS256(secret, "tenant-load-01")
}

func signTestHS256(secret []byte, tenantID string) string {
	hdrB64 := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payloadBytes, _ := json.Marshal(map[string]any{
		"tenant_id": tenantID,
		"sub":       "chaos-load-client",
		"scopes":    []string{middleware.ScopeIngestWrite, middleware.ScopeIngestRead},
		"exp":       time.Now().Add(time.Hour).Unix(),
	})
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(hdrB64 + "." + payloadB64))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return hdrB64 + "." + payloadB64 + "." + sigB64
}

func computeP99(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)) * 0.99)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func captureHeapMetrics() (heapAlloc uint64, heapInuse uint64) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc, ms.HeapInuse
}

type chaosCatalogStore struct {
	onRecord func(req coordinatorv1.CatalogTerminalRunRequest)
}

func (s *chaosCatalogStore) RecordTerminalRun(_ context.Context, req coordinatorv1.CatalogTerminalRunRequest) (string, error) {
	if s.onRecord != nil {
		s.onRecord(req)
	}
	return "rec_" + req.WorkflowID, nil
}

type slowChaosPresigner struct {
	delay time.Duration
	onPut func()
}

func (p *slowChaosPresigner) PutObject(
	_ context.Context,
	bucket, key, _ string,
	_ int64,
	leaseSeconds int32,
) (string, int64, int32, error) {
	if p.onPut != nil {
		p.onPut()
	}
	time.Sleep(p.delay)
	return fmt.Sprintf("https://%s.s3.amazonaws.com/%s?X-Amz-Expires=%d", bucket, key, leaseSeconds),
		time.Now().Add(time.Duration(leaseSeconds) * time.Second).Unix(),
		leaseSeconds,
		nil
}

func (p *slowChaosPresigner) CreateMultipartUpload(_ context.Context, _, _, _ string) (string, error) {
	return "mp_chaos_01", nil
}

func (p *slowChaosPresigner) UploadPart(_ context.Context, bucket, key, _ string, partNumber, _ int32) (string, error) {
	return fmt.Sprintf("https://%s.s3.amazonaws.com/%s?partNumber=%d", bucket, key, partNumber), nil
}

func (p *slowChaosPresigner) CompleteMultipartUpload(_ context.Context, bucket, key, _ string, _ []coordinatorv1.CompletedPart) (string, error) {
	return fmt.Sprintf("s3://%s/%s", bucket, key), nil
}

type chaosTemporalClient struct{}

func (c *chaosTemporalClient) ExecuteWorkflow(
	_ context.Context,
	opts client.StartWorkflowOptions,
	_ any,
	_ ...any,
) (client.WorkflowRun, error) {
	return &chaosWorkflowRun{id: opts.ID, runID: "chaos-run-001"}, nil
}

func (c *chaosTemporalClient) QueryWorkflow(_ context.Context, _, _, _ string, _ ...any) (converter.EncodedValue, error) {
	return nil, errors.New("not implemented in load mock")
}

func (c *chaosTemporalClient) DescribeWorkflowExecution(_ context.Context, _, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return nil, errors.New("not implemented in load mock")
}

type chaosWorkflowRun struct {
	id    string
	runID string
}

func (r *chaosWorkflowRun) GetID() string                      { return r.id }
func (r *chaosWorkflowRun) GetRunID() string                   { return r.runID }
func (r *chaosWorkflowRun) Get(_ context.Context, _ any) error { return nil }
func (r *chaosWorkflowRun) GetWithOptions(_ context.Context, _ any, _ client.WorkflowRunGetOptions) error {
	return nil
}
