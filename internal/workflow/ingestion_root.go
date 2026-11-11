package workflow

import (
	"errors"
	"fmt"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/service"
)

const (
	// StageInspecting represents the header inspection and manifest planning stage.
	StageInspecting = "INSPECTING"
	// StageExtractSegments represents the child workflow fan-out extraction stage.
	StageExtractSegments = "EXTRACT_SEGMENTS"
	// StageCompleted represents the final completed milestone stage.
	StageCompleted = "COMPLETED"
	// StageFailed represents the terminal failure milestone stage.
	StageFailed = "FAILED"
)

// IngestionRequest defines the input payload for IngestionCoordinatorWorkflow,
// including state carried across ContinueAsNew generation boundaries (ADR-002).
type IngestionRequest struct {
	TenantID                 string                           `json:"tenant_id"`
	DocumentID               string                           `json:"document_id"`
	PipelineRunID            string                           `json:"pipeline_run_id,omitempty"`
	S3Bucket                 string                           `json:"s3_bucket"`
	S3Key                    string                           `json:"s3_key"`
	ContentHash              string                           `json:"content_hash"`
	TotalBytes               int64                            `json:"total_bytes"`
	MimeType                 string                           `json:"mime_type"`
	PipelineConfigID         string                           `json:"pipeline_config_id,omitempty"`
	PipelineConfig           *coordinatorv1.PipelineConfig    `json:"pipeline_config,omitempty"`
	UseHashPrefixS3Layout    bool                             `json:"use_hash_prefix_s3_layout,omitempty"`
	Modality                 coordinatorv1.MultimodalModality `json:"modality,omitempty"`
	DurationSeconds          float64                          `json:"duration_seconds,omitempty"`
	RecommendedSlicePages    int32                            `json:"recommended_slice_pages,omitempty"`
	RecommendedWindowSeconds float64                          `json:"recommended_window_seconds,omitempty"`
	Generation               int32                            `json:"generation,omitempty"`
	StartedAtUnix            int64                            `json:"started_at_unix,omitempty"`
	NextSequenceNumber       int64                            `json:"next_sequence_number,omitempty"`
	NextSliceIndex           int32                            `json:"next_slice_index,omitempty"`
	TotalPages               int32                            `json:"total_pages,omitempty"`
	TotalSlices              int32                            `json:"total_slices,omitempty"`
	CompletedSlices          int32                            `json:"completed_slices,omitempty"`
	FailedSlices             int32                            `json:"failed_slices,omitempty"`
	FailedSliceIDs           []string                         `json:"failed_slice_ids,omitempty"`
	AccumulatedChunks        int32                            `json:"accumulated_chunks,omitempty"`
	AccumulatedTokens        int32                            `json:"accumulated_tokens,omitempty"`
	RootWorkflowID           string                           `json:"root_workflow_id,omitempty"`
}

// IngestionStatus represents the live in-flight status returned by the
// QueryTypeIngestionStatus ("ingestion_status") Temporal query handler (ADR-003).
type IngestionStatus struct {
	WorkflowID        string   `json:"workflow_id"`
	TenantID          string   `json:"tenant_id"`
	DocumentID        string   `json:"document_id"`
	Status            string   `json:"status"`
	Stage             string   `json:"stage"`
	TotalPages        int32    `json:"total_pages"`
	TotalSlices       int32    `json:"total_slices"`
	CompletedSlices   int32    `json:"completed_slices"`
	FailedSlices      int32    `json:"failed_slices"`
	FailedSliceIDs    []string `json:"failed_slice_ids,omitempty"`
	CurrentGeneration int32    `json:"current_generation"`
}

// IngestionResult summarizes the terminal outcome of IngestionCoordinatorWorkflow.
type IngestionResult struct {
	WorkflowID      string   `json:"workflow_id"`
	RunID           string   `json:"run_id"`
	TenantID        string   `json:"tenant_id"`
	DocumentID      string   `json:"document_id"`
	Status          string   `json:"status"`
	TotalPages      int32    `json:"total_pages"`
	TotalSlices     int32    `json:"total_slices"`
	CompletedSlices int32    `json:"completed_slices"`
	FailedSlices    int32    `json:"failed_slices"`
	FailedSliceIDs  []string `json:"failed_slice_ids,omitempty"`
	CompiledChunks  int32    `json:"compiled_chunks"`
	TotalTokens     int32    `json:"total_tokens"`
	DurationSeconds float64  `json:"duration_seconds"`
	Generations     int32    `json:"generations"`
}

// IngestionCoordinatorWorkflow orchestrates the end-to-end multimodal ingestion DAG:
//  1. Registers QueryTypeIngestionStatus query handler for zero dual-write status reads (ADR-003).
//  2. Upserts TenantID, DocumentID, and PipelineRunID search attributes.
//  3. Executes InspectDocumentHeaderActivity on inspection-queue and plans deterministic slices.
//  4. Fans out DocumentPartitionWorkflow child workflows in bounded batches (<= 100 per generation).
//  5. Isolates poison-pill partition failures (Runbook 3) so sibling slices complete.
//  6. Invokes ContinueAsNew when remaining slices exist to keep history <2,500 events (ADR-002).
//  7. Catalogs a single atomic terminal record in PostgreSQL at workflow completion/failure (ADR-003).
func IngestionCoordinatorWorkflow(ctx workflow.Context, req IngestionRequest) (*IngestionResult, error) {
	info := workflow.GetInfo(ctx)
	logger := workflow.GetLogger(ctx)

	rootWorkflowID := req.RootWorkflowID
	if rootWorkflowID == "" {
		rootWorkflowID = info.WorkflowExecution.ID
	}
	pipelineRunID := req.PipelineRunID
	if pipelineRunID == "" {
		pipelineRunID = info.WorkflowExecution.RunID
	}

	startedAtUnix := req.StartedAtUnix
	if startedAtUnix <= 0 {
		startedAtUnix = workflow.Now(ctx).UTC().Unix()
	}

	nextSeq := req.NextSequenceNumber
	if nextSeq <= 0 {
		nextSeq = 1
	}

	liveStatus := IngestionStatus{
		WorkflowID:        rootWorkflowID,
		TenantID:          req.TenantID,
		DocumentID:        req.DocumentID,
		Status:            "RUNNING",
		Stage:             StageInspecting,
		TotalPages:        req.TotalPages,
		TotalSlices:       req.TotalSlices,
		CompletedSlices:   req.CompletedSlices,
		FailedSlices:      req.FailedSlices,
		FailedSliceIDs:    append([]string(nil), req.FailedSliceIDs...),
		CurrentGeneration: req.Generation,
	}
	if req.Generation > 0 {
		liveStatus.Stage = StageExtractSegments
	}

	if err := workflow.SetQueryHandler(ctx, QueryTypeIngestionStatus, func() (IngestionStatus, error) {
		return liveStatus, nil
	}); err != nil {
		return nil, fmt.Errorf("failed to register query handler %s: %w", QueryTypeIngestionStatus, err)
	}

	coordCtx := workflow.WithActivityOptions(ctx, CoordinatorActivityOptions())

	publishMilestone := func(stage, status string) {
		progressPct := 0.0
		if liveStatus.TotalSlices > 0 {
			progressPct = (float64(liveStatus.CompletedSlices) / float64(liveStatus.TotalSlices)) * 100.0
		}
		event := coordinatorv1.ProgressMilestoneEvent{
			EventID:         fmt.Sprintf("%s-seq-%d", rootWorkflowID, nextSeq),
			SequenceNumber:  nextSeq,
			TenantID:        req.TenantID,
			DocumentID:      req.DocumentID,
			WorkflowID:      rootWorkflowID,
			Stage:           stage,
			Status:          status,
			ProgressPct:     progressPct,
			SlicesCompleted: liveStatus.CompletedSlices,
			TotalSlices:     liveStatus.TotalSlices,
			TimestampUnix:   workflow.Now(ctx).UTC().Unix(),
		}
		nextSeq++

		var pubResp coordinatorv1.PublishMilestoneResponse
		if pubErr := workflow.ExecuteActivity(
			coordCtx,
			activity.PublishMilestoneActivity,
			coordinatorv1.PublishMilestoneRequest{Event: event},
		).Get(coordCtx, &pubResp); pubErr != nil {
			logger.Warn("failed to publish milestone telemetry event",
				"tenant_id", req.TenantID,
				"document_id", req.DocumentID,
				"workflow_id", rootWorkflowID,
				"stage", stage,
				"error", pubErr,
			)
		}
	}

	catalogTerminalRun := func(terminalStatus, errMsg string) {
		durationSec := float64(workflow.Now(ctx).UTC().Unix() - startedAtUnix)
		if durationSec < 0 {
			durationSec = 0
		}

		catalogCtx := coordCtx
		if errors.Is(ctx.Err(), workflow.ErrCanceled) {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			catalogCtx = workflow.WithActivityOptions(disconnectedCtx, CoordinatorActivityOptions())
		}

		var catResp coordinatorv1.CatalogTerminalRunResponse
		if catErr := workflow.ExecuteActivity(
			catalogCtx,
			activity.CatalogTerminalRunActivity,
			coordinatorv1.CatalogTerminalRunRequest{
				WorkflowID:      rootWorkflowID,
				RunID:           info.WorkflowExecution.RunID,
				TenantID:        req.TenantID,
				DocumentID:      req.DocumentID,
				ContentHash:     req.ContentHash,
				Status:          terminalStatus,
				TotalPages:      liveStatus.TotalPages,
				TotalSlices:     liveStatus.TotalSlices,
				CompletedSlices: liveStatus.CompletedSlices,
				FailedSlices:    liveStatus.FailedSlices,
				DurationSeconds: durationSec,
				ErrorMessage:    errMsg,
			},
		).Get(catalogCtx, &catResp); catErr != nil {
			logger.Error("failed to catalog terminal workflow run",
				"tenant_id", req.TenantID,
				"document_id", req.DocumentID,
				"workflow_id", rootWorkflowID,
				"status", terminalStatus,
				"error", catErr,
			)
		}
	}

	modality := req.Modality
	totalPages := req.TotalPages
	durationSeconds := req.DurationSeconds
	recommendedSlicePages := req.RecommendedSlicePages
	recommendedWindowSeconds := req.RecommendedWindowSeconds

	// Initial generation (Generation == 0): upsert search attributes and inspect header.
	if req.Generation == 0 {
		_ = workflow.UpsertTypedSearchAttributes(
			ctx,
			SearchAttrTenantID.ValueSet(req.TenantID),
			SearchAttrDocumentID.ValueSet(req.DocumentID),
			SearchAttrPipelineRunID.ValueSet(pipelineRunID),
		)

		logger.Info("starting root ingestion coordinator workflow",
			"tenant_id", req.TenantID,
			"document_id", req.DocumentID,
			"workflow_id", rootWorkflowID,
			"s3_bucket", req.S3Bucket,
			"s3_key", req.S3Key,
		)

		publishMilestone(StageInspecting, "PROGRESS")

		inspectCtx := workflow.WithActivityOptions(ctx, InspectionActivityOptions())
		var inspectResp coordinatorv1.InspectDocumentResponse
		if err := workflow.ExecuteActivity(
			inspectCtx,
			activity.InspectDocumentHeaderActivity,
			coordinatorv1.InspectDocumentRequest{
				TenantID:    req.TenantID,
				DocumentID:  req.DocumentID,
				S3Bucket:    req.S3Bucket,
				S3Key:       req.S3Key,
				ContentHash: req.ContentHash,
				TotalBytes:  req.TotalBytes,
				MimeType:    req.MimeType,
			},
		).Get(inspectCtx, &inspectResp); err != nil {
			termStatus := activity.TerminalStatusFailed
			if temporal.IsCanceledError(err) || errors.Is(ctx.Err(), workflow.ErrCanceled) {
				termStatus = activity.TerminalStatusTerminated
			}
			liveStatus.Status = termStatus
			liveStatus.Stage = StageFailed
			catalogTerminalRun(termStatus, err.Error())
			publishMilestone(StageFailed, termStatus)
			return nil, fmt.Errorf("document header inspection failed: %w", err)
		}

		modality = inspectResp.Modality
		totalPages = inspectResp.TotalPages
		durationSeconds = inspectResp.DurationSeconds
		recommendedSlicePages = inspectResp.RecommendedSlicePages
		recommendedWindowSeconds = inspectResp.RecommendedWindowSeconds
	}

	// Deterministically recreate the slice plan for the current generation using manifest plan inputs.
	slices, err := service.PlanSlices(service.ManifestPlanInput{
		TenantID:              req.TenantID,
		DocumentID:            req.DocumentID,
		ContentHash:           req.ContentHash,
		S3InputBucket:         req.S3Bucket,
		S3InputKey:            req.S3Key,
		TotalBytes:            req.TotalBytes,
		Modality:              modality,
		TotalPages:            totalPages,
		DurationSeconds:       durationSeconds,
		PagesPerSlice:         recommendedSlicePages,
		WindowSeconds:         recommendedWindowSeconds,
		UseHashPrefixS3Layout: req.UseHashPrefixS3Layout,
	})
	if err != nil {
		liveStatus.Status = activity.TerminalStatusFailed
		liveStatus.Stage = StageFailed
		catalogTerminalRun(activity.TerminalStatusFailed, err.Error())
		publishMilestone(StageFailed, activity.TerminalStatusFailed)
		return nil, temporal.NewNonRetryableApplicationError(
			err.Error(),
			activity.ErrCodeCorruptInput,
			err,
		)
	}

	liveStatus.TotalPages = totalPages
	liveStatus.TotalSlices = int32(len(slices))
	liveStatus.Stage = StageExtractSegments

	// Partition current generation batch starting from NextSliceIndex (bounded to MaxSlicesPerGeneration = 100).
	startIdx := int(req.NextSliceIndex)
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx > len(slices) {
		startIdx = len(slices)
	}
	endIdx := startIdx + MaxSlicesPerGeneration
	if endIdx > len(slices) {
		endIdx = len(slices)
	}
	currentBatch := slices[startIdx:endIdx]
	nextSliceIndex := int32(endIdx)
	resolvedPipelineConfig := ResolvePipelineConfig(req.TenantID, req.PipelineConfigID, req.PipelineConfig)

	publishMilestone(StageExtractSegments, "PROGRESS")
	workflow.GetMetricsHandler(ctx).Gauge("coordinator_child_workflows_spawned").Update(float64(len(currentBatch)))

	type childTask struct {
		slice  PartitionSlice
		future workflow.ChildWorkflowFuture
	}

	childTasks := make([]childTask, 0, len(currentBatch))
	for _, slice := range currentBatch {
		cfgCopy := resolvedPipelineConfig
		slice.PipelineConfigID = resolvedPipelineConfig.ID
		slice.PipelineConfig = &cfgCopy

		childWorkflowID := fmt.Sprintf("%s-slice-%05d", rootWorkflowID, slice.SliceIndex)
		childOpts := workflow.ChildWorkflowOptions{
			WorkflowID: childWorkflowID,
			TaskQueue:  CoordinatorTaskQueue,
		}
		childCtx := workflow.WithChildOptions(ctx, childOpts)
		future := workflow.ExecuteChildWorkflow(childCtx, DocumentPartitionWorkflow, slice)
		childTasks = append(childTasks, childTask{
			slice:  slice,
			future: future,
		})
	}

	accumulatedChunks := req.AccumulatedChunks
	accumulatedTokens := req.AccumulatedTokens
	var lastSliceErr error

	for _, task := range childTasks {
		var childRes PartitionResult
		err := task.future.Get(ctx, &childRes)
		if err != nil {
			// If the root workflow itself was cancelled, abort immediately and catalog TERMINATED.
			if temporal.IsCanceledError(err) || errors.Is(ctx.Err(), workflow.ErrCanceled) {
				liveStatus.Status = activity.TerminalStatusTerminated
				liveStatus.Stage = StageFailed
				catalogTerminalRun(activity.TerminalStatusTerminated, err.Error())
				return nil, err
			}

			// Runbook 3: Poison-Pill Document Partition Isolation.
			// Isolate individual slice failure while allowing sibling slices to complete.
			liveStatus.FailedSlices++
			liveStatus.FailedSliceIDs = append(liveStatus.FailedSliceIDs, task.slice.SliceID)
			lastSliceErr = err

			logger.Warn("isolated poison-pill partition slice failure (Runbook 3)",
				"tenant_id", req.TenantID,
				"document_id", req.DocumentID,
				"workflow_id", rootWorkflowID,
				"slice_id", task.slice.SliceID,
				"slice_index", task.slice.SliceIndex,
				"s3_bucket", task.slice.S3InputBucket,
				"s3_key", task.slice.S3InputKey,
				"failed_slices", liveStatus.FailedSlices,
				"error", err,
			)
			continue
		}

		liveStatus.CompletedSlices++
		accumulatedChunks += childRes.CompiledChunks
		accumulatedTokens += childRes.TotalTokens
	}

	// ADR-002: If more slices remain, transition via ContinueAsNew to bound event history (<2,500 events).
	if int(nextSliceIndex) < len(slices) {
		cfgCopy := resolvedPipelineConfig
		nextReq := IngestionRequest{
			TenantID:                 req.TenantID,
			DocumentID:               req.DocumentID,
			PipelineRunID:            pipelineRunID,
			S3Bucket:                 req.S3Bucket,
			S3Key:                    req.S3Key,
			ContentHash:              req.ContentHash,
			TotalBytes:               req.TotalBytes,
			MimeType:                 req.MimeType,
			PipelineConfigID:         resolvedPipelineConfig.ID,
			PipelineConfig:           &cfgCopy,
			UseHashPrefixS3Layout:    req.UseHashPrefixS3Layout,
			Modality:                 modality,
			DurationSeconds:          durationSeconds,
			RecommendedSlicePages:    recommendedSlicePages,
			RecommendedWindowSeconds: recommendedWindowSeconds,
			Generation:               req.Generation + 1,
			StartedAtUnix:            startedAtUnix,
			NextSequenceNumber:       nextSeq,
			NextSliceIndex:           nextSliceIndex,
			TotalPages:               liveStatus.TotalPages,
			TotalSlices:              liveStatus.TotalSlices,
			CompletedSlices:          liveStatus.CompletedSlices,
			FailedSlices:             liveStatus.FailedSlices,
			FailedSliceIDs:           liveStatus.FailedSliceIDs,
			AccumulatedChunks:        accumulatedChunks,
			AccumulatedTokens:        accumulatedTokens,
			RootWorkflowID:           rootWorkflowID,
		}
		logger.Info("continuing root ingestion workflow as new generation",
			"tenant_id", req.TenantID,
			"document_id", req.DocumentID,
			"workflow_id", rootWorkflowID,
			"next_generation", nextReq.Generation,
			"completed_slices", nextReq.CompletedSlices,
			"next_slice_index", nextReq.NextSliceIndex,
			"remaining_slices", len(slices)-int(nextSliceIndex),
		)
		return nil, workflow.NewContinueAsNewError(ctx, IngestionCoordinatorWorkflow, nextReq)
	}

	// All slice batches have finished. Determine terminal status.
	if liveStatus.CompletedSlices == 0 && liveStatus.FailedSlices > 0 {
		errMsg := "all document partition slices failed"
		if lastSliceErr != nil {
			errMsg = fmt.Sprintf("all document partition slices failed (last error: %v)", lastSliceErr)
		}
		liveStatus.Status = activity.TerminalStatusFailed
		liveStatus.Stage = StageFailed
		catalogTerminalRun(activity.TerminalStatusFailed, errMsg)
		publishMilestone(StageFailed, activity.TerminalStatusFailed)
		return nil, fmt.Errorf("%s", errMsg)
	}

	liveStatus.Status = activity.TerminalStatusCompleted
	liveStatus.Stage = StageCompleted

	var partialErrMsg string
	if liveStatus.FailedSlices > 0 {
		partialErrMsg = fmt.Sprintf(
			"completed with %d poison-pill slice failure(s): %v",
			liveStatus.FailedSlices,
			liveStatus.FailedSliceIDs,
		)
	}

	catalogTerminalRun(activity.TerminalStatusCompleted, partialErrMsg)
	publishMilestone(StageCompleted, activity.TerminalStatusCompleted)

	durationSec := float64(workflow.Now(ctx).UTC().Unix() - startedAtUnix)
	if durationSec < 0 {
		durationSec = 0
	}

	logger.Info("completed root ingestion coordinator workflow",
		"tenant_id", req.TenantID,
		"document_id", req.DocumentID,
		"workflow_id", rootWorkflowID,
		"total_pages", liveStatus.TotalPages,
		"total_slices", liveStatus.TotalSlices,
		"completed_slices", liveStatus.CompletedSlices,
		"failed_slices", liveStatus.FailedSlices,
		"generations", req.Generation+1,
	)

	return &IngestionResult{
		WorkflowID:      rootWorkflowID,
		RunID:           info.WorkflowExecution.RunID,
		TenantID:        req.TenantID,
		DocumentID:      req.DocumentID,
		Status:          activity.TerminalStatusCompleted,
		TotalPages:      liveStatus.TotalPages,
		TotalSlices:     liveStatus.TotalSlices,
		CompletedSlices: liveStatus.CompletedSlices,
		FailedSlices:    liveStatus.FailedSlices,
		FailedSliceIDs:  liveStatus.FailedSliceIDs,
		CompiledChunks:  accumulatedChunks,
		TotalTokens:     accumulatedTokens,
		DurationSeconds: durationSec,
		Generations:     req.Generation + 1,
	}, nil
}
