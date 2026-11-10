package unit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
)

type mockTemporalClient struct {
	executeWorkflowFn           func(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error)
	queryWorkflowFn             func(ctx context.Context, workflowID string, runID string, queryType string, args ...any) (converter.EncodedValue, error)
	describeWorkflowExecutionFn func(ctx context.Context, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

func (m *mockTemporalClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	if m.executeWorkflowFn != nil {
		return m.executeWorkflowFn(ctx, options, workflow, args...)
	}
	return nil, errors.New("ExecuteWorkflow not implemented")
}

func (m *mockTemporalClient) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...any) (converter.EncodedValue, error) {
	if m.queryWorkflowFn != nil {
		return m.queryWorkflowFn(ctx, workflowID, runID, queryType, args...)
	}
	return nil, errors.New("QueryWorkflow not implemented")
}

func (m *mockTemporalClient) DescribeWorkflowExecution(ctx context.Context, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if m.describeWorkflowExecutionFn != nil {
		return m.describeWorkflowExecutionFn(ctx, workflowID, runID)
	}
	return nil, errors.New("DescribeWorkflowExecution not implemented")
}

type mockWorkflowRun struct {
	id    string
	runID string
}

func (m *mockWorkflowRun) GetID() string                  { return m.id }
func (m *mockWorkflowRun) GetRunID() string               { return m.runID }
func (m *mockWorkflowRun) Get(context.Context, any) error { return nil }
func (m *mockWorkflowRun) GetWithOptions(context.Context, any, client.WorkflowRunGetOptions) error {
	return nil
}

type mockEncodedValue struct {
	val any
	err error
}

func (m mockEncodedValue) Get(valuePtr any) error {
	if m.err != nil {
		return m.err
	}
	data, err := converter.GetDefaultDataConverter().ToPayload(m.val)
	if err != nil {
		return err
	}
	return converter.GetDefaultDataConverter().FromPayload(data, valuePtr)
}

func (m mockEncodedValue) HasValue() bool {
	return m.val != nil
}

type mockTerminalReader struct {
	getTerminalRunFn func(ctx context.Context, tenantID, workflowID string) (*coordinatorv1.GetIngestionStatusResponse, error)
}

func (m *mockTerminalReader) GetTerminalRun(ctx context.Context, tenantID, workflowID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
	if m.getTerminalRunFn != nil {
		return m.getTerminalRunFn(ctx, tenantID, workflowID)
	}
	return nil, errors.New("terminal run not found")
}

func validStartRequest() coordinatorv1.StartIngestionRequest {
	return coordinatorv1.StartIngestionRequest{
		TenantID:         "tenant-corp-01",
		DocumentID:       "doc-01HXYZ1234567890",
		S3Bucket:         "rag-ingestion-bucket",
		S3Key:            "raw/tenant-corp-01/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/doc.pdf",
		ContentHash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		TotalBytes:       1048576,
		MimeType:         "application/pdf",
		PipelineConfigID: "default-multimodal-v1",
	}
}

func TestExecutionService_StartIngestion_InputValidation(t *testing.T) {
	t.Parallel()
	svc := service.NewExecutionService(&mockTemporalClient{}, service.ExecutionServiceConfig{})

	tests := []struct {
		name        string
		reqModifier func(r *coordinatorv1.StartIngestionRequest)
		errExpected error
	}{
		{
			name:        "empty tenant_id",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.TenantID = "" },
			errExpected: service.ErrInvalidUploadRequest,
		},
		{
			name:        "invalid characters in tenant_id",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.TenantID = "tenant/corp" },
			errExpected: service.ErrInvalidUploadRequest,
		},
		{
			name:        "empty document_id",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.DocumentID = "" },
			errExpected: service.ErrInvalidExecutionRequest,
		},
		{
			name:        "path traversal in document_id",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.DocumentID = "../../doc" },
			errExpected: service.ErrInvalidExecutionRequest,
		},
		{
			name:        "empty s3_bucket",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.S3Bucket = "" },
			errExpected: service.ErrInvalidExecutionRequest,
		},
		{
			name: "cross-tenant S3 key",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) {
				r.S3Key = "raw/other-tenant/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/doc.pdf"
			},
			errExpected: service.ErrTenantMismatch,
		},
		{
			name:        "invalid content_hash not 64-hex",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.ContentHash = "not-a-hash" },
			errExpected: service.ErrInvalidUploadRequest,
		},
		{
			name:        "zero total_bytes",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.TotalBytes = 0 },
			errExpected: service.ErrZeroBytePayload,
		},
		{
			name:        "total_bytes exceeding 10 GiB",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.TotalBytes = 11 * 1024 * 1024 * 1024 },
			errExpected: service.ErrPayloadTooLarge,
		},
		{
			name:        "unsupported mime_type",
			reqModifier: func(r *coordinatorv1.StartIngestionRequest) { r.MimeType = "application/x-executable" },
			errExpected: service.ErrUnsupportedMimeType,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := validStartRequest()
			tc.reqModifier(&req)
			_, err := svc.StartIngestion(context.Background(), req)
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.errExpected)
		})
	}
}

func TestExecutionService_StartIngestion_SuccessAndDeduplication(t *testing.T) {
	t.Parallel()

	fixedNow := time.Unix(1794039600, 0).UTC()
	req := validStartRequest()
	expectedWfID := service.BuildIngestionWorkflowID(req.TenantID, req.ContentHash)

	var capturedOpts client.StartWorkflowOptions
	var capturedWorkflowName any

	mockClient := &mockTemporalClient{
		executeWorkflowFn: func(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
			capturedOpts = options
			capturedWorkflowName = workflow
			return &mockWorkflowRun{id: options.ID, runID: "run-uuid-1234"}, nil
		},
	}

	svc := service.NewExecutionService(mockClient, service.ExecutionServiceConfig{
		NowFunc: func() time.Time { return fixedNow },
	})

	resp, err := svc.StartIngestion(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, expectedWfID, resp.WorkflowID)
	assert.Equal(t, "run-uuid-1234", resp.RunID)
	assert.Equal(t, "RUNNING", resp.Status)
	assert.Equal(t, fixedNow.Unix(), resp.StartedAtUnix)

	assert.Equal(t, expectedWfID, capturedOpts.ID)
	assert.Equal(t, service.CoordinatorTaskQueue, capturedOpts.TaskQueue)
	assert.False(t, capturedOpts.WorkflowExecutionErrorWhenAlreadyStarted)
	assert.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, capturedOpts.WorkflowIDConflictPolicy)
	assert.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY, capturedOpts.WorkflowIDReusePolicy)
	assert.Equal(t, service.IngestionCoordinatorWorkflowName, capturedWorkflowName)

	// Verify deduplication: when workflow is already started, return 202 RUNNING handle.
	mockClient.executeWorkflowFn = func(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
		return nil, &serviceerror.WorkflowExecutionAlreadyStarted{
			Message: "workflow already running",
			RunId:   "existing-run-9999",
		}
	}

	dupResp, err := svc.StartIngestion(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, expectedWfID, dupResp.WorkflowID)
	assert.Equal(t, "existing-run-9999", dupResp.RunID)
	assert.Equal(t, "RUNNING", dupResp.Status)
}

func TestExecutionService_GetWorkflowStatus(t *testing.T) {
	t.Parallel()

	fixedNow := time.Unix(1794039600, 0).UTC()
	tenantID := "tenant-corp-01"
	workflowID := "ingest-tenant-corp-01-hash123"

	t.Run("live status from query handler (ADR-003)", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockTemporalClient{
			queryWorkflowFn: func(ctx context.Context, wfID, runID, qType string, args ...any) (converter.EncodedValue, error) {
				assert.Equal(t, workflowID, wfID)
				assert.Equal(t, service.QueryTypeIngestionStatus, qType)
				return mockEncodedValue{
					val: service.WorkflowIngestionStatus{
						WorkflowID:      workflowID,
						TenantID:        tenantID,
						DocumentID:      "doc-01",
						Status:          "RUNNING",
						Stage:           "EXTRACT_SEGMENTS",
						TotalPages:      100,
						TotalSlices:     4,
						CompletedSlices: 2,
						FailedSlices:    0,
					},
				}, nil
			},
		}

		svc := service.NewExecutionService(mockClient, service.ExecutionServiceConfig{
			NowFunc: func() time.Time { return fixedNow },
		})

		resp, err := svc.GetWorkflowStatus(context.Background(), workflowID, tenantID)
		require.NoError(t, err)
		assert.Equal(t, workflowID, resp.WorkflowID)
		assert.Equal(t, tenantID, resp.TenantID)
		assert.Equal(t, "RUNNING", resp.Status)
		assert.Equal(t, int32(4), resp.TotalSlices)
		assert.Equal(t, int32(2), resp.CompletedSlices)
		assert.Equal(t, int32(100), resp.TotalPages)
	})

	t.Run("query cross-tenant mismatch returns ErrTenantMismatch", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockTemporalClient{
			queryWorkflowFn: func(ctx context.Context, wfID, runID, qType string, args ...any) (converter.EncodedValue, error) {
				return mockEncodedValue{
					val: service.WorkflowIngestionStatus{
						WorkflowID: workflowID,
						TenantID:   "other-tenant",
						Status:     "RUNNING",
					},
				}, nil
			},
		}

		svc := service.NewExecutionService(mockClient, service.ExecutionServiceConfig{})
		_, err := svc.GetWorkflowStatus(context.Background(), workflowID, tenantID)
		require.Error(t, err)
		assert.ErrorIs(t, err, service.ErrTenantMismatch)
	})

	t.Run("fallback to TerminalRunReader when query fails", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockTemporalClient{
			queryWorkflowFn: func(ctx context.Context, wfID, runID, qType string, args ...any) (converter.EncodedValue, error) {
				return nil, errors.New("workflow execution already completed")
			},
		}

		terminalReader := &mockTerminalReader{
			getTerminalRunFn: func(ctx context.Context, tID, wfID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				return &coordinatorv1.GetIngestionStatusResponse{
					WorkflowID:      wfID,
					TenantID:        tID,
					Status:          "COMPLETED",
					TotalSlices:     8,
					CompletedSlices: 8,
					TotalPages:      200,
					DurationSeconds: 42.5,
				}, nil
			},
		}

		svc := service.NewExecutionService(mockClient, service.ExecutionServiceConfig{
			TerminalReader: terminalReader,
			NowFunc:        func() time.Time { return fixedNow },
		})

		resp, err := svc.GetWorkflowStatus(context.Background(), workflowID, tenantID)
		require.NoError(t, err)
		assert.Equal(t, "COMPLETED", resp.Status)
		assert.Equal(t, int32(8), resp.CompletedSlices)
		assert.Equal(t, 42.5, resp.DurationSeconds)
	})

	t.Run("fallback to DescribeWorkflowExecution when terminal store misses", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockTemporalClient{
			queryWorkflowFn: func(ctx context.Context, wfID, runID, qType string, args ...any) (converter.EncodedValue, error) {
				return nil, errors.New("query not supported on closed workflow")
			},
			describeWorkflowExecutionFn: func(ctx context.Context, wfID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				startT := fixedNow.Add(-30 * time.Second)
				closeT := fixedNow
				tenantPayload, _ := converter.GetDefaultDataConverter().ToPayload(tenantID)

				return &workflowservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						Execution: &commonpb.WorkflowExecution{
							WorkflowId: wfID,
							RunId:      "desc-run-001",
						},
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						StartTime: timestamppb.New(startT),
						CloseTime: timestamppb.New(closeT),
						SearchAttributes: &commonpb.SearchAttributes{
							IndexedFields: map[string]*commonpb.Payload{
								"TenantID": tenantPayload,
							},
						},
					},
				}, nil
			},
		}

		svc := service.NewExecutionService(mockClient, service.ExecutionServiceConfig{
			NowFunc: func() time.Time { return fixedNow },
		})

		resp, err := svc.GetWorkflowStatus(context.Background(), workflowID, tenantID)
		require.NoError(t, err)
		assert.Equal(t, workflowID, resp.WorkflowID)
		assert.Equal(t, "desc-run-001", resp.RunID)
		assert.Equal(t, "COMPLETED", resp.Status)
		assert.Equal(t, 30.0, resp.DurationSeconds)
	})

	t.Run("workflow not found", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockTemporalClient{
			queryWorkflowFn: func(ctx context.Context, wfID, runID, qType string, args ...any) (converter.EncodedValue, error) {
				return nil, errors.New("execution not found")
			},
			describeWorkflowExecutionFn: func(ctx context.Context, wfID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				return nil, &serviceerror.NotFound{Message: "workflow execution not found"}
			},
		}

		svc := service.NewExecutionService(mockClient, service.ExecutionServiceConfig{})
		_, err := svc.GetWorkflowStatus(context.Background(), workflowID, tenantID)
		require.Error(t, err)
		assert.ErrorIs(t, err, service.ErrWorkflowNotFound)
	})
}
