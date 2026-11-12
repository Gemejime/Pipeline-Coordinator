package unit_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/service"
	transportgrpc "rag-ingestion-coordinator/internal/transport/grpc"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

const bufSize = 1024 * 1024

type mockUploadCoordinator struct {
	createUploadURLFn         func(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error)
	completeMultipartUploadFn func(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error)
}

func (m *mockUploadCoordinator) CreateUploadURL(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error) {
	if m.createUploadURLFn != nil {
		return m.createUploadURLFn(ctx, req)
	}
	return nil, service.ErrInvalidUploadRequest
}

func (m *mockUploadCoordinator) CompleteMultipartUpload(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error) {
	if m.completeMultipartUploadFn != nil {
		return m.completeMultipartUploadFn(ctx, req)
	}
	return nil, service.ErrInvalidMultipartCompletion
}

func defaultGRPCTestValidator() middleware.TokenValidator {
	return middleware.NewStaticTokenValidator(map[string]middleware.AuthClaims{
		"valid-write-token": {
			TenantID: "tenant-corp-01",
			Scopes:   []string{middleware.ScopeIngestWrite},
		},
		"valid-read-token": {
			TenantID: "tenant-corp-01",
			Scopes:   []string{middleware.ScopeIngestRead},
		},
		"valid-rw-token": {
			TenantID: "tenant-corp-01",
			Scopes:   []string{middleware.ScopeIngestWrite, middleware.ScopeIngestRead},
		},
		"other-tenant-token": {
			TenantID: "tenant-other-02",
			Scopes:   []string{middleware.ScopeIngestWrite, middleware.ScopeIngestRead},
		},
	})
}

func withBearerToken(ctx context.Context, token string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
}

func setupGRPCTestServerWithOptions(
	t *testing.T,
	uploads transportgrpc.UploadCoordinator,
	executions transportgrpc.ExecutionCoordinator,
	useInterceptor bool,
	serverOpts ...transportgrpc.ServerOption,
) coordinatorv1.CoordinatorServiceClient {
	t.Helper()
	lis := bufconn.Listen(bufSize)

	var grpcOpts []grpc.ServerOption
	if useInterceptor {
		grpcOpts = append(grpcOpts, grpc.UnaryInterceptor(transportgrpc.UnaryAuthInterceptor(defaultGRPCTestValidator(), nil)))
	}
	s := grpc.NewServer(grpcOpts...)

	srv := transportgrpc.NewCoordinatorServer(uploads, executions, nil, serverOpts...)
	coordinatorv1.RegisterCoordinatorServiceServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("bufconn server stopped: %v", err)
		}
	}()

	t.Cleanup(func() {
		s.Stop()
		_ = lis.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
	})

	return coordinatorv1.NewCoordinatorServiceClient(conn)
}

func setupGRPCTestServer(
	t *testing.T,
	uploads transportgrpc.UploadCoordinator,
	executions transportgrpc.ExecutionCoordinator,
) coordinatorv1.CoordinatorServiceClient {
	return setupGRPCTestServerWithOptions(
		t,
		uploads,
		executions,
		true,
		transportgrpc.WithTokenValidator(defaultGRPCTestValidator()),
	)
}

func TestGRPC_CreateUploadURL(t *testing.T) {
	t.Parallel()

	t.Run("success single-part upload lease", func(t *testing.T) {
		t.Parallel()
		mockUp := &mockUploadCoordinator{
			createUploadURLFn: func(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error) {
				return &coordinatorv1.CreateUploadURLResponse{
					UploadID:      "upl-123",
					TenantID:      req.TenantID,
					S3Bucket:      "rag-bucket",
					S3Key:         "raw/tenant/file.pdf",
					PresignedURL:  "https://s3.amazonaws.com/raw/tenant/file.pdf",
					ExpiresAtUnix: 1794039600,
					LeaseSeconds:  900,
				}, nil
			},
		}

		client := setupGRPCTestServer(t, mockUp, &mockExecutionCoordinator{})
		resp, err := client.CreateUploadURL(withBearerToken(context.Background(), "valid-write-token"), &coordinatorv1.CreateUploadURLRequest{
			TenantID:           "tenant-corp-01",
			Filename:           "doc.pdf",
			ContentType:        "application/pdf",
			ContentLengthBytes: 1048576,
			ContentHash:        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		})
		require.NoError(t, err)
		assert.Equal(t, "upl-123", resp.UploadID)
		assert.Equal(t, "tenant-corp-01", resp.TenantID)
		assert.Equal(t, int32(900), resp.LeaseSeconds)
	})

	t.Run("error mapping unsupported MIME type to InvalidArgument", func(t *testing.T) {
		t.Parallel()
		mockUp := &mockUploadCoordinator{
			createUploadURLFn: func(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error) {
				return nil, service.ErrUnsupportedMimeType
			},
		}

		client := setupGRPCTestServer(t, mockUp, &mockExecutionCoordinator{})
		_, err := client.CreateUploadURL(withBearerToken(context.Background(), "valid-write-token"), &coordinatorv1.CreateUploadURLRequest{
			TenantID: "tenant-corp-01",
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("error mapping tenant mismatch to PermissionDenied", func(t *testing.T) {
		t.Parallel()
		mockUp := &mockUploadCoordinator{
			createUploadURLFn: func(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error) {
				return nil, service.ErrTenantMismatch
			},
		}

		client := setupGRPCTestServer(t, mockUp, &mockExecutionCoordinator{})
		_, err := client.CreateUploadURL(withBearerToken(context.Background(), "valid-write-token"), &coordinatorv1.CreateUploadURLRequest{
			TenantID: "tenant-corp-01",
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.PermissionDenied, st.Code())
	})
}

func TestGRPC_CompleteUploadURL(t *testing.T) {
	t.Parallel()

	t.Run("success multipart completion", func(t *testing.T) {
		t.Parallel()
		mockUp := &mockUploadCoordinator{
			completeMultipartUploadFn: func(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error) {
				return &coordinatorv1.CompleteMultipartUploadResponse{
					UploadID:    req.UploadID,
					Status:      "COMPLETED",
					S3Location:  "s3://" + req.S3Bucket + "/" + req.S3Key,
					CompletedAt: "2026-11-07T08:30:00Z",
				}, nil
			},
		}

		client := setupGRPCTestServer(t, mockUp, &mockExecutionCoordinator{})
		resp, err := client.CompleteUploadURL(withBearerToken(context.Background(), "valid-write-token"), &coordinatorv1.CompleteMultipartUploadRequest{
			UploadID: "mp-123",
			TenantID: "tenant-corp-01",
			S3Bucket: "rag-bucket",
			S3Key:    "raw/tenant-corp-01/hash/file.pdf",
			Parts: []coordinatorv1.CompletedPart{
				{PartNumber: 1, ETag: "\"etag1\""},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "mp-123", resp.UploadID)
		assert.Equal(t, "COMPLETED", resp.Status)
	})

	t.Run("error mapping invalid completion to InvalidArgument", func(t *testing.T) {
		t.Parallel()
		mockUp := &mockUploadCoordinator{
			completeMultipartUploadFn: func(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error) {
				return nil, service.ErrInvalidMultipartCompletion
			},
		}

		client := setupGRPCTestServer(t, mockUp, &mockExecutionCoordinator{})
		_, err := client.CompleteUploadURL(withBearerToken(context.Background(), "valid-write-token"), &coordinatorv1.CompleteMultipartUploadRequest{})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})
}

func TestGRPC_StartIngestion(t *testing.T) {
	t.Parallel()

	t.Run("success workflow schedule", func(t *testing.T) {
		t.Parallel()
		mockExec := &mockExecutionCoordinator{
			startIngestionFn: func(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error) {
				return &coordinatorv1.StartIngestionResponse{
					WorkflowID:    "ingest-tenant-corp-01-hash",
					RunID:         "run-777",
					Status:        "RUNNING",
					StartedAtUnix: 1794039600,
				}, nil
			},
		}

		client := setupGRPCTestServer(t, &mockUploadCoordinator{}, mockExec)
		resp, err := client.StartIngestion(withBearerToken(context.Background(), "valid-write-token"), &coordinatorv1.StartIngestionRequest{
			TenantID:    "tenant-corp-01",
			DocumentID:  "doc-01",
			ContentHash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		})
		require.NoError(t, err)
		assert.Equal(t, "ingest-tenant-corp-01-hash", resp.WorkflowID)
		assert.Equal(t, "RUNNING", resp.Status)
	})
}

func TestGRPC_GetIngestionStatus(t *testing.T) {
	t.Parallel()

	t.Run("success status query", func(t *testing.T) {
		t.Parallel()
		mockExec := &mockExecutionCoordinator{
			getWorkflowStatusFn: func(ctx context.Context, wfID, tID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				return &coordinatorv1.GetIngestionStatusResponse{
					WorkflowID:      wfID,
					TenantID:        tID,
					Status:          "COMPLETED",
					TotalSlices:     10,
					CompletedSlices: 10,
					TotalPages:      500,
					DurationSeconds: 65.4,
				}, nil
			},
		}

		client := setupGRPCTestServer(t, &mockUploadCoordinator{}, mockExec)
		resp, err := client.GetIngestionStatus(withBearerToken(context.Background(), "valid-read-token"), &coordinatorv1.GetIngestionStatusRequest{
			WorkflowID: "ingest-tenant-corp-01-hash",
			TenantID:   "tenant-corp-01",
		})
		require.NoError(t, err)
		assert.Equal(t, "ingest-tenant-corp-01-hash", resp.WorkflowID)
		assert.Equal(t, "COMPLETED", resp.Status)
		assert.Equal(t, int32(10), resp.TotalSlices)
	})

	t.Run("error mapping workflow not found to NotFound", func(t *testing.T) {
		t.Parallel()
		mockExec := &mockExecutionCoordinator{
			getWorkflowStatusFn: func(ctx context.Context, wfID, tID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				return nil, service.ErrWorkflowNotFound
			},
		}

		client := setupGRPCTestServer(t, &mockUploadCoordinator{}, mockExec)
		_, err := client.GetIngestionStatus(withBearerToken(context.Background(), "valid-read-token"), &coordinatorv1.GetIngestionStatusRequest{
			WorkflowID: "non-existent-wf",
			TenantID:   "tenant-corp-01",
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestGRPC_AuthAndTenantIsolation(t *testing.T) {
	t.Parallel()

	t.Run("unauthenticated when missing or invalid Bearer metadata across all RPCs", func(t *testing.T) {
		t.Parallel()
		called := false
		mockUp := &mockUploadCoordinator{
			createUploadURLFn: func(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error) {
				called = true
				return &coordinatorv1.CreateUploadURLResponse{}, nil
			},
			completeMultipartUploadFn: func(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error) {
				called = true
				return &coordinatorv1.CompleteMultipartUploadResponse{}, nil
			},
		}
		mockExec := &mockExecutionCoordinator{
			startIngestionFn: func(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error) {
				called = true
				return &coordinatorv1.StartIngestionResponse{}, nil
			},
			getWorkflowStatusFn: func(ctx context.Context, wfID, tID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				called = true
				return &coordinatorv1.GetIngestionStatusResponse{}, nil
			},
		}

		client := setupGRPCTestServer(t, mockUp, mockExec)
		for _, ctx := range []context.Context{
			context.Background(),
			withBearerToken(context.Background(), "unknown-token"),
			metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Basic abc")),
		} {
			_, err := client.CreateUploadURL(ctx, &coordinatorv1.CreateUploadURLRequest{TenantID: "tenant-corp-01"})
			assert.Equal(t, codes.Unauthenticated, status.Code(err))

			_, err = client.CompleteUploadURL(ctx, &coordinatorv1.CompleteMultipartUploadRequest{TenantID: "tenant-corp-01"})
			assert.Equal(t, codes.Unauthenticated, status.Code(err))

			_, err = client.StartIngestion(ctx, &coordinatorv1.StartIngestionRequest{TenantID: "tenant-corp-01"})
			assert.Equal(t, codes.Unauthenticated, status.Code(err))

			_, err = client.GetIngestionStatus(ctx, &coordinatorv1.GetIngestionStatusRequest{WorkflowID: "wf-1", TenantID: "tenant-corp-01"})
			assert.Equal(t, codes.Unauthenticated, status.Code(err))
		}
		assert.False(t, called)
	})

	t.Run("permission denied on missing per-method scope", func(t *testing.T) {
		t.Parallel()
		client := setupGRPCTestServer(t, &mockUploadCoordinator{}, &mockExecutionCoordinator{})

		readCtx := withBearerToken(context.Background(), "valid-read-token")
		_, err := client.CreateUploadURL(readCtx, &coordinatorv1.CreateUploadURLRequest{TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		_, err = client.CompleteUploadURL(readCtx, &coordinatorv1.CompleteMultipartUploadRequest{TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		_, err = client.StartIngestion(readCtx, &coordinatorv1.StartIngestionRequest{TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		writeCtx := withBearerToken(context.Background(), "valid-write-token")
		_, err = client.GetIngestionStatus(writeCtx, &coordinatorv1.GetIngestionStatusRequest{WorkflowID: "wf-1", TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("permission denied when request tenant_id or x-tenant-id metadata mismatches caller claims", func(t *testing.T) {
		t.Parallel()
		called := false
		mockUp := &mockUploadCoordinator{
			createUploadURLFn: func(ctx context.Context, req coordinatorv1.CreateUploadURLRequest) (*coordinatorv1.CreateUploadURLResponse, error) {
				called = true
				return &coordinatorv1.CreateUploadURLResponse{}, nil
			},
			completeMultipartUploadFn: func(ctx context.Context, req coordinatorv1.CompleteMultipartUploadRequest) (*coordinatorv1.CompleteMultipartUploadResponse, error) {
				called = true
				return &coordinatorv1.CompleteMultipartUploadResponse{}, nil
			},
		}
		mockExec := &mockExecutionCoordinator{
			startIngestionFn: func(ctx context.Context, req coordinatorv1.StartIngestionRequest) (*coordinatorv1.StartIngestionResponse, error) {
				called = true
				return &coordinatorv1.StartIngestionResponse{}, nil
			},
			getWorkflowStatusFn: func(ctx context.Context, wfID, tID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				called = true
				return &coordinatorv1.GetIngestionStatusResponse{}, nil
			},
		}

		client := setupGRPCTestServer(t, mockUp, mockExec)
		otherCtx := withBearerToken(context.Background(), "other-tenant-token")

		_, err := client.CreateUploadURL(otherCtx, &coordinatorv1.CreateUploadURLRequest{TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		_, err = client.CompleteUploadURL(otherCtx, &coordinatorv1.CompleteMultipartUploadRequest{TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		_, err = client.StartIngestion(otherCtx, &coordinatorv1.StartIngestionRequest{TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		_, err = client.GetIngestionStatus(otherCtx, &coordinatorv1.GetIngestionStatusRequest{WorkflowID: "wf-1", TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		spoofedHeaderCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
			"authorization", "Bearer valid-rw-token",
			"x-tenant-id", "tenant-other-02",
		))
		_, err = client.GetIngestionStatus(spoofedHeaderCtx, &coordinatorv1.GetIngestionStatusRequest{WorkflowID: "wf-1", TenantID: "tenant-corp-01"})
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
		assert.False(t, called)
	})

	t.Run("fail-closed default without interceptor or validator and server-level enforcement without interceptor", func(t *testing.T) {
		t.Parallel()
		failClosedClient := setupGRPCTestServerWithOptions(t, &mockUploadCoordinator{}, &mockExecutionCoordinator{}, false)
		_, err := failClosedClient.GetIngestionStatus(
			withBearerToken(context.Background(), "valid-read-token"),
			&coordinatorv1.GetIngestionStatusRequest{WorkflowID: "wf-1", TenantID: "tenant-corp-01"},
		)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))

		var observedTenant string
		mockExec := &mockExecutionCoordinator{
			getWorkflowStatusFn: func(ctx context.Context, wfID, tID string) (*coordinatorv1.GetIngestionStatusResponse, error) {
				observedTenant = tID
				return &coordinatorv1.GetIngestionStatusResponse{WorkflowID: wfID, TenantID: tID, Status: "RUNNING"}, nil
			},
		}
		serverAuthClient := setupGRPCTestServerWithOptions(
			t,
			&mockUploadCoordinator{},
			mockExec,
			false,
			transportgrpc.WithTokenValidator(defaultGRPCTestValidator()),
		)
		_, err = serverAuthClient.GetIngestionStatus(
			withBearerToken(context.Background(), "other-tenant-token"),
			&coordinatorv1.GetIngestionStatusRequest{WorkflowID: "wf-1", TenantID: "tenant-corp-01"},
		)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		resp, err := serverAuthClient.GetIngestionStatus(
			withBearerToken(context.Background(), "valid-read-token"),
			&coordinatorv1.GetIngestionStatusRequest{WorkflowID: "wf-1", TenantID: " tenant-corp-01 "},
		)
		require.NoError(t, err)
		assert.Equal(t, "tenant-corp-01", observedTenant)
		assert.Equal(t, "tenant-corp-01", resp.TenantID)
	})
}
