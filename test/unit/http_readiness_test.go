package unit_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"rag-ingestion-coordinator/internal/telemetry"
	transport "rag-ingestion-coordinator/internal/transport/http"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
)

func TestHTTP_ReadinessProbeAndDrain(t *testing.T) {
	t.Parallel()

	t.Run("200 ready when all dependencies connected", func(t *testing.T) {
		t.Parallel()
		var drain atomic.Bool
		router := transport.NewRouter(transport.RouterConfig{
			DrainState: &drain,
			ReadinessCheck: func(ctx context.Context) map[string]string {
				return map[string]string{
					"temporal": "connected",
					"postgres": "connected",
					"redis":    "connected",
				}
			},
		})

		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "ready", body["status"])
	})

	t.Run("503 unavailable when any dependency is disconnected", func(t *testing.T) {
		t.Parallel()
		failingDeps := []string{"temporal", "postgres", "redis"}
		for _, dep := range failingDeps {
			dep := dep
			t.Run(dep+"_disconnected", func(t *testing.T) {
				t.Parallel()
				router := transport.NewRouter(transport.RouterConfig{
					ReadinessCheck: func(ctx context.Context) map[string]string {
						checks := map[string]string{
							"temporal": "connected",
							"postgres": "connected",
							"redis":    "connected",
						}
						checks[dep] = "disconnected"
						return checks
					},
				})

				req := httptest.NewRequest(http.MethodGet, "/ready", nil)
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)

				assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "unavailable", body["status"])
			})
		}
	})

	t.Run("503 unavailable immediately when draining during shutdown", func(t *testing.T) {
		t.Parallel()
		var drain atomic.Bool
		router := transport.NewRouter(transport.RouterConfig{
			DrainState: &drain,
			ReadinessCheck: func(ctx context.Context) map[string]string {
				t.Fatal("ReadinessCheck must not be called once draining is active")
				return nil
			},
		})

		drain.Store(true)
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "unavailable", body["status"])
	})
}

func TestHTTP_LivePrometheusMetricsEndpoint(t *testing.T) {
	t.Parallel()

	m := telemetry.NewMetrics()
	m.IncWorkflowStart("tenant-corp-01", "RUNNING")
	m.IncRedisEventPublished()
	m.IncRedisEventError()

	temporalHandler := m.TemporalMetricsHandler().WithTags(map[string]string{
		"task_queue": "doc-ai-task-queue",
	})
	temporalHandler.Gauge("coordinator_child_workflows_spawned").Update(12)
	temporalHandler.Timer("temporal_activity_schedule_to_start_latency").Record(250 * time.Millisecond)

	router := transport.NewRouter(transport.RouterConfig{
		MetricsHandler: m.Handler(),
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `coordinator_workflow_starts_total{status="RUNNING",tenant_id="tenant-corp-01"} 1`)
	assert.Contains(t, body, `coordinator_child_workflows_spawned_count 1`)
	assert.Contains(t, body, `coordinator_redis_events_published_total 1`)
	assert.Contains(t, body, `coordinator_redis_events_errors_total 1`)
	assert.Contains(t, body, `temporal_activity_schedule_to_start_seconds_count{task_queue="doc-ai-task-queue"} 1`)
}

func TestHMACJWTValidator(t *testing.T) {
	t.Parallel()

	secret := []byte("super-secret-hmac-signing-key-32b")
	fixedNow := time.Unix(1794039600, 0).UTC()
	validator, err := middleware.NewHMACJWTValidator(secret, func() time.Time { return fixedNow })
	require.NoError(t, err)

	signHS256 := func(claims map[string]any) string {
		hdrB64 := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
		payloadBytes, _ := json.Marshal(claims)
		payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(hdrB64 + "." + payloadB64))
		sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		return hdrB64 + "." + payloadB64 + "." + sigB64
	}

	validTok := signHS256(map[string]any{
		"tenant_id": "tenant-corp-01",
		"sub":       "svc-client",
		"scopes":    []string{middleware.ScopeIngestWrite, middleware.ScopeIngestRead},
		"exp":       fixedNow.Add(15 * time.Minute).Unix(),
	})

	claims, err := validator.ValidateToken(context.Background(), validTok)
	require.NoError(t, err)
	assert.Equal(t, "tenant-corp-01", claims.TenantID)
	assert.True(t, claims.HasScope(middleware.ScopeIngestWrite))

	// Expired token rejected
	expiredTok := signHS256(map[string]any{
		"tenant_id": "tenant-corp-01",
		"exp":       fixedNow.Add(-time.Second).Unix(),
	})
	_, err = validator.ValidateToken(context.Background(), expiredTok)
	require.ErrorIs(t, err, middleware.ErrInvalidBearerToken)

	// Tampered signature rejected
	_, err = validator.ValidateToken(context.Background(), validTok+"tampered")
	require.ErrorIs(t, err, middleware.ErrInvalidBearerToken)
}
