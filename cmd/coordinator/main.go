// Command coordinator runs the rag-ingestion-coordinator control plane microservice,
// serving REST HTTP (:8080), internal gRPC (:50051), Temporal workers, and Prometheus telemetry.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"

	coordinatorv1 "rag-ingestion-coordinator/api/proto/v1"
	"rag-ingestion-coordinator/internal/activity"
	"rag-ingestion-coordinator/internal/service"
	"rag-ingestion-coordinator/internal/store/postgres"
	redisstore "rag-ingestion-coordinator/internal/store/redis"
	"rag-ingestion-coordinator/internal/store/s3"
	"rag-ingestion-coordinator/internal/telemetry"
	grpctransport "rag-ingestion-coordinator/internal/transport/grpc"
	httptransport "rag-ingestion-coordinator/internal/transport/http"
	"rag-ingestion-coordinator/internal/transport/http/middleware"
	"rag-ingestion-coordinator/internal/workflow"
	"rag-ingestion-coordinator/migrations"
)

const (
	defaultHTTPAddr           = ":8080"
	defaultGRPCAddr           = ":50051"
	defaultTemporalHostPort   = "localhost:7233"
	defaultTemporalNamespace  = "default"
	defaultShutdownDrainDelay = 5 * time.Second
	defaultShutdownTimeout    = 20 * time.Second
	defaultReadinessProbeWait = 2 * time.Second
)

func main() {
	migrateOnly := flag.Bool("migrate", false, "apply idempotent PostgreSQL schema migrations and exit")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(os.Getenv("LOG_LEVEL")),
	}))

	if err := run(context.Background(), *migrateOnly, logger); err != nil {
		logger.Error("coordinator exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(parentCtx context.Context, migrateOnly bool, logger *slog.Logger) error {
	startupCtx, cancelStartup := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancelStartup()

	pgCfg := postgres.Config{
		DSN:      os.Getenv("POSTGRES_DSN"),
		SSLMode:  envOrDefault("POSTGRES_SSLMODE", "prefer"),
		MaxConns: envInt32("POSTGRES_MAX_CONNS", postgres.DefaultMaxConns),
		MinConns: envInt32("POSTGRES_MIN_CONNS", postgres.DefaultMinConns),
	}

	pgClient, err := postgres.NewClient(startupCtx, pgCfg)
	if err != nil {
		return fmt.Errorf("postgresql initialization failed: %w", err)
	}
	defer pgClient.Close()

	if migrateOnly {
		if err := pgClient.RunMigrations(startupCtx, migrations.FS); err != nil {
			return fmt.Errorf("database migration failed: %w", err)
		}
		logger.Info("postgresql schema migrations applied successfully")
		return nil
	}

	if envBool("AUTO_MIGRATE", false) {
		if err := pgClient.RunMigrations(startupCtx, migrations.FS); err != nil {
			return fmt.Errorf("auto-migration failed: %w", err)
		}
	}

	metrics := telemetry.NewMetrics()

	redisAddrs := splitCSV(envOrDefault("REDIS_ADDRS", "localhost:6379"))
	redisPub, err := redisstore.NewPublisher(redisstore.Config{
		Addrs:      redisAddrs,
		MasterName: os.Getenv("REDIS_MASTER_NAME"),
		Username:   os.Getenv("REDIS_USERNAME"),
		Password:   os.Getenv("REDIS_PASSWORD"),
		DB:         int(envInt32("REDIS_DB", 0)),
		TLSEnabled: envBool("REDIS_TLS_ENABLED", false),
		Metrics:    metrics,
	})
	if err != nil {
		return fmt.Errorf("redis publisher initialization failed: %w", err)
	}
	defer func() {
		_ = redisPub.Close()
	}()

	if err := redisPub.Ping(startupCtx); err != nil {
		logger.Warn("redis initial ping failed; /ready will report unavailable until broker recovers",
			slog.String("error", err.Error()),
		)
	}

	s3Region := envOrDefault("S3_REGION", s3.DefaultS3Region)
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	s3Bucket := envOrDefault("S3_BUCKET", service.DefaultIngestionBucket)

	awsS3Client, err := s3.NewAWSClient(startupCtx, s3.AWSClientConfig{
		Region:          s3Region,
		Endpoint:        s3Endpoint,
		UsePathStyle:    envBool("S3_USE_PATH_STYLE", false),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	})
	if err != nil {
		return fmt.Errorf("aws s3 client initialization failed: %w", err)
	}

	presigner, err := s3.NewPresigner(awsS3Client, awsS3Client, s3.PresignerConfig{
		Endpoint: s3Endpoint,
		Region:   s3Region,
	})
	if err != nil {
		return fmt.Errorf("s3 presigner initialization failed: %w", err)
	}

	temporalHostPort := envOrDefault("TEMPORAL_HOST_PORT", defaultTemporalHostPort)
	temporalNamespace := envOrDefault("TEMPORAL_NAMESPACE", defaultTemporalNamespace)
	temporalClient, err := client.Dial(client.Options{
		HostPort:       temporalHostPort,
		Namespace:      temporalNamespace,
		MetricsHandler: metrics.TemporalMetricsHandler(),
	})
	if err != nil {
		return fmt.Errorf("temporal client dial failed: %w", err)
	}
	defer temporalClient.Close()

	runRepo := postgres.NewRunRepository(pgClient.Pool())
	pipelineRepo := postgres.NewPipelineRepo(pgClient.Pool())

	coordWorker := worker.New(temporalClient, workflow.CoordinatorTaskQueue, worker.Options{})
	coordWorker.RegisterWorkflow(workflow.IngestionCoordinatorWorkflow)
	coordWorker.RegisterWorkflow(workflow.DocumentPartitionWorkflow)

	if err := activity.NewPresignActivities(presigner).Register(coordWorker); err != nil {
		return fmt.Errorf("failed to register presign activities: %w", err)
	}
	if err := activity.NewNotificationActivities(redisPub).Register(coordWorker); err != nil {
		return fmt.Errorf("failed to register notification activities: %w", err)
	}
	if err := activity.NewCatalogActivities(runRepo).Register(coordWorker); err != nil {
		return fmt.Errorf("failed to register catalog activities: %w", err)
	}
	if err := coordWorker.Start(); err != nil {
		return fmt.Errorf("failed to start temporal coordinator worker: %w", err)
	}

	var inspectWorker worker.Worker
	if envBool("ENABLE_INSPECTION_WORKER", false) {
		inspectWorker = worker.New(temporalClient, workflow.InspectionTaskQueue, worker.Options{})
		if err := activity.NewInspectionActivitiesWithRangeReader(presigner).Register(inspectWorker); err != nil {
			coordWorker.Stop()
			return fmt.Errorf("failed to register inspection activities: %w", err)
		}
		if err := inspectWorker.Start(); err != nil {
			coordWorker.Stop()
			return fmt.Errorf("failed to start temporal inspection worker: %w", err)
		}
	}

	validator, err := buildProductionTokenValidator(logger)
	if err != nil {
		stopWorkers(coordWorker, inspectWorker)
		return fmt.Errorf("auth validator initialization failed: %w", err)
	}

	uploadSvc := service.NewUploadService(presigner, service.UploadServiceConfig{
		DefaultBucket: s3Bucket,
	})
	execSvc := service.NewExecutionService(temporalClient, service.ExecutionServiceConfig{
		TerminalReader:        runRepo,
		PipelineConfigReader:  pipelineRepo,
		UseHashPrefixS3Layout: envBool("USE_HASH_PREFIX_S3_LAYOUT", false),
		Metrics:               metrics,
	})

	var draining atomic.Bool
	readinessFn := func(ctx context.Context) map[string]string {
		probeCtx, cancel := context.WithTimeout(ctx, defaultReadinessProbeWait)
		defer cancel()

		checks := map[string]string{
			"temporal": "connected",
			"postgres": "connected",
			"redis":    "connected",
		}
		if _, err := temporalClient.CheckHealth(probeCtx, &client.CheckHealthRequest{}); err != nil {
			checks["temporal"] = "disconnected"
		}
		if err := pgClient.Ping(probeCtx); err != nil {
			checks["postgres"] = "disconnected"
		}
		if err := redisPub.Ping(probeCtx); err != nil {
			checks["redis"] = "disconnected"
		}
		return checks
	}

	router := httptransport.NewRouter(httptransport.RouterConfig{
		UploadHandler:    httptransport.NewUploadHandler(uploadSvc, logger),
		ExecutionHandler: httptransport.NewExecutionHandler(execSvc, logger),
		TokenValidator:   validator,
		Logger:           logger,
		ReadinessCheck:   readinessFn,
		DrainState:       &draining,
		MetricsHandler:   metrics.Handler(),
		OpenAPIPath:      envOrDefault("OPENAPI_SPEC_PATH", "api/openapi/openapi.yaml"),
	})

	httpAddr := envOrDefault("HTTP_ADDR", defaultHTTPAddr)
	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	grpcAddr := envOrDefault("GRPC_ADDR", defaultGRPCAddr)
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		stopWorkers(coordWorker, inspectWorker)
		return fmt.Errorf("failed to listen on gRPC address %s: %w", grpcAddr, err)
	}
	grpcSrv := grpc.NewServer()
	coordinatorv1.RegisterCoordinatorServiceServer(
		grpcSrv,
		grpctransport.NewCoordinatorServer(uploadSvc, execSvc, logger),
	)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting HTTP control plane server", slog.String("addr", httpAddr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server failed: %w", err)
		}
	}()
	go func() {
		logger.Info("starting gRPC control plane server", slog.String("addr", grpcAddr))
		if err := grpcSrv.Serve(grpcLis); err != nil {
			errCh <- fmt.Errorf("grpc server failed: %w", err)
		}
	}()

	sigCtx, stopSignals := signal.NotifyContext(parentCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	select {
	case <-sigCtx.Done():
		logger.Info("shutdown signal received; entering readiness drain")
	case srvErr := <-errCh:
		draining.Store(true)
		stopWorkers(coordWorker, inspectWorker)
		return srvErr
	}

	// Zero-drop rolling/canary drain: immediately mark /ready as 503 draining and wait
	// SHUTDOWN_DRAIN_DELAY so Kubernetes Endpoints / kube-proxy de-register this pod
	// before stopping HTTP/gRPC listeners (distroless has no shell for preStop sleep).
	draining.Store(true)
	drainDelay := envDuration("SHUTDOWN_DRAIN_DELAY", defaultShutdownDrainDelay)
	if drainDelay > 0 {
		logger.Info("waiting for endpoint drain window before server shutdown",
			slog.Duration("drain_delay", drainDelay),
		)
		waitDrainWindow(drainDelay)
	}
	stopWorkers(coordWorker, inspectWorker)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancelShutdown()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server graceful shutdown error", slog.String("error", err.Error()))
	}
	grpcSrv.GracefulStop()
	logger.Info("coordinator shutdown complete")
	return nil
}

func waitDrainWindow(d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	<-timer.C
}

func stopWorkers(coordWorker, inspectWorker worker.Worker) {
	if inspectWorker != nil {
		inspectWorker.Stop()
	}
	if coordWorker != nil {
		coordWorker.Stop()
	}
}

func buildProductionTokenValidator(logger *slog.Logger) (middleware.TokenValidator, error) {
	if secret := strings.TrimSpace(os.Getenv("AUTH_HMAC_SECRET")); secret != "" {
		return middleware.NewHMACJWTValidator([]byte(secret), nil)
	}
	if rawJSON := strings.TrimSpace(os.Getenv("AUTH_STATIC_TOKENS_JSON")); rawJSON != "" {
		var tokenMap map[string]middleware.AuthClaims
		if err := json.Unmarshal([]byte(rawJSON), &tokenMap); err != nil {
			return nil, fmt.Errorf("invalid AUTH_STATIC_TOKENS_JSON: %w", err)
		}
		return middleware.NewStaticTokenValidator(tokenMap), nil
	}
	logger.Warn("no production auth configuration provided; defaulting to fail-closed token validator")
	return middleware.NewFailClosedTokenValidator(), nil
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt32(key string, fallback int32) int32 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(parsed)
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(v)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseLogLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
