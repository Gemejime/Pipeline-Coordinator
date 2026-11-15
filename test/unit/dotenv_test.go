package unit_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"rag-ingestion-coordinator/internal/config"
)

func TestLoadDotEnv_AppliesChangesWithoutOverridingProcessEnv(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")

	content := strings.Join([]string{
		"# Comment line",
		"",
		"DOTENV_TEST_HTTP_ADDR=:9090 # inline comment",
		"export DOTENV_TEST_LOG_LEVEL='debug'",
		`DOTENV_TEST_TOKENS={"dev":{"tenant_id":"t1","scopes":["ingest:write"]}}`,
		"DOTENV_TEST_QUOTED=\"hello world\"\r",
		"DOTENV_TEST_EXISTING=from-dotenv",
	}, "\n")
	require.NoError(t, os.WriteFile(envPath, []byte(content), 0o600))

	t.Setenv("DOTENV_TEST_EXISTING", "from-process")
	for _, key := range []string{
		"DOTENV_TEST_HTTP_ADDR",
		"DOTENV_TEST_LOG_LEVEL",
		"DOTENV_TEST_TOKENS",
		"DOTENV_TEST_QUOTED",
	} {
		k := key
		prev, had := os.LookupEnv(k)
		_ = os.Unsetenv(k)
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(k, prev)
			} else {
				_ = os.Unsetenv(k)
			}
		})
	}

	require.NoError(t, config.LoadDotEnv(envPath))

	assert.Equal(t, ":9090", os.Getenv("DOTENV_TEST_HTTP_ADDR"))
	assert.Equal(t, "debug", os.Getenv("DOTENV_TEST_LOG_LEVEL"))
	assert.Equal(t, `{"dev":{"tenant_id":"t1","scopes":["ingest:write"]}}`, os.Getenv("DOTENV_TEST_TOKENS"))
	assert.Equal(t, "hello world", os.Getenv("DOTENV_TEST_QUOTED"))
	assert.Equal(t, "from-process", os.Getenv("DOTENV_TEST_EXISTING"))
}

func TestLoadDotEnv_MissingFileIsNoOp(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent.env")
	require.NoError(t, config.LoadDotEnv(missing))
}

func TestEnvExample_ContainsAllCoordinatorVariables(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", ".env.example"))
	require.NoError(t, err)
	text := string(raw)

	requiredVars := []string{
		"LOG_LEVEL",
		"HTTP_ADDR",
		"GRPC_ADDR",
		"SHUTDOWN_DRAIN_DELAY",
		"OPENAPI_SPEC_PATH",
		"AUTH_HMAC_SECRET",
		"AUTH_STATIC_TOKENS_JSON",
		"POSTGRES_DSN",
		"POSTGRES_SSLMODE",
		"POSTGRES_MAX_CONNS",
		"POSTGRES_MIN_CONNS",
		"AUTO_MIGRATE",
		"REDIS_ADDRS",
		"REDIS_MASTER_NAME",
		"REDIS_USERNAME",
		"REDIS_PASSWORD",
		"REDIS_DB",
		"REDIS_TLS_ENABLED",
		"TEMPORAL_HOST_PORT",
		"TEMPORAL_NAMESPACE",
		"TEMPORAL_TASK_QUEUE",
		"ENABLE_INSPECTION_WORKER",
		"S3_REGION",
		"S3_BUCKET",
		"S3_ENDPOINT",
		"S3_USE_PATH_STYLE",
		"USE_HASH_PREFIX_S3_LAYOUT",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
	}

	for _, name := range requiredVars {
		assert.Contains(t, text, name+"=", "missing %s in .env.example", name)
	}
}
