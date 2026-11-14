// Package postgres provides PgBouncer-compatible PostgreSQL connection pooling,
// idempotent migration execution, and terminal pipeline run / config repositories (ADR-003).
package postgres

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultMaxConns caps concurrent PostgreSQL connections per coordinator pod.
	DefaultMaxConns int32 = 20
	// DefaultMinConns maintains warm pool connections.
	DefaultMinConns int32 = 2
	// DefaultMaxConnLifetime bounds connection age for PgBouncer / RDS rotation.
	DefaultMaxConnLifetime = 30 * time.Minute
	// DefaultMaxConnIdleTime closes idle connections promptly.
	DefaultMaxConnIdleTime = 5 * time.Minute
	// DefaultHealthCheckPeriod defines pool background health check cadence.
	DefaultHealthCheckPeriod = 1 * time.Minute
)

// ErrMissingDSN indicates that no PostgreSQL connection string was configured.
var ErrMissingDSN = errors.New("postgresql DSN is required")

// DBTX abstracts pgxpool.Pool and pgx.Tx query execution for repository decoupling and testing.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Config configures the PostgreSQL pgxpool connection pool.
type Config struct {
	DSN               string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	SSLMode           string
	QueryExecMode     pgx.QueryExecMode
}

// ParsePoolConfig builds a PgBouncer transaction-pooling compatible *pgxpool.Config.
// It explicitly disables server-side prepared statement caching by defaulting to pgx.QueryExecModeExec.
func ParsePoolConfig(cfg Config) (*pgxpool.Config, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, ErrMissingDSN
	}

	if sslMode := strings.TrimSpace(cfg.SSLMode); sslMode != "" {
		dsn = applySSLModeToDSN(dsn, sslMode)
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgresql DSN (%s): %w", SanitizeDSN(cfg.DSN), sanitizePGError(err, cfg.DSN))
	}

	// PgBouncer transaction pooling compatibility: never use prepared statement caching
	// (QueryExecModeCacheStatement = 0). Default to QueryExecModeExec unless SimpleProtocol is chosen.
	switch cfg.QueryExecMode {
	case pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec:
		poolCfg.ConnConfig.DefaultQueryExecMode = cfg.QueryExecMode
	default:
		poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	}

	maxConns := cfg.MaxConns
	if maxConns <= 0 {
		maxConns = DefaultMaxConns
	}
	minConns := cfg.MinConns
	if minConns < 0 {
		minConns = 0
	}
	if minConns == 0 && maxConns >= DefaultMinConns {
		minConns = DefaultMinConns
	}
	if minConns > maxConns {
		minConns = maxConns
	}

	poolCfg.MaxConns = maxConns
	poolCfg.MinConns = minConns

	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	} else {
		poolCfg.MaxConnLifetime = DefaultMaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	} else {
		poolCfg.MaxConnIdleTime = DefaultMaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	} else {
		poolCfg.HealthCheckPeriod = DefaultHealthCheckPeriod
	}

	// Ensure minimum TLS 1.2 when TLS is enabled.
	if poolCfg.ConnConfig.TLSConfig != nil {
		if poolCfg.ConnConfig.TLSConfig.MinVersion == 0 {
			poolCfg.ConnConfig.TLSConfig.MinVersion = tls.VersionTLS12
		}
	}

	return poolCfg, nil
}

// Client wraps a configured *pgxpool.Pool with health probing and idempotent migration support.
type Client struct {
	pool *pgxpool.Pool
}

// NewClient initializes and verifies a PostgreSQL pgxpool connection pool.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	poolCfg, err := ParsePoolConfig(cfg)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgresql pool (%s): %w", SanitizeDSN(cfg.DSN), sanitizePGError(err, cfg.DSN))
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgresql (%s): %w", SanitizeDSN(cfg.DSN), sanitizePGError(err, cfg.DSN))
	}

	return &Client{pool: pool}, nil
}

// Pool returns the underlying *pgxpool.Pool.
func (c *Client) Pool() *pgxpool.Pool {
	if c == nil {
		return nil
	}
	return c.pool
}

// Ping verifies live PostgreSQL connectivity for the /ready probe.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.pool == nil {
		return errors.New("postgresql pool is not initialized")
	}
	return c.pool.Ping(ctx)
}

// Close releases all pooled PostgreSQL connections.
func (c *Client) Close() {
	if c != nil && c.pool != nil {
		c.pool.Close()
	}
}

// RunMigrations executes all embedded *.up.sql migration files in deterministic lexicographical order.
func (c *Client) RunMigrations(ctx context.Context, migrationFS fs.FS) error {
	if c == nil || c.pool == nil {
		return errors.New("postgresql pool is not initialized")
	}
	if migrationFS == nil {
		return errors.New("migration filesystem is nil")
	}

	entries, err := fs.ReadDir(migrationFS, ".")
	if err != nil {
		return fmt.Errorf("failed to list migrations: %w", err)
	}

	var upFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".up.sql") {
			upFiles = append(upFiles, name)
		}
	}
	sort.Strings(upFiles)

	for _, name := range upFiles {
		sqlBytes, err := fs.ReadFile(migrationFS, filepath.ToSlash(name))
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", name, err)
		}
		if _, err := c.pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", name, err)
		}
	}
	return nil
}

// SanitizeDSN redacts embedded passwords from PostgreSQL URL or key=value DSN strings for safe logging.
func SanitizeDSN(rawDSN string) string {
	trimmed := strings.TrimSpace(rawDSN)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return "[REDACTED_DSN]"
		}
		if u.User != nil {
			username := u.User.Username()
			if _, hasPass := u.User.Password(); hasPass {
				u.User = url.UserPassword(username, "REDACTED")
			}
		}
		return u.String()
	}

	parts := strings.Fields(trimmed)
	for i, part := range parts {
		if strings.HasPrefix(strings.ToLower(part), "password=") {
			parts[i] = "password=REDACTED"
		}
	}
	return strings.Join(parts, " ")
}

func applySSLModeToDSN(dsn, sslMode string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return dsn
		}
		q := u.Query()
		if q.Get("sslmode") == "" {
			q.Set("sslmode", sslMode)
			u.RawQuery = q.Encode()
		}
		return u.String()
	}
	if !strings.Contains(strings.ToLower(dsn), "sslmode=") {
		return dsn + " sslmode=" + sslMode
	}
	return dsn
}

func sanitizePGError(err error, rawDSN string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if rawDSN != "" && strings.Contains(msg, rawDSN) {
		msg = strings.ReplaceAll(msg, rawDSN, SanitizeDSN(rawDSN))
		return errors.New(msg)
	}
	return err
}
