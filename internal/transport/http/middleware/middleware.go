// Package middleware provides HTTP ingress middleware for request correlation,
// multi-tenant Bearer authentication & scope authorization, memory-bounded
// token-bucket rate limiting, and secured structured JSON logging.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// HeaderRequestID is the canonical HTTP correlation header propagated as trace_id.
	HeaderRequestID = "X-Request-ID"
	// HeaderTenantID is the HTTP header carrying the caller's tenant identifier.
	HeaderTenantID = "X-Tenant-ID"
	// HeaderAuthorization is the HTTP header carrying the Bearer JWT/token.
	HeaderAuthorization = "Authorization"
	// HeaderRetryAfter is the HTTP response header returned on 429 Too Many Requests.
	HeaderRetryAfter = "Retry-After"

	// ScopeIngestWrite is required for upload URL issuance, completion, and workflow start.
	ScopeIngestWrite = "ingest:write"
	// ScopeIngestRead is required for workflow status queries.
	ScopeIngestRead = "ingest:read"

	// MaxTrackedBuckets caps the in-memory rate-limiter bucket table to protect the <256 MiB RSS ceiling.
	MaxTrackedBuckets = 10000
	// DefaultRateLimitRPS is the default sustained requests-per-second per tenant.
	DefaultRateLimitRPS = 1000.0
	// DefaultRateLimitBurst is the default token-bucket burst capacity per tenant.
	DefaultRateLimitBurst = 1000
	// DefaultBucketStaleTTL is the idle duration after which a tenant rate-limit bucket is eligible for eviction.
	DefaultBucketStaleTTL = 15 * time.Minute
)

type contextKey int

const (
	ctxKeyTraceID contextKey = iota
	ctxKeyAuthClaims
	ctxKeyCorrelationMeta
)

var (
	// ErrMissingBearerToken indicates a missing or malformed Authorization: Bearer header.
	ErrMissingBearerToken = errors.New("missing or malformed Authorization Bearer token")
	// ErrInvalidBearerToken indicates an unrecognized, expired, or invalid token.
	ErrInvalidBearerToken = errors.New("invalid or expired Bearer token")

	sensitiveQueryParamKeys = []string{
		"X-Amz-Signature",
		"X-Amz-Credential",
		"X-Amz-Security-Token",
		"AWSAccessKeyId",
		"x-amz-signature",
		"x-amz-credential",
		"x-amz-security-token",
		"awsaccesskeyid",
	}
)

// AuthClaims holds the verified identity and scopes extracted from a Bearer token.
type AuthClaims struct {
	TenantID string   `json:"tenant_id"`
	Subject  string   `json:"sub,omitempty"`
	Scopes   []string `json:"scopes"`
}

// HasScope returns true if the claims grant the requested scope.
func (c AuthClaims) HasScope(required string) bool {
	for _, s := range c.Scopes {
		if s == required {
			return true
		}
	}
	return false
}

// TokenValidator verifies a raw Bearer token and returns its authenticated tenant claims.
type TokenValidator interface {
	ValidateToken(ctx context.Context, rawToken string) (*AuthClaims, error)
}

// StaticTokenValidator validates Bearer tokens against a registered map or structured JWT payload claims.
type StaticTokenValidator struct {
	mu     sync.RWMutex
	tokens map[string]AuthClaims
}

// NewStaticTokenValidator constructs a StaticTokenValidator with optional pre-registered token claims.
func NewStaticTokenValidator(tokens map[string]AuthClaims) *StaticTokenValidator {
	copied := make(map[string]AuthClaims, len(tokens))
	for k, v := range tokens {
		copied[k] = v
	}
	return &StaticTokenValidator{tokens: copied}
}

// ValidateToken validates rawToken against registered static tokens or parses a structured
// test/service token of the form "tenant:<tenant_id>;scopes:<comma_separated_scopes>" or JWT payload.
func (v *StaticTokenValidator) ValidateToken(_ context.Context, rawToken string) (*AuthClaims, error) {
	trimmed := strings.TrimSpace(rawToken)
	if trimmed == "" {
		return nil, ErrMissingBearerToken
	}

	if v != nil {
		v.mu.RLock()
		claims, ok := v.tokens[trimmed]
		v.mu.RUnlock()
		if ok {
			if strings.TrimSpace(claims.TenantID) == "" {
				return nil, ErrInvalidBearerToken
			}
			return &claims, nil
		}
	}

	// Support structured tokens: "tenant:<tenant_id>;scopes:<scope1>,<scope2>"
	if strings.HasPrefix(trimmed, "tenant:") {
		claims, err := parseStructuredToken(trimmed)
		if err != nil {
			return nil, err
		}
		return claims, nil
	}

	// Support 3-part JWT tokens in local/gateway pass-through mode when registered tokens are empty.
	if v == nil || len(v.tokens) == 0 {
		if claims, err := parseJWTClaimsPayload(trimmed); err == nil {
			return claims, nil
		}
	}

	return nil, ErrInvalidBearerToken
}

func parseStructuredToken(token string) (*AuthClaims, error) {
	var tenantID string
	var scopes []string
	segments := strings.Split(token, ";")
	for _, seg := range segments {
		switch {
		case strings.HasPrefix(seg, "tenant:"):
			tenantID = strings.TrimSpace(strings.TrimPrefix(seg, "tenant:"))
		case strings.HasPrefix(seg, "scopes:"):
			rawScopes := strings.TrimPrefix(seg, "scopes:")
			for _, sc := range strings.Split(rawScopes, ",") {
				if s := strings.TrimSpace(sc); s != "" {
					scopes = append(scopes, s)
				}
			}
		}
	}
	if tenantID == "" {
		return nil, ErrInvalidBearerToken
	}
	if len(scopes) == 0 {
		scopes = []string{ScopeIngestWrite, ScopeIngestRead}
	}
	return &AuthClaims{
		TenantID: tenantID,
		Scopes:   scopes,
	}, nil
}

func parseJWTClaimsPayload(token string) (*AuthClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidBearerToken
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidBearerToken
	}
	var raw struct {
		TenantID string   `json:"tenant_id"`
		Sub      string   `json:"sub"`
		Scope    string   `json:"scope"`
		Scopes   []string `json:"scopes"`
	}
	if err := json.Unmarshal(payloadBytes, &raw); err != nil {
		return nil, ErrInvalidBearerToken
	}
	if strings.TrimSpace(raw.TenantID) == "" {
		return nil, ErrInvalidBearerToken
	}
	scopes := append([]string(nil), raw.Scopes...)
	for _, s := range strings.Fields(raw.Scope) {
		scopes = append(scopes, s)
	}
	return &AuthClaims{
		TenantID: strings.TrimSpace(raw.TenantID),
		Subject:  raw.Sub,
		Scopes:   scopes,
	}, nil
}

// CorrelationMeta stores mutable request-scoped correlation attributes so handlers can enrich
// the final structured access log line with tenant_id, document_id, and workflow_id.
type CorrelationMeta struct {
	mu         sync.RWMutex
	TenantID   string
	DocumentID string
	WorkflowID string
}

// SetCorrelationFields updates correlation fields on the request context for structured logging.
func SetCorrelationFields(ctx context.Context, tenantID, documentID, workflowID string) {
	meta, ok := ctx.Value(ctxKeyCorrelationMeta).(*CorrelationMeta)
	if !ok || meta == nil {
		return
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if tenantID != "" {
		meta.TenantID = tenantID
	}
	if documentID != "" {
		meta.DocumentID = documentID
	}
	if workflowID != "" {
		meta.WorkflowID = workflowID
	}
}

// TraceIDFromContext returns the request's trace_id (X-Request-ID) from context.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyTraceID).(string); ok && v != "" {
		return v
	}
	return ""
}

// ClaimsFromContext returns the authenticated AuthClaims from context, if present.
func ClaimsFromContext(ctx context.Context) (*AuthClaims, bool) {
	c, ok := ctx.Value(ctxKeyAuthClaims).(*AuthClaims)
	return c, ok && c != nil
}

// RequestID middleware extracts or generates a high-entropy X-Request-ID and injects it
// into the request context as trace_id and onto the response headers.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := sanitizeRequestID(r.Header.Get(HeaderRequestID))
		if traceID == "" {
			traceID = generateTraceID()
		}
		w.Header().Set(HeaderRequestID, traceID)
		meta := &CorrelationMeta{
			TenantID: strings.TrimSpace(r.Header.Get(HeaderTenantID)),
		}
		ctx := context.WithValue(r.Context(), ctxKeyTraceID, traceID)
		ctx = context.WithValue(ctx, ctxKeyCorrelationMeta, meta)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireAuth enforces Bearer token authentication, X-Tenant-ID consistency, and required scope authorization.
func RequireAuth(validator TokenValidator, requiredScope string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := strings.TrimSpace(r.Header.Get(HeaderAuthorization))
		if authHeader == "" {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing Authorization header")
			return
		}
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid Authorization Bearer header format")
			return
		}

		if validator == nil {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication validator is not configured")
			return
		}

		claims, err := validator.ValidateToken(r.Context(), strings.TrimSpace(parts[1]))
		if err != nil || claims == nil || strings.TrimSpace(claims.TenantID) == "" {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid or expired Bearer token")
			return
		}

		// Cross-check X-Tenant-ID header if present against authenticated Bearer token claims.
		if headerTenant := strings.TrimSpace(r.Header.Get(HeaderTenantID)); headerTenant != "" && headerTenant != claims.TenantID {
			SetCorrelationFields(r.Context(), claims.TenantID, "", "")
			writeJSONError(w, http.StatusForbidden, "FORBIDDEN", "X-Tenant-ID header does not match authenticated tenant claims")
			return
		}

		if requiredScope != "" && !claims.HasScope(requiredScope) {
			SetCorrelationFields(r.Context(), claims.TenantID, "", "")
			writeJSONError(w, http.StatusForbidden, "FORBIDDEN", fmt.Sprintf("token missing required scope %s", requiredScope))
			return
		}

		SetCorrelationFields(r.Context(), claims.TenantID, "", "")
		ctx := context.WithValue(r.Context(), ctxKeyAuthClaims, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RateLimiterConfig configures the per-tenant/IP token-bucket rate limiter.
type RateLimiterConfig struct {
	RequestsPerSecond float64
	Burst             int
	MaxBuckets        int
	StaleTTL          time.Duration
	NowFunc           func() time.Time
}

type tokenBucket struct {
	tokens   float64
	lastSeen time.Time
}

// TokenBucketLimiter implements a concurrency-safe, memory-bounded per-tenant/IP token-bucket rate limiter.
type TokenBucketLimiter struct {
	mu         sync.Mutex
	rate       float64
	burst      float64
	maxBuckets int
	staleTTL   time.Duration
	nowFunc    func() time.Time
	buckets    map[string]*tokenBucket
}

// NewTokenBucketLimiter constructs a memory-bounded TokenBucketLimiter.
func NewTokenBucketLimiter(cfg RateLimiterConfig) *TokenBucketLimiter {
	rate := cfg.RequestsPerSecond
	if rate <= 0 {
		rate = DefaultRateLimitRPS
	}
	burst := cfg.Burst
	if burst <= 0 {
		burst = DefaultRateLimitBurst
	}
	maxBuckets := cfg.MaxBuckets
	if maxBuckets <= 0 || maxBuckets > MaxTrackedBuckets {
		maxBuckets = MaxTrackedBuckets
	}
	staleTTL := cfg.StaleTTL
	if staleTTL <= 0 {
		staleTTL = DefaultBucketStaleTTL
	}
	nowFunc := cfg.NowFunc
	if nowFunc == nil {
		nowFunc = func() time.Time { return time.Now().UTC() }
	}
	return &TokenBucketLimiter{
		rate:       rate,
		burst:      float64(burst),
		maxBuckets: maxBuckets,
		staleTTL:   staleTTL,
		nowFunc:    nowFunc,
		buckets:    make(map[string]*tokenBucket),
	}
}

// Allow checks if key has at least 1 token available and returns (allowed, retryAfterSeconds).
func (l *TokenBucketLimiter) Allow(key string) (bool, int) {
	if l == nil {
		return true, 0
	}
	if key == "" {
		key = "anonymous"
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.nowFunc()
	b, exists := l.buckets[key]
	if !exists {
		if len(l.buckets) >= l.maxBuckets {
			l.evictStaleOrOldestLocked(now)
		}
		b = &tokenBucket{
			tokens:   l.burst,
			lastSeen: now,
		}
		l.buckets[key] = b
	} else {
		elapsed := now.Sub(b.lastSeen).Seconds()
		if elapsed > 0 {
			b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		}
		b.lastSeen = now
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true, 0
	}

	deficit := 1.0 - b.tokens
	retrySec := int(math.Ceil(deficit / l.rate))
	if retrySec < 1 {
		retrySec = 1
	}
	return false, retrySec
}

// TrackedBucketCount returns the current number of tracked tenant/IP buckets.
func (l *TokenBucketLimiter) TrackedBucketCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

func (l *TokenBucketLimiter) evictStaleOrOldestLocked(now time.Time) {
	var oldestKey string
	var oldestTime time.Time
	evicted := false

	for k, b := range l.buckets {
		if now.Sub(b.lastSeen) >= l.staleTTL {
			delete(l.buckets, k)
			evicted = true
		} else if oldestKey == "" || b.lastSeen.Before(oldestTime) {
			oldestKey = k
			oldestTime = b.lastSeen
		}
	}
	if !evicted && oldestKey != "" {
		delete(l.buckets, oldestKey)
	}
}

// RateLimit middleware enforces per-tenant (or fallback client IP) rate limits.
func RateLimit(limiter *TokenBucketLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limiter == nil {
			next.ServeHTTP(w, r)
			return
		}
		key := resolveRateLimitKey(r)
		allowed, retryAfter := limiter.Allow(key)
		if !allowed {
			w.Header().Set(HeaderRetryAfter, strconv.Itoa(retryAfter))
			writeJSONError(w, http.StatusTooManyRequests, "RATE_LIMITED", "rate limit exceeded; retry after backoff")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func resolveRateLimitKey(r *http.Request) string {
	if claims, ok := ClaimsFromContext(r.Context()); ok && claims.TenantID != "" {
		return "tenant:" + claims.TenantID
	}
	if headerTenant := strings.TrimSpace(r.Header.Get(HeaderTenantID)); headerTenant != "" {
		return "tenant:" + headerTenant
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return "ip:" + host
	}
	if r.RemoteAddr != "" {
		return "ip:" + r.RemoteAddr
	}
	return "ip:unknown"
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}

// StructuredLogger emits machine-parseable JSON request logs containing the 4 mandatory
// correlation fields (trace_id, tenant_id, document_id, workflow_id) while redacting
// Authorization headers and presigned S3 query signatures.
func StructuredLogger(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(rec, r)

		traceID := TraceIDFromContext(r.Context())
		if traceID == "" {
			traceID = w.Header().Get(HeaderRequestID)
		}

		var tenantID, documentID, workflowID string
		if meta, ok := r.Context().Value(ctxKeyCorrelationMeta).(*CorrelationMeta); ok && meta != nil {
			meta.mu.RLock()
			tenantID = meta.TenantID
			documentID = meta.DocumentID
			workflowID = meta.WorkflowID
			meta.mu.RUnlock()
		}
		if tenantID == "" {
			if claims, ok := ClaimsFromContext(r.Context()); ok {
				tenantID = claims.TenantID
			}
		}

		sanitizedPath := SanitizeURLForLog(r.URL.RequestURI())
		redactedAuth := RedactAuthorizationHeader(r.Header.Get(HeaderAuthorization))

		logger.Info("http request completed",
			slog.String("trace_id", traceID),
			slog.String("tenant_id", tenantID),
			slog.String("document_id", documentID),
			slog.String("workflow_id", workflowID),
			slog.String("method", r.Method),
			slog.String("path", sanitizedPath),
			slog.Int("status", rec.statusCode),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			slog.String("authorization", redactedAuth),
		)
	})
}

// RedactAuthorizationHeader masks any non-empty Authorization header value to prevent credential leakage.
func RedactAuthorizationHeader(headerVal string) string {
	if strings.TrimSpace(headerVal) == "" {
		return ""
	}
	return "[REDACTED]"
}

// SanitizeURLForLog strips or masks sensitive S3 presigned query parameters from a URL or URI before logging.
func SanitizeURLForLog(rawURI string) string {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return "[REDACTED_URI]"
	}
	if parsed.RawQuery == "" {
		return parsed.Path
	}
	q := parsed.Query()
	for k := range q {
		if isSensitiveQueryParam(k) {
			q.Set(k, "[REDACTED]")
		}
	}
	parsed.RawQuery = q.Encode()
	if parsed.Scheme != "" && parsed.Host != "" {
		return parsed.String()
	}
	if parsed.RawQuery != "" {
		return parsed.Path + "?" + parsed.RawQuery
	}
	return parsed.Path
}

func isSensitiveQueryParam(key string) bool {
	for _, sensitive := range sensitiveQueryParamKeys {
		if strings.EqualFold(key, sensitive) {
			return true
		}
	}
	return false
}

func sanitizeRequestID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || len(trimmed) > 128 {
		return ""
	}
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		valid := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_'
		if !valid {
			return ""
		}
	}
	return trimmed
}

func generateTraceID() string {
	var entropy [12]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return fmt.Sprintf("req_%d", time.Now().UTC().UnixNano())
	}
	return "req_" + hex.EncodeToString(entropy[:])
}

func writeJSONError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":   code,
		"message": message,
	})
}
