package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// ErrEmptyJWTSecret indicates that an empty HMAC signing secret was supplied to NewHMACJWTValidator.
var ErrEmptyJWTSecret = errors.New("HMAC JWT secret must not be empty")

// HMACJWTValidator verifies HS256-signed Bearer JWTs in production without external dependencies.
type HMACJWTValidator struct {
	secret  []byte
	nowFunc func() time.Time
}

// NewHMACJWTValidator constructs an HS256 JWT TokenValidator using the provided HMAC secret.
func NewHMACJWTValidator(secret []byte, nowFunc func() time.Time) (*HMACJWTValidator, error) {
	if len(secret) == 0 {
		return nil, ErrEmptyJWTSecret
	}
	copied := make([]byte, len(secret))
	copy(copied, secret)
	if nowFunc == nil {
		nowFunc = func() time.Time { return time.Now().UTC() }
	}
	return &HMACJWTValidator{
		secret:  copied,
		nowFunc: nowFunc,
	}, nil
}

// ValidateToken verifies the HS256 JWT signature, expiration, and tenant claims.
func (v *HMACJWTValidator) ValidateToken(ctx context.Context, rawToken string) (*AuthClaims, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(rawToken)
	if trimmed == "" {
		return nil, ErrMissingBearerToken
	}
	if v == nil || len(v.secret) == 0 {
		return nil, ErrInvalidBearerToken
	}

	parts := strings.Split(trimmed, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidBearerToken
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrInvalidBearerToken
	}
	var hdr struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerBytes, &hdr); err != nil || hdr.Alg != "HS256" {
		return nil, ErrInvalidBearerToken
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, ErrInvalidBearerToken
	}
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expectedSig := mac.Sum(nil)
	if !hmac.Equal(sigBytes, expectedSig) {
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
		Exp      int64    `json:"exp"`
		Nbf      int64    `json:"nbf"`
	}
	if err := json.Unmarshal(payloadBytes, &raw); err != nil {
		return nil, ErrInvalidBearerToken
	}

	nowUnix := v.nowFunc().UTC().Unix()
	if raw.Exp > 0 && nowUnix >= raw.Exp {
		return nil, ErrInvalidBearerToken
	}
	if raw.Nbf > 0 && nowUnix < raw.Nbf {
		return nil, ErrInvalidBearerToken
	}
	if strings.TrimSpace(raw.TenantID) == "" {
		return nil, ErrInvalidBearerToken
	}

	scopes := make([]string, 0, len(raw.Scopes))
	scopes = append(scopes, raw.Scopes...)
	for _, s := range strings.Fields(raw.Scope) {
		scopes = append(scopes, s)
	}

	return &AuthClaims{
		TenantID: strings.TrimSpace(raw.TenantID),
		Subject:  raw.Sub,
		Scopes:   scopes,
	}, nil
}
