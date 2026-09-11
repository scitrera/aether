// Package login implements the optional browser-OAuth login flow for the
// auth-proxy: provider-driven OIDC login redirects, callback verification,
// and session-cookie issuance. Once a session is established, the caller's
// subsequent requests carry a session cookie that the auth-proxy middleware
// converts into a session_token credential, validated by the
// auth.SessionAuthenticator.
//
// The login flow is OPTIONAL — auth-proxy still works as a stateless
// API-key/JWT validator if no providers are configured.
package login

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// SessionData is the persisted record for an authenticated browser session.
//
// Provider is the OIDC provider name that produced this session (e.g.
// "azure", "google"). Claims is the verified ID token claim set —
// authenticator-shaped data the IdentityResolver uses to map an email to a
// tenant context. UserID is the canonical user identifier (sub or email,
// depending on provider config).
type SessionData struct {
	UserID    string         `json:"user_id"`
	Email     string         `json:"email,omitempty"`
	Name      string         `json:"name,omitempty"`
	Provider  string         `json:"provider"`
	Claims    map[string]any `json:"claims"`
	IssuedAt  time.Time      `json:"issued_at"`
	ExpiresAt time.Time      `json:"expires_at"`
}

// IsExpired reports whether the session has passed its ExpiresAt.
func (s *SessionData) IsExpired() bool {
	return !s.ExpiresAt.IsZero() && time.Now().After(s.ExpiresAt)
}

// SessionStore persists and retrieves SessionData by opaque session id.
//
// Implementations MUST be safe for concurrent use. The opaque session id is
// the value placed in the session cookie; lookup misses are NOT an error
// (return nil, nil).
type SessionStore interface {
	Name() string
	// New creates and persists a new session, returning its opaque id.
	New(ctx context.Context, data *SessionData) (string, error)
	// Get fetches a session by id. Returns (nil, nil) if not found.
	Get(ctx context.Context, id string) (*SessionData, error)
	// Delete removes a session by id. A missing session is not an error.
	Delete(ctx context.Context, id string) error
}

// ErrSessionNotFound is returned by some helpers when a session is missing.
// Stores themselves return (nil, nil) for misses; this error is for callers
// that prefer an explicit signal.
var ErrSessionNotFound = errors.New("session not found")

// RedisOpaqueSessionStore stores sessions in Redis under
// a namespaced key derived from the cookie, with per-subject indexes.
// Only the browser receives the bearer credential; management IDs cannot log in.
type RedisOpaqueSessionStore struct {
	client *redis.Client
	prefix string
	idLen  int // bytes of randomness for the opaque id (default 32)
}

// NewRedisOpaqueSessionStore returns a Redis-backed session store. prefix is
// the Redis key prefix (e.g. "session:"); pass "" for the default
// "auth-session:".
func NewRedisOpaqueSessionStore(client *redis.Client, prefix string) *RedisOpaqueSessionStore {
	if prefix == "" {
		prefix = "auth-session:"
	}
	return &RedisOpaqueSessionStore{client: client, prefix: prefix, idLen: 32}
}

// Name implements SessionStore.
func (s *RedisOpaqueSessionStore) Name() string { return "redis_opaque" }

// newOpaqueID returns a hex-encoded random id of n bytes.
func newOpaqueID(n int) (string, error) {
	if n <= 0 {
		n = 32
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
