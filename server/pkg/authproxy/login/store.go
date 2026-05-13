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
	"encoding/json"
	"errors"
	"fmt"
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
// "<prefix>:<opaque-id>", with the opaque id placed in the cookie. This is
// the production default — server-side revocation is a single Redis DEL.
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

// New implements SessionStore. The opaque id is 64 hex chars (32 bytes of
// crypto/rand). TTL is derived from data.ExpiresAt; if unset, the session
// is persisted with no Redis TTL (caller is expected to set a sane default).
func (s *RedisOpaqueSessionStore) New(ctx context.Context, data *SessionData) (string, error) {
	id, err := newOpaqueID(s.idLen)
	if err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("marshal session: %w", err)
	}
	var ttl time.Duration
	if !data.ExpiresAt.IsZero() {
		ttl = time.Until(data.ExpiresAt)
		if ttl <= 0 {
			return "", errors.New("session ExpiresAt is in the past")
		}
	}
	if err := s.client.Set(ctx, s.prefix+id, payload, ttl).Err(); err != nil {
		return "", fmt.Errorf("redis set: %w", err)
	}
	return id, nil
}

// Get implements SessionStore.
func (s *RedisOpaqueSessionStore) Get(ctx context.Context, id string) (*SessionData, error) {
	if id == "" {
		return nil, nil
	}
	payload, err := s.client.Get(ctx, s.prefix+id).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("redis get: %w", err)
	}
	var data SessionData
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, fmt.Errorf("unmarshal session: %w", err)
	}
	if data.IsExpired() {
		// Best-effort cleanup; ignore delete errors.
		_ = s.client.Del(ctx, s.prefix+id).Err()
		return nil, nil
	}
	return &data, nil
}

// Delete implements SessionStore.
func (s *RedisOpaqueSessionStore) Delete(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	if err := s.client.Del(ctx, s.prefix+id).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

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
