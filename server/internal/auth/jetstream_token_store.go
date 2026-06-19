// JetStreamAPITokenStore is the gateway-facing APITokenStore decorator that
// mirrors API-token mutations into a JetStream KV bucket (aether_api_tokens)
// for best-effort cross-gateway propagation, while passing every read method
// through to the inner store unchanged.
//
// Why this exists. In single-node lite the SQLite api_tokens table is the
// whole story. In cluster (horizontal-scale) mode, several gateways each run
// their own validation hot-path; a token created / revoked / deleted on one
// peer must become visible on the others quickly. This decorator publishes
// every mutation to a per-token KV key, exactly parallel to how
// internal/storage/acl.JetStreamACLRuleStore mirrors ACL-rule mutations.
//
// Scope of this decorator. This file implements the WRITE-mirror side only:
//   - CreateToken: inner SQL insert (canonical) -> best-effort KV Put of a
//     revocation/expiry SNAPSHOT (never the plaintext token, never the hash).
//   - RevokeToken: inner SQL revoke (canonical) -> best-effort KV Put of the
//     post-revoke snapshot (revoked=true).
//   - DeleteToken: inner SQL delete (canonical) -> best-effort KV Delete.
//   - ValidateToken / GetToken / ListTokens: pass through to the inner store
//     UNCHANGED. The inner store remains the authority for validation, so
//     revocation + expiry semantics are byte-for-byte identical with or
//     without this decorator. The KV projection is a propagation signal for
//     peer watchers, not a second source of truth.
//
// The READ side (a watcher goroutine that reflects peer token changes into a
// local cache) is intentionally NOT started by the constructor — it is the
// gateway-wiring code's responsibility, mirroring JetStreamACLRuleStore.
//
// Failure semantics. The inner SQL write is canonical. A failure to write the
// KV bucket is logged via the supplied logger and swallowed — the caller never
// sees it. The token is already durably persisted on the authoritative path,
// and peers re-bootstrap from the shared SQLite store / next watch reconnect,
// so the divergence is bounded even when the KV write fails. This matches the
// ACL rule store's contract exactly.
//
// SECURITY: the KV payload carries ONLY non-secret token metadata needed for
// propagation (id, principal_type, created_by, revoked, expires_at, ...). It
// never carries the plaintext token or the token hash — peers that need to
// validate a presented token still hash-and-look-up against their own
// canonical store.

package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/scitrera/aether/internal/router/natscodec"
)

// APITokensKVBucket is the NATS JetStream KV bucket name for API-token
// projections. Each key encodes the token's id; each value is a JSON-encoded
// non-secret snapshot of the token row. Centralized here so callers (cluster
// wiring, future read-side watch code) reference the same constant.
const APITokensKVBucket = "aether_api_tokens"

// apiTokensKVHistory controls revision retention for the bucket. We only need
// the latest snapshot per key — a peer that reconnects after a long outage
// re-bootstraps from the canonical SQL store anyway.
const apiTokensKVHistory = 1

// apiTokensKVDescription is set on the bucket at create time so operators
// inspecting JetStream state see a self-explanatory description.
const apiTokensKVDescription = "Aether API token projection (cross-gateway live propagation; non-secret metadata only)."

// JSLogger is the minimal logging surface the decorator uses for best-effort
// KV-write warnings. internal/logging.Logger satisfies it; nil is tolerated.
type JSLogger interface {
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// JetStreamAPITokenStore decorates an inner APITokenStore, routing every
// token-mutating method through an inner SQL write followed by a best-effort
// KV mirror write. Read methods pass through to the inner unchanged.
//
// Thread-safety: the type holds no mutable state of its own. The inner store
// and the JetStream KeyValue handle are independently goroutine-safe.
type JetStreamAPITokenStore struct {
	// inner is the canonical store (SQLite/Postgres). All reads and the
	// authoritative write hit it first. Declared explicitly (not embedded)
	// because APITokenStore is a small, fully-overridden interface — there
	// are no pass-through methods worth promoting via embedding.
	inner APITokenStore

	// kv is the per-bucket KeyValue handle, opened idempotently in the
	// constructor. Guaranteed non-nil post-construction.
	kv jetstream.KeyValue

	// log receives best-effort warnings when a KV mirror write fails. nil is
	// tolerated (warnings are dropped).
	log JSLogger
}

// Compile-time conformance assertion: the decorator must satisfy the full
// APITokenStore interface.
var _ APITokenStore = (*JetStreamAPITokenStore)(nil)

// NewJetStreamAPITokenStore constructs the decorator. The aether_api_tokens
// KV bucket is created idempotently on construction.
//
// inner is the underlying token store. It MUST NOT be nil.
// js is the JetStream context. It MUST NOT be nil.
// replicas is the JetStream replica count (clamped to >= 1).
// logger is optional (nil tolerated).
func NewJetStreamAPITokenStore(
	ctx context.Context,
	inner APITokenStore,
	js jetstream.JetStream,
	replicas int,
	logger JSLogger,
) (*JetStreamAPITokenStore, error) {
	if inner == nil {
		return nil, errors.New("jetstream api token store: inner is required")
	}
	if js == nil {
		return nil, errors.New("jetstream api token store: js is required")
	}
	if replicas < 1 {
		replicas = 1
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      APITokensKVBucket,
		Description: apiTokensKVDescription,
		History:     apiTokensKVHistory,
		Replicas:    replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream api token store: open KV bucket %s: %w", APITokensKVBucket, err)
	}

	return &JetStreamAPITokenStore{
		inner: inner,
		kv:    kv,
		log:   logger,
	}, nil
}

// ---------------------------------------------------------------------------
// Mutating methods — inner SQL write first, then best-effort KV mirror.
// ---------------------------------------------------------------------------

// CreateToken inserts via the inner store (canonical), then mirrors the
// non-secret token snapshot into the KV bucket. On KV failure: warn + continue.
// The returned plaintext token comes from the inner result and is NEVER
// mirrored to KV.
func (s *JetStreamAPITokenStore) CreateToken(ctx context.Context, name, principalType string, workspacePatterns, scopes []string, createdBy string, expiresAt *time.Time) (*APITokenCreateResult, error) {
	res, err := s.inner.CreateToken(ctx, name, principalType, workspacePatterns, scopes, createdBy, expiresAt)
	if err != nil {
		return nil, err
	}
	if res != nil {
		s.publishToken(ctx, res.APIToken)
	}
	return res, nil
}

// RevokeToken revokes via the inner store (canonical), then mirrors the
// post-revoke snapshot so peer watchers can invalidate any cached view.
// We re-read the token via the inner GetToken to mirror an accurate snapshot;
// if that read fails we still publish a minimal revoked marker by id.
func (s *JetStreamAPITokenStore) RevokeToken(ctx context.Context, tokenID string) error {
	if err := s.inner.RevokeToken(ctx, tokenID); err != nil {
		return err
	}
	if tok, gerr := s.inner.GetToken(ctx, tokenID); gerr == nil && tok != nil {
		s.publishToken(ctx, tok)
	} else {
		s.publishRevokedMarker(ctx, tokenID)
	}
	return nil
}

// DeleteToken hard-deletes via the inner store (canonical), then deletes the
// corresponding KV key.
func (s *JetStreamAPITokenStore) DeleteToken(ctx context.Context, tokenID string) error {
	if err := s.inner.DeleteToken(ctx, tokenID); err != nil {
		return err
	}
	s.deleteTokenKey(ctx, tokenID)
	return nil
}

// ---------------------------------------------------------------------------
// Read methods — pass through to inner UNCHANGED.
//
// Validation (and thus revocation + expiry enforcement) stays on the canonical
// store, so the decorator cannot weaken token security. The KV projection is a
// propagation signal only.
// ---------------------------------------------------------------------------

// ValidateToken delegates to the inner canonical store. Revocation and expiry
// are enforced there exactly as in the undecorated case.
func (s *JetStreamAPITokenStore) ValidateToken(ctx context.Context, tokenStr string) (*APIToken, error) {
	return s.inner.ValidateToken(ctx, tokenStr)
}

// GetToken delegates to the inner canonical store.
func (s *JetStreamAPITokenStore) GetToken(ctx context.Context, tokenID string) (*APIToken, error) {
	return s.inner.GetToken(ctx, tokenID)
}

// ListTokens delegates to the inner canonical store.
func (s *JetStreamAPITokenStore) ListTokens(ctx context.Context, limit, offset int, includeRevoked bool) ([]*APIToken, error) {
	return s.inner.ListTokens(ctx, limit, offset, includeRevoked)
}

// ---------------------------------------------------------------------------
// KV side-effect helpers
// ---------------------------------------------------------------------------

// apiTokenKVPayload is the JSON shape mirrored into aether_api_tokens. It is a
// non-secret subset of APIToken — deliberately excluding TokenHash and the
// plaintext token. Peer-side watchers decode into this type explicitly.
type apiTokenKVPayload struct {
	ID                string     `json:"id"`
	Name              string     `json:"name,omitempty"`
	PrincipalType     string     `json:"principal_type,omitempty"`
	WorkspacePatterns []string   `json:"workspace_patterns,omitempty"`
	Scopes            []string   `json:"scopes,omitempty"`
	CreatedBy         string     `json:"created_by,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	Revoked           bool       `json:"revoked"`
	RevokedAt         *time.Time `json:"revoked_at,omitempty"`
}

// publishToken mirrors a single token's non-secret snapshot into the KV
// bucket. Failures are logged and swallowed.
func (s *JetStreamAPITokenStore) publishToken(ctx context.Context, tok *APIToken) {
	if tok == nil || tok.ID == "" {
		return
	}
	payload := apiTokenKVPayload{
		ID:                tok.ID,
		Name:              tok.Name,
		PrincipalType:     tok.PrincipalType,
		WorkspacePatterns: tok.WorkspacePatterns,
		Scopes:            tok.Scopes,
		CreatedBy:         tok.CreatedBy,
		ExpiresAt:         tok.ExpiresAt,
		Revoked:           tok.Revoked,
		RevokedAt:         tok.RevokedAt,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		s.warnf("api token kv marshal failed for id=%s: %v", tok.ID, err)
		return
	}
	key := tokenKVKey(tok.ID)
	if _, err := s.kv.Put(ctx, key, data); err != nil {
		s.warnf("api token kv put failed (key=%s): %v", key, err)
	}
}

// publishRevokedMarker mirrors a minimal revoked snapshot when the post-revoke
// inner re-read failed. Carries just the id + revoked=true so peers can still
// invalidate by id.
func (s *JetStreamAPITokenStore) publishRevokedMarker(ctx context.Context, tokenID string) {
	now := time.Now().UTC()
	payload := apiTokenKVPayload{ID: tokenID, Revoked: true, RevokedAt: &now}
	data, err := json.Marshal(payload)
	if err != nil {
		s.warnf("api token kv marshal (revoked marker) failed for id=%s: %v", tokenID, err)
		return
	}
	key := tokenKVKey(tokenID)
	if _, err := s.kv.Put(ctx, key, data); err != nil {
		s.warnf("api token kv put (revoked marker) failed (key=%s): %v", key, err)
	}
}

// deleteTokenKey removes a single token's KV entry. ErrKeyNotFound is benign.
func (s *JetStreamAPITokenStore) deleteTokenKey(ctx context.Context, tokenID string) {
	key := tokenKVKey(tokenID)
	if err := s.kv.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return
		}
		s.warnf("api token kv delete failed (key=%s): %v", key, err)
	}
}

// warnf forwards to the configured logger, tolerating nil.
func (s *JetStreamAPITokenStore) warnf(format string, args ...any) {
	if s.log == nil {
		return
	}
	s.log.Warnf(format, args...)
}

// tokenKVKey builds the bucket key for a single token. The token id is escaped
// so any reserved character cannot break the NATS subject token. The "token."
// prefix scopes token projections within the bucket.
func tokenKVKey(tokenID string) string {
	return "token." + natscodec.EscapeForKVKey(tokenID)
}
