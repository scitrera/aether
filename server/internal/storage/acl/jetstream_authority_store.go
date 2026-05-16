// JetStreamAuthorityStore is the gateway-facing Store decorator that routes
// the authority-request lifecycle methods through a JetStream-backed wrapper
// while passing every other Store method through to the inner unchanged.
//
// Why this exists: the gateway's ACL field is typed *aclstore.Store* (the
// wide ~40-method interface), but the JetStream wrapper in
// internal/acl/authority_request_lifecycle_jetstream.go only satisfies the
// narrow acl.Lifecycle surface (6 methods). Without a Store decorator the
// wrapper is never on the hot path — Submit/Approve/Deny/Cancel/Sweep calls
// from the gateway bypass it entirely. This decorator wires it back in.
//
// Embedding strategy: the decorator anonymously embeds aclstore.Store so all
// ~40 methods are promoted automatically. Go's method resolution prefers an
// explicit method on the outer struct over a promoted method on the embedded
// interface, so the 6 lifecycle methods we declare on JetStreamAuthorityStore
// shadow the inner's versions for callers that reach us through aclstore.Store.
// This keeps the decorator small (six method shells, no per-method delegate
// boilerplate) and immune to inner Store surface drift.

package acl

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	legacy "github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/models"
)

// JetStreamAuthorityStore decorates an inner aclstore.Store, routing the
// authority-request lifecycle methods through a JetStream-backed wrapper
// (CAS-on-KV mirror + per-workspace event publish) while delegating every
// other Store method to the inner via interface-embedding promotion.
//
// Thread-safety: this type holds no mutable state of its own. The inner
// Store and the JetStreamAuthorityLifecycle are independently goroutine-safe
// per their own contracts.
type JetStreamAuthorityStore struct {
	// Store is the inner ACL store. Anonymously embedded so all ~40
	// methods are promoted by Go method-set resolution. We shadow the 6
	// authority-request lifecycle methods below to route through the
	// JetStream wrapper.
	Store

	// lifecycle is the JetStream-backed wrapper that owns the KV CAS +
	// event publish path. It is constructed from the same inner Store
	// passed to NewJetStreamAuthorityStore, so the wrapper's authoritative
	// SQL/Postgres write still lands on the same store the gateway reads
	// from for non-lifecycle methods.
	lifecycle legacy.Lifecycle
}

// Compile-time conformance assertion. The decorator must satisfy the full
// aclstore.Store interface — embedding gives us this for free for the ~35
// pass-through methods, and the 6 explicit lifecycle methods below cover
// the remainder. This line catches surface drift if a new method is added
// to Store and the embedded type doesn't yet satisfy it.
var _ Store = (*JetStreamAuthorityStore)(nil)

// Compile-time assertion that aclstore.Store satisfies legacy.Lifecycle.
// Required because NewJetStreamAuthorityLifecycle takes a legacy.Lifecycle —
// we pass the inner Store directly to it. If a future Store surface change
// removes one of the 6 lifecycle methods, this assertion fails at compile
// time instead of producing a nil-method-set runtime panic.
var _ legacy.Lifecycle = (Store)(nil)

// NewJetStreamAuthorityStore constructs the decorator. The JetStream
// wrapper's constructor is invoked internally, which idempotently provisions
// the authority-requests KV bucket, the authority-grants KV bucket, and the
// authreq event stream — same side-effects as before, but now we retain the
// returned wrapper so lifecycle calls can flow through it.
//
// inner is the underlying ACL store the gateway will use for non-lifecycle
// methods. It MUST NOT be nil (the ACL nil-tolerance policy in store.go §14.1
// makes this a hard requirement).
//
// js is the JetStream context for the embedded NATS / cluster NATS server.
//
// replicas is the JetStream replica count for the KV bucket + event stream.
// Pass 1 for single-node, the cluster peer count (clamped to 3) for
// multi-node. Values <1 are clamped to 1 by the wrapper.
//
// logger is the optional best-effort logger for KV/publish failures the
// wrapper logs internally. nil is tolerated.
func NewJetStreamAuthorityStore(
	inner Store,
	js jetstream.JetStream,
	replicas int,
	logger legacy.JSLogger,
) (*JetStreamAuthorityStore, error) {
	if inner == nil {
		return nil, errors.New("jetstream authority store: inner is required")
	}
	if js == nil {
		return nil, errors.New("jetstream authority store: js is required")
	}

	// The wrapper is constructed from a context-aware constructor so that
	// the idempotent stream + KV-bucket creates can be cancelled cleanly if
	// the caller's bootstrap is racing a shutdown. We use a background
	// context here because the decorator outlives any single request; the
	// caller's bootstrap goroutine is the natural scope, and on shutdown
	// the wrapper holds no goroutines of its own.
	wrapper, err := legacy.NewJetStreamAuthorityLifecycle(context.Background(), inner, js, replicas, logger)
	if err != nil {
		return nil, err
	}

	return &JetStreamAuthorityStore{
		Store:     inner,
		lifecycle: wrapper,
	}, nil
}

// ---------------------------------------------------------------------------
// Lifecycle methods — explicit overrides routed through the wrapper.
//
// Each method shadows the promoted inner-Store version (Go method resolution
// prefers explicit > embedded). The wrapper's internal contract is:
//   1. Call the inner Lifecycle (the same inner Store passed at construction)
//      to perform the authoritative SQL/Postgres write.
//   2. Mirror the row into the JetStream KV bucket via CAS.
//   3. Publish a lifecycle event on the per-workspace authreq subject.
//
// See authority_request_lifecycle_jetstream.go for the full transition
// ordering and failure semantics.
// ---------------------------------------------------------------------------

// GetAuthorityRequest delegates to the JetStream wrapper. The wrapper itself
// is a pure passthrough for reads in this task (the KV mirror is write-only
// from the wrapper's perspective today), so behavior is unchanged versus
// calling the inner directly. We route it through the wrapper for symmetry —
// future work can add a KV read-through cache without touching this site.
func (s *JetStreamAuthorityStore) GetAuthorityRequest(ctx context.Context, requestID string) (*AuthorityRequest, error) {
	return s.lifecycle.GetAuthorityRequest(ctx, requestID)
}

// SubmitAuthorityRequest routes through the JetStream wrapper. On success
// the wrapper has written the inner row, created the KV entry, and published
// a "created" event on the per-workspace subject.
func (s *JetStreamAuthorityStore) SubmitAuthorityRequest(ctx context.Context, req *AuthorityRequest) (*AuthorityRequest, error) {
	return s.lifecycle.SubmitAuthorityRequest(ctx, req)
}

// ApproveAuthorityRequest routes through the JetStream wrapper. On success
// the inner row is APPROVED, the KV entry is CAS-updated, and an "approved"
// event is published carrying the minted grant_id.
func (s *JetStreamAuthorityStore) ApproveAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, decision *ApproveDecision) (*AuthorityRequest, error) {
	return s.lifecycle.ApproveAuthorityRequest(ctx, requestID, approverIdentity, decision)
}

// DenyAuthorityRequest routes through the JetStream wrapper. On success the
// inner row is DENIED, the KV entry is CAS-updated, and a "denied" event is
// published.
func (s *JetStreamAuthorityStore) DenyAuthorityRequest(ctx context.Context, requestID string, approverIdentity models.Identity, reason string) (*AuthorityRequest, error) {
	return s.lifecycle.DenyAuthorityRequest(ctx, requestID, approverIdentity, reason)
}

// CancelOpenAuthorityRequest routes through the JetStream wrapper. On
// success the inner row is CANCELLED, the KV entry is CAS-updated, and a
// "cancelled" event is published.
func (s *JetStreamAuthorityStore) CancelOpenAuthorityRequest(ctx context.Context, requestID string, reason string) (*AuthorityRequest, error) {
	return s.lifecycle.CancelOpenAuthorityRequest(ctx, requestID, reason)
}

// SweepExpiredAuthorityRequests routes through the JetStream wrapper. Each
// expired row gets its own KV update + "expired" event emit; per-row failures
// are logged best-effort and do not abort the sweep.
func (s *JetStreamAuthorityStore) SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*AuthorityRequest, error) {
	return s.lifecycle.SweepExpiredAuthorityRequests(ctx, now, limit)
}
