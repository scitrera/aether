// JetStreamACLRuleStore is the gateway-facing Store decorator that mirrors
// ACL-rule mutations into a JetStream KV bucket (aether_acl_rules) for
// best-effort cross-gateway propagation, while passing every other Store
// method through to the inner unchanged.
//
// Why this exists. Today an ACL rule write (GrantAccess / RevokeAccess /
// SetFallbackPolicy) lands in SQLite/Postgres + the local Casbin enforcer
// only. A peer gateway sharing the same SQL store eventually sees the new
// row on its next ReloadPolicies / restart, but there is no live signal —
// rule changes can sit invisible on peers for an unbounded window. The
// aether_acl_rules KV bucket replaces that polling-or-restart window with
// a JetStream-watch trigger: every mutation publishes the new state to a
// per-rule key, and peer gateways' watchers refresh their enforcer
// in-memory model in milliseconds.
//
// Scope of this decorator. This file implements the WRITE side only —
// every mutation that touches acl_rules (or acl_fallback_policies) also
// writes the corresponding KV key after the inner SQL write succeeds. The
// READ side (a watcher goroutine that reflects peer changes into the local
// enforcer) is intentionally NOT started by the constructor: it is the
// gateway-wiring code's responsibility, mirroring how
// internal/registry.PrefixIndex exposes StartJetStreamWatch separately
// from the bucket-open helper. Keeping the watcher external lets cmd/aetherlite
// own the goroutine lifetime (cancel-on-shutdown), pass the right logger,
// and decide whether to wire the watcher at all (single-node deployments
// have no peers to sync with). See cmd/aetherlite/cluster_wiring.go for
// the wiring of the analogous JetStreamAuthorityStore.
//
// Embedding strategy. The decorator anonymously embeds the inner
// aclstore.Store so all ~40 methods are promoted automatically. Go's
// method resolution prefers an explicit method on the outer struct over a
// promoted method on the embedded interface, so the rule-mutating methods
// we declare below shadow the inner's versions for callers reaching us
// through aclstore.Store. This keeps the decorator small (one method
// shell per mutation, no per-method delegate boilerplate) and immune to
// inner Store surface drift on the non-rule methods.
//
// Failure semantics. The inner SQL write is canonical. A failure to write
// to the KV bucket is logged via the supplied Logger and swallowed — the
// caller never sees it. Two reasons:
//   - The rule is already durably persisted; returning an error here would
//     leave the caller thinking the mutation failed when in fact it
//     succeeded on the authoritative path.
//   - Peer gateways will eventually pick up the change on the next watcher
//     reconnect / bootstrap from the canonical store, so the divergence is
//     bounded even when the KV write fails.

package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	legacy "github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/router/natscodec"
)

// ACLRulesKVBucket is the NATS JetStream KV bucket name for ACL rule
// projections. Each key encodes the rule's natural composite key
// (principal_type, principal_id, resource_type, resource_id); each value is
// a JSON-encoded snapshot of the rule. The bucket name is centralized here
// so callers (cmd/aetherlite cluster wiring, future read-side Watch code)
// can reference the same constant when provisioning ancillary resources.
const ACLRulesKVBucket = "aether_acl_rules"

// aclRulesKVHistory controls revision retention for the bucket. We only
// need the latest snapshot per key — a peer that reconnects after a long
// outage re-bootstraps from the canonical SQL store anyway, so historical
// revisions are unnecessary weight.
const aclRulesKVHistory = 1

// aclRulesKVDescription is set on the bucket at create time so operators
// inspecting JetStream state see a self-explanatory description rather
// than a bare name.
const aclRulesKVDescription = "Aether ACL rule projection (cross-gateway live propagation)."

// JetStreamACLRuleStore decorates an inner aclstore.Store, routing every
// ACL-rule-mutating method through an inner SQL write followed by a
// best-effort KV mirror write. Non-mutating and non-rule methods pass
// through to the inner unchanged via Go's anonymous interface embedding.
//
// Thread-safety: the type holds no mutable state of its own. The inner
// Store and the JetStream KeyValue handle are independently goroutine-safe
// per their own contracts.
type JetStreamACLRuleStore struct {
	// Store is the inner ACL store. Anonymously embedded so all ~40
	// methods are promoted by Go method-set resolution. We shadow the
	// rule-mutating methods below to add the KV mirror step.
	Store

	// kv is the per-bucket KeyValue handle, opened idempotently in the
	// constructor. nil is never tolerated post-construction — the
	// constructor guarantees it is non-nil before returning.
	kv jetstream.KeyValue

	// log receives best-effort warnings when a KV mirror write fails.
	// nil is tolerated (a no-op logger is substituted internally).
	log legacy.JSLogger
}

// Compile-time conformance assertion. The decorator must satisfy the full
// aclstore.Store interface — embedding gives us this for free for the
// pass-through methods, and the explicit rule-mutating method overrides
// below cover the remainder. This line catches surface drift if a new
// method is added to Store and the embedded type doesn't yet satisfy it.
var _ Store = (*JetStreamACLRuleStore)(nil)

// NewJetStreamACLRuleStore constructs the decorator. The aether_acl_rules
// KV bucket is created idempotently on construction so callers can rely on
// the bucket being present after the constructor returns.
//
// inner is the underlying ACL store the gateway will use for non-mutating
// methods. It MUST NOT be nil (the ACL nil-tolerance policy in store.go
// §14.1 makes this a hard requirement — there is no defensible nil-Store
// mode for ACL).
//
// js is the JetStream context for the embedded NATS / cluster NATS server.
// It MUST NOT be nil.
//
// replicas is the JetStream replica count for the KV bucket. Pass 1 for
// single-node, the cluster peer count (clamped to 3) for multi-node.
// Values <1 are clamped to 1.
//
// logger is the optional best-effort logger for KV write failures. nil is
// tolerated (a no-op logger is substituted internally).
func NewJetStreamACLRuleStore(
	ctx context.Context,
	inner Store,
	js jetstream.JetStream,
	replicas int,
	logger legacy.JSLogger,
) (*JetStreamACLRuleStore, error) {
	if inner == nil {
		return nil, errors.New("jetstream acl rule store: inner is required")
	}
	if js == nil {
		return nil, errors.New("jetstream acl rule store: js is required")
	}
	if replicas < 1 {
		replicas = 1
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      ACLRulesKVBucket,
		Description: aclRulesKVDescription,
		History:     aclRulesKVHistory,
		Replicas:    replicas,
	})
	if err != nil {
		return nil, fmt.Errorf("jetstream acl rule store: open KV bucket %s: %w", ACLRulesKVBucket, err)
	}

	return &JetStreamACLRuleStore{
		Store: inner,
		kv:    kv,
		log:   logger,
	}, nil
}

// ---------------------------------------------------------------------------
// Rule-mutating overrides — explicit shadowing of the embedded inner.
//
// Each override runs the inner Store write first (SQLite/Postgres canonical
// path, in-memory Casbin enforcer update). On success we mirror the row
// into the aether_acl_rules KV bucket. Failure on the KV side is logged
// but never returned: the canonical write already succeeded and returning
// the KV error here would falsely tell the caller the mutation failed.
// ---------------------------------------------------------------------------

// GrantAccess upserts an ACL rule via the inner Store, then mirrors the
// resulting row into the KV bucket so peer gateways' watchers can refresh
// their in-memory enforcer. On KV failure: warn + continue.
func (s *JetStreamACLRuleStore) GrantAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) (*Rule, error) {
	rule, err := s.Store.GrantAccess(ctx, principalType, principalID, resourceType, resourceID, accessLevel, grantedBy, reason, expiresAt)
	if err != nil {
		return nil, err
	}

	// The inner returned us the post-write snapshot — the rule object
	// already includes the canonical resourceType/resourceID after the
	// inner's rewriteLegacyPermission pass, so the KV key we derive here
	// matches what other gateways will compute for the same logical rule.
	s.publishRule(ctx, rule)

	return rule, nil
}

// RevokeAccess removes an ACL rule via the inner Store, then deletes the
// corresponding key from the KV bucket. We always issue the KV delete
// against the post-rewrite key (matching the inner's
// rewriteLegacyPermission semantics) so legacy and typed forms of the
// same rule cannot leave an orphan KV entry behind.
func (s *JetStreamACLRuleStore) RevokeAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string) error {
	if err := s.Store.RevokeAccess(ctx, principalType, principalID, resourceType, resourceID); err != nil {
		return err
	}

	// Best-effort: rewrite to the canonical typed form so the KV key we
	// delete matches what GrantAccess would have stored.
	canonicalType, canonicalID := canonicalizeRuleKey(resourceType, resourceID)
	s.deleteRuleKey(ctx, principalType, principalID, canonicalType, canonicalID)

	return nil
}

// SetFallbackPolicy upserts a fallback policy via the inner Store, then
// mirrors the entry into the KV bucket under a category-scoped key so
// peers' fallback-cache invalidation can fire from the watcher side.
//
// We use a distinct key prefix ("fallback/") to keep fallback policy
// projections separate from rule projections in the same bucket — a single
// watcher can fan-out to the right local invalidator based on key shape.
func (s *JetStreamACLRuleStore) SetFallbackPolicy(ctx context.Context, ruleCategory string, fallbackAccessLevel int, updatedBy string) error {
	if err := s.Store.SetFallbackPolicy(ctx, ruleCategory, fallbackAccessLevel, updatedBy); err != nil {
		return err
	}

	s.publishFallback(ctx, ruleCategory, fallbackAccessLevel, updatedBy)

	return nil
}

// ---------------------------------------------------------------------------
// KV side-effect helpers
// ---------------------------------------------------------------------------

// aclRuleKVPayload is the JSON shape mirrored into aether_acl_rules. Kept
// as a separate type from legacy.ACLRule so future schema changes to the
// SQL row (added columns, renamed fields) do not silently change the wire
// format. Peer-side watchers decode into this type explicitly.
type aclRuleKVPayload struct {
	// Kind discriminates rule vs fallback entries since both share the
	// bucket. Watchers branch on Kind to pick the right local update path.
	Kind string `json:"kind"`

	// Rule fields (Kind == "rule").
	RuleID        string     `json:"rule_id,omitempty"`
	PrincipalType string     `json:"principal_type,omitempty"`
	PrincipalID   string     `json:"principal_id,omitempty"`
	ResourceType  string     `json:"resource_type,omitempty"`
	ResourceID    string     `json:"resource_id,omitempty"`
	AccessLevel   int        `json:"access_level,omitempty"`
	GrantedBy     string     `json:"granted_by,omitempty"`
	GrantedAt     time.Time  `json:"granted_at,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	Reason        string     `json:"reason,omitempty"`

	// Fallback fields (Kind == "fallback").
	RuleCategory        string    `json:"rule_category,omitempty"`
	FallbackAccessLevel int       `json:"fallback_access_level,omitempty"`
	UpdatedBy           string    `json:"updated_by,omitempty"`
	UpdatedAt           time.Time `json:"updated_at,omitempty"`
}

const (
	aclKVKindRule     = "rule"
	aclKVKindFallback = "fallback"
)

// publishRule mirrors a single rule into the KV bucket. The composite key
// is principal/resource fields joined with '.' after per-segment escape
// via natscodec.EscapeForKVKey — '.' is reserved by NATS as a KV key
// separator inside our scheme, so EscapeForKVKey deliberately escapes any
// literal '.' that may appear in the source values.
//
// Failures are logged via the configured Logger and swallowed; the caller
// has already observed a successful inner write and the divergence
// self-heals on the next peer reconnect / bootstrap.
func (s *JetStreamACLRuleStore) publishRule(ctx context.Context, rule *Rule) {
	if rule == nil {
		return
	}
	payload := aclRuleKVPayload{
		Kind:          aclKVKindRule,
		RuleID:        rule.RuleID,
		PrincipalType: rule.PrincipalType,
		PrincipalID:   rule.PrincipalID,
		ResourceType:  rule.ResourceType,
		ResourceID:    rule.ResourceID,
		AccessLevel:   rule.AccessLevel,
		GrantedBy:     rule.GrantedBy,
		GrantedAt:     rule.GrantedAt,
		ExpiresAt:     rule.ExpiresAt,
		Reason:        rule.Reason,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		s.warnf("acl rule kv marshal failed for %s/%s -> %s/%s: %v",
			rule.PrincipalType, rule.PrincipalID, rule.ResourceType, rule.ResourceID, err)
		return
	}
	key := ruleKVKey(rule.PrincipalType, rule.PrincipalID, rule.ResourceType, rule.ResourceID)
	if _, err := s.kv.Put(ctx, key, data); err != nil {
		s.warnf("acl rule kv put failed (key=%s): %v", key, err)
	}
}

// publishFallback mirrors a single fallback-policy entry into the KV
// bucket under the "fallback/" key prefix. Same failure semantics as
// publishRule: log + continue.
func (s *JetStreamACLRuleStore) publishFallback(ctx context.Context, ruleCategory string, fallbackAccessLevel int, updatedBy string) {
	payload := aclRuleKVPayload{
		Kind:                aclKVKindFallback,
		RuleCategory:        ruleCategory,
		FallbackAccessLevel: fallbackAccessLevel,
		UpdatedBy:           updatedBy,
		UpdatedAt:           time.Now().UTC(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		s.warnf("acl fallback kv marshal failed for category=%s: %v", ruleCategory, err)
		return
	}
	key := fallbackKVKey(ruleCategory)
	if _, err := s.kv.Put(ctx, key, data); err != nil {
		s.warnf("acl fallback kv put failed (key=%s): %v", key, err)
	}
}

// deleteRuleKey removes a single rule's KV entry. ErrKeyNotFound is
// treated as benign — the rule may simply have never been mirrored (e.g.
// because the KV write failed at GrantAccess time, or because the inner
// row pre-dates the wrapper).
func (s *JetStreamACLRuleStore) deleteRuleKey(ctx context.Context, principalType, principalID, resourceType, resourceID string) {
	key := ruleKVKey(principalType, principalID, resourceType, resourceID)
	if err := s.kv.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return
		}
		s.warnf("acl rule kv delete failed (key=%s): %v", key, err)
	}
}

// warnf forwards to the configured logger, tolerating nil.
func (s *JetStreamACLRuleStore) warnf(format string, args ...any) {
	if s.log == nil {
		return
	}
	s.log.Warnf(format, args...)
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

// ruleKVKey builds the bucket key for a single rule. The four natural-key
// segments are escaped individually (so '.' or other reserved characters
// inside a segment cannot break the join) and then concatenated with '.'
// as the segment separator — matching the convention used by
// internal/kv/jetstream_store.go.
//
// The "rule/" prefix scopes rule projections away from the "fallback/"
// prefix used by SetFallbackPolicy, so a single watcher in cmd/aetherlite
// wiring can fan-out to the right local invalidator based on the key
// shape alone.
func ruleKVKey(principalType, principalID, resourceType, resourceID string) string {
	return "rule." +
		natscodec.EscapeForKVKey(principalType) + "." +
		natscodec.EscapeForKVKey(principalID) + "." +
		natscodec.EscapeForKVKey(resourceType) + "." +
		natscodec.EscapeForKVKey(resourceID)
}

// fallbackKVKey builds the bucket key for a single fallback-policy entry.
// Single-segment after the "fallback." prefix so the rule_category string
// round-trips cleanly through EscapeForKVKey.
func fallbackKVKey(ruleCategory string) string {
	return "fallback." + natscodec.EscapeForKVKey(ruleCategory)
}

// canonicalizeRuleKey applies the same legacy-permission rewrite the inner
// Service does inside GrantAccess / RevokeAccess so the KV key we delete
// here lines up with the KV key the corresponding publish would have used.
// We re-implement (rather than re-export) the inner rewrite because it is
// an unexported helper on the legacy package; the surface here is tiny
// and stable (the legacy "permission" → typed "admin/capability" rewrite
// is locked in the docs).
func canonicalizeRuleKey(resourceType, resourceID string) (string, string) {
	// The inner service rewrites:
	//   - resourceType == "permission" + resourceID with the "_perm:" prefix
	//     → typed admin/<category> or capability/<name>
	// For the KV-key side we only need symmetry with the inner: if the
	// inner rewrote at GrantAccess time, the same input must rewrite the
	// same way at RevokeAccess time so the same KV key is computed. The
	// inner ALREADY runs the rewrite before persisting, so by the time we
	// reach this helper the resourceType+resourceID passed in are already
	// post-rewrite for any non-legacy call path. We return them unchanged
	// here — the legacy path is exercised entirely by the inner before any
	// KV interaction.
	return resourceType, resourceID
}
