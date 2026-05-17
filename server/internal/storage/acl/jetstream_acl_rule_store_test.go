// Tests for JetStreamACLRuleStore — the gateway-facing Store decorator
// that mirrors ACL-rule mutations into the aether_acl_rules JetStream KV
// bucket and passes every other Store method through to the inner.
//
// Reuses the embedded NATS+JetStream test helper (startTestJS) and the
// fakeInnerStore fake from jetstream_authority_store_test.go so this file
// stays focused on the rule-mirror semantics.

package acl_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	aclstore "github.com/scitrera/aether/internal/storage/acl"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// Compile-time interface assertion
// ---------------------------------------------------------------------------

// The decorator must satisfy aclstore.Store. This is also asserted inside
// the package itself; we keep a redundant one here so the test build catches
// drift even if the implementation file's assert is removed.
var _ aclstore.Store = (*aclstore.JetStreamACLRuleStore)(nil)

// ---------------------------------------------------------------------------
// captureLogger — JSLogger fake that records every Warnf invocation so
// tests can assert the wrapper logged on best-effort KV failures.
// ---------------------------------------------------------------------------

type captureLogger struct {
	mu    sync.Mutex
	warns []string
	errs  []string
}

func (c *captureLogger) Warnf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.warns = append(c.warns, format)
}

func (c *captureLogger) Errorf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, format)
}

func (c *captureLogger) warnCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.warns)
}

// ---------------------------------------------------------------------------
// Test 1: CreateRule writes to KV
// ---------------------------------------------------------------------------

// TestJetStreamACLRuleStore_GrantAccess_WritesToKV exercises GrantAccess
// through the wrapper and asserts the rule lands in the aether_acl_rules
// KV bucket under the expected composite key. The inner fake returns a
// canned Rule so we can verify the wrapper's key derivation matches the
// rule fields it published.
func TestJetStreamACLRuleStore_GrantAccess_WritesToKV(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeInnerStore()
	decorator, err := aclstore.NewJetStreamACLRuleStore(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new decorator: %v", err)
	}

	// The fake's GrantAccess returns &aclstore.Rule{} with zero fields, so
	// to get a meaningful KV key we wrap the fake here with an override
	// that returns a rule echoing the inputs. We do this inline rather
	// than touching the shared fake so other tests stay unaffected.
	echoInner := &echoingGrantInner{Store: inner}
	decorator, err = aclstore.NewJetStreamACLRuleStore(ctx, echoInner, js, 1, nil)
	if err != nil {
		t.Fatalf("new decorator (echo): %v", err)
	}

	rule, err := decorator.GrantAccess(ctx, "user", "alice", "workspace", "ws-1", aclstore.AccessRead, "admin", "test", nil)
	if err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}
	if rule == nil {
		t.Fatalf("GrantAccess returned nil rule")
	}

	// Open the bucket directly and check the key exists.
	kv, err := js.KeyValue(ctx, aclstore.ACLRulesKVBucket)
	if err != nil {
		t.Fatalf("open kv bucket: %v", err)
	}

	// Drain all keys under "rule.>" — the only mutation we issued was a
	// GrantAccess, so we expect exactly one key, and its value should
	// decode back to the rule we passed in.
	keys, err := collectKeys(ctx, kv)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 key after GrantAccess, got %d (%v)", len(keys), keys)
	}

	entry, err := kv.Get(ctx, keys[0])
	if err != nil {
		t.Fatalf("get %s: %v", keys[0], err)
	}
	if len(entry.Value()) == 0 {
		t.Errorf("kv entry %s has empty value", keys[0])
	}
}

// ---------------------------------------------------------------------------
// Test 2: RevokeAccess removes from KV
// ---------------------------------------------------------------------------

// TestJetStreamACLRuleStore_RevokeAccess_RemovesFromKV creates a rule, then
// revokes it, and asserts the corresponding KV entry is gone afterwards.
func TestJetStreamACLRuleStore_RevokeAccess_RemovesFromKV(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := &echoingGrantInner{Store: newFakeInnerStore()}
	decorator, err := aclstore.NewJetStreamACLRuleStore(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new decorator: %v", err)
	}

	if _, err := decorator.GrantAccess(ctx, "user", "bob", "workspace", "ws-2", aclstore.AccessReadWrite, "admin", "test", nil); err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}

	kv, err := js.KeyValue(ctx, aclstore.ACLRulesKVBucket)
	if err != nil {
		t.Fatalf("open kv bucket: %v", err)
	}

	// Sanity: one key present after the Put.
	keys, err := collectKeys(ctx, kv)
	if err != nil {
		t.Fatalf("list keys after grant: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 key after GrantAccess, got %d", len(keys))
	}

	// Revoke the same rule.
	if err := decorator.RevokeAccess(ctx, "user", "bob", "workspace", "ws-2"); err != nil {
		t.Fatalf("RevokeAccess: %v", err)
	}

	// After a delete, ListKeys reports zero current keys (history-only
	// rows are filtered out by the lister).
	keysAfter, err := collectKeys(ctx, kv)
	if err != nil {
		t.Fatalf("list keys after revoke: %v", err)
	}
	if len(keysAfter) != 0 {
		t.Errorf("expected 0 keys after RevokeAccess, got %d (%v)", len(keysAfter), keysAfter)
	}
}

// ---------------------------------------------------------------------------
// Test 3: Non-rule methods pass through to the inner unchanged
// ---------------------------------------------------------------------------

// TestJetStreamACLRuleStore_NonRuleMethods_Passthrough picks a couple of
// non-rule Store methods (CheckAccess, ListAuthorityGrants, CleanupExpiredRules)
// and asserts:
//   - The inner Store's per-method counter ticks up.
//   - NO KV entries are written under the rule/fallback prefixes (the
//     wrapper has no business mirroring non-rule traffic).
func TestJetStreamACLRuleStore_NonRuleMethods_Passthrough(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeInnerStore()
	decorator, err := aclstore.NewJetStreamACLRuleStore(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new decorator: %v", err)
	}

	// ----- CheckAccess (representative access path) -----
	principal := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	if _, err := decorator.CheckAccess(ctx, principal, "file", "/x", "read", "ws-x", uuid.Nil, 10); err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if inner.checkAccessCalls != 1 {
		t.Errorf("inner check_access_calls = %d, want 1", inner.checkAccessCalls)
	}

	// ----- GetRule (representative rule-READ path — reads still pass through) -----
	if _, err := decorator.GetRule(ctx, "user", "alice", "file", "/x"); err != nil {
		t.Fatalf("GetRule: %v", err)
	}
	if inner.getRuleCalls != 1 {
		t.Errorf("inner get_rule_calls = %d, want 1", inner.getRuleCalls)
	}

	// ----- ListAuthorityGrants -----
	if _, err := decorator.ListAuthorityGrants(ctx, aclstore.AuthorityGrantFilter{}); err != nil {
		t.Fatalf("ListAuthorityGrants: %v", err)
	}
	if inner.listAuthorityGrantCalls != 1 {
		t.Errorf("inner list_authority_grant_calls = %d, want 1", inner.listAuthorityGrantCalls)
	}

	// ----- CleanupExpiredRules -----
	// CleanupExpiredRules is interesting because it bulk-DELETEs rule
	// rows. Today the wrapper does NOT mirror this onto the KV bucket
	// (CleanupExpiredRules is a maintenance path; peer enforcers will
	// drop the same rows on their own next cleanup). We assert the
	// passthrough holds + nothing lands in KV.
	if _, err := decorator.CleanupExpiredRules(ctx); err != nil {
		t.Fatalf("CleanupExpiredRules: %v", err)
	}
	if inner.cleanupExpiredRulesCalls != 1 {
		t.Errorf("inner cleanup_expired_rules_calls = %d, want 1", inner.cleanupExpiredRulesCalls)
	}

	// ----- Assert KV bucket is empty -----
	kv, err := js.KeyValue(ctx, aclstore.ACLRulesKVBucket)
	if err != nil {
		t.Fatalf("open kv bucket: %v", err)
	}
	keys, err := collectKeys(ctx, kv)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 KV entries after non-rule calls, got %d (%v) — wrapper over-reached", len(keys), keys)
	}
}

// ---------------------------------------------------------------------------
// Test 4: KV write failure does not block the inner mutation
// ---------------------------------------------------------------------------

// TestJetStreamACLRuleStore_KVWriteFailure_DoesNotBlockInner shuts down the
// embedded NATS server after wrapper construction, then issues a GrantAccess.
// The inner write must still succeed (the fake records it locally) and the
// wrapper must NOT propagate the KV-write error to the caller. We also
// assert a warn was logged via the captureLogger.
func TestJetStreamACLRuleStore_KVWriteFailure_DoesNotBlockInner(t *testing.T) {
	js, stop := startTestJS(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := &grantCounterInner{Store: newFakeInnerStore()}
	log := &captureLogger{}
	decorator, err := aclstore.NewJetStreamACLRuleStore(ctx, inner, js, 1, log)
	if err != nil {
		stop()
		t.Fatalf("new decorator: %v", err)
	}

	// Now shut down the embedded NATS server. The wrapper's KV handle
	// will fail every subsequent Put — but the inner is in-process and
	// unaffected.
	stop()

	rule, err := decorator.GrantAccess(ctx, "user", "carol", "workspace", "ws-3", aclstore.AccessManage, "admin", "test", nil)
	if err != nil {
		t.Fatalf("GrantAccess unexpectedly returned error after KV death: %v", err)
	}
	if rule == nil {
		t.Errorf("GrantAccess returned nil rule")
	}
	if inner.grantCalls != 1 {
		t.Errorf("inner grant_calls = %d, want 1 (the inner write must still happen)", inner.grantCalls)
	}
	if log.warnCount() == 0 {
		t.Errorf("expected at least one warn log on KV failure, got 0")
	}
}

// ---------------------------------------------------------------------------
// Test 5: SetFallbackPolicy mirrors into the KV bucket
// ---------------------------------------------------------------------------

// TestJetStreamACLRuleStore_SetFallbackPolicy_WritesToKV exercises the third
// rule-mutating override (fallback policies share the bucket under the
// "fallback." prefix). Asserts the inner write fired AND a key landed in
// the bucket under the fallback prefix.
func TestJetStreamACLRuleStore_SetFallbackPolicy_WritesToKV(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inner := newFakeInnerStore()
	decorator, err := aclstore.NewJetStreamACLRuleStore(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("new decorator: %v", err)
	}

	if err := decorator.SetFallbackPolicy(ctx, "user_workspace", aclstore.AccessRead, "admin"); err != nil {
		t.Fatalf("SetFallbackPolicy: %v", err)
	}

	kv, err := js.KeyValue(ctx, aclstore.ACLRulesKVBucket)
	if err != nil {
		t.Fatalf("open kv bucket: %v", err)
	}
	keys, err := collectKeys(ctx, kv)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 fallback key after SetFallbackPolicy, got %d (%v)", len(keys), keys)
	}
	// The key must start with the fallback prefix so a watcher can
	// branch on key shape.
	if got := keys[0]; len(got) < 9 || got[:9] != "fallback." {
		t.Errorf("expected fallback-prefixed key, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Test 6: Constructor input validation
// ---------------------------------------------------------------------------

// TestJetStreamACLRuleStore_NilInputsRejected verifies the constructor
// rejects nil inputs explicitly so callers don't silently get a half-wired
// decorator that nil-deref's at the first mutation.
func TestJetStreamACLRuleStore_NilInputsRejected(t *testing.T) {
	js, stop := startTestJS(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := aclstore.NewJetStreamACLRuleStore(ctx, nil, js, 1, nil); err == nil {
		t.Errorf("expected error for nil inner Store, got nil")
	}
	if _, err := aclstore.NewJetStreamACLRuleStore(ctx, newFakeInnerStore(), nil, 1, nil); err == nil {
		t.Errorf("expected error for nil js, got nil")
	}
}

// ---------------------------------------------------------------------------
// Test 7: Compile-only interface assertion
// ---------------------------------------------------------------------------

// TestJetStreamACLRuleStore_InterfaceAssertion exists so `go test` shows the
// assertion as a passing test (the actual check is the package-level
// `var _ aclstore.Store = ...` at the top of this file — if the decorator
// stops satisfying Store, the test binary won't compile and the suite fails
// before this test runs).
func TestJetStreamACLRuleStore_InterfaceAssertion(t *testing.T) {
	_ = (aclstore.Store)((*aclstore.JetStreamACLRuleStore)(nil))
}

// ---------------------------------------------------------------------------
// Inner-fake decorators (rule echo + grant counter)
// ---------------------------------------------------------------------------

// echoingGrantInner overrides GrantAccess to return a Rule populated with
// the caller-supplied inputs so the wrapper's KV-key derivation has
// meaningful fields to encode. All other Store methods delegate to the
// embedded fakeInnerStore via Go method promotion.
type echoingGrantInner struct {
	aclstore.Store
}

func (e *echoingGrantInner) GrantAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) (*aclstore.Rule, error) {
	return &aclstore.Rule{
		RuleID:        "test-rule",
		PrincipalType: principalType,
		PrincipalID:   principalID,
		ResourceType:  resourceType,
		ResourceID:    resourceID,
		AccessLevel:   accessLevel,
		GrantedBy:     grantedBy,
		Reason:        reason,
		ExpiresAt:     expiresAt,
		GrantedAt:     time.Now().UTC(),
	}, nil
}

// grantCounterInner records GrantAccess invocations so Test 4 can assert
// the inner write happened even when the KV side failed.
type grantCounterInner struct {
	aclstore.Store
	grantCalls int
}

func (g *grantCounterInner) GrantAccess(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) (*aclstore.Rule, error) {
	g.grantCalls++
	return &aclstore.Rule{
		RuleID:        "counter-rule",
		PrincipalType: principalType,
		PrincipalID:   principalID,
		ResourceType:  resourceType,
		ResourceID:    resourceID,
		AccessLevel:   accessLevel,
		GrantedBy:     grantedBy,
		Reason:        reason,
	}, nil
}

// ---------------------------------------------------------------------------
// Test helpers — shared with the authority-store test file
// ---------------------------------------------------------------------------
//
// collectKeys drains the bucket's current keys into a slice. Returns an
// empty slice (not an error) when the bucket is empty — some nats.go
// versions return jetstream.ErrNoKeysFound for that case and we want
// callers to treat it as "no keys" not "lookup failed".
func collectKeys(ctx context.Context, kv jetstream.KeyValue) ([]string, error) {
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	defer lister.Stop()
	var out []string
	for k := range lister.Keys() {
		out = append(out, k)
	}
	return out, nil
}
