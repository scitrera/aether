// Package quota — pure logic tests that run without any Redis connection.
//
// QuotaManager's hot-path methods call getEffectiveQuota, which checks the
// in-memory override cache FIRST and only falls through to Redis when the
// workspace is absent from the cache. All tests in this file pre-populate the
// cache via injectOverride so the nil Redis client is never reached.
//
// Coverage provided:
//   - CheckKVQuota: under limit, at limit (exact boundary), over limit, unlimited (0), override limits
//   - CheckKVValueSize: under limit, at limit, over limit, unlimited (0), override limits
//   - QuotaExceededError: without identity, with identity, implements error interface
//   - Default limit fallback: zero-valued override fields defer to DefaultQuotas
//   - NewQuotaManager: construction sanity check
//   - WorkspaceQuota: field round-trip
//
// Redis-dependent methods (CheckAndIncrementConnections, DecrementConnections,
// CheckMessageQuota, SetWorkspaceQuota, GetWorkspaceQuota) are covered by
// manager_test.go when a live Redis instance is available.
package quota

import (
	"context"
	"errors"
	"testing"

	pkgerrors "github.com/scitrera/aether/pkg/errors"
)

// newPureQM builds a QuotaManager with a nil Redis client.
// Only safe to call methods whose code paths are fully satisfied by the
// in-memory override cache (i.e., always call injectOverride first).
func newPureQM(defaults DefaultQuotas) *QuotaManager {
	return &QuotaManager{
		redis:     nil,
		defaults:  defaults,
		overrides: make(map[string]*WorkspaceQuota),
	}
}

// injectOverride pre-populates the in-memory cache so getEffectiveQuota
// returns the override without ever touching Redis.
func injectOverride(qm *QuotaManager, workspace string, q WorkspaceQuota) {
	q.Workspace = workspace
	qm.mu.Lock()
	qm.overrides[workspace] = &q
	qm.mu.Unlock()
}

// ---------------------------------------------------------------------------
// QuotaExceededError formatting
// ---------------------------------------------------------------------------

func TestQuotaExceededError_withoutIdentity(t *testing.T) {
	err := &pkgerrors.QuotaExceededError{
		Resource:  "connections",
		Workspace: "ws1",
		Current:   10,
		Limit:     5,
	}
	want := "quota exceeded for connections in workspace 'ws1': current 10, limit 5"
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}

func TestQuotaExceededError_withIdentity(t *testing.T) {
	err := &pkgerrors.QuotaExceededError{
		Resource:  "message_rate",
		Workspace: "ws1",
		Identity:  "ag::ws1::impl::spec",
		Current:   101,
		Limit:     100,
	}
	want := "quota exceeded for message_rate in workspace 'ws1' (identity: ag::ws1::impl::spec): current 101, limit 100"
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}

func TestQuotaExceededError_implementsErrorInterface(t *testing.T) {
	var err error = &pkgerrors.QuotaExceededError{Resource: "kv_keys", Workspace: "w", Current: 1, Limit: 1}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

// ---------------------------------------------------------------------------
// CheckKVQuota — pure logic, no Redis
// ---------------------------------------------------------------------------

func TestCheckKVQuota_underLimit_returnsNil(t *testing.T) {
	qm := newPureQM(DefaultQuotas{MaxKVKeysPerNamespace: 100})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVKeys: 100})
	if err := qm.CheckKVQuota(context.Background(), "ws", "ns", 50); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestCheckKVQuota_atLimit_returnsQuotaExceeded(t *testing.T) {
	// The check is >= limit, so exactly at the limit is rejected.
	qm := newPureQM(DefaultQuotas{MaxKVKeysPerNamespace: 100})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVKeys: 100})
	err := qm.CheckKVQuota(context.Background(), "ws", "ns", 100)
	if err == nil {
		t.Fatal("expected error at limit, got nil")
	}
	var qErr *pkgerrors.QuotaExceededError
	if !errors.As(err, &qErr) {
		t.Fatalf("expected *QuotaExceededError, got %T", err)
	}
	if qErr.Resource != "kv_keys" {
		t.Errorf("expected resource 'kv_keys', got %q", qErr.Resource)
	}
	if qErr.Current != 100 {
		t.Errorf("expected current 100, got %d", qErr.Current)
	}
	if qErr.Limit != 100 {
		t.Errorf("expected limit 100, got %d", qErr.Limit)
	}
	if qErr.Workspace != "ws" {
		t.Errorf("expected workspace 'ws', got %q", qErr.Workspace)
	}
}

func TestCheckKVQuota_overLimit_returnsQuotaExceeded(t *testing.T) {
	qm := newPureQM(DefaultQuotas{MaxKVKeysPerNamespace: 10})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVKeys: 10})
	err := qm.CheckKVQuota(context.Background(), "ws", "ns", 999)
	if err == nil {
		t.Fatal("expected error over limit, got nil")
	}
	var qErr *pkgerrors.QuotaExceededError
	if !errors.As(err, &qErr) {
		t.Fatalf("expected *QuotaExceededError, got %T", err)
	}
	if qErr.Current != 999 {
		t.Errorf("expected current 999, got %d", qErr.Current)
	}
}

func TestCheckKVQuota_zeroLimit_isUnlimited(t *testing.T) {
	// A zero MaxKVKeys override defers to the defaults. A zero default means unlimited.
	qm := newPureQM(DefaultQuotas{MaxKVKeysPerNamespace: 0})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVKeys: 0})
	if err := qm.CheckKVQuota(context.Background(), "ws", "ns", 999999); err != nil {
		t.Fatalf("expected nil for unlimited quota, got %v", err)
	}
}

func TestCheckKVQuota_workspaceOverrideAllowsHigherCount(t *testing.T) {
	// Default is 100; override raises it to 500 — count of 200 should pass.
	qm := newPureQM(DefaultQuotas{MaxKVKeysPerNamespace: 100})
	injectOverride(qm, "ws-big", WorkspaceQuota{MaxKVKeys: 500})
	if err := qm.CheckKVQuota(context.Background(), "ws-big", "ns", 200); err != nil {
		t.Fatalf("expected nil with override limit 500, got %v", err)
	}
}

func TestCheckKVQuota_workspaceOverrideEnforcesLowerLimit(t *testing.T) {
	// Default is 100; override restricts to 10 — count of 11 should fail.
	qm := newPureQM(DefaultQuotas{MaxKVKeysPerNamespace: 100})
	injectOverride(qm, "ws-small", WorkspaceQuota{MaxKVKeys: 10})
	if err := qm.CheckKVQuota(context.Background(), "ws-small", "ns", 11); err == nil {
		t.Fatal("expected error with override limit 10, got nil")
	}
}

func TestCheckKVQuota_justBelowLimit_returnsNil(t *testing.T) {
	qm := newPureQM(DefaultQuotas{MaxKVKeysPerNamespace: 50})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVKeys: 50})
	if err := qm.CheckKVQuota(context.Background(), "ws", "ns", 49); err != nil {
		t.Fatalf("expected nil one below limit, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CheckKVValueSize — pure logic, no Redis
// ---------------------------------------------------------------------------

func TestCheckKVValueSize_underLimit_returnsNil(t *testing.T) {
	qm := newPureQM(DefaultQuotas{MaxKVValueSize: 1024})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVValueSize: 1024})
	if err := qm.CheckKVValueSize(context.Background(), "ws", 512); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestCheckKVValueSize_atLimit_returnsNil(t *testing.T) {
	// The check is strictly greater-than (valueSize > limit), so exactly at
	// the limit is allowed.
	qm := newPureQM(DefaultQuotas{MaxKVValueSize: 1024})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVValueSize: 1024})
	if err := qm.CheckKVValueSize(context.Background(), "ws", 1024); err != nil {
		t.Fatalf("expected nil at exact limit, got %v", err)
	}
}

func TestCheckKVValueSize_oneByteOverLimit_returnsQuotaExceeded(t *testing.T) {
	qm := newPureQM(DefaultQuotas{MaxKVValueSize: 1024})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVValueSize: 1024})
	err := qm.CheckKVValueSize(context.Background(), "ws", 1025)
	if err == nil {
		t.Fatal("expected error one byte over limit, got nil")
	}
	var qErr *pkgerrors.QuotaExceededError
	if !errors.As(err, &qErr) {
		t.Fatalf("expected *QuotaExceededError, got %T", err)
	}
	if qErr.Resource != "kv_value_size" {
		t.Errorf("expected resource 'kv_value_size', got %q", qErr.Resource)
	}
	if qErr.Current != 1025 {
		t.Errorf("expected current 1025, got %d", qErr.Current)
	}
	if qErr.Limit != 1024 {
		t.Errorf("expected limit 1024, got %d", qErr.Limit)
	}
	if qErr.Workspace != "ws" {
		t.Errorf("expected workspace 'ws', got %q", qErr.Workspace)
	}
}

func TestCheckKVValueSize_zeroLimit_isUnlimited(t *testing.T) {
	qm := newPureQM(DefaultQuotas{MaxKVValueSize: 0})
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVValueSize: 0})
	if err := qm.CheckKVValueSize(context.Background(), "ws", 999999999); err != nil {
		t.Fatalf("expected nil for unlimited value size, got %v", err)
	}
}

func TestCheckKVValueSize_workspaceOverrideAllowsLargerValue(t *testing.T) {
	// Default is 1 KB; override is 10 MB — 5 MB should pass.
	qm := newPureQM(DefaultQuotas{MaxKVValueSize: 1024})
	injectOverride(qm, "ws-large", WorkspaceQuota{MaxKVValueSize: 10 * 1024 * 1024})
	if err := qm.CheckKVValueSize(context.Background(), "ws-large", 5*1024*1024); err != nil {
		t.Fatalf("expected nil with 10 MB override, got %v", err)
	}
}

func TestCheckKVValueSize_workspaceOverrideEnforcesLowerLimit(t *testing.T) {
	// Default is 1 MB; override restricts to 256 bytes — 512 bytes should fail.
	qm := newPureQM(DefaultQuotas{MaxKVValueSize: 1024 * 1024})
	injectOverride(qm, "ws-tiny", WorkspaceQuota{MaxKVValueSize: 256})
	if err := qm.CheckKVValueSize(context.Background(), "ws-tiny", 512); err == nil {
		t.Fatal("expected error with override limit 256, got nil")
	}
}

// ---------------------------------------------------------------------------
// Default limit fallback: zero-valued override fields defer to DefaultQuotas
// ---------------------------------------------------------------------------

func TestDefaultLimits_zeroOverrideFieldsDeferToDefaults(t *testing.T) {
	defaults := DefaultQuotas{
		MaxKVKeysPerNamespace: 5000,
		MaxKVValueSize:        512 * 1024,
	}
	qm := newPureQM(defaults)
	// A zero-value WorkspaceQuota means MaxKVKeys == 0 and MaxKVValueSize == 0,
	// so getKVKeysLimit and getKVValueSizeLimit fall back to defaults.
	injectOverride(qm, "ws", WorkspaceQuota{MaxKVKeys: 0, MaxKVValueSize: 0})

	// Key count: 4999 < 5000 default → allowed.
	if err := qm.CheckKVQuota(context.Background(), "ws", "ns", 4999); err != nil {
		t.Fatalf("expected nil under default key limit, got %v", err)
	}
	// Key count: 5000 == default limit → rejected (>= check).
	if err := qm.CheckKVQuota(context.Background(), "ws", "ns", 5000); err == nil {
		t.Fatal("expected error at default key limit, got nil")
	}

	// Value size: exactly at default limit (512 KB) → allowed (> check).
	if err := qm.CheckKVValueSize(context.Background(), "ws", 512*1024); err != nil {
		t.Fatalf("expected nil at exact default value size limit, got %v", err)
	}
	// Value size: one byte over → rejected.
	if err := qm.CheckKVValueSize(context.Background(), "ws", 512*1024+1); err == nil {
		t.Fatal("expected error over default value size limit, got nil")
	}
}

// ---------------------------------------------------------------------------
// NewQuotaManager — construction
// ---------------------------------------------------------------------------

func TestNewQuotaManager_storesDefaultsAndInitializesOverridesMap(t *testing.T) {
	defaults := DefaultQuotas{
		MaxConnectionsPerWorkspace: 100,
		MaxMessageRatePerIdentity:  50,
		MaxKVKeysPerNamespace:      1000,
		MaxKVValueSize:             65536,
	}
	qm := NewQuotaManager(nil, defaults)
	if qm == nil {
		t.Fatal("expected non-nil QuotaManager")
	}
	if qm.defaults != defaults {
		t.Errorf("defaults not stored correctly: got %+v", qm.defaults)
	}
	if qm.overrides == nil {
		t.Error("overrides map should be initialized, got nil")
	}
}

// ---------------------------------------------------------------------------
// WorkspaceQuota — struct field access
// ---------------------------------------------------------------------------

func TestWorkspaceQuota_fieldsStoredCorrectly(t *testing.T) {
	q := WorkspaceQuota{
		Workspace:                 "test-ws",
		MaxConnections:            25,
		MaxMessageRatePerIdentity: 75.5,
		MaxKVKeys:                 200,
		MaxKVValueSize:            2048,
	}
	if q.Workspace != "test-ws" {
		t.Errorf("unexpected Workspace: %q", q.Workspace)
	}
	if q.MaxConnections != 25 {
		t.Errorf("unexpected MaxConnections: %d", q.MaxConnections)
	}
	if q.MaxMessageRatePerIdentity != 75.5 {
		t.Errorf("unexpected MaxMessageRatePerIdentity: %f", q.MaxMessageRatePerIdentity)
	}
	if q.MaxKVKeys != 200 {
		t.Errorf("unexpected MaxKVKeys: %d", q.MaxKVKeys)
	}
	if q.MaxKVValueSize != 2048 {
		t.Errorf("unexpected MaxKVValueSize: %d", q.MaxKVValueSize)
	}
}
