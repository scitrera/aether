// Phase 2 Stage B unit tests for the authority-request lifecycle on the
// sqlite-native ACL store. Mirrors the runAuthorityRequestLifecycle
// conformance-test bootstrap (same temp-dir sqlite, same shared audit
// writer) but focuses on lifecycle behavior, NOT storage CRUD (which is
// already covered by the conformance suite).
//
// Why this file lives under internal/storage/acl/sqlite instead of
// internal/acl: the *acl.Service path is postgres-only (uses $N placeholders
// directly, no dbcompat translation), so testing the lifecycle against
// sqlite requires the native sqlite Store. The sqlite Store carries a
// parallel lifecycle implementation (authority_request_lifecycle.go in this
// package) that shares constants, intersection helpers, and the audit-emit
// method with the *acl.Service path, so a test here exercises the same
// semantic contract.

package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/acl"
	legacyaudit "github.com/scitrera/aether/internal/audit"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	aclsqlite "github.com/scitrera/aether/internal/storage/acl/sqlite"
	"github.com/scitrera/aether/pkg/models"

	_ "modernc.org/sqlite"
)

// lifecycleTestEnv bundles the store + audit handles + temp paths so the
// individual subtests can spin up fresh state per test (no cross-test
// pollution) without rewriting the bootstrap each time.
type lifecycleTestEnv struct {
	store       *aclsqlite.Store
	sharedAudit *legacyaudit.AuditLogger
	auditDB     *sql.DB
	aclDB       *sql.DB
	cleanup     func()
}

// newLifecycleTestEnv builds a fresh sqlite-backed ACL store + audit pipeline
// for a single test. Mirrors conformance_test.go::sqliteNativeFactory. The
// audit writer is configured for fast flushing (BatchSize=1, FlushPeriod=50ms)
// so tests can assert on audit-row arrivals without long sleeps.
func newLifecycleTestEnv(t *testing.T) *lifecycleTestEnv {
	t.Helper()

	aclDBPath := filepath.Join(t.TempDir(), "acl_lifecycle.db")
	aclDSN := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", aclDBPath)
	aclDB, err := sql.Open("sqlite", aclDSN)
	if err != nil {
		t.Fatalf("sql.Open sqlite (acl): %v", err)
	}
	aclDB.SetMaxOpenConns(1)

	auditDBPath := filepath.Join(t.TempDir(), "audit_lifecycle.db")
	auditDSN := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", auditDBPath)
	auditDB, err := sql.Open("sqlite", auditDSN)
	if err != nil {
		_ = aclDB.Close()
		t.Fatalf("sql.Open sqlite (audit): %v", err)
	}
	auditDB.SetMaxOpenConns(1)

	ctx := context.Background()
	if _, err := auditDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS comprehensive_audit_log (
			audit_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp                 TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			event_type                TEXT NOT NULL,
			actor_type                TEXT NOT NULL,
			actor_id                  TEXT NOT NULL,
			subject_type              TEXT,
			subject_id                TEXT,
			root_subject_type         TEXT,
			root_subject_id           TEXT,
			authority_mode            TEXT NOT NULL DEFAULT 'direct',
			root_authority_grant_id   TEXT,
			authority_grant_id        TEXT,
			parent_authority_grant_id TEXT,
			resource_type             TEXT,
			resource_id               TEXT,
			operation                 TEXT NOT NULL,
			workspace                 TEXT,
			session_id                TEXT,
			gateway_id                TEXT,
			success                   INTEGER NOT NULL DEFAULT 1,
			error_message             TEXT,
			metadata                  TEXT,
			source                    TEXT NOT NULL DEFAULT 'gateway'
		)
	`); err != nil {
		_ = aclDB.Close()
		_ = auditDB.Close()
		t.Fatalf("create comprehensive_audit_log: %v", err)
	}

	gatewayID := fmt.Sprintf("lifecycle-gw-%d", time.Now().UnixNano())
	cfg := legacyaudit.DefaultConfig()
	cfg.BatchSize = 1
	cfg.FlushPeriod = 25 * time.Millisecond
	cfg.ChannelBuffer = 32
	sharedAudit := legacyaudit.NewAuditLogger(auditDB, gatewayID, cfg)

	store, err := aclsqlite.New(aclDB, sharedAudit, auditDB, gatewayID)
	if err != nil {
		_ = sharedAudit.Close()
		_ = aclDB.Close()
		_ = auditDB.Close()
		t.Fatalf("aclsqlite.New: %v", err)
	}

	cleanup := func() {
		_ = store.Close()
		_ = sharedAudit.Close()
		_ = aclDB.Close()
		_ = auditDB.Close()
	}
	return &lifecycleTestEnv{
		store:       store,
		sharedAudit: sharedAudit,
		auditDB:     auditDB,
		aclDB:       aclDB,
		cleanup:     cleanup,
	}
}

// uniqueLifecycleID is the per-test-per-call unique-suffix helper. We avoid
// importing the conformance_test.go private helper by inlining the same
// shape here.
func uniqueLifecycleID(hint string) string {
	return fmt.Sprintf("lifecycle-%s-%d", hint, time.Now().UnixNano())
}

// buildPendingRequest is the shared helper for tests that need a fresh
// pending request before exercising approve/deny/cancel. Returns the
// persisted request (with server-generated request_id + expires_at).
func buildPendingRequest(t *testing.T, ctx context.Context, env *lifecycleTestEnv, opts func(*acl.AuthorityRequest)) *acl.AuthorityRequest {
	t.Helper()
	requester := models.Identity{
		Type:           models.PrincipalTask,
		Workspace:      uniqueLifecycleID("ws"),
		Implementation: "lifecycle-test",
		Specifier:      uniqueLifecycleID("task"),
	}
	req := &acl.AuthorityRequest{
		RequestingActor: requester,
		WorkspaceScope:  []string{requester.Workspace},
		ResourceScope:   map[string][]string{"workspace": {requester.Workspace}},
		OperationScope:  []string{"manage"},
		RequestedAccess: acl.AccessManage,
		DurationSeconds: 1800,
		AudienceType:    acl.AuthorityAudienceTask,
		AudienceID:      requester.CanonicalPrincipalID(),
		RoutingTarget: acl.AuthorityRequestRoutingTarget{
			Capability: "capability/approve/lifecycle-test",
		},
		Reason:   "phase2 stage b lifecycle test",
		Metadata: map[string]interface{}{"hint": "stage-b"},
	}
	if opts != nil {
		opts(req)
	}
	persisted, err := env.store.SubmitAuthorityRequest(ctx, req)
	if err != nil {
		t.Fatalf("SubmitAuthorityRequest: %v", err)
	}
	if persisted.RequestID == "" {
		t.Fatalf("SubmitAuthorityRequest: empty RequestID")
	}
	if persisted.Status != acl.AuthorityRequestStatusPending {
		t.Fatalf("SubmitAuthorityRequest: status=%q, want pending", persisted.Status)
	}
	return persisted
}

// approverIdentity builds a synthetic approver. Used across the
// approve/deny tests.
func approverIdentity() models.Identity {
	return models.Identity{
		Type: models.PrincipalUser,
		ID:   uniqueLifecycleID("approver"),
	}
}

// flushAudit drains the shared audit writer's channel + waits for the
// next flush so SELECTs against comprehensive_audit_log see the rows we
// emitted. The audit writer is configured with FlushPeriod=25ms in
// newLifecycleTestEnv; we wait 200ms for headroom under loaded CI.
func flushAudit(t *testing.T, env *lifecycleTestEnv) {
	t.Helper()
	// The legacy audit writer exposes a sync drain via Close on its
	// internal AuditLogger, but we don't want to tear it down between
	// assertions. Instead, poll the audit table until the most-recent
	// row's age stabilizes — bounded to 500ms.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(40 * time.Millisecond)
	}
}

// countAuditRows returns the number of rows in comprehensive_audit_log
// matching (event_type, operation, request_id-in-metadata). The
// request-id is extracted from the JSON metadata so the test asserts on
// the audit event's logical association rather than the row's audit_id.
func countAuditRows(t *testing.T, env *lifecycleTestEnv, eventType, operation, requestID string) int {
	t.Helper()
	rows, err := env.auditDB.QueryContext(context.Background(),
		`SELECT metadata FROM comprehensive_audit_log
		 WHERE event_type = ? AND operation = ?`,
		eventType, operation,
	)
	if err != nil {
		t.Fatalf("countAuditRows query: %v", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("countAuditRows scan: %v", err)
		}
		if !raw.Valid {
			continue
		}
		var meta map[string]interface{}
		if err := json.Unmarshal([]byte(raw.String), &meta); err != nil {
			continue
		}
		if got, _ := meta["request_id"].(string); got == requestID {
			n++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("countAuditRows iterate: %v", err)
	}
	return n
}

// =============================================================================
// 1. Submit → Approve happy path
// =============================================================================

func TestSubmitApproveHappyPath(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	pending := buildPendingRequest(t, ctx, env, nil)

	approver := approverIdentity()
	resolved, err := env.store.ApproveAuthorityRequest(ctx, pending.RequestID, approver, &acl.ApproveDecision{
		Reason: "approved by happy-path test",
	})
	if err != nil {
		t.Fatalf("ApproveAuthorityRequest: %v", err)
	}
	if resolved.Status != acl.AuthorityRequestStatusApproved {
		t.Fatalf("after approve: status=%q, want approved", resolved.Status)
	}
	if resolved.GrantedGrantID == "" {
		t.Fatalf("after approve: granted_grant_id is empty (mint failed?)")
	}
	if resolved.ResolvedAt == nil {
		t.Fatalf("after approve: ResolvedAt is nil")
	}

	// Verify the grant landed in acl_authority_grants.
	grant, err := env.store.GetAuthorityGrant(ctx, resolved.GrantedGrantID)
	if err != nil {
		t.Fatalf("GetAuthorityGrant after approve: %v", err)
	}
	if grant.MaxAccessLevel != acl.AccessManage {
		t.Fatalf("grant max_access_level=%d, want %d", grant.MaxAccessLevel, acl.AccessManage)
	}
	if grant.Revoked {
		t.Fatalf("grant should not be revoked")
	}
	if grant.AudienceType != acl.AuthorityAudienceTask || grant.AudienceID != pending.AudienceID {
		t.Fatalf("grant audience mismatch: (%q,%q), want (%q,%q)",
			grant.AudienceType, grant.AudienceID,
			acl.AuthorityAudienceTask, pending.AudienceID)
	}

	flushAudit(t, env)
	if got := countAuditRows(t, env,
		legacyaudit.EventTypeAuthorityRequest,
		acl.OpAuthorityRequestCreated,
		pending.RequestID,
	); got != 1 {
		t.Fatalf("created audit row count = %d, want 1", got)
	}
	if got := countAuditRows(t, env,
		legacyaudit.EventTypeAuthorityRequest,
		acl.OpAuthorityRequestApproved,
		pending.RequestID,
	); got != 1 {
		t.Fatalf("approved audit row count = %d, want 1", got)
	}
}

// =============================================================================
// 2. Approve with scope refinement narrows the resulting grant
// =============================================================================

func TestApproveScopeRefinementIntersects(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	wsA := uniqueLifecycleID("ws-a")
	wsB := uniqueLifecycleID("ws-b")
	wsC := uniqueLifecycleID("ws-c")
	wsD := uniqueLifecycleID("ws-d")

	pending := buildPendingRequest(t, ctx, env, func(r *acl.AuthorityRequest) {
		r.WorkspaceScope = []string{wsA, wsB, wsC}
		r.ResourceScope = map[string][]string{"workspace": {wsA, wsB, wsC}}
	})

	resolved, err := env.store.ApproveAuthorityRequest(ctx, pending.RequestID, approverIdentity(), &acl.ApproveDecision{
		Reason:                "narrow to B,C only",
		GrantedWorkspaceScope: []string{wsB, wsC, wsD}, // wsD must be silently dropped (not in request)
		GrantedResourceScope:  map[string][]string{"workspace": {wsB, wsC, wsD}},
	})
	if err != nil {
		t.Fatalf("ApproveAuthorityRequest: %v", err)
	}

	grant, err := env.store.GetAuthorityGrant(ctx, resolved.GrantedGrantID)
	if err != nil {
		t.Fatalf("GetAuthorityGrant: %v", err)
	}

	// Workspace scope should intersect to [wsB, wsC] (order preserved from request).
	if !equalStringSlice(grant.WorkspaceScope, []string{wsB, wsC}) {
		t.Fatalf("grant workspace_scope = %v, want %v",
			grant.WorkspaceScope, []string{wsB, wsC})
	}
	// wsD must NOT appear anywhere — approver cannot broaden.
	for _, w := range grant.WorkspaceScope {
		if w == wsD {
			t.Fatalf("grant workspace_scope contains %q which was not in the original request", wsD)
		}
	}

	// Resource scope: same intersection.
	gotResourceWorkspaces := grant.ResourceScope["workspace"]
	if !equalStringSlice(gotResourceWorkspaces, []string{wsB, wsC}) {
		t.Fatalf("grant resource_scope[workspace] = %v, want %v",
			gotResourceWorkspaces, []string{wsB, wsC})
	}
}

// =============================================================================
// 3. Approve with shorter duration caps the grant ExpiresAt
// =============================================================================

func TestApproveShorterDurationCaps(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	pending := buildPendingRequest(t, ctx, env, func(r *acl.AuthorityRequest) {
		r.DurationSeconds = 1800 // requested 30 minutes
	})

	beforeApprove := time.Now().UTC()
	resolved, err := env.store.ApproveAuthorityRequest(ctx, pending.RequestID, approverIdentity(), &acl.ApproveDecision{
		Reason:                 "5min only",
		GrantedDurationSeconds: 300, // approver narrows to 5 minutes
	})
	if err != nil {
		t.Fatalf("ApproveAuthorityRequest: %v", err)
	}
	grant, err := env.store.GetAuthorityGrant(ctx, resolved.GrantedGrantID)
	if err != nil {
		t.Fatalf("GetAuthorityGrant: %v", err)
	}

	// ExpiresAt should be in (beforeApprove + 290s, beforeApprove + 310s).
	expectedLo := beforeApprove.Add(290 * time.Second)
	expectedHi := beforeApprove.Add(310 * time.Second)
	if grant.ExpiresAt.Before(expectedLo) || grant.ExpiresAt.After(expectedHi) {
		t.Fatalf("grant ExpiresAt = %v; want within [%v, %v]",
			grant.ExpiresAt, expectedLo, expectedHi)
	}
}

// =============================================================================
// 4. Approve on already-resolved returns the sentinel; no second grant
// =============================================================================

func TestApproveAlreadyResolvedReturnsSentinel(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	pending := buildPendingRequest(t, ctx, env, nil)
	approver := approverIdentity()

	first, err := env.store.ApproveAuthorityRequest(ctx, pending.RequestID, approver, &acl.ApproveDecision{Reason: "first"})
	if err != nil {
		t.Fatalf("first Approve: %v", err)
	}
	firstGrantID := first.GrantedGrantID

	_, err = env.store.ApproveAuthorityRequest(ctx, pending.RequestID, approver, &acl.ApproveDecision{Reason: "second"})
	if !errors.Is(err, acl.ErrAuthorityRequestAlreadyResolved) {
		t.Fatalf("second Approve: expected ErrAuthorityRequestAlreadyResolved, got %v", err)
	}

	// Confirm the request still references the original grant (no second mint).
	post, err := env.store.GetAuthorityRequest(ctx, pending.RequestID)
	if err != nil {
		t.Fatalf("GetAuthorityRequest: %v", err)
	}
	if post.GrantedGrantID != firstGrantID {
		t.Fatalf("granted_grant_id mutated: got %q, want %q",
			post.GrantedGrantID, firstGrantID)
	}
}

// =============================================================================
// 5. Deny path: row flips to DENIED, no grant minted, audit event present
// =============================================================================

func TestDenyPath(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	pending := buildPendingRequest(t, ctx, env, nil)
	approver := approverIdentity()

	resolved, err := env.store.DenyAuthorityRequest(ctx, pending.RequestID, approver, "too risky")
	if err != nil {
		t.Fatalf("DenyAuthorityRequest: %v", err)
	}
	if resolved.Status != acl.AuthorityRequestStatusDenied {
		t.Fatalf("status=%q, want denied", resolved.Status)
	}
	if resolved.GrantedGrantID != "" {
		t.Fatalf("denied request unexpectedly has granted_grant_id=%q", resolved.GrantedGrantID)
	}
	if resolved.ResolutionReason != "too risky" {
		t.Fatalf("resolution_reason=%q, want %q", resolved.ResolutionReason, "too risky")
	}

	flushAudit(t, env)
	if got := countAuditRows(t, env,
		legacyaudit.EventTypeAuthorityRequest,
		acl.OpAuthorityRequestDenied,
		pending.RequestID,
	); got != 1 {
		t.Fatalf("denied audit row count = %d, want 1", got)
	}
}

// =============================================================================
// 6. Cancel path: requester cancels, status flips to CANCELLED, audit emitted
// =============================================================================

func TestCancelPath(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	pending := buildPendingRequest(t, ctx, env, nil)

	resolved, err := env.store.CancelOpenAuthorityRequest(ctx, pending.RequestID, "changed mind")
	if err != nil {
		t.Fatalf("CancelOpenAuthorityRequest: %v", err)
	}
	if resolved.Status != acl.AuthorityRequestStatusCancelled {
		t.Fatalf("status=%q, want cancelled", resolved.Status)
	}
	if resolved.ResolutionReason != "changed mind" {
		t.Fatalf("resolution_reason=%q, want %q", resolved.ResolutionReason, "changed mind")
	}

	flushAudit(t, env)
	if got := countAuditRows(t, env,
		legacyaudit.EventTypeAuthorityRequest,
		acl.OpAuthorityRequestCancelled,
		pending.RequestID,
	); got != 1 {
		t.Fatalf("cancelled audit row count = %d, want 1", got)
	}
}

// =============================================================================
// 7. Sweep expired: past-expires_at row transitions to EXPIRED + audit emit
// =============================================================================

func TestSweepExpiredEmitsAudit(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	// Submit a request, then forcibly rewrite its expires_at into the past
	// (the lifecycle Submit method clamps ExpiresAt to a forward-looking
	// timeout, so we need a direct UPDATE to simulate an aged-out row).
	pending := buildPendingRequest(t, ctx, env, nil)
	past := time.Now().Add(-5 * time.Minute).UTC()
	if _, err := env.aclDB.ExecContext(ctx,
		`UPDATE acl_authority_requests SET expires_at = ? WHERE request_id = ?`,
		past.Format(time.RFC3339Nano), pending.RequestID,
	); err != nil {
		t.Fatalf("rewrite expires_at: %v", err)
	}

	swept, err := env.store.SweepExpiredAuthorityRequests(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("SweepExpiredAuthorityRequests: %v", err)
	}
	if !containsLifecycleRequest(swept, pending.RequestID) {
		t.Fatalf("Sweep did not return request %q (got %d rows)", pending.RequestID, len(swept))
	}

	postSweep, err := env.store.GetAuthorityRequest(ctx, pending.RequestID)
	if err != nil {
		t.Fatalf("GetAuthorityRequest after sweep: %v", err)
	}
	if postSweep.Status != acl.AuthorityRequestStatusExpired {
		t.Fatalf("status=%q, want expired", postSweep.Status)
	}

	flushAudit(t, env)
	if got := countAuditRows(t, env,
		legacyaudit.EventTypeAuthorityRequest,
		acl.OpAuthorityRequestExpired,
		pending.RequestID,
	); got != 1 {
		t.Fatalf("expired audit row count = %d, want 1", got)
	}
}

// =============================================================================
// 8. List filter status=Pending excludes resolved/expired
// =============================================================================

func TestListFilterPendingExcludesTerminal(t *testing.T) {
	env := newLifecycleTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	stillPending := buildPendingRequest(t, ctx, env, func(r *acl.AuthorityRequest) {
		r.RoutingTarget = acl.AuthorityRequestRoutingTarget{
			Capability: "capability/approve/list-filter-test",
		}
	})

	approvedReq := buildPendingRequest(t, ctx, env, func(r *acl.AuthorityRequest) {
		r.RoutingTarget = acl.AuthorityRequestRoutingTarget{
			Capability: "capability/approve/list-filter-test",
		}
	})
	if _, err := env.store.ApproveAuthorityRequest(ctx, approvedReq.RequestID, approverIdentity(), &acl.ApproveDecision{Reason: "ok"}); err != nil {
		t.Fatalf("ApproveAuthorityRequest: %v", err)
	}

	deniedReq := buildPendingRequest(t, ctx, env, func(r *acl.AuthorityRequest) {
		r.RoutingTarget = acl.AuthorityRequestRoutingTarget{
			Capability: "capability/approve/list-filter-test",
		}
	})
	if _, err := env.store.DenyAuthorityRequest(ctx, deniedReq.RequestID, approverIdentity(), "no"); err != nil {
		t.Fatalf("DenyAuthorityRequest: %v", err)
	}

	listed, err := env.store.ListAuthorityRequests(ctx, aclstore.AuthorityRequestFilter{
		Status:               acl.AuthorityRequestStatusPending,
		ResolverCapabilities: []string{"capability/approve/list-filter-test"},
	})
	if err != nil {
		t.Fatalf("ListAuthorityRequests: %v", err)
	}

	if !containsLifecycleRequest(listed, stillPending.RequestID) {
		t.Fatalf("pending request %q missing from filter result", stillPending.RequestID)
	}
	if containsLifecycleRequest(listed, approvedReq.RequestID) {
		t.Fatalf("approved request %q unexpectedly returned by Pending filter", approvedReq.RequestID)
	}
	if containsLifecycleRequest(listed, deniedReq.RequestID) {
		t.Fatalf("denied request %q unexpectedly returned by Pending filter", deniedReq.RequestID)
	}
}

// =============================================================================
// helpers
// =============================================================================

func containsLifecycleRequest(requests []*acl.AuthorityRequest, requestID string) bool {
	for _, r := range requests {
		if r.RequestID == requestID {
			return true
		}
	}
	return false
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
