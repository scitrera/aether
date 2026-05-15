// Package acl_test contains the cross-backend conformance suite for
// acl.Store. The same test cases run against every implementation —
// postgres today, sqlite-native once Stage 2 lands. Drift between
// implementations gets caught here.
//
// Per `.slop/20260514_storage_interfaces_stage0.md` §8, the suite is
// table-driven with one subtest per backend. The postgres subtest skips
// when DATABASE_URL / dev infra is unavailable; the sqlite subtest is
// always runnable since it spins up a temp-dir SQLite file.
//
// Backend gaps in Stage 1 (closed in Stage 2):
//   - sqlite has no acl_audit_log VIEW (postgres migration 010 creates the
//     view; the sqlite migration set deliberately skips it because audit
//     rows go to comprehensive_audit_log directly). QueryAuditLog is
//     therefore not exercised on the sqlite backend.
//   - sqlite has no cleanup_expired_acl_rules() stored function; the
//     legacy CleanupExpiredRules call invokes it directly so that subtest
//     is skipped on sqlite.
//   - sqlite has no cleanup_old_audit_logs() stored function; the
//     CleanupOldAuditLogs subtest is skipped on sqlite for the same
//     reason.
//   - sqlite's acl_fallback_policies.policy_id has no DEFAULT
//     (postgres uses gen_random_uuid()); the legacy SetFallbackPolicy
//     omits policy_id from its INSERT and relies on the DB default. The
//     FallbackPolicy subtest is therefore skipped on sqlite — Stage 2
//     ships a native impl that generates the UUID in Go code.
//
// Stage 2 closes all four gaps with parameterized SQL + Go-side UUID
// minting in the sqlite-native impl.
package acl_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	legacyaudit "github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/storage/acl"
	aclpg "github.com/scitrera/aether/internal/storage/acl/postgres"
	aclsqlite "github.com/scitrera/aether/internal/storage/acl/sqlite"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// storeFactory builds a Store and returns flags describing backend
// capabilities plus a cleanup func. The factory may call t.Skip if its
// prerequisites are unmet (e.g. postgres dev infra not running) — the
// harness honors that and reports the subtest as skipped.
type storeFactory func(t *testing.T) (store acl.Store, caps backendCaps, cleanup func())

// backendCaps reports which legacy postgres-function-dependent features
// the backend supports in Stage 1. Stage 2's sqlite-native impl will flip
// all of these to true.
type backendCaps struct {
	supportsAuditQuery     bool // acl_audit_log view exists
	supportsCleanupExpired bool // cleanup_expired_acl_rules() exists
	supportsCleanupAudit   bool // cleanup_old_audit_logs() exists
	supportsFallbackUpsert bool // acl_fallback_policies.policy_id has a server-side default
}

func TestStoreConformance(t *testing.T) {
	backends := []struct {
		name    string
		factory storeFactory
	}{
		{name: "postgres", factory: postgresFactory},
		{name: "sqlite_native", factory: sqliteNativeFactory},
	}

	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			t.Run("GrantRevokeRoundTrip", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runGrantRevokeRoundTrip(t, store)
			})
			t.Run("AccessCheck", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAccessCheck(t, store)
			})
			t.Run("FallbackPolicy", func(t *testing.T) {
				store, caps, cleanup := b.factory(t)
				defer cleanup()
				if !caps.supportsFallbackUpsert {
					t.Skip("SetFallbackPolicy upsert relies on acl_fallback_policies.policy_id DB-side DEFAULT (gen_random_uuid); the sqlite schema has no equivalent. Stage 2 ships a native sqlite impl that generates the UUID in Go.")
				}
				runFallbackPolicy(t, store)
			})
			t.Run("AuthorityGrantLifecycle", func(t *testing.T) {
				store, _, cleanup := b.factory(t)
				defer cleanup()
				runAuthorityGrantLifecycle(t, store)
			})
			t.Run("CleanupExpiredRules", func(t *testing.T) {
				store, caps, cleanup := b.factory(t)
				defer cleanup()
				if !caps.supportsCleanupExpired {
					t.Skip("CleanupExpiredRules requires cleanup_expired_acl_rules() PG stored function; Stage 2 ships a native sqlite equivalent")
				}
				runCleanupExpiredRules(t, store)
			})
		})
	}
}

// runGrantRevokeRoundTrip verifies the GrantAccess → GetRule → ListRules
// → RevokeAccess → GetRule(nil) round trip.
func runGrantRevokeRoundTrip(t *testing.T, store acl.Store) {
	t.Helper()
	ctx := context.Background()

	principalID := uniqueID(t, "user")
	resourceID := uniqueID(t, "ws")

	rule, err := store.GrantAccess(ctx,
		acl.PrincipalTypeUser, principalID,
		acl.ResourceTypeWorkspace, resourceID,
		acl.AccessReadWrite, "_system", "conformance grant", nil,
	)
	if err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}
	if rule.RuleID == "" {
		t.Fatalf("GrantAccess returned rule with empty RuleID")
	}

	got, err := store.GetRule(ctx, acl.PrincipalTypeUser, principalID, acl.ResourceTypeWorkspace, resourceID)
	if err != nil {
		t.Fatalf("GetRule after grant: %v", err)
	}
	if got.AccessLevel != acl.AccessReadWrite {
		t.Fatalf("expected AccessReadWrite (%d), got %d", acl.AccessReadWrite, got.AccessLevel)
	}

	rules, err := store.ListRules(ctx, acl.RuleFilter{PrincipalType: acl.PrincipalTypeUser, PrincipalID: principalID})
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if !containsRule(rules, resourceID) {
		t.Fatalf("ListRules did not include the new rule for resource %q (got %d rules)", resourceID, len(rules))
	}

	if err := store.RevokeAccess(ctx, acl.PrincipalTypeUser, principalID, acl.ResourceTypeWorkspace, resourceID); err != nil {
		t.Fatalf("RevokeAccess: %v", err)
	}

	_, err = store.GetRule(ctx, acl.PrincipalTypeUser, principalID, acl.ResourceTypeWorkspace, resourceID)
	if err != acl.ErrRuleNotFound {
		t.Fatalf("GetRule after revoke: expected ErrRuleNotFound, got %v", err)
	}
}

// runAccessCheck verifies that a grant at level N permits CheckAccess at
// level <= N and denies CheckAccess at level > N.
func runAccessCheck(t *testing.T, store acl.Store) {
	t.Helper()
	ctx := context.Background()

	principalID := uniqueID(t, "user")
	resourceID := uniqueID(t, "ws")

	// Set a deny-by-default fallback for this isolated user_workspace
	// category so the test isn't sensitive to seeded fallback values.
	// (Cannot scope by user; we use a fresh principal so the row-level
	// rule fully controls the decision.)
	if _, err := store.GrantAccess(ctx,
		acl.PrincipalTypeUser, principalID,
		acl.ResourceTypeWorkspace, resourceID,
		acl.AccessReadWrite, "_system", "conformance access check", nil,
	); err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}

	principal := models.Identity{
		Type:      models.PrincipalUser,
		ID:        principalID,
		Workspace: resourceID,
	}

	// At or below granted level: ALLOW.
	for _, level := range []int{acl.AccessRead, acl.AccessReadWrite} {
		dec, err := store.CheckAccess(ctx, principal, acl.ResourceTypeWorkspace, resourceID, "connect", resourceID, uuid.New(), level)
		if err != nil {
			t.Fatalf("CheckAccess(level=%d): %v", level, err)
		}
		if !dec.Allowed {
			t.Fatalf("CheckAccess(level=%d): expected ALLOW, got DENY (%s)", level, dec.Reason)
		}
	}

	// Above granted level: DENY.
	dec, err := store.CheckAccess(ctx, principal, acl.ResourceTypeWorkspace, resourceID, "manage", resourceID, uuid.New(), acl.AccessAdmin)
	if err != nil {
		t.Fatalf("CheckAccess(AccessAdmin): %v", err)
	}
	if dec.Allowed {
		t.Fatalf("CheckAccess(AccessAdmin): expected DENY, got ALLOW (level=%d, %s)", dec.EffectiveAccessLevel, dec.Reason)
	}
}

// runFallbackPolicy verifies that SetFallbackPolicy → GetFallbackPolicy
// returns the written value.
func runFallbackPolicy(t *testing.T, store acl.Store) {
	t.Helper()
	ctx := context.Background()

	// Use a unique category so the test doesn't collide with seeded rows
	// or other concurrent tests sharing the dev database.
	category := fmt.Sprintf("conformance_%d", time.Now().UnixNano())

	if err := store.SetFallbackPolicy(ctx, category, acl.AccessRead, "_test"); err != nil {
		t.Fatalf("SetFallbackPolicy: %v", err)
	}

	got, err := store.GetFallbackPolicy(ctx, category)
	if err != nil {
		t.Fatalf("GetFallbackPolicy: %v", err)
	}
	if got.FallbackAccessLevel != acl.AccessRead {
		t.Fatalf("expected fallback level %d, got %d", acl.AccessRead, got.FallbackAccessLevel)
	}
	if got.RuleCategory != category {
		t.Fatalf("expected category %q, got %q", category, got.RuleCategory)
	}
}

// runAuthorityGrantLifecycle verifies the CreateAuthorityGrant →
// GetAuthorityGrant → ListAuthorityGrants → RevokeAuthorityGrant flow.
func runAuthorityGrantLifecycle(t *testing.T, store acl.Store) {
	t.Helper()
	ctx := context.Background()

	subject := models.Identity{Type: models.PrincipalUser, ID: uniqueID(t, "subj")}
	delegate := models.Identity{Type: models.PrincipalService, Implementation: "frontend", Specifier: uniqueID(t, "del")}
	issuedBy := subject

	audienceID := uuid.New().String()
	now := time.Now()
	req := acl.CreateAuthorityGrantRequest{
		Subject:                  subject,
		Delegate:                 delegate,
		IssuedBy:                 issuedBy,
		MayDelegate:              false,
		RemainingHops:            0,
		MaxAccessLevel:           acl.AccessRead,
		AudienceType:             acl.AuthorityAudienceSession,
		AudienceID:               audienceID,
		ValidWhileAudienceActive: false,
		ExpiresAt:                now.Add(1 * time.Hour),
		RenewableUntil:           now.Add(24 * time.Hour),
		Reason:                   "conformance grant",
	}

	grant, err := store.CreateAuthorityGrant(ctx, req)
	if err != nil {
		t.Fatalf("CreateAuthorityGrant: %v", err)
	}
	if grant.GrantID == "" {
		t.Fatalf("CreateAuthorityGrant returned grant with empty GrantID")
	}

	got, err := store.GetAuthorityGrant(ctx, grant.GrantID)
	if err != nil {
		t.Fatalf("GetAuthorityGrant: %v", err)
	}
	if got.GrantID != grant.GrantID {
		t.Fatalf("GetAuthorityGrant: got grant_id %q, want %q", got.GrantID, grant.GrantID)
	}
	if got.MaxAccessLevel != acl.AccessRead {
		t.Fatalf("GetAuthorityGrant: max_access_level=%d, want %d", got.MaxAccessLevel, acl.AccessRead)
	}

	grants, err := store.ListAuthorityGrants(ctx, acl.AuthorityGrantFilter{
		SubjectType: acl.PrincipalTypeForModel(subject.Type),
		SubjectID:   subject.CanonicalPrincipalID(),
	})
	if err != nil {
		t.Fatalf("ListAuthorityGrants: %v", err)
	}
	if !containsGrant(grants, grant.GrantID) {
		t.Fatalf("ListAuthorityGrants did not include grant %q (got %d grants)", grant.GrantID, len(grants))
	}

	if err := store.RevokeAuthorityGrant(ctx, grant.GrantID); err != nil {
		t.Fatalf("RevokeAuthorityGrant: %v", err)
	}

	revoked, err := store.GetAuthorityGrant(ctx, grant.GrantID)
	if err != nil {
		t.Fatalf("GetAuthorityGrant after revoke: %v", err)
	}
	if !revoked.Revoked {
		t.Fatalf("expected grant to be marked revoked after RevokeAuthorityGrant")
	}
	if revoked.RevokedAt == nil {
		t.Fatalf("expected RevokedAt to be populated after revoke")
	}
}

// runCleanupExpiredRules verifies GrantAccess with a past ExpiresAt is
// removed by CleanupExpiredRules. Skipped on backends without the
// cleanup_expired_acl_rules() PG stored function (sqlite Stage 1).
func runCleanupExpiredRules(t *testing.T, store acl.Store) {
	t.Helper()
	ctx := context.Background()

	principalID := uniqueID(t, "expired")
	resourceID := uniqueID(t, "ws-expired")
	past := time.Now().Add(-1 * time.Hour)

	if _, err := store.GrantAccess(ctx,
		acl.PrincipalTypeUser, principalID,
		acl.ResourceTypeWorkspace, resourceID,
		acl.AccessRead, "_system", "conformance expired", &past,
	); err != nil {
		t.Fatalf("GrantAccess: %v", err)
	}

	deleted, err := store.CleanupExpiredRules(ctx)
	if err != nil {
		t.Fatalf("CleanupExpiredRules: %v", err)
	}
	if deleted < 1 {
		t.Fatalf("expected at least 1 deleted expired rule, got %d", deleted)
	}

	_, err = store.GetRule(ctx, acl.PrincipalTypeUser, principalID, acl.ResourceTypeWorkspace, resourceID)
	if err != acl.ErrRuleNotFound {
		t.Fatalf("GetRule after cleanup: expected ErrRuleNotFound, got %v", err)
	}
}

// =============================================================================
// Backend factories
// =============================================================================

// postgresFactory connects to the dev postgres instance via testutil.
// Skips when the dev infra isn't reachable.
func postgresFactory(t *testing.T) (acl.Store, backendCaps, func()) {
	t.Helper()
	testDB, cleanupDB := testutil.SetupTestDB(t)
	if testDB == nil {
		return nil, backendCaps{}, func() {}
	}

	gatewayID := fmt.Sprintf("conformance-gw-%d", time.Now().UnixNano())
	cfg := legacyaudit.DefaultConfig()
	cfg.BatchSize = 1
	cfg.FlushPeriod = 50 * time.Millisecond
	cfg.ChannelBuffer = 16
	sharedAudit := legacyaudit.NewAuditLogger(testDB.DB, gatewayID, cfg)
	store := aclpg.NewWithSharedAudit(testDB.DB, sharedAudit, testDB.DB, gatewayID)

	cleanup := func() {
		_ = store.Close()
		_ = sharedAudit.Close()
		cleanupDB()
	}
	return store, backendCaps{
		supportsAuditQuery:     true,
		supportsCleanupExpired: true,
		supportsCleanupAudit:   true,
		supportsFallbackUpsert: true,
	}, cleanup
}

// sqliteNativeFactory opens a fresh temp-dir SQLite database via the bare
// "sqlite" driver (modernc.org/sqlite) and constructs a native
// internal/storage/acl/sqlite.Store. This backend handles all SQL natively
// (no dbcompat translation) with inline time.Time parsing, Go-side UUID
// generation for fallback policy upserts, parameterized DELETEs replacing
// postgres stored functions, and a native acl_audit_log view.
//
// All caps are true: the native impl closes every Stage 1 gap.
func sqliteNativeFactory(t *testing.T) (acl.Store, backendCaps, func()) {
	t.Helper()

	// ACL state DB.
	aclDBPath := filepath.Join(t.TempDir(), "acl_native.db")
	aclDSN := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", aclDBPath)
	aclDB, err := sql.Open("sqlite", aclDSN)
	if err != nil {
		t.Fatalf("sql.Open sqlite (acl): %v", err)
	}
	// Single-writer pool per section 14.3 of the storage-interfaces plan.
	aclDB.SetMaxOpenConns(1)

	// Audit DB (separate file, mirrors lite-mode audit.db split).
	auditDBPath := filepath.Join(t.TempDir(), "audit_native.db")
	auditDSN := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", auditDBPath)
	auditDB, err := sql.Open("sqlite", auditDSN)
	if err != nil {
		_ = aclDB.Close()
		t.Fatalf("sql.Open sqlite (audit): %v", err)
	}
	auditDB.SetMaxOpenConns(1)

	// Bootstrap the comprehensive_audit_log table in the audit DB so the
	// shared audit writer has somewhere to INSERT and the acl_audit_log
	// view (created by the Store constructor) has a base table.
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

	gatewayID := fmt.Sprintf("conformance-gw-%d", time.Now().UnixNano())
	cfg := legacyaudit.DefaultConfig()
	cfg.BatchSize = 1
	cfg.FlushPeriod = 50 * time.Millisecond
	cfg.ChannelBuffer = 16
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
	return store, backendCaps{
		supportsAuditQuery:     true,
		supportsCleanupExpired: true,
		supportsCleanupAudit:   true,
		supportsFallbackUpsert: true,
	}, cleanup
}

// =============================================================================
// Helpers
// =============================================================================

// uniqueID returns a per-test-per-call identifier that won't collide
// with rows other tests inserted into the shared dev database.
func uniqueID(t *testing.T, hint string) string {
	t.Helper()
	return fmt.Sprintf("conformance-%s-%d", hint, time.Now().UnixNano())
}

func containsRule(rules []*acl.Rule, resourceID string) bool {
	for _, r := range rules {
		if r.ResourceID == resourceID {
			return true
		}
	}
	return false
}

func containsGrant(grants []*acl.AuthorityGrant, grantID string) bool {
	for _, g := range grants {
		if g.GrantID == grantID {
			return true
		}
	}
	return false
}
