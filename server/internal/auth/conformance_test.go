// Package auth_test contains the cross-backend conformance suite for
// auth.APITokenStore. The same test cases run against every implementation —
// postgres and sqlite_native. Drift between implementations gets caught here.
//
// The postgres subtest skips when DATABASE_URL / dev infra is unavailable;
// the sqlite_native subtest is always runnable since it spins up a temp-dir
// SQLite file.
package auth_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"database/sql"

	"github.com/scitrera/aether/internal/auth"
	authsqlite "github.com/scitrera/aether/internal/auth/sqlite"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/crypto"
)

// storeFactory builds a Store and returns a cleanup func.
type storeFactory func(t *testing.T) (store auth.APITokenStore, cleanup func())

func init() {
	// Initialize HMAC key for token hashing in tests.
	crypto.InitTokenHMAC([]byte("test-conformance-hmac-key-32bytes!"))
}

func TestAPITokenStoreConformance(t *testing.T) {
	backends := []struct {
		name    string
		factory storeFactory
	}{
		{name: "postgres", factory: postgresTokenFactory},
		{name: "sqlite_native", factory: sqliteNativeTokenFactory},
	}

	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			t.Run("CreateAndGetToken", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runCreateAndGetToken(t, store)
			})
			t.Run("ValidateToken_Roundtrip", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runValidateTokenRoundtrip(t, store)
			})
			t.Run("ValidateToken_Revoked", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runValidateTokenRevoked(t, store)
			})
			t.Run("ValidateToken_Expired", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runValidateTokenExpired(t, store)
			})
			t.Run("ListTokens_Pagination", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runListTokensPagination(t, store)
			})
			t.Run("ListTokens_ExcludeRevoked", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runListTokensExcludeRevoked(t, store)
			})
			t.Run("DeleteToken", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runDeleteToken(t, store)
			})
			t.Run("RevokeToken_NotFound", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runRevokeTokenNotFound(t, store)
			})
			t.Run("DeleteToken_NotFound", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runDeleteTokenNotFound(t, store)
			})
			t.Run("GetToken_NotFound", func(t *testing.T) {
				store, cleanup := b.factory(t)
				defer cleanup()
				runGetTokenNotFound(t, store)
			})
			t.Run("WorkspacePatternMatching", func(t *testing.T) {
				// This tests the free function, not the store, but
				// we include it in the conformance suite for completeness.
				runWorkspacePatternMatching(t)
			})
		})
	}
}

// =============================================================================
// Test implementations
// =============================================================================

func runCreateAndGetToken(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTokenTag(t, "create")
	patterns := []string{"prod-*", "staging"}
	scopes := []string{"connect", "read"}
	expires := time.Now().Add(24 * time.Hour)

	result, err := store.CreateToken(ctx, tag, "Agent", patterns, scopes, "admin", &expires)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if result.Token == "" {
		t.Fatal("expected non-empty plaintext token")
	}
	if result.APIToken.ID == "" {
		t.Fatal("expected non-empty token ID")
	}
	if result.APIToken.Name != tag {
		t.Errorf("Name = %q, want %q", result.APIToken.Name, tag)
	}
	if result.APIToken.PrincipalType != "Agent" {
		t.Errorf("PrincipalType = %q, want Agent", result.APIToken.PrincipalType)
	}
	if len(result.APIToken.WorkspacePatterns) != 2 {
		t.Errorf("WorkspacePatterns len = %d, want 2", len(result.APIToken.WorkspacePatterns))
	}
	if len(result.APIToken.Scopes) != 2 {
		t.Errorf("Scopes len = %d, want 2", len(result.APIToken.Scopes))
	}
	if result.APIToken.Revoked {
		t.Error("newly created token should not be revoked")
	}
	if result.APIToken.ExpiresAt == nil {
		t.Error("expected non-nil ExpiresAt")
	}

	// Retrieve by ID.
	got, err := store.GetToken(ctx, result.APIToken.ID)
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if got.Name != tag {
		t.Errorf("GetToken Name = %q, want %q", got.Name, tag)
	}
	if got.TokenHash != "" {
		t.Error("GetToken should clear TokenHash")
	}
	if got.PrincipalType != "Agent" {
		t.Errorf("GetToken PrincipalType = %q, want Agent", got.PrincipalType)
	}
	if len(got.WorkspacePatterns) != 2 {
		t.Errorf("GetToken WorkspacePatterns len = %d, want 2", len(got.WorkspacePatterns))
	}
}

func runValidateTokenRoundtrip(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTokenTag(t, "validate")
	result, err := store.CreateToken(ctx, tag, "User", []string{"*"}, []string{"connect"}, "admin", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	// Validate the plaintext token.
	validated, err := store.ValidateToken(ctx, result.Token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if validated.ID != result.APIToken.ID {
		t.Errorf("validated ID = %q, want %q", validated.ID, result.APIToken.ID)
	}
	if validated.TokenHash != "" {
		t.Error("ValidateToken should clear TokenHash")
	}
	if validated.LastUsedAt == nil {
		t.Error("ValidateToken should set LastUsedAt")
	}
}

func runValidateTokenRevoked(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTokenTag(t, "revoked")
	result, err := store.CreateToken(ctx, tag, "Agent", []string{"*"}, []string{"connect"}, "admin", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	// Revoke the token.
	if err := store.RevokeToken(ctx, result.APIToken.ID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	// Validate should fail.
	_, err = store.ValidateToken(ctx, result.Token)
	if err == nil {
		t.Fatal("expected error validating revoked token")
	}

	// GetToken should show revoked state.
	got, err := store.GetToken(ctx, result.APIToken.ID)
	if err != nil {
		t.Fatalf("GetToken after revoke: %v", err)
	}
	if !got.Revoked {
		t.Error("expected Revoked=true after RevokeToken")
	}
	if got.RevokedAt == nil {
		t.Error("expected non-nil RevokedAt after RevokeToken")
	}
}

func runValidateTokenExpired(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTokenTag(t, "expired")
	pastTime := time.Now().Add(-1 * time.Hour)
	result, err := store.CreateToken(ctx, tag, "Agent", []string{"*"}, []string{"connect"}, "admin", &pastTime)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	// Validate should fail because it's expired.
	_, err = store.ValidateToken(ctx, result.Token)
	if err == nil {
		t.Fatal("expected error validating expired token")
	}
}

func runListTokensPagination(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	// Create 3 tokens.
	for i := 0; i < 3; i++ {
		tag := uniqueTokenTag(t, fmt.Sprintf("list-%d", i))
		if _, err := store.CreateToken(ctx, tag, "Agent", []string{"*"}, []string{"connect"}, "admin", nil); err != nil {
			t.Fatalf("CreateToken %d: %v", i, err)
		}
	}

	// List with limit=2.
	tokens, err := store.ListTokens(ctx, 2, 0, true)
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	if len(tokens) != 2 {
		t.Errorf("ListTokens(limit=2) returned %d tokens, want 2", len(tokens))
	}

	// List with offset=2.
	tokens2, err := store.ListTokens(ctx, 10, 2, true)
	if err != nil {
		t.Fatalf("ListTokens offset: %v", err)
	}
	if len(tokens2) != 1 {
		t.Errorf("ListTokens(offset=2) returned %d tokens, want 1", len(tokens2))
	}

	// Default limit (0 → 100).
	all, err := store.ListTokens(ctx, 0, 0, true)
	if err != nil {
		t.Fatalf("ListTokens default: %v", err)
	}
	if len(all) < 3 {
		t.Errorf("ListTokens(default) returned %d tokens, want >= 3", len(all))
	}
}

func runListTokensExcludeRevoked(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	tag1 := uniqueTokenTag(t, "excl-active")
	r1, err := store.CreateToken(ctx, tag1, "Agent", []string{"*"}, []string{"connect"}, "admin", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	tag2 := uniqueTokenTag(t, "excl-revoked")
	r2, err := store.CreateToken(ctx, tag2, "Agent", []string{"*"}, []string{"connect"}, "admin", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	if err := store.RevokeToken(ctx, r2.APIToken.ID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	// includeRevoked=false should exclude the revoked one.
	tokens, err := store.ListTokens(ctx, 100, 0, false)
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	for _, tok := range tokens {
		if tok.ID == r2.APIToken.ID {
			t.Error("revoked token should be excluded when includeRevoked=false")
		}
	}
	found := false
	for _, tok := range tokens {
		if tok.ID == r1.APIToken.ID {
			found = true
		}
	}
	if !found {
		t.Error("active token should be present when includeRevoked=false")
	}
}

func runDeleteToken(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	tag := uniqueTokenTag(t, "delete")
	result, err := store.CreateToken(ctx, tag, "Agent", []string{"*"}, []string{"connect"}, "admin", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	if err := store.DeleteToken(ctx, result.APIToken.ID); err != nil {
		t.Fatalf("DeleteToken: %v", err)
	}

	// GetToken should fail.
	_, err = store.GetToken(ctx, result.APIToken.ID)
	if err == nil {
		t.Fatal("expected error getting deleted token")
	}
}

func runRevokeTokenNotFound(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	err := store.RevokeToken(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error revoking nonexistent token")
	}
}

func runDeleteTokenNotFound(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	err := store.DeleteToken(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error deleting nonexistent token")
	}
}

func runGetTokenNotFound(t *testing.T, store auth.APITokenStore) {
	t.Helper()
	ctx := context.Background()

	_, err := store.GetToken(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error getting nonexistent token")
	}
}

func runWorkspacePatternMatching(t *testing.T) {
	t.Helper()

	tests := []struct {
		patterns  []string
		workspace string
		want      bool
	}{
		{[]string{}, "any", true},                          // empty = match all
		{[]string{"*"}, "anything", true},                  // wildcard
		{[]string{"prod-*"}, "prod-eu", true},              // glob match
		{[]string{"prod-*"}, "staging-eu", false},          // glob no-match
		{[]string{"staging", "prod-*"}, "prod-asia", true}, // multi-pattern
		{[]string{"production"}, "production", true},       // exact
		{[]string{"production"}, "staging", false},         // exact no-match
	}
	for _, tt := range tests {
		token := &auth.APIToken{WorkspacePatterns: tt.patterns}
		got := auth.MatchesWorkspace(token, tt.workspace)
		if got != tt.want {
			t.Errorf("MatchesWorkspace(%v, %q) = %v, want %v", tt.patterns, tt.workspace, got, tt.want)
		}
	}
}

// =============================================================================
// Backend factories
// =============================================================================

func postgresTokenFactory(t *testing.T) (auth.APITokenStore, func()) {
	t.Helper()
	testDB, cleanupDB := testutil.SetupTestDB(t)
	if testDB == nil {
		return nil, func() {}
	}

	store := auth.NewPostgresAPITokenStore(testDB.DB)
	cleanup := func() {
		cleanupDB()
	}
	return store, cleanup
}

func sqliteNativeTokenFactory(t *testing.T) (auth.APITokenStore, func()) {
	t.Helper()
	dbPath := fmt.Sprintf("file:%s/tokens.db?_journal_mode=WAL&_busy_timeout=5000",
		t.TempDir())
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}

	store, err := authsqlite.New(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("authsqlite.New: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
	}
	return store, cleanup
}

// =============================================================================
// Helpers
// =============================================================================

func uniqueTokenTag(t *testing.T, hint string) string {
	t.Helper()
	return fmt.Sprintf("conformance-%s-%s-%d", hint, t.Name(), time.Now().UnixNano())
}
