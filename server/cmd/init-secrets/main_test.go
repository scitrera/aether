package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/aether/server/internal/acl"
	authsqlite "github.com/scitrera/aether/server/internal/auth/sqlite"
	"github.com/scitrera/aether/server/internal/config"
	"github.com/scitrera/aether/server/pkg/crypto"
	"github.com/scitrera/aether/server/pkg/models"
)

func init() {
	// Token hashing requires an initialized HMAC key. createTokenSQLite also
	// initializes it from cfg, but we set a stable key here so the post-create
	// validation (which re-hashes the plaintext) is consistent.
	crypto.InitTokenHMAC([]byte("init-secrets-test-hmac-key-32byte!"))
}

// TestCreateTokenSQLite_Direct exercises the SQLite-direct token-creation path
// used by aetherlite topologies: no PostgreSQL, no running gateway. It confirms
// the token is created, tokens.db is materialized, and the plaintext validates
// against the same store.
func TestCreateTokenSQLite_Direct(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()

	cfg := &config.Config{}
	cfg.Gateway.GatewayID = "test-init-secrets"
	cfg.Auth.TokenHMACKey = "init-secrets-test-hmac-key-32byte!"

	plaintext, err := createTokenSQLite(ctx, cfg, dataDir, "test-bootstrap", models.PrincipalMetricsBridge, acl.AccessAdmin)
	if err != nil {
		t.Fatalf("createTokenSQLite: %v", err)
	}
	if plaintext == "" {
		t.Fatal("expected non-empty plaintext token")
	}

	// tokens.db and acl.db must have been created under dataDir.
	for _, name := range []string{"tokens.db", "acl.db"} {
		if _, statErr := os.Stat(filepath.Join(dataDir, name)); statErr != nil {
			t.Errorf("expected %s to exist: %v", name, statErr)
		}
	}

	// Re-open the token store and validate the minted plaintext token.
	tokensDB, err := openSQLiteNative(ctx, filepath.Join(dataDir, "tokens.db"))
	if err != nil {
		t.Fatalf("reopen tokens.db: %v", err)
	}
	defer tokensDB.Close()

	store, err := authsqlite.New(tokensDB)
	if err != nil {
		t.Fatalf("authsqlite.New: %v", err)
	}

	tok, err := store.ValidateToken(ctx, plaintext)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if tok.Name != "test-bootstrap" {
		t.Errorf("token Name = %q, want %q", tok.Name, "test-bootstrap")
	}
	if tok.PrincipalType != string(models.PrincipalMetricsBridge) {
		t.Errorf("token PrincipalType = %q, want %q", tok.PrincipalType, models.PrincipalMetricsBridge)
	}
	if tok.CreatedBy != acl.SystemPrincipal {
		t.Errorf("token CreatedBy = %q, want %q", tok.CreatedBy, acl.SystemPrincipal)
	}
}

// TestCreateTokenSQLite_RequiresDataDir confirms the SQLite path rejects an
// empty data dir rather than silently writing to the working directory.
func TestCreateTokenSQLite_RequiresDataDir(t *testing.T) {
	cfg := &config.Config{}
	_, err := createTokenSQLite(context.Background(), cfg, "", "x", models.PrincipalUser, acl.AccessAdmin)
	if err == nil {
		t.Fatal("expected error when data-dir is empty")
	}
}
