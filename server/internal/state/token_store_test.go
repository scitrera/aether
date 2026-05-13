package state

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/pkg/crypto"
)

func TestMain(m *testing.M) {
	// Token hashing requires an HMAC key; use a fixed test key so unit tests
	// are hermetic and do not depend on server startup.
	crypto.InitTokenHMAC([]byte("test-hmac-key-32-bytes-long-here"))
	m.Run()
}

// newTestTokenStore starts an in-process miniredis and returns a TokenStore backed by it.
// The caller must close the returned *miniredis.Miniredis when done.
func newTestTokenStore(t *testing.T) (*RedisTokenStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewRedisTokenStore(client), mr
}

func TestTokenStore_GenerateToken_returnsTokenWithFields(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tok, err := store.GenerateToken(ctx, "task-1", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}
	if tok.Token == "" {
		t.Error("Token field should be non-empty on creation")
	}
	if tok.TokenHash == "" {
		t.Error("TokenHash should be set")
	}
	if tok.TaskID != "task-1" {
		t.Errorf("TaskID = %q, want %q", tok.TaskID, "task-1")
	}
	if tok.TargetIdentity != "ag::ws::impl::spec" {
		t.Errorf("TargetIdentity = %q, want %q", tok.TargetIdentity, "ag::ws::impl::spec")
	}
	if tok.Workspace != "ws" {
		t.Errorf("Workspace = %q, want %q", tok.Workspace, "ws")
	}
	if tok.OrchestratorID != "orch-1" {
		t.Errorf("OrchestratorID = %q, want %q", tok.OrchestratorID, "orch-1")
	}
	if tok.Revoked {
		t.Error("Revoked should be false on creation")
	}
}

func TestTokenStore_GenerateToken_eachCallProducesUniqueToken(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tok1, err := store.GenerateToken(ctx, "task-1", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("first GenerateToken() error = %v", err)
	}
	tok2, err := store.GenerateToken(ctx, "task-1", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("second GenerateToken() error = %v", err)
	}
	if tok1.Token == tok2.Token {
		t.Error("two generated tokens should not be equal")
	}
	if tok1.TokenHash == tok2.TokenHash {
		t.Error("two token hashes should not be equal")
	}
}

func TestTokenStore_ValidateToken_returnsDataForValidToken(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tok, err := store.GenerateToken(ctx, "task-99", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	got, err := store.ValidateToken(ctx, tok.Token)
	if err != nil {
		t.Fatalf("ValidateToken() error = %v", err)
	}
	if got.TaskID != "task-99" {
		t.Errorf("TaskID = %q, want %q", got.TaskID, "task-99")
	}
	// Token field must be cleared for security on validate
	if got.Token != "" {
		t.Error("Token field should be empty after ValidateToken (security)")
	}
}

func TestTokenStore_ValidateToken_errorsOnUnknownToken(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	_, err := store.ValidateToken(ctx, "completely-made-up-token-value")
	if err == nil {
		t.Error("ValidateToken() should return error for unknown token")
	}
}

func TestTokenStore_ValidateToken_errorsOnRevokedToken(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tok, err := store.GenerateToken(ctx, "task-1", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	if err := store.RevokeToken(ctx, tok.Token); err != nil {
		t.Fatalf("RevokeToken() error = %v", err)
	}

	_, err = store.ValidateToken(ctx, tok.Token)
	if err == nil {
		t.Error("ValidateToken() should return error for revoked token")
	}
}

func TestTokenStore_RevokeToken_isIdempotent(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tok, err := store.GenerateToken(ctx, "task-1", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	if err := store.RevokeToken(ctx, tok.Token); err != nil {
		t.Fatalf("first RevokeToken() error = %v", err)
	}
	// Second revocation should not error
	if err := store.RevokeToken(ctx, tok.Token); err != nil {
		t.Errorf("second RevokeToken() error = %v, want nil", err)
	}
}

func TestTokenStore_RevokeToken_onNonExistentTokenIsNoOp(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	err := store.RevokeToken(ctx, "nonexistent-token-value")
	if err != nil {
		t.Errorf("RevokeToken() on nonexistent token error = %v, want nil", err)
	}
}

func TestTokenStore_RevokeTokensForTask_revokesAllTaskTokens(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tok1, err := store.GenerateToken(ctx, "task-multi", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() tok1 error = %v", err)
	}
	tok2, err := store.GenerateToken(ctx, "task-multi", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() tok2 error = %v", err)
	}

	if err := store.RevokeTokensForTask(ctx, "task-multi"); err != nil {
		t.Fatalf("RevokeTokensForTask() error = %v", err)
	}

	if _, err := store.ValidateToken(ctx, tok1.Token); err == nil {
		t.Error("tok1 should be revoked after RevokeTokensForTask()")
	}
	if _, err := store.ValidateToken(ctx, tok2.Token); err == nil {
		t.Error("tok2 should be revoked after RevokeTokensForTask()")
	}
}

func TestTokenStore_RevokeTokensForTask_onEmptyTaskIsNoOp(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	err := store.RevokeTokensForTask(ctx, "task-with-no-tokens")
	if err != nil {
		t.Errorf("RevokeTokensForTask() on empty task error = %v, want nil", err)
	}
}

func TestTokenStore_ListTokensForTask_returnsAllTaskTokens(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	_, err := store.GenerateToken(ctx, "task-list", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() #1 error = %v", err)
	}
	_, err = store.GenerateToken(ctx, "task-list", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() #2 error = %v", err)
	}

	tokens, err := store.ListTokensForTask(ctx, "task-list")
	if err != nil {
		t.Fatalf("ListTokensForTask() error = %v", err)
	}
	if len(tokens) != 2 {
		t.Errorf("ListTokensForTask() returned %d tokens, want 2", len(tokens))
	}
	for _, tok := range tokens {
		// Token plaintext should not be exposed in listings
		if tok.Token != "" {
			t.Error("ListTokensForTask() should not expose plaintext token value")
		}
	}
}

func TestTokenStore_ListTokensForTask_returnsNilForUnknownTask(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tokens, err := store.ListTokensForTask(ctx, "unknown-task")
	if err != nil {
		t.Fatalf("ListTokensForTask() error = %v", err)
	}
	if tokens != nil {
		t.Errorf("ListTokensForTask() = %v, want nil for unknown task", tokens)
	}
}

func TestTokenStore_ListTokensForTask_includesRevokedTokensByDefault(t *testing.T) {
	store, _ := newTestTokenStore(t)
	ctx := context.Background()

	tok, err := store.GenerateToken(ctx, "task-revlist", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}
	if err := store.RevokeToken(ctx, tok.Token); err != nil {
		t.Fatalf("RevokeToken() error = %v", err)
	}

	tokens, err := store.ListTokensForTask(ctx, "task-revlist")
	if err != nil {
		t.Fatalf("ListTokensForTask() error = %v", err)
	}
	// The revoked token was stored with a 1h TTL so it should still appear
	if len(tokens) != 1 {
		t.Errorf("ListTokensForTask() returned %d tokens, want 1 (revoked included)", len(tokens))
	}
	if !tokens[0].Revoked {
		t.Error("token in list should be marked Revoked=true")
	}
}

func TestTokenStore_TokenTTL_isSetOnGenerate(t *testing.T) {
	store, mr := newTestTokenStore(t)
	ctx := context.Background()

	tok, err := store.GenerateToken(ctx, "task-ttl", "ag::ws::impl::spec", "ws", "orch-1")
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	tokenKey := tokenKeyPrefix + tok.TokenHash
	ttl := mr.TTL(tokenKey)
	if ttl <= 0 || ttl > maxTokenTTL+time.Second {
		t.Errorf("token TTL = %v, want ~%v", ttl, maxTokenTTL)
	}
}
