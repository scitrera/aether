package state

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func newBadgerTokenStore(t *testing.T) *BadgerTokenStore {
	t.Helper()
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewBadgerTokenStore(db)
}

func TestBadgerToken_GenerateAndValidate(t *testing.T) {
	ctx := context.Background()
	store := newBadgerTokenStore(t)

	tok, err := store.GenerateToken(ctx, "task1", "ag::test::impl::spec", "test", "orch1")
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if tok.Token == "" {
		t.Error("expected non-empty Token field after generation")
	}

	// Validate using the plaintext token string.
	validated, err := store.ValidateToken(ctx, tok.Token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}

	// Plaintext token must be cleared in the returned result (security).
	if validated.Token != "" {
		t.Error("expected Token field to be empty in validated result")
	}
	if validated.TaskID != "task1" {
		t.Errorf("TaskID: got %q, want %q", validated.TaskID, "task1")
	}
	if validated.Revoked {
		t.Error("expected Revoked=false for freshly generated token")
	}
}

func TestBadgerToken_Revoke(t *testing.T) {
	ctx := context.Background()
	store := newBadgerTokenStore(t)

	tok, err := store.GenerateToken(ctx, "task2", "ag::test::impl::spec", "test", "orch1")
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	tokenStr := tok.Token

	// Revoke the token.
	if err := store.RevokeToken(ctx, tokenStr); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	// Validate should now return an error.
	if _, err := store.ValidateToken(ctx, tokenStr); err == nil {
		t.Error("expected error validating revoked token, got nil")
	}
}

func TestBadgerToken_RevokeForTask(t *testing.T) {
	ctx := context.Background()
	store := newBadgerTokenStore(t)

	tok1, err := store.GenerateToken(ctx, "task3", "ag::test::impl::spec1", "test", "orch1")
	if err != nil {
		t.Fatalf("GenerateToken tok1: %v", err)
	}
	tok2, err := store.GenerateToken(ctx, "task3", "ag::test::impl::spec2", "test", "orch1")
	if err != nil {
		t.Fatalf("GenerateToken tok2: %v", err)
	}

	// Revoke all tokens for the task.
	if err := store.RevokeTokensForTask(ctx, "task3"); err != nil {
		t.Fatalf("RevokeTokensForTask: %v", err)
	}

	// Both tokens should now fail validation.
	if _, err := store.ValidateToken(ctx, tok1.Token); err == nil {
		t.Error("tok1: expected error after RevokeTokensForTask, got nil")
	}
	if _, err := store.ValidateToken(ctx, tok2.Token); err == nil {
		t.Error("tok2: expected error after RevokeTokensForTask, got nil")
	}
}

func TestBadgerToken_ListForTask(t *testing.T) {
	ctx := context.Background()
	store := newBadgerTokenStore(t)

	// Generate 2 tokens for task1.
	if _, err := store.GenerateToken(ctx, "task1", "ag::test::impl::spec1", "test", "orch1"); err != nil {
		t.Fatalf("GenerateToken 1: %v", err)
	}
	if _, err := store.GenerateToken(ctx, "task1", "ag::test::impl::spec2", "test", "orch1"); err != nil {
		t.Fatalf("GenerateToken 2: %v", err)
	}

	// ListTokensForTask "task1" — should return 2.
	tokens, err := store.ListTokensForTask(ctx, "task1")
	if err != nil {
		t.Fatalf("ListTokensForTask task1: %v", err)
	}
	if len(tokens) != 2 {
		t.Errorf("task1: got %d tokens, want 2", len(tokens))
	}

	// ListTokensForTask "task2" — should return 0.
	tokens2, err := store.ListTokensForTask(ctx, "task2")
	if err != nil {
		t.Fatalf("ListTokensForTask task2: %v", err)
	}
	if len(tokens2) != 0 {
		t.Errorf("task2: got %d tokens, want 0", len(tokens2))
	}
}
