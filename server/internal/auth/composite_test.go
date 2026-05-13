package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/scitrera/aether/pkg/models"
)

// mockAuthenticator implements the Authenticator interface for testing.
type mockAuthenticator struct {
	name   string
	result *AuthResult
	err    error
}

func (m *mockAuthenticator) Name() string { return m.name }
func (m *mockAuthenticator) Authenticate(_ context.Context, _ map[string]string) (*AuthResult, error) {
	return m.result, m.err
}

func TestCompositeAuthenticator_FirstMatch(t *testing.T) {
	auth1 := &mockAuthenticator{
		name: "first",
		result: &AuthResult{
			Authenticated: true,
			Identity:      models.Identity{Type: models.PrincipalUser, ID: "user-1"},
			Method:        "first",
		},
	}
	auth2 := &mockAuthenticator{
		name: "second",
		result: &AuthResult{
			Authenticated: true,
			Identity:      models.Identity{Type: models.PrincipalUser, ID: "user-2"},
			Method:        "second",
		},
	}

	composite := NewCompositeAuthenticator(auth1, auth2)
	result, err := composite.Authenticate(context.Background(), map[string]string{"key": "val"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Identity.ID != "user-1" {
		t.Errorf("expected first authenticator to win, got ID=%s", result.Identity.ID)
	}
}

func TestCompositeAuthenticator_SkipsNonApplicable(t *testing.T) {
	// First returns nil,nil (doesn't apply)
	auth1 := &mockAuthenticator{name: "skip", result: nil, err: nil}
	auth2 := &mockAuthenticator{
		name: "match",
		result: &AuthResult{
			Authenticated: true,
			Identity:      models.Identity{Type: models.PrincipalAgent, ID: "agent-1"},
			Method:        "match",
		},
	}

	composite := NewCompositeAuthenticator(auth1, auth2)
	result, err := composite.Authenticate(context.Background(), map[string]string{"key": "val"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Identity.ID != "agent-1" {
		t.Errorf("expected second authenticator, got ID=%s", result.Identity.ID)
	}
}

func TestCompositeAuthenticator_AllFail(t *testing.T) {
	auth1 := &mockAuthenticator{name: "fail1", result: nil, err: fmt.Errorf("bad key")}
	auth2 := &mockAuthenticator{name: "fail2", result: nil, err: fmt.Errorf("bad token")}

	composite := NewCompositeAuthenticator(auth1, auth2)
	result, err := composite.Authenticate(context.Background(), map[string]string{"key": "val"})

	if err == nil {
		t.Fatal("expected error when all authenticators fail")
	}
	if result != nil {
		t.Error("expected nil result")
	}
}

func TestCompositeAuthenticator_NoneMatch(t *testing.T) {
	auth1 := &mockAuthenticator{name: "skip1", result: nil, err: nil}
	auth2 := &mockAuthenticator{name: "skip2", result: nil, err: nil}

	composite := NewCompositeAuthenticator(auth1, auth2)
	result, err := composite.Authenticate(context.Background(), map[string]string{"key": "val"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil result when no authenticator matches")
	}
}

func TestCompositeAuthenticator_FailThenMatch(t *testing.T) {
	// First fails with error, second succeeds
	auth1 := &mockAuthenticator{name: "fail", result: nil, err: fmt.Errorf("invalid")}
	auth2 := &mockAuthenticator{
		name: "success",
		result: &AuthResult{
			Authenticated: true,
			Identity:      models.Identity{Type: models.PrincipalUser, ID: "user-ok"},
			Method:        "success",
		},
	}

	composite := NewCompositeAuthenticator(auth1, auth2)
	result, err := composite.Authenticate(context.Background(), map[string]string{"key": "val"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Identity.ID != "user-ok" {
		t.Errorf("expected second authenticator to succeed, got ID=%s", result.Identity.ID)
	}
}

func TestCompositeAuthenticator_Empty(t *testing.T) {
	composite := NewCompositeAuthenticator()
	result, err := composite.Authenticate(context.Background(), map[string]string{"key": "val"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil result from empty composite")
	}
}
