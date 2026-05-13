package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/pkg/models"
)

// fakeTokenStore is a hand-rolled state.TokenStore for unit-testing the
// TaskTokenAuthenticator without standing up Redis. We can't use a stock
// mock here because state.TokenStore returns *state.TaskAuthToken (a
// concrete pointer), so we just stash a fixed response per token string.
type fakeTokenStore struct {
	byToken map[string]*state.TaskAuthToken
	err     error
}

func (f *fakeTokenStore) GenerateToken(_ context.Context, taskID, targetIdentity, workspace, orchestratorID string) (*state.TaskAuthToken, error) {
	return nil, errors.New("unused")
}
func (f *fakeTokenStore) ValidateToken(_ context.Context, tokenStr string) (*state.TaskAuthToken, error) {
	if f.err != nil {
		return nil, f.err
	}
	if t, ok := f.byToken[tokenStr]; ok {
		return t, nil
	}
	return nil, errors.New("token not found")
}
func (f *fakeTokenStore) RevokeToken(_ context.Context, _ string) error         { return nil }
func (f *fakeTokenStore) RevokeTokensForTask(_ context.Context, _ string) error { return nil }
func (f *fakeTokenStore) ListTokensForTask(_ context.Context, _ string) ([]*state.TaskAuthToken, error) {
	return nil, nil
}

func TestTaskTokenAuthenticator_NilStore_ReturnsNoApply(t *testing.T) {
	a := NewTaskTokenAuthenticator(nil)
	res, err := a.Authenticate(context.Background(), map[string]string{CredKeyToken: "anything"})
	if err != nil || res != nil {
		t.Fatalf("nil store should produce (nil, nil); got (%v, %v)", res, err)
	}
}

func TestTaskTokenAuthenticator_NoCredential_ReturnsNoApply(t *testing.T) {
	a := NewTaskTokenAuthenticator(&fakeTokenStore{})
	res, err := a.Authenticate(context.Background(), map[string]string{"unrelated": "x"})
	if err != nil || res != nil {
		t.Fatalf("missing credential should produce (nil, nil); got (%v, %v)", res, err)
	}
}

func TestTaskTokenAuthenticator_InvalidToken_ReturnsError(t *testing.T) {
	store := &fakeTokenStore{byToken: map[string]*state.TaskAuthToken{}}
	a := NewTaskTokenAuthenticator(store)
	_, err := a.Authenticate(context.Background(), map[string]string{CredKeyToken: "bogus"})
	if err == nil {
		t.Fatal("invalid token should produce an error so the composite chain can reject it")
	}
}

func TestTaskTokenAuthenticator_ValidToken_ResolvesIdentity(t *testing.T) {
	store := &fakeTokenStore{
		byToken: map[string]*state.TaskAuthToken{
			"plaintext-xyz": {
				Token:          "plaintext-xyz",
				TaskID:         "task-1",
				TargetIdentity: "ag::_sandbox::sandbox-sidecar::sbx-001",
				Workspace:      "_sandbox",
				OrchestratorID: "ag::_apps::CoworkAgent::user@example.com",
			},
		},
	}
	a := NewTaskTokenAuthenticator(store)
	res, err := a.Authenticate(context.Background(), map[string]string{CredKeyToken: "plaintext-xyz"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil || !res.Authenticated {
		t.Fatalf("expected authenticated result, got %+v", res)
	}
	if res.Method != "task_token" {
		t.Errorf("Method = %q, want task_token", res.Method)
	}
	if res.Identity.Type != models.PrincipalAgent {
		t.Errorf("Identity.Type = %q, want Agent", res.Identity.Type)
	}
	if res.Identity.Workspace != "_sandbox" {
		t.Errorf("Identity.Workspace = %q, want _sandbox", res.Identity.Workspace)
	}
	if res.Identity.Implementation != "sandbox-sidecar" {
		t.Errorf("Identity.Implementation = %q, want sandbox-sidecar", res.Identity.Implementation)
	}
	if res.Identity.Specifier != "sbx-001" {
		t.Errorf("Identity.Specifier = %q, want sbx-001", res.Identity.Specifier)
	}
	if res.Metadata["task_id"] != "task-1" {
		t.Errorf("Metadata.task_id = %v, want task-1", res.Metadata["task_id"])
	}
}

func TestTaskTokenAuthenticator_AcceptsTaskTokenKey(t *testing.T) {
	// Forward-compat: callers may send the token under "task_token" instead
	// of the more generic "token" key. Both must validate.
	store := &fakeTokenStore{
		byToken: map[string]*state.TaskAuthToken{
			"plaintext-zzz": {
				Token:          "plaintext-zzz",
				TaskID:         "task-2",
				TargetIdentity: "ag::ws::impl::spec",
				Workspace:      "ws",
			},
		},
	}
	a := NewTaskTokenAuthenticator(store)
	res, err := a.Authenticate(context.Background(), map[string]string{CredKeyTaskToken: "plaintext-zzz"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected authenticated result for task_token credential key")
	}
	if res.Identity.Specifier != "spec" {
		t.Errorf("Identity.Specifier = %q, want spec", res.Identity.Specifier)
	}
}

func TestTaskTokenAuthenticator_MalformedTargetIdentity_ReturnsError(t *testing.T) {
	store := &fakeTokenStore{
		byToken: map[string]*state.TaskAuthToken{
			"x": {
				Token:          "x",
				TaskID:         "task-3",
				TargetIdentity: "garbage-not-a-valid-identity-string",
			},
		},
	}
	a := NewTaskTokenAuthenticator(store)
	_, err := a.Authenticate(context.Background(), map[string]string{CredKeyToken: "x"})
	if err == nil {
		t.Fatal("malformed TargetIdentity must surface as an authentication error so the chain can fall through")
	}
}

func TestTaskTokenAuthenticator_PrecedesAPIKeyInComposite(t *testing.T) {
	// Spec §3.2: mTLS > Task token > API key/OAuth. Confirm the chain
	// honors that order when both authenticators would match — since
	// they react to disjoint credential keys in practice this just
	// asserts the expected order returns the task-token result first.
	store := &fakeTokenStore{
		byToken: map[string]*state.TaskAuthToken{
			"tt": {
				Token:          "tt",
				TaskID:         "task-4",
				TargetIdentity: "ag::ws::impl::spec",
			},
		},
	}
	taskTokenAuth := NewTaskTokenAuthenticator(store)
	mockAPIKey := &mockAuthenticator{
		name: "api_key",
		result: &AuthResult{
			Authenticated: true,
			Identity:      models.Identity{Type: models.PrincipalUser, ID: "user-A"},
			Method:        "api_key",
		},
	}
	composite := NewCompositeAuthenticator(taskTokenAuth, mockAPIKey)
	res, err := composite.Authenticate(context.Background(), map[string]string{CredKeyToken: "tt"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil || res.Method != "task_token" {
		t.Fatalf("task_token must precede api_key in chain; got %+v", res)
	}
}
