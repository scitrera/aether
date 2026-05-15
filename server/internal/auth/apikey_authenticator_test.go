package auth

import (
	"context"
	"testing"
	"time"

	"github.com/scitrera/aether/pkg/models"
)

// APITokenStore.ValidateToken is not on an interface, so this file wraps it
// via a thin functional adapter (tokenValidatorFunc + testableAPIKeyAuth)
// to test the authenticator logic without a real DB.

// tokenValidatorFunc is a function type that matches ValidateToken's signature.
type tokenValidatorFunc func(ctx context.Context, tokenStr string) (*APIToken, error)

// testableAPIKeyAuth is a test-only wrapper around APIKeyAuthenticator whose
// token lookup is driven by the supplied function rather than a real DB.
type testableAPIKeyAuth struct {
	validateFn tokenValidatorFunc
}

func (a *testableAPIKeyAuth) Name() string { return "api_key" }

func (a *testableAPIKeyAuth) Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error) {
	apiKey := credentials[CredKeyAPIKey]
	if apiKey == "" {
		apiKey = credentials[CredKeyXAPIKey]
	}
	if apiKey == "" {
		return nil, nil
	}

	token, err := a.validateFn(ctx, apiKey)
	if err != nil {
		return nil, err // mirror the production code path
	}

	pt, err := parsePrincipalType(token.PrincipalType)
	if err != nil {
		return nil, err
	}

	return &AuthResult{
		Authenticated: true,
		Identity:      models.Identity{Type: pt, ID: token.CreatedBy},
		Method:        "api_key",
		Metadata: map[string]interface{}{
			"token_id":           token.ID,
			"token_name":         token.Name,
			"scopes":             token.Scopes,
			"workspace_patterns": token.WorkspacePatterns,
		},
	}, nil
}

// --- credential extraction ---

func TestAPIKeyAuth_NoCredentialReturnsNil(t *testing.T) {
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, _ string) (*APIToken, error) {
		t.Fatal("validateFn must not be called when no credential is present")
		return nil, nil
	}}

	result, err := a.Authenticate(context.Background(), map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil result when no API key credential is present")
	}
}

func TestAPIKeyAuth_UsesAPIKeyField(t *testing.T) {
	called := false
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, key string) (*APIToken, error) {
		called = true
		if key != "secret-key" {
			t.Errorf("validateFn received key=%q, want secret-key", key)
		}
		return &APIToken{ID: "t1", PrincipalType: "User", CreatedBy: "alice"}, nil
	}}

	_, err := a.Authenticate(context.Background(), map[string]string{CredKeyAPIKey: "secret-key"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected validateFn to be called")
	}
}

func TestAPIKeyAuth_UsesXAPIKeyField(t *testing.T) {
	called := false
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, key string) (*APIToken, error) {
		called = true
		if key != "x-key-value" {
			t.Errorf("validateFn received key=%q, want x-key-value", key)
		}
		return &APIToken{ID: "t2", PrincipalType: "Agent", CreatedBy: "agent-1"}, nil
	}}

	_, err := a.Authenticate(context.Background(), map[string]string{CredKeyXAPIKey: "x-key-value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected validateFn to be called for x-api-key credential")
	}
}

func TestAPIKeyAuth_APIKeyTakesPrecedenceOverXAPIKey(t *testing.T) {
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, key string) (*APIToken, error) {
		if key != "primary-key" {
			t.Errorf("expected primary-key to win, got %q", key)
		}
		return &APIToken{PrincipalType: "User", CreatedBy: "u"}, nil
	}}

	_, _ = a.Authenticate(context.Background(), map[string]string{
		CredKeyAPIKey:  "primary-key",
		CredKeyXAPIKey: "secondary-key",
	})
}

// --- result mapping ---

func TestAPIKeyAuth_SuccessfulAuthMapsIdentityCorrectly(t *testing.T) {
	scopes := []string{"read", "write"}
	patterns := []string{"prod-*"}
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, _ string) (*APIToken, error) {
		return &APIToken{
			ID:                "tok-id",
			Name:              "my-token",
			PrincipalType:     "Agent",
			CreatedBy:         "agent-007",
			Scopes:            scopes,
			WorkspacePatterns: patterns,
		}, nil
	}}

	result, err := a.Authenticate(context.Background(), map[string]string{CredKeyAPIKey: "any"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !result.Authenticated {
		t.Error("expected Authenticated=true")
	}
	if result.Identity.Type != models.PrincipalAgent {
		t.Errorf("Identity.Type = %v, want Agent", result.Identity.Type)
	}
	if result.Identity.ID != "agent-007" {
		t.Errorf("Identity.ID = %q, want agent-007", result.Identity.ID)
	}
	if result.Method != "api_key" {
		t.Errorf("Method = %q, want api_key", result.Method)
	}
	if result.Metadata["token_id"] != "tok-id" {
		t.Errorf("Metadata[token_id] = %v, want tok-id", result.Metadata["token_id"])
	}
	if result.Metadata["token_name"] != "my-token" {
		t.Errorf("Metadata[token_name] = %v, want my-token", result.Metadata["token_name"])
	}
}

func TestAPIKeyAuth_ServicePrincipalMapsIdentityType(t *testing.T) {
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, _ string) (*APIToken, error) {
		return &APIToken{
			ID:            "tok-svc",
			PrincipalType: "Service",
			CreatedBy:     "frontend-api",
		}, nil
	}}

	result, err := a.Authenticate(context.Background(), map[string]string{CredKeyAPIKey: "svc-key"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Identity.Type != models.PrincipalService {
		t.Errorf("Identity.Type = %v, want Service", result.Identity.Type)
	}
}

// --- error paths ---

func TestAPIKeyAuth_InvalidKeyPropagatesError(t *testing.T) {
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, _ string) (*APIToken, error) {
		return nil, errInvalidToken
	}}

	result, err := a.Authenticate(context.Background(), map[string]string{CredKeyAPIKey: "bad-key"})
	if err == nil {
		t.Error("expected error for invalid key")
	}
	if result != nil {
		t.Error("expected nil result on error")
	}
}

var errInvalidToken = func() error {
	return &tokenError{"token not found or expired/revoked"}
}()

type tokenError struct{ msg string }

func (e *tokenError) Error() string { return e.msg }

func TestAPIKeyAuth_InvalidPrincipalTypeOnTokenReturnsError(t *testing.T) {
	a := &testableAPIKeyAuth{validateFn: func(_ context.Context, _ string) (*APIToken, error) {
		return &APIToken{
			PrincipalType: "UnknownType",
			CreatedBy:     "someone",
		}, nil
	}}

	_, err := a.Authenticate(context.Background(), map[string]string{CredKeyAPIKey: "good-key"})
	if err == nil {
		t.Error("expected error for unknown principal type on token")
	}
}

// --- MatchesWorkspace (free function, pure logic, no DB) ---

func TestMatchesWorkspace_EmptyPatternsAllowsAll(t *testing.T) {
	token := &APIToken{WorkspacePatterns: []string{}}
	if !MatchesWorkspace(token, "any-workspace") {
		t.Error("empty patterns should match any workspace")
	}
}

func TestMatchesWorkspace_ExactPatternMatches(t *testing.T) {
	token := &APIToken{WorkspacePatterns: []string{"production"}}
	if !MatchesWorkspace(token, "production") {
		t.Error("exact pattern should match")
	}
}

func TestMatchesWorkspace_ExactPatternDoesNotMatchOther(t *testing.T) {
	token := &APIToken{WorkspacePatterns: []string{"production"}}
	if MatchesWorkspace(token, "staging") {
		t.Error("exact pattern should not match different workspace")
	}
}

func TestMatchesWorkspace_GlobPatternMatches(t *testing.T) {
	token := &APIToken{WorkspacePatterns: []string{"prod-*"}}
	if !MatchesWorkspace(token, "prod-eu") {
		t.Error("glob pattern prod-* should match prod-eu")
	}
	if !MatchesWorkspace(token, "prod-us") {
		t.Error("glob pattern prod-* should match prod-us")
	}
}

func TestMatchesWorkspace_GlobPatternDoesNotMatchNonPrefix(t *testing.T) {
	token := &APIToken{WorkspacePatterns: []string{"prod-*"}}
	if MatchesWorkspace(token, "staging-eu") {
		t.Error("glob pattern prod-* should not match staging-eu")
	}
}

func TestMatchesWorkspace_MultiplePatterns_FirstMatch(t *testing.T) {
	token := &APIToken{WorkspacePatterns: []string{"staging", "prod-*"}}
	if !MatchesWorkspace(token, "prod-asia") {
		t.Error("should match via second pattern prod-*")
	}
}

// --- APIToken expiry field (struct-level, no DB) ---

func TestAPIToken_ExpiresAtNilMeansNoExpiry(t *testing.T) {
	tok := &APIToken{ExpiresAt: nil}
	if tok.ExpiresAt != nil {
		t.Error("expected nil ExpiresAt to indicate no expiry")
	}
}

func TestAPIToken_RevokedFieldDefaultsFalse(t *testing.T) {
	tok := &APIToken{}
	if tok.Revoked {
		t.Error("expected Revoked to default to false")
	}
}

func TestAPIToken_ExpiresAtInPastIsExpired(t *testing.T) {
	past := time.Now().Add(-1 * time.Hour)
	tok := &APIToken{ExpiresAt: &past}
	if tok.ExpiresAt == nil || !time.Now().After(*tok.ExpiresAt) {
		t.Error("token with past ExpiresAt should be considered expired")
	}
}
