package auth

import (
	"context"
	"fmt"

	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/pkg/models"
)

// TaskTokenAuthenticator validates short-lived task auth tokens against the
// orchestration token store (Redis/Badger, HMAC-SHA256 hashed at rest, 24h
// TTL). Tokens are minted by the gateway on CreateTask when the caller
// declares a target_identity AND passes the issue-token ACL gate (see
// orchestration_integration.go::maybeIssueTaskToken). Once minted the token
// authenticates AS the declared TaskAuthToken.TargetIdentity for the life
// of the task — the worker presents it via Credentials{"token": ...} at
// connection init.
//
// Precedence (per spec.md §3.2): mTLS identity > Task token > API key /
// OAuth. The composite chain enforces this by ordering: this
// authenticator must be inserted BEFORE the API key authenticator.
type TaskTokenAuthenticator struct {
	store state.TokenStore
}

// NewTaskTokenAuthenticator wraps a state.TokenStore for use in the auth
// chain. nil store → the authenticator is a no-op (returns (nil, nil) for
// every request) so deployments without orchestration wired up still
// boot cleanly.
func NewTaskTokenAuthenticator(store state.TokenStore) *TaskTokenAuthenticator {
	return &TaskTokenAuthenticator{store: store}
}

// Name returns the authenticator name used in audit logs and chain debug
// output.
func (a *TaskTokenAuthenticator) Name() string { return "task_token" }

// Authenticate consumes the "token" or "task_token" credential, validates
// it against the token store, and resolves the bound TargetIdentity into
// a models.Identity. Returns (nil, nil) when no relevant credential is
// present so the composite chain can fall through to the next
// authenticator.
func (a *TaskTokenAuthenticator) Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error) {
	if a.store == nil {
		return nil, nil
	}
	tokenStr := credentials[CredKeyToken]
	if tokenStr == "" {
		// Forward-compat: accept the explicit task_token key too. The
		// existing CredKeyToken constant is what current sidecar/SDK
		// callers actually send; CredKeyTaskToken is reserved for callers
		// that want to disambiguate from API-key-style "token" usage.
		tokenStr = credentials[CredKeyTaskToken]
	}
	if tokenStr == "" {
		return nil, nil
	}

	token, err := a.store.ValidateToken(ctx, tokenStr)
	if err != nil {
		return nil, fmt.Errorf("invalid task token: %w", err)
	}

	// TargetIdentity is the canonical identity-string form
	// ("ag::workspace::impl::spec"). Parse it back into a structured
	// Identity so downstream auth/ACL machinery sees the same shape it
	// gets from API key / OAuth / mTLS authenticators.
	identity, err := models.ParseIdentity(token.TargetIdentity)
	if err != nil {
		return nil, fmt.Errorf("task token has malformed target_identity %q: %w", token.TargetIdentity, err)
	}

	return &AuthResult{
		Authenticated: true,
		Identity:      identity,
		Method:        "task_token",
		Metadata: map[string]interface{}{
			"task_id":         token.TaskID,
			"workspace":       token.Workspace,
			"orchestrator_id": token.OrchestratorID,
		},
	}, nil
}
