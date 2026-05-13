package auth

import (
	"context"
	"fmt"

	"github.com/scitrera/aether/pkg/authproxy/login"
	"github.com/scitrera/aether/pkg/models"
)

// CredKeySession is the credentials-map key carrying an opaque session id
// (or signed JWT for the stateless store) read from the session cookie by
// the auth-proxy middleware.
const CredKeySession = "session_token"

// SessionAuthenticator validates a browser-issued session against a
// login.SessionStore and converts it into the canonical AuthResult shape.
//
// It plugs into the composite Authenticator chain alongside the API key,
// OAuth, Entra, and task-token authenticators. When the session_token
// credential is absent, Authenticate returns (nil, nil) so the next
// authenticator in the chain can run.
type SessionAuthenticator struct {
	store login.SessionStore
}

// NewSessionAuthenticator returns an authenticator backed by store. Pass the
// same store the login.Handlers were configured with.
func NewSessionAuthenticator(store login.SessionStore) *SessionAuthenticator {
	return &SessionAuthenticator{store: store}
}

// Name implements Authenticator.
func (a *SessionAuthenticator) Name() string { return "session" }

// Authenticate looks up the session id, returns AuthResult with Method =
// "session" and Metadata = the verified provider claims. Method scoping in
// the IdentityResolver layer keys off this name.
func (a *SessionAuthenticator) Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error) {
	id, ok := credentials[CredKeySession]
	if !ok || id == "" {
		return nil, nil
	}
	if a.store == nil {
		return nil, fmt.Errorf("session authenticator: no store configured")
	}
	data, err := a.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("session lookup: %w", err)
	}
	if data == nil {
		// Cookie present but unknown to the store — treat as auth failure
		// so the middleware can return 401 and the browser can be redirected
		// to /auth/login by upstream nginx config.
		return nil, fmt.Errorf("session not found or expired")
	}

	metadata := map[string]any{}
	for k, v := range data.Claims {
		metadata[k] = v
	}
	// Convenience top-level fields that the resolver layer commonly reads.
	if data.Email != "" {
		metadata["email"] = data.Email
	}
	if data.Name != "" {
		metadata["name"] = data.Name
	}
	metadata["provider"] = data.Provider
	metadata["session_user_id"] = data.UserID

	return &AuthResult{
		Authenticated: true,
		Identity: models.Identity{
			Type: models.PrincipalUser,
			ID:   data.UserID,
		},
		Method:   "session",
		Metadata: metadata,
	}, nil
}
