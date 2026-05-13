package auth

import (
	"context"

	"github.com/scitrera/aether/pkg/models"
)

// Credential key constants used across authenticators and SDK clients.
const (
	CredKeyAPIKey    = "api_key"
	CredKeyXAPIKey   = "x-api-key"
	CredKeyTaskToken = "task_token"
	CredKeyToken     = "token"
)

// AuthResult represents the result of an authentication attempt
type AuthResult struct {
	// Authenticated indicates whether authentication was successful
	Authenticated bool
	// Identity is the authenticated identity (set when Authenticated is true)
	Identity models.Identity
	// Method is the authentication method that succeeded (e.g., "api_key", "oauth", "task_token")
	Method string
	// Metadata contains additional auth-specific data (e.g., token name, OAuth claims)
	Metadata map[string]interface{}
}

// Authenticator validates credentials and returns an identity
type Authenticator interface {
	// Name returns the authenticator name (e.g., "api_key", "oauth")
	Name() string
	// Authenticate attempts to authenticate using the provided credentials.
	// Returns (nil, nil) if this authenticator doesn't apply (no matching credential keys).
	// Returns (nil, error) if credentials were provided but invalid.
	// Returns (result, nil) if authentication succeeded.
	Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error)
}
