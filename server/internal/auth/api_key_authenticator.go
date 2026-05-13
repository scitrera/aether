package auth

import (
	"context"
	"fmt"

	"github.com/scitrera/aether/pkg/models"
)

// APIKeyAuthenticator validates API keys against the PostgreSQL token store
type APIKeyAuthenticator struct {
	store *APITokenStore
}

// NewAPIKeyAuthenticator creates a new API key authenticator backed by the given token store
func NewAPIKeyAuthenticator(store *APITokenStore) *APIKeyAuthenticator {
	return &APIKeyAuthenticator{store: store}
}

// Name returns the authenticator name
func (a *APIKeyAuthenticator) Name() string {
	return "api_key"
}

// Authenticate validates an API key from the credentials map.
// It checks both "api_key" and "x-api-key" keys (the Go SDK's WithAPIKey sets "x-api-key").
// Returns (nil, nil) if no API key credential is present.
// Returns (nil, error) if a key is present but invalid.
// Returns (result, nil) on successful authentication.
func (a *APIKeyAuthenticator) Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error) {
	// Check for API key in credentials — support both key names
	apiKey := credentials[CredKeyAPIKey]
	if apiKey == "" {
		apiKey = credentials[CredKeyXAPIKey]
	}
	if apiKey == "" {
		// No API key credential provided; this authenticator does not apply
		return nil, nil
	}

	// Validate the token against the store
	token, err := a.store.ValidateToken(ctx, apiKey)
	if err != nil {
		return nil, fmt.Errorf("invalid API key: %w", err)
	}

	// Parse the principal type from the token's stored value
	principalType, err := parsePrincipalType(token.PrincipalType)
	if err != nil {
		return nil, fmt.Errorf("invalid principal type on token %q: %w", token.Name, err)
	}

	identity := models.Identity{
		Type: principalType,
		ID:   token.CreatedBy,
	}

	return &AuthResult{
		Authenticated: true,
		Identity:      identity,
		Method:        "api_key",
		Metadata: map[string]interface{}{
			"token_id":           token.ID,
			"token_name":         token.Name,
			"scopes":             token.Scopes,
			"workspace_patterns": token.WorkspacePatterns,
		},
	}, nil
}

// parsePrincipalType converts a string to a models.PrincipalType.
func parsePrincipalType(s string) (models.PrincipalType, error) {
	switch models.PrincipalType(s) {
	case models.PrincipalAgent:
		return models.PrincipalAgent, nil
	case models.PrincipalTask:
		return models.PrincipalTask, nil
	case models.PrincipalUser:
		return models.PrincipalUser, nil
	case models.PrincipalWorkflowEngine:
		return models.PrincipalWorkflowEngine, nil
	case models.PrincipalMetricsBridge:
		return models.PrincipalMetricsBridge, nil
	case models.PrincipalOrchestrator:
		return models.PrincipalOrchestrator, nil
	case models.PrincipalBridge:
		return models.PrincipalBridge, nil
	case models.PrincipalService:
		return models.PrincipalService, nil
	default:
		return "", fmt.Errorf("unknown principal type: %s", s)
	}
}
