package auth

import (
	"context"
	"path/filepath"
	"time"
)

// APITokenStore is the storage interface for long-lived API tokens.
// Implementations must hash tokens via pkg/crypto (HMAC-SHA256), never store
// plaintext, and enforce expiration + revocation semantics identically.
//
// Two implementations exist:
//   - *PostgresAPITokenStore  (internal/auth/api_token_store.go) — full gateway
//   - *SQLiteAPITokenStore    (internal/auth/sqlite/store.go)    — aetherlite
type APITokenStore interface {
	// CreateToken generates a new API token, hashes it, and persists it.
	// The plaintext token is only available in the returned result.
	CreateToken(ctx context.Context, name, principalType string, workspacePatterns, scopes []string, createdBy string, expiresAt *time.Time) (*APITokenCreateResult, error)

	// ValidateToken hashes the provided token string, looks it up by hash,
	// checks that it is not revoked or expired, atomically updates
	// last_used_at, and returns the token record (with TokenHash cleared).
	ValidateToken(ctx context.Context, tokenStr string) (*APIToken, error)

	// RevokeToken sets revoked=true and revoked_at=now for the given token ID.
	RevokeToken(ctx context.Context, tokenID string) error

	// GetToken retrieves a token by its ID (with TokenHash cleared).
	GetToken(ctx context.Context, tokenID string) (*APIToken, error)

	// ListTokens returns API tokens with pagination.
	// limit <= 0 defaults to 100; limit is capped at 1000. offset < 0 defaults to 0.
	// If includeRevoked is false, revoked tokens are excluded.
	ListTokens(ctx context.Context, limit, offset int, includeRevoked bool) ([]*APIToken, error)

	// DeleteToken performs a hard delete of a token by its ID.
	DeleteToken(ctx context.Context, tokenID string) error
}

// MatchesWorkspace checks if a workspace matches any of the token's
// workspace_patterns using filepath.Match style globbing. This is pure logic
// with no storage dependency, so it lives as a free function rather than an
// interface method.
func MatchesWorkspace(token *APIToken, workspace string) bool {
	if len(token.WorkspacePatterns) == 0 {
		// No patterns means access to all workspaces
		return true
	}
	for _, pattern := range token.WorkspacePatterns {
		matched, err := filepath.Match(pattern, workspace)
		if err != nil {
			// Invalid pattern, skip it
			continue
		}
		if matched {
			return true
		}
	}
	return false
}
