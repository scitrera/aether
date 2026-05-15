package auth

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/scitrera/aether/pkg/crypto"
)

// APIToken represents a stored API token
type APIToken struct {
	ID                string     `json:"id"`
	TokenHash         string     `json:"token_hash"`
	Name              string     `json:"name"`
	PrincipalType     string     `json:"principal_type"`
	WorkspacePatterns []string   `json:"workspace_patterns"`
	Scopes            []string   `json:"scopes"`
	CreatedBy         string     `json:"created_by"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	LastUsedAt        *time.Time `json:"last_used_at,omitempty"`
	Revoked           bool       `json:"revoked"`
	RevokedAt         *time.Time `json:"revoked_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

// APITokenCreateResult is returned when creating a new token — includes the plaintext token (only time it's available)
type APITokenCreateResult struct {
	Token    string    `json:"token"` // Plaintext token - only available at creation time
	APIToken *APIToken `json:"api_token"`
}

// PostgresAPITokenStore manages long-lived API tokens in PostgreSQL.
// It implements the APITokenStore interface.
type PostgresAPITokenStore struct {
	db          *sql.DB
	tokenLength int
}

// Compile-time conformance assert.
var _ APITokenStore = (*PostgresAPITokenStore)(nil)

// NewPostgresAPITokenStore creates a new PostgreSQL-backed API token store.
func NewPostgresAPITokenStore(db *sql.DB) *PostgresAPITokenStore {
	return &PostgresAPITokenStore{
		db:          db,
		tokenLength: 32, // 256 bits
	}
}

// NewAPITokenStore is a backwards-compatible alias for NewPostgresAPITokenStore.
// Callers that have not yet migrated to the new name can continue using this.
func NewAPITokenStore(db *sql.DB) *PostgresAPITokenStore {
	return NewPostgresAPITokenStore(db)
}

// CreateToken generates a new API token, hashes it with SHA256, and stores it in the api_tokens table.
// The plaintext token is only available in the returned result; it is never stored.
func (s *PostgresAPITokenStore) CreateToken(ctx context.Context, name, principalType string, workspacePatterns, scopes []string, createdBy string, expiresAt *time.Time) (*APITokenCreateResult, error) {
	// Generate cryptographically secure random token
	tokenStr, tokenHash, err := crypto.GenerateToken(s.tokenLength)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	query := `
		INSERT INTO api_tokens (token_hash, name, principal_type, workspace_patterns, scopes, created_by, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id`

	var id string
	err = s.db.QueryRowContext(ctx, query,
		tokenHash,
		name,
		principalType,
		pq.Array(workspacePatterns),
		pq.Array(scopes),
		createdBy,
		expiresAt,
		now,
		now,
	).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("failed to insert token: %w", err)
	}

	apiToken := &APIToken{
		ID:                id,
		TokenHash:         tokenHash,
		Name:              name,
		PrincipalType:     principalType,
		WorkspacePatterns: workspacePatterns,
		Scopes:            scopes,
		CreatedBy:         createdBy,
		ExpiresAt:         expiresAt,
		Revoked:           false,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	return &APITokenCreateResult{
		Token:    tokenStr,
		APIToken: apiToken,
	}, nil
}

// ValidateToken hashes the provided token string, looks it up by hash, checks that it is
// not revoked or expired, atomically updates last_used_at, and returns the token record.
func (s *PostgresAPITokenStore) ValidateToken(ctx context.Context, tokenStr string) (*APIToken, error) {
	tokenHash, err := crypto.HashToken(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("failed to hash token: %w", err)
	}

	// Single atomic UPDATE...RETURNING query: update last_used_at and return the row
	query := `
		UPDATE api_tokens
		SET last_used_at = $1, updated_at = $1
		WHERE token_hash = $2
		  AND revoked = false
		  AND (expires_at IS NULL OR expires_at > $1)
		RETURNING id, token_hash, name, principal_type, workspace_patterns, scopes, created_by, expires_at, last_used_at, revoked, revoked_at, created_at, updated_at`

	now := time.Now()
	token := &APIToken{}
	err = s.db.QueryRowContext(ctx, query, now, tokenHash).Scan(
		&token.ID,
		&token.TokenHash,
		&token.Name,
		&token.PrincipalType,
		pq.Array(&token.WorkspacePatterns),
		pq.Array(&token.Scopes),
		&token.CreatedBy,
		&token.ExpiresAt,
		&token.LastUsedAt,
		&token.Revoked,
		&token.RevokedAt,
		&token.CreatedAt,
		&token.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("token not found or expired/revoked")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to validate token: %w", err)
	}

	// Clear hash before returning - never expose hash outside the store
	token.TokenHash = ""
	return token, nil
}

// RevokeToken sets revoked=true and revoked_at=now for the token identified by ID
func (s *PostgresAPITokenStore) RevokeToken(ctx context.Context, tokenID string) error {
	now := time.Now()
	query := `UPDATE api_tokens SET revoked = true, revoked_at = $1, updated_at = $1 WHERE id = $2`
	result, err := s.db.ExecContext(ctx, query, now, tokenID)
	if err != nil {
		return fmt.Errorf("failed to revoke token: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("token not found: %s", tokenID)
	}
	return nil
}

// GetToken retrieves a token by its ID
func (s *PostgresAPITokenStore) GetToken(ctx context.Context, tokenID string) (*APIToken, error) {
	query := `
		SELECT id, token_hash, name, principal_type, workspace_patterns, scopes, created_by, expires_at, last_used_at, revoked, revoked_at, created_at, updated_at
		FROM api_tokens
		WHERE id = $1`

	token := &APIToken{}
	err := s.db.QueryRowContext(ctx, query, tokenID).Scan(
		&token.ID,
		&token.TokenHash,
		&token.Name,
		&token.PrincipalType,
		pq.Array(&token.WorkspacePatterns),
		pq.Array(&token.Scopes),
		&token.CreatedBy,
		&token.ExpiresAt,
		&token.LastUsedAt,
		&token.Revoked,
		&token.RevokedAt,
		&token.CreatedAt,
		&token.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("token not found: %s", tokenID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}
	token.TokenHash = "" // Never expose hash outside the store
	return token, nil
}

// ListTokens returns API tokens with pagination (for admin use).
// limit <= 0 defaults to 100; limit is capped at 1000. offset < 0 defaults to 0.
// If includeRevoked is false, revoked tokens are excluded from the result.
func (s *PostgresAPITokenStore) ListTokens(ctx context.Context, limit, offset int, includeRevoked bool) ([]*APIToken, error) {
	if limit <= 0 || limit > 1000 {
		if limit <= 0 {
			limit = 100
		} else {
			limit = 1000
		}
	}
	if offset < 0 {
		offset = 0
	}

	baseQuery := `
		SELECT id, token_hash, name, principal_type, workspace_patterns, scopes, created_by, expires_at, last_used_at, revoked, revoked_at, created_at, updated_at
		FROM api_tokens`
	if !includeRevoked {
		baseQuery += ` WHERE revoked = false`
	}
	query := baseQuery + ` ORDER BY created_at DESC LIMIT $1 OFFSET $2`

	rows, err := s.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list tokens: %w", err)
	}
	defer rows.Close()

	var tokens []*APIToken
	for rows.Next() {
		token := &APIToken{}
		if err := rows.Scan(
			&token.ID,
			&token.TokenHash,
			&token.Name,
			&token.PrincipalType,
			pq.Array(&token.WorkspacePatterns),
			pq.Array(&token.Scopes),
			&token.CreatedBy,
			&token.ExpiresAt,
			&token.LastUsedAt,
			&token.Revoked,
			&token.RevokedAt,
			&token.CreatedAt,
			&token.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan token: %w", err)
		}
		token.TokenHash = "" // Never expose hash outside the store
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tokens: %w", err)
	}

	return tokens, nil
}

// DeleteToken performs a hard delete of a token by its ID
func (s *PostgresAPITokenStore) DeleteToken(ctx context.Context, tokenID string) error {
	query := `DELETE FROM api_tokens WHERE id = $1`
	result, err := s.db.ExecContext(ctx, query, tokenID)
	if err != nil {
		return fmt.Errorf("failed to delete token: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("token not found: %s", tokenID)
	}
	return nil
}
