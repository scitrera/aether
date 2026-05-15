// Package sqlite provides the native-sqlite implementation of auth.APITokenStore.
// It is the Stage 2 counterpart to the PostgreSQL implementation in
// internal/auth/api_token_store.go: same interface, sqlite-native SQL, no
// dbcompat translation layer.
//
// Design decisions:
//
//   - Single *sql.DB handle with SetMaxOpenConns(1) to serialize writes and
//     avoid SQLITE_BUSY in WAL mode (per plan section 14.3).
//
//   - Timestamps are stored as ISO-8601 TEXT via strftime('%Y-%m-%dT%H:%M:%fZ',
//     'now') in the schema defaults. The implementation formats/parses
//     time.Time inline using the same layout (no driver-level coercion).
//
//   - Array columns (workspace_patterns, scopes) are stored as JSON TEXT and
//     marshaled/unmarshaled in Go.
//
//   - Boolean columns (revoked) are stored as INTEGER (0/1).
//
//   - IDs are generated in Go via google/uuid (SQLite has no gen_random_uuid).
//
//   - The bare "sqlite" driver (modernc.org/sqlite) is used directly — not
//     "sqlite_compat". This is correct for Stage 2 native impls because we
//     own all our SQL and do our own timestamp parsing inline (per plan
//     section 15.4).
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/auth"
	sqlitetokenmigrations "github.com/scitrera/aether/migrations/sqlite_tokens"
	"github.com/scitrera/aether/pkg/crypto"

	_ "modernc.org/sqlite" // register bare "sqlite" driver
)

// timestampLayout is the canonical ISO-8601 format used for all timestamp
// storage and retrieval. Matches the strftime format in the migration schema.
const timestampLayout = "2006-01-02T15:04:05.000Z"

// additionalTimestampLayouts are fallback formats for parsing timestamps
// that may have been written by other code paths.
var additionalTimestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05",
}

// parseTimestamp parses a TEXT timestamp from sqlite into time.Time.
func parseTimestamp(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	t, err := time.Parse(timestampLayout, s)
	if err == nil {
		return t, nil
	}
	for _, layout := range additionalTimestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse timestamp %q", s)
}

// parseNullableTimestamp parses a nullable TEXT timestamp.
func parseNullableTimestamp(ns sql.NullString) (*time.Time, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	t, err := parseTimestamp(ns.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// nowTimestamp returns the current time formatted in the canonical layout.
func nowTimestamp() string {
	return time.Now().UTC().Format(timestampLayout)
}

// formatTimestamp formats a time.Time as a canonical timestamp string.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(timestampLayout)
}

// SQLiteAPITokenStore is the native-sqlite implementation of auth.APITokenStore.
type SQLiteAPITokenStore struct {
	db          *sql.DB
	tokenLength int
}

// Compile-time conformance assert.
var _ auth.APITokenStore = (*SQLiteAPITokenStore)(nil)

// New constructs a native-sqlite API token store. The caller provides an
// already-opened *sql.DB using the bare "sqlite" driver, pointed at the
// tokens.db file. The caller retains ownership; nothing on the store closes it.
//
// New runs the per-domain migration set from migrations/sqlite_tokens/ against
// db before returning. It also enforces SetMaxOpenConns(1) on the handle to
// serialize writes (per plan section 14.3).
func New(db *sql.DB) (*SQLiteAPITokenStore, error) {
	// Enforce single-writer to prevent SQLITE_BUSY in WAL mode.
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := applyMigrations(ctx, db); err != nil {
		return nil, fmt.Errorf("token sqlite migrations: %w", err)
	}

	return &SQLiteAPITokenStore{
		db:          db,
		tokenLength: 32, // 256 bits — matches PostgresAPITokenStore
	}, nil
}

// CreateToken generates a new API token, hashes it with HMAC-SHA256, and
// stores it in the api_tokens table. The plaintext token is only available
// in the returned result; it is never stored.
func (s *SQLiteAPITokenStore) CreateToken(ctx context.Context, name, principalType string, workspacePatterns, scopes []string, createdBy string, expiresAt *time.Time) (*auth.APITokenCreateResult, error) {
	tokenStr, tokenHash, err := crypto.GenerateToken(s.tokenLength)
	if err != nil {
		return nil, err
	}

	id := uuid.New().String()
	now := nowTimestamp()

	wpJSON, err := json.Marshal(workspacePatterns)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal workspace_patterns: %w", err)
	}
	scJSON, err := json.Marshal(scopes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal scopes: %w", err)
	}

	var expiresAtStr *string
	if expiresAt != nil {
		s := formatTimestamp(*expiresAt)
		expiresAtStr = &s
	}

	query := `
		INSERT INTO api_tokens (id, token_hash, name, principal_type, workspace_patterns, scopes, created_by, expires_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = s.db.ExecContext(ctx, query,
		id, tokenHash, name, principalType,
		string(wpJSON), string(scJSON),
		createdBy, expiresAtStr, now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert token: %w", err)
	}

	nowTime, _ := parseTimestamp(now)
	apiToken := &auth.APIToken{
		ID:                id,
		TokenHash:         tokenHash,
		Name:              name,
		PrincipalType:     principalType,
		WorkspacePatterns: workspacePatterns,
		Scopes:            scopes,
		CreatedBy:         createdBy,
		ExpiresAt:         expiresAt,
		Revoked:           false,
		CreatedAt:         nowTime,
		UpdatedAt:         nowTime,
	}

	return &auth.APITokenCreateResult{
		Token:    tokenStr,
		APIToken: apiToken,
	}, nil
}

// ValidateToken hashes the provided token string, looks it up by hash,
// checks that it is not revoked or expired, updates last_used_at, and
// returns the token record.
//
// SQLite does not support UPDATE...RETURNING in all versions, so this uses
// a SELECT + UPDATE pair within the single-writer serialized connection.
func (s *SQLiteAPITokenStore) ValidateToken(ctx context.Context, tokenStr string) (*auth.APIToken, error) {
	tokenHash, err := crypto.HashToken(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("failed to hash token: %w", err)
	}

	now := nowTimestamp()

	// SELECT the token first, checking revocation and expiration.
	selectQuery := `
		SELECT id, token_hash, name, principal_type, workspace_patterns, scopes,
		       created_by, expires_at, last_used_at, revoked, revoked_at, created_at, updated_at
		FROM api_tokens
		WHERE token_hash = ?
		  AND revoked = 0
		  AND (expires_at IS NULL OR expires_at > ?)`

	token, err := s.scanToken(s.db.QueryRowContext(ctx, selectQuery, tokenHash, now))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("token not found or expired/revoked")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to validate token: %w", err)
	}

	// Update last_used_at and updated_at.
	updateQuery := `UPDATE api_tokens SET last_used_at = ?, updated_at = ? WHERE id = ?`
	if _, err := s.db.ExecContext(ctx, updateQuery, now, now, token.ID); err != nil {
		return nil, fmt.Errorf("failed to update last_used_at: %w", err)
	}

	nowTime, _ := parseTimestamp(now)
	token.LastUsedAt = &nowTime
	token.UpdatedAt = nowTime

	// Clear hash before returning — never expose hash outside the store.
	token.TokenHash = ""
	return token, nil
}

// RevokeToken sets revoked=1 and revoked_at=now for the token identified by ID.
func (s *SQLiteAPITokenStore) RevokeToken(ctx context.Context, tokenID string) error {
	now := nowTimestamp()
	query := `UPDATE api_tokens SET revoked = 1, revoked_at = ?, updated_at = ? WHERE id = ?`
	result, err := s.db.ExecContext(ctx, query, now, now, tokenID)
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

// GetToken retrieves a token by its ID.
func (s *SQLiteAPITokenStore) GetToken(ctx context.Context, tokenID string) (*auth.APIToken, error) {
	query := `
		SELECT id, token_hash, name, principal_type, workspace_patterns, scopes,
		       created_by, expires_at, last_used_at, revoked, revoked_at, created_at, updated_at
		FROM api_tokens
		WHERE id = ?`

	token, err := s.scanToken(s.db.QueryRowContext(ctx, query, tokenID))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("token not found: %s", tokenID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}
	token.TokenHash = "" // Never expose hash outside the store.
	return token, nil
}

// ListTokens returns API tokens with pagination (for admin use).
func (s *SQLiteAPITokenStore) ListTokens(ctx context.Context, limit, offset int, includeRevoked bool) ([]*auth.APIToken, error) {
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
		SELECT id, token_hash, name, principal_type, workspace_patterns, scopes,
		       created_by, expires_at, last_used_at, revoked, revoked_at, created_at, updated_at
		FROM api_tokens`
	if !includeRevoked {
		baseQuery += ` WHERE revoked = 0`
	}
	query := baseQuery + ` ORDER BY created_at DESC LIMIT ? OFFSET ?`

	rows, err := s.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list tokens: %w", err)
	}
	defer rows.Close()

	var tokens []*auth.APIToken
	for rows.Next() {
		token, err := s.scanTokenFromRows(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan token: %w", err)
		}
		token.TokenHash = "" // Never expose hash outside the store.
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tokens: %w", err)
	}

	return tokens, nil
}

// DeleteToken performs a hard delete of a token by its ID.
func (s *SQLiteAPITokenStore) DeleteToken(ctx context.Context, tokenID string) error {
	query := `DELETE FROM api_tokens WHERE id = ?`
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

// =============================================================================
// Row scanning helpers
// =============================================================================

// scanToken scans a single row into an APIToken.
func (s *SQLiteAPITokenStore) scanToken(row *sql.Row) (*auth.APIToken, error) {
	var (
		token      auth.APIToken
		wpJSON     string
		scJSON     string
		expiresAt  sql.NullString
		lastUsedAt sql.NullString
		revokedInt int
		revokedAt  sql.NullString
		createdStr string
		updatedStr string
	)

	err := row.Scan(
		&token.ID, &token.TokenHash, &token.Name, &token.PrincipalType,
		&wpJSON, &scJSON,
		&token.CreatedBy, &expiresAt, &lastUsedAt,
		&revokedInt, &revokedAt,
		&createdStr, &updatedStr,
	)
	if err != nil {
		return nil, err
	}

	return s.populateToken(&token, wpJSON, scJSON, expiresAt, lastUsedAt, revokedInt, revokedAt, createdStr, updatedStr)
}

// scanTokenFromRows scans a row from *sql.Rows into an APIToken.
func (s *SQLiteAPITokenStore) scanTokenFromRows(rows *sql.Rows) (*auth.APIToken, error) {
	var (
		token      auth.APIToken
		wpJSON     string
		scJSON     string
		expiresAt  sql.NullString
		lastUsedAt sql.NullString
		revokedInt int
		revokedAt  sql.NullString
		createdStr string
		updatedStr string
	)

	err := rows.Scan(
		&token.ID, &token.TokenHash, &token.Name, &token.PrincipalType,
		&wpJSON, &scJSON,
		&token.CreatedBy, &expiresAt, &lastUsedAt,
		&revokedInt, &revokedAt,
		&createdStr, &updatedStr,
	)
	if err != nil {
		return nil, err
	}

	return s.populateToken(&token, wpJSON, scJSON, expiresAt, lastUsedAt, revokedInt, revokedAt, createdStr, updatedStr)
}

// populateToken fills the parsed fields of an APIToken from raw scanned values.
func (s *SQLiteAPITokenStore) populateToken(
	token *auth.APIToken,
	wpJSON, scJSON string,
	expiresAt, lastUsedAt sql.NullString,
	revokedInt int,
	revokedAt sql.NullString,
	createdStr, updatedStr string,
) (*auth.APIToken, error) {
	// Parse JSON arrays.
	if err := json.Unmarshal([]byte(wpJSON), &token.WorkspacePatterns); err != nil {
		return nil, fmt.Errorf("failed to unmarshal workspace_patterns: %w", err)
	}
	if err := json.Unmarshal([]byte(scJSON), &token.Scopes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal scopes: %w", err)
	}

	// Parse nullable timestamps.
	var err error
	token.ExpiresAt, err = parseNullableTimestamp(expiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse expires_at: %w", err)
	}
	token.LastUsedAt, err = parseNullableTimestamp(lastUsedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse last_used_at: %w", err)
	}
	token.RevokedAt, err = parseNullableTimestamp(revokedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse revoked_at: %w", err)
	}

	// Parse boolean.
	token.Revoked = revokedInt != 0

	// Parse required timestamps.
	token.CreatedAt, err = parseTimestamp(createdStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse created_at: %w", err)
	}
	token.UpdatedAt, err = parseTimestamp(updatedStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse updated_at: %w", err)
	}

	return token, nil
}

// =============================================================================
// Migration runner
// =============================================================================

func applyMigrations(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := sqlitetokenmigrations.MigrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read embed fs: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version,
		).Scan(&count); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}
		content, err := sqlitetokenmigrations.MigrationFS.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("exec %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES (?)", version,
		); err != nil {
			return fmt.Errorf("record %s: %w", version, err)
		}
	}
	return nil
}
