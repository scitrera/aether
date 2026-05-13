package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"
)

// Run executes all pending workflow SQLite migrations.
// Uses a separate tracking table (workflow_schema_migrations) to avoid
// conflicts with the gateway's schema_migrations table.
func Run(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS workflow_schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create workflow_schema_migrations table: %w", err)
	}

	entries, err := MigrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read workflow sqlite migrations directory: %w", err)
	}

	var migrationFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			migrationFiles = append(migrationFiles, entry.Name())
		}
	}
	sort.Strings(migrationFiles)

	applied := make(map[string]bool)
	rows, err := db.QueryContext(ctx, "SELECT version FROM workflow_schema_migrations")
	if err != nil {
		return fmt.Errorf("failed to query applied workflow sqlite migrations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("failed to scan workflow sqlite migration version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating workflow sqlite migrations: %w", err)
	}

	appliedCount := 0
	for _, filename := range migrationFiles {
		version := strings.TrimSuffix(filename, ".sql")
		if applied[version] {
			continue
		}

		log.Debug().Str("version", version).Msg("applying workflow sqlite migration")

		content, err := MigrationFS.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("failed to read workflow sqlite migration %s: %w", filename, err)
		}

		// Execute each statement individually. SQLite ALTER TABLE is not
		// transactional — columns are added immediately even if the tx rolls
		// back. Executing per-statement and ignoring "duplicate column" errors
		// makes migrations idempotent after partial failures.
		stmts := splitStatements(string(content))
		for _, stmt := range stmts {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				if isDuplicateColumnError(err) {
					log.Debug().Str("version", version).Msg("skipping duplicate column (prior partial migration)")
					continue
				}
				return fmt.Errorf("failed to execute workflow sqlite migration %s: %w", filename, err)
			}
		}

		if _, err := db.ExecContext(ctx, "INSERT INTO workflow_schema_migrations (version) VALUES (?)", version); err != nil {
			return fmt.Errorf("failed to record workflow sqlite migration %s: %w", filename, err)
		}

		log.Debug().Str("version", version).Msg("workflow sqlite migration applied")
		appliedCount++
	}

	if appliedCount == 0 {
		log.Debug().Msg("all workflow sqlite migrations already applied")
	} else {
		log.Info().Int("count", appliedCount).Msg("workflow sqlite migrations applied")
	}

	return nil
}

// splitStatements splits SQL text on semicolons into individual statements,
// skipping empty/comment-only fragments.
func splitStatements(sql string) []string {
	raw := strings.Split(sql, ";")
	var out []string
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		// skip comment-only fragments
		lines := strings.Split(s, "\n")
		hasCode := false
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if l != "" && !strings.HasPrefix(l, "--") {
				hasCode = true
				break
			}
		}
		if hasCode {
			out = append(out, s)
		}
	}
	return out
}

// isDuplicateColumnError checks if a SQLite error is a "duplicate column name" error,
// which happens when ALTER TABLE ADD COLUMN is re-run after a partial migration.
func isDuplicateColumnError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate column name")
}
