package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"
)

//go:embed *.sql
var migrationFS embed.FS

// Run executes all pending workflow SQL migrations.
// Uses a separate tracking table (workflow_schema_migrations) to avoid
// conflicts with the gateway's schema_migrations table.
func Run(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS workflow_schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create workflow_schema_migrations table: %w", err)
	}

	entries, err := migrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read workflow migrations directory: %w", err)
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
		return fmt.Errorf("failed to query applied workflow migrations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("failed to scan workflow migration version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating workflow migrations: %w", err)
	}

	appliedCount := 0
	for _, filename := range migrationFiles {
		version := strings.TrimSuffix(filename, ".sql")
		if applied[version] {
			continue
		}

		log.Debug().Str("version", version).Msg("applying workflow migration")

		content, err := migrationFS.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("failed to read workflow migration %s: %w", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for %s: %w", filename, err)
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute workflow migration %s: %w", filename, err)
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO workflow_schema_migrations (version) VALUES ($1)", version); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record workflow migration %s: %w", filename, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit workflow migration %s: %w", filename, err)
		}

		log.Debug().Str("version", version).Msg("workflow migration applied")
		appliedCount++
	}

	if appliedCount == 0 {
		log.Debug().Msg("all workflow migrations already applied")
	} else {
		log.Info().Int("count", appliedCount).Msg("workflow migrations applied")
	}

	return nil
}
