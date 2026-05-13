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

// Run executes all pending msgbridge SQL migrations.
// Uses a separate tracking table (msgbridge_schema_migrations) to avoid
// conflicts with the gateway's and workflow server's schema_migrations tables.
func Run(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS msgbridge_schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create msgbridge_schema_migrations table: %w", err)
	}

	entries, err := migrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read msgbridge migrations directory: %w", err)
	}

	var migrationFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			migrationFiles = append(migrationFiles, entry.Name())
		}
	}
	sort.Strings(migrationFiles)

	applied := make(map[string]bool)
	rows, err := db.QueryContext(ctx, "SELECT version FROM msgbridge_schema_migrations")
	if err != nil {
		return fmt.Errorf("failed to query applied msgbridge migrations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("failed to scan msgbridge migration version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating msgbridge migrations: %w", err)
	}

	appliedCount := 0
	for _, filename := range migrationFiles {
		version := strings.TrimSuffix(filename, ".sql")
		if applied[version] {
			continue
		}

		log.Debug().Str("version", version).Msg("applying msgbridge migration")

		content, err := migrationFS.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("failed to read msgbridge migration %s: %w", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for %s: %w", filename, err)
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute msgbridge migration %s: %w", filename, err)
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO msgbridge_schema_migrations (version) VALUES ($1)", version); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record msgbridge migration %s: %w", filename, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit msgbridge migration %s: %w", filename, err)
		}

		log.Debug().Str("version", version).Msg("msgbridge migration applied")
		appliedCount++
	}

	if appliedCount == 0 {
		log.Debug().Msg("all msgbridge migrations already applied")
	} else {
		log.Info().Int("count", appliedCount).Msg("msgbridge migrations applied")
	}

	return nil
}
