package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"
)

// Run executes all pending msgbridge SQLite migrations.
// Uses a separate tracking table (msgbridge_schema_migrations) to avoid
// conflicts with the gateway's and workflow server's schema_migrations tables.
func Run(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS msgbridge_schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create msgbridge_schema_migrations table: %w", err)
	}

	entries, err := MigrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read msgbridge sqlite migrations directory: %w", err)
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
		return fmt.Errorf("failed to query applied msgbridge sqlite migrations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("failed to scan msgbridge sqlite migration version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating msgbridge sqlite migrations: %w", err)
	}

	appliedCount := 0
	for _, filename := range migrationFiles {
		version := strings.TrimSuffix(filename, ".sql")
		if applied[version] {
			continue
		}

		log.Debug().Str("version", version).Msg("applying msgbridge sqlite migration")

		content, err := MigrationFS.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("failed to read msgbridge sqlite migration %s: %w", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for %s: %w", filename, err)
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute msgbridge sqlite migration %s: %w", filename, err)
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO msgbridge_schema_migrations (version) VALUES (?)", version); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record msgbridge sqlite migration %s: %w", filename, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit msgbridge sqlite migration %s: %w", filename, err)
		}

		log.Debug().Str("version", version).Msg("msgbridge sqlite migration applied")
		appliedCount++
	}

	if appliedCount == 0 {
		log.Debug().Msg("all msgbridge sqlite migrations already applied")
	} else {
		log.Info().Int("count", appliedCount).Msg("msgbridge sqlite migrations applied")
	}

	return nil
}
