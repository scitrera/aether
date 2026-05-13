// Package migrations provides embedded SQL migrations for Aether.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"

	"github.com/scitrera/aether/internal/logging"
	"strings"
)

//go:embed *.sql
var migrationFS embed.FS

// ListMigrations returns all available migration files in sorted order.
// This is useful for dry-run mode and tooling that needs to inspect migrations.
func ListMigrations() ([]string, error) {
	entries, err := migrationFS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var migrationFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			migrationFiles = append(migrationFiles, entry.Name())
		}
	}
	sort.Strings(migrationFiles)

	return migrationFiles, nil
}

// Run executes all pending SQL migrations in order.
// Migrations are tracked in the schema_migrations table.
func Run(ctx context.Context, db *sql.DB) error {
	// Create schema_migrations table if it doesn't exist
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	// Get list of migration files
	entries, err := migrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Sort migrations by filename (they should be numbered like 001_, 002_, etc.)
	var migrationFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			migrationFiles = append(migrationFiles, entry.Name())
		}
	}
	sort.Strings(migrationFiles)

	// Get already applied migrations
	applied := make(map[string]bool)
	rows, err := db.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("failed to scan migration version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating migrations: %w", err)
	}

	// Apply pending migrations
	appliedCount := 0
	for _, filename := range migrationFiles {
		version := strings.TrimSuffix(filename, ".sql")
		if applied[version] {
			continue
		}

		logging.Logger.Debug().Str("version", version).Msg("applying migration")

		content, err := migrationFS.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", filename, err)
		}

		// Execute the migration in a transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for %s: %w", filename, err)
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute migration %s: %w", filename, err)
		}

		// Record the migration
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record migration %s: %w", filename, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %s: %w", filename, err)
		}

		logging.Logger.Debug().Str("version", version).Msg("migration applied successfully")
		appliedCount++
	}

	if appliedCount == 0 {
		logging.Logger.Debug().Msg("all migrations already applied")
	} else {
		logging.Logger.Info().Int("count", appliedCount).Msg("migrations applied")
	}

	return nil
}
