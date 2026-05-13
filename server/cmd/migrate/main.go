package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/migrations"
)

var (
	dryRun = flag.Bool("dry-run", false, "List pending migrations without executing them")
	dbURL  = flag.String("db", "", "PostgreSQL connection string (default: from DATABASE_URL env or dev default)")
)

func main() {
	flag.Parse()

	// Set DATABASE_URL from flag, env, or default
	connStr := *dbURL
	if connStr == "" {
		connStr = os.Getenv("DATABASE_URL")
	}
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/aether?sslmode=disable"
	}

	ctx := context.Background()

	// Initialize structured logger
	logging.Init("info")

	if *dryRun {
		if err := runDryRun(ctx); err != nil {
			logging.Logger.Fatal().Err(err).Msg("dry run failed")
		}
		return
	}

	// Connect to database for real migration run
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to ping database")
	}

	// Run migrations
	if err := migrations.Run(ctx, db); err != nil {
		logging.Logger.Fatal().Err(err).Msg("migration failed")
	}

	logging.Logger.Info().Msg("migrations completed successfully")
}

// runDryRun lists all available migrations without connecting to database
func runDryRun(ctx context.Context) error {
	logging.Logger.Info().Msg("dry run mode - listing available migrations")

	migrationFiles, err := migrations.ListMigrations()
	if err != nil {
		return fmt.Errorf("failed to list migrations: %w", err)
	}

	if len(migrationFiles) == 0 {
		logging.Logger.Info().Msg("no migrations found")
		return nil
	}

	logging.Logger.Info().Int("count", len(migrationFiles)).Msg("found migrations")
	for _, filename := range migrationFiles {
		version := strings.TrimSuffix(filename, ".sql")
		logging.Logger.Info().Str("version", version).Msg("migration available")
	}

	return nil
}
