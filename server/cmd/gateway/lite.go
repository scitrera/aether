package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"

	"github.com/scitrera/aether/internal/checkpoint"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/gateway"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/lite"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/quota"
	routerpkg "github.com/scitrera/aether/internal/router"
	"github.com/scitrera/aether/internal/state"
	taskiface "github.com/scitrera/aether/internal/storage/tasks"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	sqlitemigrations "github.com/scitrera/aether/migrations/sqlite"
	_ "github.com/scitrera/aether/pkg/dbcompat" // registers "sqlite_compat" driver
)

// liteBackends holds all the lite-mode backend instances for lifecycle management.
type liteBackends struct {
	db           *sql.DB
	badgerDB     *badger.DB
	sessions     gateway.SessionManager
	kvStore      gateway.KVReadWriter
	checkpoints  gateway.CheckpointManager
	tokenStore   state.TokenStore
	router       gateway.MessageRouter
	taskStore    taskiface.Store
	quotaManager gateway.QuotaChecker
	gatewayOpts  []gateway.GatewayOption
}

// Close releases all lite-mode resources.
func (lb *liteBackends) Close() {
	if r, ok := lb.router.(*routerpkg.BadgerRouter); ok {
		r.Close()
	}
	if lb.db != nil {
		lb.db.Close()
	}
	if lb.badgerDB != nil {
		lb.badgerDB.Close()
	}
}

// initLiteBackends initializes all embedded backends for lite mode.
func initLiteBackends(ctx context.Context, cfg *config.Config) (*liteBackends, error) {
	dataDir := cfg.Lite.GetDataDir()

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create lite data directory %s: %w", dataDir, err)
	}

	logging.Logger.Info().Str("data_dir", dataDir).Msg("initializing AetherLite backends")

	// 1. Open Badger database
	badgerDir := filepath.Join(dataDir, "badger")
	badgerDB, err := lite.OpenBadger(badgerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open Badger database: %w", err)
	}
	lite.RunGC(ctx, badgerDB, 0)
	logging.Logger.Debug().Str("dir", badgerDir).Msg("Badger database opened")

	// 2. Open SQLite database
	sqlitePath := filepath.Join(dataDir, "aether.db")
	db, err := sql.Open("sqlite_compat", sqlitePath)
	if err != nil {
		badgerDB.Close()
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}
	// SQLite tuning for WAL mode and concurrent access
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		logging.Logger.Warn().Err(err).Msg("failed to set SQLite WAL mode")
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		logging.Logger.Warn().Err(err).Msg("failed to set SQLite busy timeout")
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		logging.Logger.Warn().Err(err).Msg("failed to enable SQLite foreign keys")
	}
	logging.Logger.Debug().Str("path", sqlitePath).Msg("SQLite database opened")

	// 3. Run SQLite migrations
	if err := runSQLiteMigrations(ctx, db); err != nil {
		db.Close()
		badgerDB.Close()
		return nil, fmt.Errorf("failed to run SQLite migrations: %w", err)
	}
	logging.Logger.Debug().Msg("SQLite migrations applied")

	// 4. Initialize Badger-backed subsystems
	sessions := state.NewBadgerSessionRegistry(badgerDB)
	kvStore := kv.NewBadgerKVStore(badgerDB)
	checkpointStore := checkpoint.NewBadgerCheckpointStore(badgerDB)
	tokenStore := state.NewBadgerTokenStore(badgerDB)
	msgRouter := routerpkg.NewBadgerRouter(badgerDB)

	// 5. Task store (SQLite-backed)
	taskStore := taskpg.New(db)

	// 6. Quota manager (in-memory)
	defaults := quota.DefaultQuotas{
		MaxConnectionsPerWorkspace: 1000,
		MaxMessageRatePerIdentity:  100,
		MaxKVKeysPerNamespace:      10000,
		MaxKVValueSize:             1048576,
	}
	if cfg.Quotas.MaxConnectionsPerWorkspace > 0 {
		defaults.MaxConnectionsPerWorkspace = cfg.Quotas.MaxConnectionsPerWorkspace
	}
	if cfg.Quotas.MaxMessageRatePerIdentity > 0 {
		defaults.MaxMessageRatePerIdentity = cfg.Quotas.MaxMessageRatePerIdentity
	}
	quotaManager := quota.NewMemoryQuotaManager(defaults)

	// 7. Build gateway options
	var gatewayOpts []gateway.GatewayOption
	gatewayOpts = append(gatewayOpts, gateway.WithQuotaManager(quotaManager))

	// 8. Orchestration services (lite mode — polling dispatcher, no AMQP, no pq.Listener)
	dispatcher := orchestration.NewPollingTaskDispatcher(taskStore)
	orchServices := &gateway.OrchestrationServices{
		Dispatcher:  dispatcher,
		QueueCloser: orchestration.NewNoopQueueCloser(),
		TokenStore:  tokenStore,
		TaskService: orchestration.NewTaskAssignmentService(
			db, taskStore, nil, nil, nil, nil,
		),
	}
	orchServices.TaskService.SetTokenStore(tokenStore)
	gatewayOpts = append(gatewayOpts, gateway.WithOrchestrationServices(orchServices))

	logging.Logger.Info().Msg("AetherLite backends initialized successfully")

	return &liteBackends{
		db:           db,
		badgerDB:     badgerDB,
		sessions:     sessions,
		kvStore:      kvStore,
		checkpoints:  checkpointStore,
		tokenStore:   tokenStore,
		router:       msgRouter,
		taskStore:    taskStore,
		quotaManager: quotaManager,
		gatewayOpts:  gatewayOpts,
	}, nil
}

// runSQLiteMigrations applies SQLite migrations from the embedded filesystem.
func runSQLiteMigrations(ctx context.Context, db *sql.DB) error {
	// Create schema_migrations table
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	// Read migration files from embedded FS
	entries, err := sqlitemigrations.MigrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read SQLite migrations: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}

		version := entry.Name()[:len(entry.Name())-4] // strip .sql

		// Check if already applied
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version).Scan(&count); err != nil {
			return fmt.Errorf("failed to check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}

		// Read and execute migration
		content, err := sqlitemigrations.MigrationFS.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", entry.Name(), err)
		}

		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", entry.Name(), err)
		}

		if _, err := db.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (?)", version); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", entry.Name(), err)
		}

		logging.Logger.Debug().Str("version", version).Msg("SQLite migration applied")
	}

	return nil
}
