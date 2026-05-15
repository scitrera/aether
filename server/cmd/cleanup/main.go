// Package main provides a standalone cleanup command for Aether.
// This can be run via cron or manually to execute cleanup jobs
// independently of the gateway's background goroutines.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/cleanup"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/state"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	versionpkg "github.com/scitrera/aether/internal/version"
)

var (
	version = versionpkg.Version
)

var (
	// Config file
	configFile = flag.String("config", "configs/dev.yaml", "Path to configuration file")

	// Job selection flags (default: run all)
	runAll         = flag.Bool("all", false, "Run all cleanup jobs")
	runTaskPurge   = flag.Bool("task-purge", false, "Run task purge job")
	runReconcile   = flag.Bool("reconcile", false, "Run orphaned task reconciliation")
	runStaleLocks  = flag.Bool("stale-locks", false, "Run stale lock cleanup")
	runStaleClaims = flag.Bool("stale-claims", false, "Run stale claim recovery for orchestration tasks")

	// Override retention periods (uses config defaults if not specified)
	completedRetention = flag.String("completed-retention", "", "Override completed task retention (e.g., '168h')")
	failedRetention    = flag.String("failed-retention", "", "Override failed task retention (e.g., '336h')")
	cancelledRetention = flag.String("cancelled-retention", "", "Override cancelled task retention (e.g., '168h')")

	// Other options
	dryRun      = flag.Bool("dry-run", false, "Show what would be done without making changes (not yet implemented)")
	showVersion = flag.Bool("version", false, "Show version and exit")
	showHelp    = flag.Bool("help", false, "Show this help message")
)

func main() {
	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Aether Cleanup Command v%s\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Runs cleanup jobs for the Aether gateway. Can be used with cron\n")
		fmt.Fprintf(os.Stderr, "for external scheduling when gateway background jobs are disabled.\n\n")
		fmt.Fprintf(os.Stderr, "Jobs:\n")
		fmt.Fprintf(os.Stderr, "  task-purge    Delete old completed/failed/cancelled tasks\n")
		fmt.Fprintf(os.Stderr, "  reconcile     Fail tasks whose agents/orchestrators disconnected\n")
		fmt.Fprintf(os.Stderr, "  stale-locks   Remove Redis locks with no TTL (legacy cleanup)\n")
		fmt.Fprintf(os.Stderr, "  stale-claims  Recover orchestration tasks stuck in 'claimed' status\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Run all cleanup jobs:\n")
		fmt.Fprintf(os.Stderr, "  %s -all\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Run only task purge:\n")
		fmt.Fprintf(os.Stderr, "  %s -task-purge\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Run reconciliation and stale lock cleanup:\n")
		fmt.Fprintf(os.Stderr, "  %s -reconcile -stale-locks\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Override retention periods:\n")
		fmt.Fprintf(os.Stderr, "  %s -task-purge -completed-retention 24h\n\n", os.Args[0])
	}

	flag.Parse()

	// Handle special flags
	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Printf("Aether Cleanup Command v%s\n", version)
		os.Exit(0)
	}

	// If no jobs specified, show help
	if !*runAll && !*runTaskPurge && !*runReconcile && !*runStaleLocks && !*runStaleClaims {
		fmt.Fprintf(os.Stderr, "Error: No jobs specified. Use -all to run all jobs, or specify individual jobs.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *dryRun {
		log.Printf("Dry run mode enabled (changes will not be applied)")
		log.Printf("Note: Dry run is not yet fully implemented")
	}

	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize structured logger
	logging.Init("info")

	logging.Logger.Info().Str("version", version).Msg("Aether Cleanup Command")
	logging.Logger.Info().Str("config", *configFile).Msg("configuration loaded")

	// Initialize PostgreSQL
	ctx := context.Background()
	db, err := initDatabase(ctx, cfg)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to initialize database")
	}
	if db != nil {
		defer db.Close()
	}

	// Initialize Redis session registry
	if len(cfg.Redis.Cluster) == 0 {
		logging.Logger.Fatal().Msg("Redis addresses not configured")
	}
	rdb := cfg.Redis.NewClient()
	sessions := state.NewSessionRegistryFromClient(rdb)
	logging.Logger.Info().Strs("addrs", cfg.Redis.Cluster).Str("mode", cfg.Redis.GetMode()).Msg("session registry initialized")

	// Initialize task store
	var taskStore *taskpg.Store
	if db != nil {
		taskStore = taskpg.New(db)
	}

	// Initialize task service for reconciliation
	// Note: TaskAssignmentService requires many dependencies. For the cleanup command,
	// we create a minimal service using the full constructor with nil for unused deps.
	var taskService *orchestration.TaskAssignmentService
	if db != nil {
		taskService = orchestration.NewTaskAssignmentService(
			taskStore,
			nil, // agentRegistry - not needed for reconciliation
			sessions,
			nil, // queueManager - not needed for reconciliation
			nil, // profileManager - not needed for reconciliation
		)
	}

	// Build cleanup config from flags and config file
	cleanupConfig := buildCleanupConfig(cfg)

	// Create cleanup service
	cleanupSvc := cleanup.NewService(taskStore, taskService, sessions, cleanupConfig)

	// Initialize dispatcher for stale claim recovery if task store is available
	if taskStore != nil {
		dispatcher, err := orchestration.NewNotifyTaskDispatcher(taskStore, "", 0, nil)
		if err != nil {
			logging.Logger.Warn().Err(err).Msg("failed to create dispatcher for stale claim recovery")
		} else {
			cleanupSvc.SetDispatcher(dispatcher)
		}
	}

	// Run selected jobs
	var results []cleanup.JobResult

	if *runAll || *runStaleLocks {
		logging.Logger.Info().Msg("running stale lock cleanup")
		result := cleanupSvc.CleanupStaleLocks(ctx)
		results = append(results, result)
		logResult(result)
	}

	if *runAll || *runStaleClaims {
		logging.Logger.Info().Msg("running stale claim recovery")
		result := cleanupSvc.CleanupStaleClaims(ctx)
		results = append(results, result)
		logResult(result)
	}

	if *runAll || *runReconcile {
		logging.Logger.Info().Msg("running orphaned task reconciliation")
		result := cleanupSvc.ReconcileOrphanedTasks(ctx)
		results = append(results, result)
		logResult(result)
	}

	if *runAll || *runTaskPurge {
		logging.Logger.Info().Msg("running task purge")
		result := cleanupSvc.PurgeTasks(ctx)
		results = append(results, result)
		logResult(result)
	}

	// Print summary
	printSummary(results)
}

func loadConfig() (*config.Config, error) {
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", *configFile)
	}

	cfg, err := config.Load(*configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	return cfg, nil
}

func initDatabase(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
	dsn := cfg.Postgres.DSN()
	logging.Logger.Info().Msg("connecting to PostgreSQL")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	logging.Logger.Info().Msg("PostgreSQL connection established")
	return db, nil
}

func buildCleanupConfig(cfg *config.Config) *cleanup.Config {
	cleanupCfg := &cleanup.Config{
		TaskPurgeInterval:      0, // Not used in command mode
		CompletedTaskRetention: cfg.Cleanup.GetCompletedTaskRetention(),
		FailedTaskRetention:    cfg.Cleanup.GetFailedTaskRetention(),
		CancelledTaskRetention: cfg.Cleanup.GetCancelledTaskRetention(),
		ReconciliationInterval: 0, // Not used in command mode
	}

	// Apply flag overrides
	if *completedRetention != "" {
		if d, err := time.ParseDuration(*completedRetention); err == nil {
			cleanupCfg.CompletedTaskRetention = d
		} else {
			logging.Logger.Warn().Str("value", *completedRetention).Msg("invalid completed-retention, using default")
		}
	}
	if *failedRetention != "" {
		if d, err := time.ParseDuration(*failedRetention); err == nil {
			cleanupCfg.FailedTaskRetention = d
		} else {
			logging.Logger.Warn().Str("value", *failedRetention).Msg("invalid failed-retention, using default")
		}
	}
	if *cancelledRetention != "" {
		if d, err := time.ParseDuration(*cancelledRetention); err == nil {
			cleanupCfg.CancelledTaskRetention = d
		} else {
			logging.Logger.Warn().Str("value", *cancelledRetention).Msg("invalid cancelled-retention, using default")
		}
	}

	return cleanupCfg
}

func logResult(result cleanup.JobResult) {
	if result.Error != nil {
		logging.Logger.Error().Err(result.Error).Str("job", result.JobName).Dur("duration", result.Duration).Msg("job failed")
	} else {
		logging.Logger.Info().Str("job", result.JobName).Str("details", result.Details).Dur("duration", result.Duration).Msg("job completed")
	}
}

func printSummary(results []cleanup.JobResult) {
	successCount := 0
	failCount := 0
	totalItems := int64(0)

	for _, r := range results {
		if r.Success {
			successCount++
			totalItems += r.ItemCount
		} else {
			failCount++
		}
	}

	logging.Logger.Info().
		Int("jobs_run", len(results)).
		Int("successful", successCount).
		Int("failed", failCount).
		Int64("total_items", totalItems).
		Msg("cleanup summary")

	if failCount > 0 {
		os.Exit(1)
	}
}
