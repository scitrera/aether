// Package main implements the AetherLite single-binary server.
// It runs the gateway, workflow server, and messaging bridge together in one
// process using embedded SQLite and Badger backends — no external Redis,
// RabbitMQ, or PostgreSQL required.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/checkpoint"
	"github.com/scitrera/aether/internal/cleanup"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/gateway"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/lite"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/msgbridge"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/quota"
	"github.com/scitrera/aether/internal/registry"
	routerpkg "github.com/scitrera/aether/internal/router"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/internal/tracing"
	versionpkg "github.com/scitrera/aether/internal/version"
	"github.com/scitrera/aether/internal/workflow"
	sqlitemigrations "github.com/scitrera/aether/migrations/sqlite"
	pb_health "google.golang.org/grpc/health/grpc_health_v1"

	_ "github.com/scitrera/aether/pkg/dbcompat" // registers "sqlite_compat" driver
	"github.com/scitrera/aether/pkg/tasks"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelgrpcfilters "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/keepalive"

	"database/sql"
)

var (
	version = versionpkg.Version
	banner  = `
    _         _   _               _     _ _
   / \   ___ | |_| |__   ___ _ __| |   (_) |_ ___
  / _ \ / _ \| __| '_ \ / _ \ '__| |   | | __/ _ \
 / ___ \  __/| |_| | | |  __/ |  | |___| | ||  __/
/_/   \_\___| \__|_| |_|\___|_|  |_____|_|\__\___|

AetherLite v%s — embedded single-binary server
`
)

// Every CLI flag has a matching AETHERLITE_* environment variable.
// Precedence: explicit CLI flag > environment variable > compiled-in default.
// config.EnvStr/EnvInt/EnvBool set the flag's default at init time, so the
// user can still override on the command line.
var (
	configFile    = flag.String("config", config.EnvStr("AETHERLITE_CONFIG", ""), "Optional path to a gateway config file (env: AETHERLITE_CONFIG)")
	dataDir       = flag.String("data-dir", config.EnvStr("AETHERLITE_DATA_DIR", "./aether-lite-data"), "Data directory for SQLite and Badger storage (env: AETHERLITE_DATA_DIR)")
	port          = flag.Int("port", config.EnvInt("AETHERLITE_PORT", 50051), "gRPC server port (env: AETHERLITE_PORT)")
	adminPort     = flag.Int("admin-port", config.EnvInt("AETHERLITE_ADMIN_PORT", 31880), "Admin UI port (env: AETHERLITE_ADMIN_PORT)")
	devMode       = flag.Bool("dev", config.EnvBool("AETHERLITE_DEV", false), "Development mode (relaxed security, CORS wildcard) (env: AETHERLITE_DEV)")
	insecureAdmin = flag.Bool("insecure-admin", config.EnvBool("AETHERLITE_INSECURE_ADMIN", false), "Allow admin API without authentication (NOT FOR PRODUCTION) (env: AETHERLITE_INSECURE_ADMIN)")
	showVersion   = flag.Bool("version", false, "Show version and exit")
	showHelp      = flag.Bool("help", false, "Show this help message")
	// Workflow options
	enableWorkflow     = flag.Bool("workflow", config.EnvBool("AETHERLITE_WORKFLOW", true), "Enable embedded workflow server (env: AETHERLITE_WORKFLOW)")
	workflowConfigFile = flag.String("workflow-config", config.EnvStr("AETHERLITE_WORKFLOW_CONFIG", ""), "Optional workflow config file (overrides auto-config) (env: AETHERLITE_WORKFLOW_CONFIG)")
	workflowAdminPort  = flag.Int("workflow-admin-port", config.EnvInt("AETHERLITE_WORKFLOW_ADMIN_PORT", 31881), "Workflow admin API port (env: AETHERLITE_WORKFLOW_ADMIN_PORT)")
	// Msgbridge options
	enableMsgbridge     = flag.Bool("msgbridge", config.EnvBool("AETHERLITE_MSGBRIDGE", false), "Enable embedded messaging bridge server (env: AETHERLITE_MSGBRIDGE)")
	msgbridgeConfigFile = flag.String("msgbridge-config", config.EnvStr("AETHERLITE_MSGBRIDGE_CONFIG", ""), "Optional msgbridge config file (overrides auto-config) (env: AETHERLITE_MSGBRIDGE_CONFIG)")
	msgbridgeAdminPort  = flag.Int("msgbridge-admin-port", config.EnvInt("AETHERLITE_MSGBRIDGE_ADMIN_PORT", 31882), "Msgbridge admin API port (env: AETHERLITE_MSGBRIDGE_ADMIN_PORT)")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, banner, version)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "AetherLite runs the gateway, workflow server, and messaging bridge\n")
		fmt.Fprintf(os.Stderr, "together in a single process with embedded SQLite + Badger backends.\n")
		fmt.Fprintf(os.Stderr, "No external Redis, RabbitMQ, or PostgreSQL is required.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}
	if *showVersion {
		fmt.Printf("AetherLite v%s\n", version)
		os.Exit(0)
	}

	// --insecure-admin requires opt-in env var to prevent accidental production use.
	if *insecureAdmin && os.Getenv("AETHER_ALLOW_DEV_MODE") == "" {
		log.Fatal("--insecure-admin requires AETHER_ALLOW_DEV_MODE env var to be set; this flag is NOT for production use")
	}

	log.Printf(banner, version)

	// Build gateway config (always lite mode).
	cfg := buildGatewayConfig()

	// Initialize structured logger.
	logging.Init(cfg.LogLevel)

	// Initialize OpenTelemetry tracing.
	tracingShutdown, err := tracing.InitTracer("aether-lite")
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to initialize tracing")
	}
	defer func() {
		// Best-effort tracing shutdown on exit; any error is unactionable here.
		_ = tracingShutdown(context.Background())
	}()

	// Setup graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// ===== Initialize lite backends =====
	logging.Logger.Info().Str("data_dir", *dataDir).Msg("starting AetherLite (embedded backends)")

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		logging.Logger.Fatal().Err(err).Str("data_dir", *dataDir).Msg("failed to create data directory")
	}

	// Open Badger.
	badgerDir := filepath.Join(*dataDir, "badger")
	badgerDB, err := lite.OpenBadger(badgerDir)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to open Badger database")
	}
	defer badgerDB.Close()
	lite.RunGC(ctx, badgerDB, 0)
	logging.Logger.Debug().Str("dir", badgerDir).Msg("Badger database opened")

	// Open SQLite.
	sqlitePath := filepath.Join(*dataDir, "aether.db")
	db, err := openSQLite(ctx, sqlitePath)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to open SQLite database")
	}
	defer db.Close()

	// Run gateway SQLite migrations.
	if err := runSQLiteMigrations(ctx, db); err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to run SQLite migrations")
	}

	// Build lite-mode subsystems.
	sessions := state.NewBadgerSessionRegistry(badgerDB)
	kvStore := kv.NewBadgerKVStore(badgerDB)
	checkpointStore := checkpoint.NewBadgerCheckpointStore(badgerDB)
	tokenStore := state.NewBadgerTokenStore(badgerDB)
	msgRouter := routerpkg.NewBadgerRouter(badgerDB)
	taskStore := tasks.NewTaskStore(db)

	quotaDefaults := quota.DefaultQuotas{
		MaxConnectionsPerWorkspace: 1000,
		MaxMessageRatePerIdentity:  100,
		MaxKVKeysPerNamespace:      10000,
		MaxKVValueSize:             1048576,
	}
	quotaManager := quota.NewMemoryQuotaManager(quotaDefaults)

	// Orchestration (memory dispatcher — no AMQP).
	dispatcher := orchestration.NewMemoryTaskDispatcher(db)
	orchServices := &gateway.OrchestrationServices{
		Dispatcher:  dispatcher,
		QueueCloser: orchestration.NewMemoryQueueCloser(),
		TokenStore:  tokenStore,
		TaskService: orchestration.NewTaskAssignmentService(
			db, taskStore, nil, nil, nil, nil,
		),
	}
	orchServices.TaskService.SetTokenStore(tokenStore)

	// Gateway options.
	var gatewayOpts []gateway.GatewayOption
	gatewayOpts = append(gatewayOpts, gateway.WithQuotaManager(quotaManager))
	gatewayOpts = append(gatewayOpts, gateway.WithOrchestrationServices(orchServices))
	gatewayOpts = append(gatewayOpts, gateway.WithCheckpointDefaultTTL(cfg.Checkpoint.GetDefaultTTL()))

	// Workspace rate limiter.
	workspaceRL := quota.NewWorkspaceRateLimiter(cfg.Gateway.MessageRateLimit)
	gatewayOpts = append(gatewayOpts, gateway.WithWorkspaceRateLimiter(workspaceRL))

	// Per-principal rate limiter for foreign audit submissions (SubmitAuditEvent).
	// Default 100 events/sec/principal; tunable via AETHER_AUDIT_FOREIGN_RATE_PER_SEC.
	foreignAuditRL := quota.NewPrincipalRateLimiter(float64(config.EnvInt("AETHER_AUDIT_FOREIGN_RATE_PER_SEC", 100)))
	gatewayOpts = append(gatewayOpts, gateway.WithForeignAuditRateLimiter(foreignAuditRL))

	// ACL service (SQLite-backed).
	sharedACLService := acl.NewService(db, cfg.Gateway.GatewayID)
	gatewayOpts = append(gatewayOpts, gateway.WithACLService(sharedACLService))

	// Cleanup service.
	cleanupConfig := &cleanup.Config{
		TaskPurgeInterval:      cfg.Cleanup.GetTaskPurgeInterval(),
		CompletedTaskRetention: cfg.Cleanup.GetCompletedTaskRetention(),
		FailedTaskRetention:    cfg.Cleanup.GetFailedTaskRetention(),
		CancelledTaskRetention: cfg.Cleanup.GetCancelledTaskRetention(),
		ReconciliationInterval: cfg.Cleanup.GetReconciliationInterval(),
	}
	gatewayOpts = append(gatewayOpts, gateway.WithCleanupService(cleanupConfig))

	// Audit logger.
	auditCfg := audit.DefaultConfig()
	auditCfg.Enabled = cfg.Audit.Enabled
	auditLogger := audit.NewAuditLogger(db, cfg.Gateway.GatewayID, auditCfg)
	defer auditLogger.Close()

	// mTLS config (disabled in lite mode by default).
	mtlsConfig := gateway.MTLSConfig{
		Required: false,
		Mode:     gateway.MTLSModeStrict,
	}

	// Create gateway server.
	gatewayServer := gateway.NewGatewayServer(
		sessions, msgRouter, kvStore, checkpointStore, taskStore,
		db, cfg.Gateway.GatewayID, auditLogger, mtlsConfig, gatewayOpts...,
	)
	defer gatewayServer.Stop()

	// gRPC server options.
	// Filter Connect out of the otelgrpc stats handler — bidi streams live
	// for the full client session (hours), and the per-RPC span would
	// swallow every per-message child span as a long-lived parent. See
	// gateway/connect.go for the per-message spans that become root traces
	// once this envelope span is gone.
	otelSkipLongStreams := otelgrpcfilters.None(
		otelgrpcfilters.FullMethodName("/aether.v1.AetherGateway/Connect"),
	)
	serverOpts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(otelSkipLongStreams))),
		grpc.MaxRecvMsgSize(4 * 1024 * 1024),
		grpc.MaxSendMsgSize(16 * 1024 * 1024),
		grpc.MaxConcurrentStreams(1000),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute,
			MaxConnectionAge:      2 * time.Hour,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: false,
		}),
	}

	grpcServer := grpc.NewServer(serverOpts...)
	pb.RegisterAetherGatewayServer(grpcServer, gatewayServer)

	// gRPC health service.
	healthServer := health.NewServer()
	pb_health.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", pb_health.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("aether.v1.AetherGateway", pb_health.HealthCheckResponse_SERVING)

	// Start gRPC server.
	grpcAddr := fmt.Sprintf(":%d", cfg.Gateway.Port)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logging.Logger.Fatal().Err(err).Str("addr", grpcAddr).Msg("failed to listen")
	}
	go func() {
		logging.Logger.Info().Str("addr", grpcAddr).Msg("AetherLite gRPC gateway listening")
		if err := grpcServer.Serve(lis); err != nil {
			logging.Logger.Fatal().Err(err).Msg("gRPC server error")
		}
	}()

	// State provider for admin.
	agentRegistry := registry.NewAgentRegistry(db)
	stateProvider := gateway.NewGatewayStateProvider(
		cfg.Gateway.GatewayID,
		nil, // no Redis session registry in lite mode
		nil, // no Redis KV store in lite mode
		taskStore,
		agentRegistry,
		nil, // no orchestrator profile manager in lite mode
		sharedACLService,
		db,
		nil, // no RabbitMQ router in lite mode
	)
	stateProvider.SetGateway(gatewayServer)
	stateProvider.SetWorkspaceRateLimiter(workspaceRL)
	gatewayServer.SetAdminProvider(stateProvider)

	// Ops server (health + metrics).
	opsServer := admin.NewOpsServer(cfg.Gateway.GetOpsPort(), stateProvider)
	opsServer.SetReady(true)
	go func() {
		if err := opsServer.Start(); err != nil && err != http.ErrServerClosed {
			logging.Logger.Error().Err(err).Msg("ops server error")
		}
	}()

	// Admin UI server.
	var adminServer *admin.Server
	if cfg.Admin.Enabled {
		adminServer = admin.NewServer(admin.ServerConfig{
			Port:           cfg.Admin.Port,
			DevMode:        *devMode,
			InsecureNoAuth: *insecureAdmin || *devMode,
			CORSOrigin:     cfg.Admin.CORSOrigin,
		}, stateProvider)
		go func() {
			if err := adminServer.Start(); err != nil && err != http.ErrServerClosed {
				logging.Logger.Error().Err(err).Msg("admin server error")
			}
		}()
		logging.Logger.Info().Int("port", cfg.Admin.Port).Msg("admin UI listening")
	}

	// ===== Start embedded workflow server =====
	if *enableWorkflow {
		wfCfg := buildWorkflowConfig()
		wfSrv, err := workflow.NewServer(wfCfg)
		if err != nil {
			logging.Logger.Fatal().Err(err).Msg("failed to create workflow server")
		}
		go func() {
			if err := wfSrv.Run(ctx); err != nil && ctx.Err() == nil {
				logging.Logger.Error().Err(err).Msg("workflow server error")
			}
		}()
		logging.Logger.Info().Str("gateway", wfCfg.Aether.Address).Msg("embedded workflow server starting")
	}

	// ===== Start embedded msgbridge server (only if enabled) =====
	if *enableMsgbridge {
		mbCfg := buildMsgbridgeConfig()
		mbSrv, err := msgbridge.NewServer(mbCfg)
		if err != nil {
			logging.Logger.Fatal().Err(err).Msg("failed to create msgbridge server")
		}
		go func() {
			if err := mbSrv.Run(ctx); err != nil && ctx.Err() == nil {
				logging.Logger.Error().Err(err).Msg("msgbridge server error")
			}
		}()
		logging.Logger.Info().Str("gateway", mbCfg.Aether.Address).Msg("embedded msgbridge server starting")
	}

	logging.Logger.Info().
		Int("grpc_port", cfg.Gateway.Port).
		Int("admin_port", cfg.Admin.Port).
		Str("data_dir", *dataDir).
		Bool("workflow", *enableWorkflow).
		Bool("msgbridge", *enableMsgbridge).
		Msg("AetherLite is ready")

	// Wait for shutdown signal.
	<-sigChan
	logging.Logger.Info().Msg("shutdown signal received, gracefully stopping")

	cancel() // stop workflow and msgbridge goroutines

	gracefulTimeout := cfg.Shutdown.GetGracefulTimeout()

	{
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), gracefulTimeout)
		defer shutdownCancel()
		if err := opsServer.Stop(shutdownCtx); err != nil {
			logging.Logger.Error().Err(err).Msg("ops server shutdown error")
		}
	}
	if adminServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), gracefulTimeout)
		defer shutdownCancel()
		if err := adminServer.Stop(shutdownCtx); err != nil {
			logging.Logger.Error().Err(err).Msg("admin server shutdown error")
		}
	}

	gatewayServer.Stop()
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		logging.Logger.Info().Msg("gRPC server stopped gracefully")
	case <-time.After(gracefulTimeout):
		logging.Logger.Warn().Dur("timeout", gracefulTimeout).Msg("graceful shutdown timed out, forcing stop")
		grpcServer.Stop()
	}

	logging.Logger.Info().Msg("AetherLite stopped")
}

// buildGatewayConfig constructs a gateway config for lite mode from CLI flags
// and an optional config file.
func buildGatewayConfig() *config.Config {
	var cfg *config.Config

	if *configFile != "" {
		loaded, err := config.Load(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config file %s: %v", *configFile, err)
		}
		cfg = loaded
		log.Printf("Configuration loaded from: %s", *configFile)
	} else {
		cfg = &config.Config{}
		cfg.LogLevel = "info"
		cfg.Admin.CORSOrigin = "*"
	}

	// Always force lite mode.
	cfg.Mode = "lite"
	cfg.Lite.DataDir = *dataDir

	// Apply CLI port overrides.
	if *port != 0 {
		cfg.Gateway.Port = *port
	}
	if cfg.Gateway.Port == 0 {
		cfg.Gateway.Port = 50051
	}
	if *adminPort != 0 {
		cfg.Admin.Port = *adminPort
	}
	if cfg.Admin.Port == 0 {
		cfg.Admin.Port = 31880
	}

	// Enable admin UI.
	cfg.Admin.Enabled = true

	// In dev mode, set AETHER_DEV_MODE so config validation passes.
	if *devMode {
		os.Setenv("AETHER_DEV_MODE", "true")
	}
	// CORS wildcard requires dev mode env var.
	if cfg.Admin.CORSOrigin == "*" && os.Getenv("AETHER_ALLOW_DEV_MODE") == "" {
		os.Setenv("AETHER_ALLOW_DEV_MODE", "true")
	}

	// Set gateway ID if not already set.
	if cfg.Gateway.GatewayID == "" {
		cfg.Gateway.GatewayID = "aetherlite"
	}

	// Validate (lite mode skips Postgres/Redis/RabbitMQ checks).
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Configuration invalid: %v", err)
	}

	return cfg
}

// buildWorkflowConfig constructs a workflow.Config for lite mode.
// If --workflow-config is provided it is loaded and mode overridden to lite;
// otherwise sensible defaults pointing at the local gateway are used.
func buildWorkflowConfig() *workflow.Config {
	if *workflowConfigFile != "" {
		cfg, err := workflow.LoadConfig(*workflowConfigFile)
		if err != nil {
			logging.Logger.Fatal().Err(err).Str("file", *workflowConfigFile).Msg("failed to load workflow config")
		}
		cfg.Mode = workflow.ModeLite
		cfg.SQLite.Path = filepath.Join(*dataDir, "workflow.db")
		return cfg
	}

	cfg := &workflow.Config{}
	cfg.Mode = workflow.ModeLite
	cfg.SQLite.Path = filepath.Join(*dataDir, "workflow.db")
	cfg.Aether.Address = fmt.Sprintf("localhost:%d", *port)
	cfg.Aether.Implementation = "aether-workflow"
	cfg.Aether.Workspace = "_system"
	cfg.Admin.Enabled = true
	cfg.Admin.Port = *workflowAdminPort
	cfg.Logging.Level = "info"
	return cfg
}

// buildMsgbridgeConfig constructs a msgbridge.Config for lite mode.
// If --msgbridge-config is provided it is loaded and mode overridden to lite;
// otherwise sensible defaults pointing at the local gateway are used.
func buildMsgbridgeConfig() *msgbridge.Config {
	if *msgbridgeConfigFile != "" {
		cfg, err := msgbridge.LoadConfig(*msgbridgeConfigFile)
		if err != nil {
			logging.Logger.Fatal().Err(err).Str("file", *msgbridgeConfigFile).Msg("failed to load msgbridge config")
		}
		cfg.Mode = "sqlite"
		cfg.SQLite.Path = filepath.Join(*dataDir, "msgbridge.db")
		return cfg
	}

	cfg := &msgbridge.Config{}
	cfg.Mode = "sqlite"
	cfg.SQLite.Path = filepath.Join(*dataDir, "msgbridge.db")
	cfg.Aether.Address = fmt.Sprintf("localhost:%d", *port)
	cfg.Aether.Implementation = "aether-msgbridge"
	cfg.Aether.Specifier = "instance-1"
	cfg.Admin.Enabled = true
	cfg.Admin.Port = *msgbridgeAdminPort
	cfg.Logging.Level = "info"
	return cfg
}

// openSQLite opens a SQLite database with pragmas tuned for WAL mode.
func openSQLite(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite_compat", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database %s: %w", path, err)
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			logging.Logger.Warn().Err(err).Str("pragma", pragma).Msg("SQLite pragma failed")
		}
	}
	logging.Logger.Debug().Str("path", path).Msg("SQLite database opened")
	return db, nil
}

// runSQLiteMigrations applies gateway SQLite migrations from the embedded filesystem.
func runSQLiteMigrations(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	entries, err := sqlitemigrations.MigrationFS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("failed to read SQLite migrations: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version := entry.Name()[:len(entry.Name())-4]

		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version,
		).Scan(&count); err != nil {
			return fmt.Errorf("failed to check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}

		content, err := sqlitemigrations.MigrationFS.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES (?)", version,
		); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", entry.Name(), err)
		}
		logging.Logger.Debug().Str("version", version).Msg("SQLite migration applied")
	}
	return nil
}
