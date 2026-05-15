// Package main implements the AetherLite single-binary server.
// It runs the gateway, workflow server, and messaging bridge together in one
// process using embedded SQLite and Badger backends — no external Redis,
// RabbitMQ, or PostgreSQL required.
package main

import (
	"context"
	"embed"
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
	"github.com/scitrera/aether/internal/secrets"
	"github.com/scitrera/aether/internal/state"
	aclsqlite "github.com/scitrera/aether/internal/storage/acl/sqlite"
	auditsqlite "github.com/scitrera/aether/internal/storage/audit/sqlite"
	regsqlite "github.com/scitrera/aether/internal/storage/registry/sqlite"
	tasksqlite "github.com/scitrera/aether/internal/storage/tasks/sqlite"
	wfsqlite "github.com/scitrera/aether/internal/storage/workflow/sqlite"
	"github.com/scitrera/aether/internal/tracing"
	versionpkg "github.com/scitrera/aether/internal/version"
	"github.com/scitrera/aether/internal/workflow"
	sqlitemigrations "github.com/scitrera/aether/migrations/sqlite"
	sqliteregistrymigrations "github.com/scitrera/aether/migrations/sqlite_registry"
	pb_health "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/scitrera/aether/pkg/crypto"
	_ "github.com/scitrera/aether/pkg/dbcompat" // registers "sqlite_compat" driver
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelgrpcfilters "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/test/bufconn"

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
	secretsFile   = flag.String("secrets-file", config.EnvStr("AETHERLITE_SECRETS_FILE", ""), "Optional generated-secrets.yaml; merged into config (HMAC, admin key, TLS paths) (env: AETHERLITE_SECRETS_FILE)")
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

	// Merge a generated-secrets.yaml (if provided / present) into cfg.
	// This is how `init-secrets --generate-tls` hands us the TLS cert/key/CA
	// paths and the admin/HMAC keys — same mechanism the full gateway uses.
	// Missing file is non-fatal; we just run without auto-populated secrets.
	if *secretsFile != "" {
		if _, err := secrets.EnsureSecrets(cfg, *secretsFile, false); err != nil {
			log.Printf("WARNING: failed to load secrets file %q: %v", *secretsFile, err)
		}
	}

	// Initialize HMAC key for token hashing before any token-mint path runs.
	// Required by crypto.HashAPIToken / GenerateToken — without this call,
	// orchestrated task creation logs `"failed to generate auth token: crypto:
	// HMAC key not initialized"` and the agent boots with task_token=None.
	// Full gateway does the same at cmd/gateway/main.go:232-235; lite was
	// missing the call.
	if cfg.Auth.TokenHMACKey != "" {
		crypto.InitTokenHMAC([]byte(cfg.Auth.TokenHMACKey))
		log.Println("Token HMAC key initialized")
	}

	// Reloadable credential wrapper — same as the full gateway. Holds the
	// admin API key, TLS keypair, and token HMAC key behind atomics so
	// SIGHUP rotation lands on the next handshake / next admin auth check.
	// configFile may be empty in pure-CLI mode, in which case Reload is a
	// no-op but the wrapper still serves the initially-loaded values.
	reloadableCfg := config.NewReloadableConfig(*configFile, cfg)

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

	// SIGHUP hot-reloads admin API key, TLS cert/key, and token HMAC key.
	// Shared helper with the full gateway — keep them in lockstep.
	gateway.RunSIGHUPReloader(ctx, reloadableCfg)

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

	// Open dedicated audit SQLite handle (Stage 2 native impl). audit.db
	// is isolated onto its own WAL writer lock — the comprehensive_audit_log
	// writer is an async batcher (auditsqlite.Store + acl.AuditLogger
	// adapter forwarding via audit.EventSink) so the writer goroutine doesn't
	// block the synchronous task-read hot path. Bare "sqlite" driver: the
	// native impl owns all its own SQL and runs its own migrations from
	// migrations/sqlite_audit_native/ on construction.
	auditSQLitePath := filepath.Join(*dataDir, "audit.db")
	auditDB, err := openSQLiteNative(ctx, auditSQLitePath)
	if err != nil {
		logging.Logger.Fatal().Err(err).Str("path", auditSQLitePath).Msg("failed to open audit SQLite database")
	}
	defer auditDB.Close()

	// Open dedicated ACL state SQLite handle (Stage 2 native impl). acl.db
	// holds acl_rules, acl_fallback_policies, acl_authority_grants. The
	// native impl runs migrations/sqlite_acl/ on construction (seeds all
	// 11 fallback policies — §15.1 parity gap closed).
	aclSQLitePath := filepath.Join(*dataDir, "acl.db")
	aclDB, err := openSQLiteNative(ctx, aclSQLitePath)
	if err != nil {
		logging.Logger.Fatal().Err(err).Str("path", aclSQLitePath).Msg("failed to open acl SQLite database")
	}
	defer aclDB.Close()

	// Open dedicated registry SQLite handle (Stage 2 native impl). registry.db
	// holds agent_registry + orchestrator_profiles tables. The native sqlite
	// impl uses the bare "sqlite" driver and runs its own per-domain migration
	// set; vestigial registry tables in aether.db from the dbcompat-translated
	// path are left untouched for transition safety.
	registrySQLitePath := filepath.Join(*dataDir, "registry.db")
	registryDB, err := openSQLiteNative(ctx, registrySQLitePath)
	if err != nil {
		logging.Logger.Fatal().Err(err).Str("path", registrySQLitePath).Msg("failed to open registry SQLite database")
	}
	defer registryDB.Close()

	// Open dedicated tasks SQLite handle (Stage 2 native impl). tasks.db
	// holds the tasks domain tables (tasks, task_audit_events, task_timers,
	// task_checkpoints, task_assignments, task_dlq, orchestrated_task_queue).
	// The native sqlite impl uses the bare "sqlite" driver and runs its own
	// per-domain migration set; vestigial tasks tables in aether.db from the
	// dbcompat-translated path are left untouched for transition safety.
	tasksSQLitePath := filepath.Join(*dataDir, "tasks.db")
	tasksDB, err := openSQLiteNative(ctx, tasksSQLitePath)
	if err != nil {
		logging.Logger.Fatal().Err(err).Str("path", tasksSQLitePath).Msg("failed to open tasks SQLite database")
	}
	defer tasksDB.Close()

	// Build lite-mode subsystems.
	sessions := state.NewBadgerSessionRegistry(badgerDB)
	kvStore := kv.NewBadgerKVStore(badgerDB)
	checkpointStore := checkpoint.NewBadgerCheckpointStore(badgerDB)
	tokenStore := state.NewBadgerTokenStore(badgerDB)
	msgRouter := routerpkg.NewBadgerRouter(badgerDB)
	taskStore, err := tasksqlite.New(tasksDB)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to construct native sqlite tasks store")
	}

	quotaDefaults := quota.DefaultQuotas{
		MaxConnectionsPerWorkspace: 1000,
		MaxMessageRatePerIdentity:  100,
		MaxKVKeysPerNamespace:      10000,
		MaxKVValueSize:             1048576,
	}
	quotaManager := quota.NewMemoryQuotaManager(quotaDefaults)

	// Orchestration (polling dispatcher — no AMQP, no pq.Listener).
	dispatcher := orchestration.NewPollingTaskDispatcher(taskStore)
	// Badger-backed profile state store gives lite mode round-robin
	// orchestrator selection without needing Redis. Wires up the first-ever
	// Registry in aetherlite; previously the legacy ProfileManager was nil
	// and every orchestrator connection logged "orchestration not initialized".
	//
	// As of Stage 1 of the storage-interfaces refactor, registry.AgentRegistry
	// and registry.OrchestratorProfileManager are bundled into a single
	// internal/storage/registry.Store; the consumer interface methods
	// (`Exists`, `RegisterProfiles`, etc.) are unchanged so call sites do not
	// require additional updates beyond the field rename.
	profileStateStore := registry.NewBadgerProfileStateStore(badgerDB)
	registryStore, err := regsqlite.New(registryDB, profileStateStore, sqliteregistrymigrations.MigrationFS)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to construct native sqlite registry store")
	}
	orchServices := &gateway.OrchestrationServices{
		Registry:    registryStore,
		Dispatcher:  dispatcher,
		QueueCloser: orchestration.NewNoopQueueCloser(),
		TokenStore:  tokenStore,
		TaskService: orchestration.NewTaskAssignmentService(
			db,
			taskStore,
			registryStore,
			sessions, // orchestration.SessionLivenessRegistry — was nil; consumer derefs IsOnline.
			nil,      // queueManager: AMQP-only legacy; nil-tolerant (never deref'd).
			registryStore,
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

	// Audit logger (constructed before the ACL service so we can share
	// the single batched writer goroutine — see acl/audit.go for the
	// contention rationale). Native sqlite impl satisfies both audit.Store
	// (consumed by gateway server) and audit.EventSink (consumed by ACL
	// via the legacy adapter).
	auditCfg := audit.DefaultConfig()
	auditCfg.Enabled = cfg.Audit.Enabled
	auditLogger, err := auditsqlite.New(auditDB, cfg.Gateway.GatewayID, auditCfg)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to construct native sqlite audit store")
	}
	defer auditLogger.Close()

	// ACL service (SQLite-backed, Stage 2 native). ACL rules live in acl.db;
	// audit writes funnel through the shared auditLogger above (single writer
	// goroutine), and audit READS use auditDB directly. Per-domain WAL
	// isolation keeps writers from contending across domains.
	sharedACLService, err := aclsqlite.New(aclDB, auditLogger, auditDB, cfg.Gateway.GatewayID)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to construct native sqlite acl store")
	}
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

	// mTLS config — Required defaults to false in lite mode, but when the
	// config supplies an explicit mTLS block we honour it. Mode controls how
	// strictly identity assertions are bound to the presented cert (strict /
	// relaxed); matches the full gateway's semantics.
	mtlsConfig := gateway.MTLSConfig{
		Required: cfg.Auth.MTLS.Required,
		Mode:     gateway.MTLSMode(cfg.Auth.MTLS.Mode),
	}
	if mtlsConfig.Mode == "" {
		mtlsConfig.Mode = gateway.MTLSModeStrict
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

	// Create gRPC server with optional TLS via the shared helper. When
	// TLS is enabled, the helper wires a dynamic cert callback against
	// reloadableCfg so SIGHUP rotation lands on the next handshake — same
	// behaviour as the full gateway.
	grpcServer, tlsEnabled, err := gateway.NewGRPCServerFromConfig(cfg, reloadableCfg, serverOpts...)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to create gRPC server")
	}
	if tlsEnabled {
		logging.Logger.Info().
			Str("cert", cfg.Gateway.TLS.CertFile).
			Str("key", cfg.Gateway.TLS.KeyFile).
			Str("ca", cfg.Gateway.TLS.CAFile).
			Str("client_auth", cfg.Gateway.TLS.ClientAuth).
			Msg("AetherLite TLS enabled (dynamic certificate rotation active)")
	} else {
		logging.Logger.Debug().Msg("AetherLite TLS disabled (plaintext gRPC)")
	}
	pb.RegisterAetherGatewayServer(grpcServer, gatewayServer)

	// gRPC health service.
	healthServer := health.NewServer()
	pb_health.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", pb_health.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("aether.v1.AetherGateway", pb_health.HealthCheckResponse_SERVING)

	// In-process gRPC listener for embedded clients (workflow engine).
	//
	// Wires a second *grpc.Server backed by google.golang.org/grpc/test/bufconn
	// (memory-only listener) with InProcessUnary/StreamInterceptor installed.
	// The interceptors tag every incoming RPC context with an in-process marker
	// so authenticateMTLS treats the connection like an anonymous-mTLS cert —
	// trust the InitConnection-claimed identity, no transport cert required.
	//
	// Why: AetherLite runs the gateway and the workflow engine in the same
	// process. Dialing TCP+TLS over loopback is overhead with no security gain
	// (bytes never leave the process) and failing to wire TLS at all causes
	// the workflow engine to fail handshake against an mTLS-required gateway
	// and enter a reconnect loop. bufconn sidesteps both.
	//
	// Note: msgbridge is also embedded in lite mode. It still uses the
	// network path (cfg.Aether.Address) for now — out of scope for this
	// change but the primitives above are reusable when needed.
	const inProcBufSize = 1024 * 1024
	bufLis := bufconn.Listen(inProcBufSize)
	inProcServer := grpc.NewServer(
		grpc.UnaryInterceptor(gateway.InProcessUnaryInterceptor),
		grpc.StreamInterceptor(gateway.InProcessStreamInterceptor),
		grpc.MaxRecvMsgSize(4*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
	)
	pb.RegisterAetherGatewayServer(inProcServer, gatewayServer)
	pb_health.RegisterHealthServer(inProcServer, healthServer)
	go func() {
		logging.Logger.Info().Msg("AetherLite in-process gRPC listener active (bufconn)")
		if err := inProcServer.Serve(bufLis); err != nil {
			// During shutdown bufLis.Close + inProcServer.GracefulStop produce
			// a benign closed-listener error; only log when ctx is still alive.
			if ctx.Err() == nil {
				logging.Logger.Error().Err(err).Msg("in-process gRPC server error")
			}
		}
	}()

	// Pre-dialed *grpc.ClientConn for the in-process listener. Used by the
	// embedded workflow engine (and reusable for any other embedded client
	// that needs to call into the gateway). insecure creds are fine here —
	// the conn never leaves the process; trust is established by the
	// in-process interceptors on the server side.
	inProcConn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(dialCtx context.Context, _ string) (net.Conn, error) {
			return bufLis.DialContext(dialCtx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to construct in-process gRPC client conn")
	}
	defer inProcConn.Close()

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

	// State provider for admin. Registry already constructed above for the
	// orchestration wiring; reuse it here for both agentRegistry+profileMgr
	// slots (the bundled internal/storage/registry.Store covers both).
	stateProvider := gateway.NewGatewayStateProvider(
		cfg.Gateway.GatewayID,
		nil,     // no Redis session registry in lite mode
		kvStore, // Badger-backed KV store (satisfies KVReadWriter)
		taskStore,
		registryStore,
		registryStore,
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
		// inProcConn drives the workflow engine's aether client through the
		// bufconn in-process listener — no TLS, no localhost dial, no
		// reconnect-loop trap.
		wfCfg := buildWorkflowConfig(inProcConn)
		// Open workflow.db with the bare "sqlite" driver and construct the
		// native sqlite store (Stage 2). wfsqlite.New runs migrations from
		// migrations/sqlite_workflow/ internally. We then inject the store
		// into the workflow server via NewServerWithStore so the engine
		// skips its legacy sqlite_compat path entirely (§15.4).
		wfDB, err := openSQLiteNative(ctx, wfCfg.SQLite.Path)
		if err != nil {
			logging.Logger.Fatal().Err(err).Str("path", wfCfg.SQLite.Path).Msg("failed to open workflow SQLite database")
		}
		defer wfDB.Close()
		wfStore, err := wfsqlite.New(wfDB)
		if err != nil {
			logging.Logger.Fatal().Err(err).Msg("failed to construct native sqlite workflow store")
		}
		wfSrv, err := workflow.NewServerWithStore(wfCfg, wfStore)
		if err != nil {
			logging.Logger.Fatal().Err(err).Msg("failed to create workflow server")
		}
		go func() {
			if err := wfSrv.Run(ctx); err != nil && ctx.Err() == nil {
				logging.Logger.Error().Err(err).Msg("workflow server error")
			}
		}()
		logging.Logger.Info().Msg("embedded workflow server starting (in-process gRPC)")
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

	// Stop the in-process gRPC server first. The workflow engine's aether
	// client is connected through bufconn; tearing down the server here
	// pushes it into the same shutdown path as a network disconnect, and
	// the ctx.Done() above already signaled its reconnect loop to exit.
	inProcServer.GracefulStop()
	if err := bufLis.Close(); err != nil {
		logging.Logger.Debug().Err(err).Msg("bufconn listener close error (likely benign)")
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
//
// inProcConn, when non-nil, is stashed on cfg.Aether.InProcessConn so the
// workflow engine constructs its aether client off the in-process bufconn
// listener instead of dialing. Address is cleared in that case — it would
// be misleading to keep "localhost:50051" around when the conn is in-process.
func buildWorkflowConfig(inProcConn *grpc.ClientConn) *workflow.Config {
	if *workflowConfigFile != "" {
		cfg, err := workflow.LoadConfig(*workflowConfigFile)
		if err != nil {
			logging.Logger.Fatal().Err(err).Str("file", *workflowConfigFile).Msg("failed to load workflow config")
		}
		cfg.Mode = workflow.ModeLite
		cfg.SQLite.Path = filepath.Join(*dataDir, "workflow.db")
		if inProcConn != nil {
			cfg.Aether.InProcessConn = inProcConn
			cfg.Aether.Address = "" // In-process takes precedence; address ignored.
		}
		return cfg
	}

	cfg := &workflow.Config{}
	cfg.Mode = workflow.ModeLite
	cfg.SQLite.Path = filepath.Join(*dataDir, "workflow.db")
	if inProcConn != nil {
		cfg.Aether.InProcessConn = inProcConn
		// Address intentionally left empty — InProcessConn supersedes it.
	} else {
		cfg.Aether.Address = fmt.Sprintf("localhost:%d", *port)
	}
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

// openSQLite opens a SQLite database via the sqlite_compat driver (dbcompat
// translation layer). Used for handles that still emit postgres-flavored SQL
// — i.e. domains not yet cut over to native sqlite impls. See openSQLiteNative
// for handles backing internal/storage/<domain>/sqlite/ stores.
func openSQLite(ctx context.Context, path string) (*sql.DB, error) {
	return openSQLiteWithDriver(ctx, path, "sqlite_compat")
}

// openSQLiteNative opens a SQLite database via the bare "sqlite" driver
// (modernc.org/sqlite). For per-domain handles backing native sqlite store
// impls — they own their own SQL and don't need dbcompat translation.
func openSQLiteNative(ctx context.Context, path string) (*sql.DB, error) {
	return openSQLiteWithDriver(ctx, path, "sqlite")
}

func openSQLiteWithDriver(ctx context.Context, path, driver string) (*sql.DB, error) {
	db, err := sql.Open(driver, path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database %s (driver=%s): %w", path, driver, err)
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

// runSQLiteMigrations applies gateway SQLite migrations (aether.db) from the embedded filesystem.
func runSQLiteMigrations(ctx context.Context, db *sql.DB) error {
	return applySQLiteMigrations(ctx, db, sqlitemigrations.MigrationFS, "aether.db")
}

// applySQLiteMigrations is the shared body used by both migration runners.
// `fs` provides the *.sql files (alphabetical order); `label` is included
// in log messages so operators can tell which DB the migration ran against.
func applySQLiteMigrations(ctx context.Context, db *sql.DB, fs embed.FS, label string) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	entries, err := fs.ReadDir(".")
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

		content, err := fs.ReadFile(entry.Name())
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
		logging.Logger.Debug().Str("db", label).Str("version", version).Msg("SQLite migration applied")
	}
	return nil
}
