package main

import (
	"context"
	"database/sql"
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

	"github.com/redis/go-redis/v9"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/checkpoint"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/internal/cleanup"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/gateway"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/quota"
	"github.com/scitrera/aether/internal/readiness"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/router"
	"github.com/scitrera/aether/internal/secrets"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/internal/tracing"
	versionpkg "github.com/scitrera/aether/internal/version"
	"github.com/scitrera/aether/pkg/certgen"
	"github.com/scitrera/aether/pkg/crypto"
	"github.com/scitrera/aether/pkg/tasks"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelgrpcfilters "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

var (
	version = versionpkg.Version
	banner  = `
    _         _   _
   / \   ___ | |_| |__   ___ _ __
  / _ \ / _ \| __| '_ \ / _ \ '__|
 / ___ \  __/| |_| | | |  __/ |
/_/   \_\___| \__|_| |_|\___|_|

Aether Gateway v%s
`
)

var (
	// Config file
	configFile = flag.String("config", "", "Path to configuration file (required unless --dev is set)")

	// Server options
	port = flag.Int("port", 0, "gRPC server port (overrides config)")

	// mTLS options
	enableTLS = flag.Bool("tls", false, "Enable mTLS (requires cert-file, key-file, ca-file)")
	certFile  = flag.String("cert-file", "", "Server certificate file for mTLS")
	keyFile   = flag.String("key-file", "", "Server key file for mTLS")
	caFile    = flag.String("ca-file", "", "CA certificate file for mTLS client verification")

	// Database options
	dbHost = flag.String("db-host", "", "PostgreSQL host (overrides config)")
	dbPort = flag.Int("db-port", 0, "PostgreSQL port (overrides config)")
	dbUser = flag.String("db-user", "", "PostgreSQL user (overrides config)")
	dbName = flag.String("db-name", "", "PostgreSQL database name (overrides config)")

	// Redis options
	redisAddr = flag.String("redis", "", "Redis address (overrides config, format: host:port)")

	// RabbitMQ options
	streamURL = flag.String("stream-url", "", "RabbitMQ Stream URL (overrides config)")
	amqpURL   = flag.String("amqp-url", "", "RabbitMQ AMQP URL (overrides config)")

	// Admin UI options (override config)
	adminPort     = flag.Int("admin-port", 0, "Admin UI port (overrides config)")
	adminDevPath  = flag.String("admin-dev-path", "", "Path to admin static files in dev mode")
	insecureAdmin = flag.Bool("insecure-admin", false, "Allow admin API to run without authentication when no AETHER_ADMIN_API_KEY is set (NOT FOR PRODUCTION)")

	// Secrets options
	secretsFile = flag.String("secrets-file", "/etc/aether/generated-secrets.yaml", "Path to generated secrets file")

	// Other options
	showVersion = flag.Bool("version", false, "Show version and exit")
	showHelp    = flag.Bool("help", false, "Show this help message")
	devMode     = flag.Bool("dev", false, "Allow startup with hardcoded development defaults when config file is missing (NOT FOR PRODUCTION)")
	liteMode    = flag.Bool("lite", false, "Run in lite mode with embedded backends (SQLite + Badger, no external Redis/RabbitMQ/PostgreSQL required)")
)

func main() {
	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, banner, version)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Aether is a distributed control plane for routing structured messages,\n")
		fmt.Fprintf(os.Stderr, "tracking tasks, and managing connection lifecycles.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Start with default config:\n")
		fmt.Fprintf(os.Stderr, "  %s\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Start with custom config:\n")
		fmt.Fprintf(os.Stderr, "  %s -config /path/to/config.yaml\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Start with mTLS enabled:\n")
		fmt.Fprintf(os.Stderr, "  %s -tls -cert-file ./certs/gateway-cert.pem -key-file ./certs/gateway-key.pem -ca-file ./certs/ca-cert.pem\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Override port:\n")
		fmt.Fprintf(os.Stderr, "  %s -port 8080\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Environment Variables:\n")
		fmt.Fprintf(os.Stderr, "  Config values can be overridden by environment variables.\n")
		fmt.Fprintf(os.Stderr, "  See configs/dev.yaml for examples.\n\n")
	}

	flag.Parse()

	// --insecure-admin bypasses all authentication on the admin API.
	// Require AETHER_ALLOW_DEV_MODE to prevent accidental use in production.
	if *insecureAdmin && os.Getenv("AETHER_ALLOW_DEV_MODE") == "" {
		log.Fatal("--insecure-admin requires AETHER_ALLOW_DEV_MODE env var to be set; this flag is NOT for production use")
	}

	// Handle special flags
	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Printf("Aether Gateway v%s\n", version)
		os.Exit(0)
	}

	// Print banner
	log.Printf(banner, version)

	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Apply CLI flag overrides
	applyFlagOverrides(cfg)

	// In dev mode, auto-generate missing secrets so the gateway can start
	// without manually providing HMAC key and admin API key.
	if *devMode {
		if _, err := secrets.EnsureSecrets(cfg, *secretsFile, false); err != nil {
			log.Printf("WARNING: auto-init-secrets failed: %v", err)
		}
	}

	// CLI TLS flags override config
	if *certFile != "" {
		cfg.Gateway.TLS.CertFile = *certFile
	}
	if *keyFile != "" {
		cfg.Gateway.TLS.KeyFile = *keyFile
	}
	if *caFile != "" {
		cfg.Gateway.TLS.CAFile = *caFile
	}
	if *enableTLS && cfg.Gateway.TLS.ClientAuth == "" {
		cfg.Gateway.TLS.ClientAuth = "require"
	}

	// Auto-TLS: generate self-signed certificates if AETHER_AUTO_TLS=true
	// and no TLS cert/key is configured yet
	if os.Getenv("AETHER_AUTO_TLS") == "true" && cfg.Gateway.TLS.CertFile == "" && cfg.Gateway.TLS.KeyFile == "" {
		tlsDir := filepath.Join(filepath.Dir(*secretsFile), "tls")
		caCertPath := filepath.Join(tlsDir, "ca-cert.pem")
		caKeyPath := filepath.Join(tlsDir, "ca-key.pem")
		serverCertPath := filepath.Join(tlsDir, "server-cert.pem")
		serverKeyPath := filepath.Join(tlsDir, "server-key.pem")

		ca, autoErr := certgen.EnsureCA(caCertPath, caKeyPath, certgen.CAOptions{})
		if autoErr != nil {
			log.Fatalf("Auto-TLS: failed to ensure CA: %v", autoErr)
		}

		// Generate server cert if not present
		if _, statErr := os.Stat(serverCertPath); os.IsNotExist(statErr) {
			bundle, genErr := ca.GenerateServerCert(certgen.ServerCertOptions{})
			if genErr != nil {
				log.Fatalf("Auto-TLS: failed to generate server certificate: %v", genErr)
			}
			if saveErr := bundle.SaveToFiles(serverCertPath, serverKeyPath); saveErr != nil {
				log.Fatalf("Auto-TLS: failed to save server certificate: %v", saveErr)
			}
		}

		cfg.Gateway.TLS.CertFile = serverCertPath
		cfg.Gateway.TLS.KeyFile = serverKeyPath
		cfg.Gateway.TLS.CAFile = caCertPath
		if cfg.Gateway.TLS.ClientAuth == "" {
			cfg.Gateway.TLS.ClientAuth = "request" // allow both anonymous and cert-bearing connections
		}
		log.Printf("Auto-TLS: generated self-signed certificates in %s", tlsDir)
	}

	// Determine if TLS is effectively enabled (from config, flags, or auto-TLS)
	tlsEnabled := *enableTLS || (cfg.Gateway.TLS.CertFile != "" && cfg.Gateway.TLS.KeyFile != "")

	// Validate configuration — fail fast before any components are initialized
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Refuse to run credential-based auth over plaintext gRPC unless explicitly overridden
	if !tlsEnabled && (cfg.Auth.IsAuthModeEnabled("api_key") || cfg.Auth.IsAuthModeEnabled("oauth")) {
		log.Println("SECURITY WARNING: credential-based auth (api_key/oauth) is enabled but gRPC TLS is disabled; credentials will be transmitted in plaintext")
		if os.Getenv("AETHER_DEV_MODE") != "true" && !*devMode {
			log.Fatal("refusing to start with credential-based auth over plaintext gRPC; enable -tls or set AETHER_DEV_MODE=true to override")
		}
	}

	// Initialize HMAC key for token hashing if configured
	if cfg.Auth.TokenHMACKey != "" {
		crypto.InitTokenHMAC([]byte(cfg.Auth.TokenHMACKey))
		log.Println("Token HMAC key initialized")
	}

	// Build reloadable config wrapper for hot-reloadable credentials (SIGHUP).
	// configFile is empty in pure dev-mode-defaults path, in which case reload is a no-op.
	reloadableCfg := config.NewReloadableConfig(*configFile, cfg)

	// Initialize structured logger
	logging.Init(cfg.LogLevel)

	// Initialize OpenTelemetry tracing
	tracingShutdown, err := tracing.InitTracer("aether-gateway")
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to initialize tracing")
	}
	defer func() {
		// Best-effort tracing shutdown on exit; any error is unactionable here.
		_ = tracingShutdown(context.Background())
	}()
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		logging.Logger.Info().Str("endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")).Msg("OpenTelemetry tracing enabled")
	} else {
		logging.Logger.Debug().Msg("OpenTelemetry tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")
	}

	// Initialize OpenTelemetry log bridge — exports structured zerolog
	// events to SigNoz via OTLP with trace context correlation.
	logBridge, err := tracing.NewLogBridge(context.Background(), "aether-gateway")
	if err != nil {
		logging.Logger.Warn().Err(err).Msg("failed to initialize log bridge, continuing without OTLP log export")
	}
	if logBridge != nil {
		logging.AddHook(logBridge)
		defer func() {
			if err := logBridge.Shutdown(context.Background()); err != nil {
				logging.Logger.Warn().Err(err).Msg("log bridge shutdown error")
			}
		}()
		logging.Logger.Debug().Msg("OpenTelemetry log bridge initialized")
	}

	// Print configuration summary
	logging.Logger.Info().
		Str("gateway_id", cfg.Gateway.GatewayID).
		Int("grpc_port", cfg.Gateway.Port).
		Bool("admin_enabled", cfg.Admin.Enabled).
		Int("admin_port", cfg.Admin.Port).
		Str("postgres", fmt.Sprintf("%s:%d/%s", cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database)).
		Strs("redis_cluster", cfg.Redis.Cluster).
		Str("rabbitmq", redactURL(cfg.RabbitMQ.StreamURL)).
		Str("log_level", cfg.LogLevel).
		Msg("Configuration loaded")

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// SIGHUP triggers a hot-reload of reloadable credentials.
	// Shared helper with aetherlite — keeps the two binaries in lockstep.
	gateway.RunSIGHUPReloader(ctx, reloadableCfg)

	// Backend variables — set by either lite-mode or standard initialization.
	var db *sql.DB
	var sessions gateway.SessionManager
	var kvStore gateway.KVReadWriter
	var checkpointStore gateway.CheckpointManager
	var msgRouter gateway.MessageRouter
	var taskStore *tasks.TaskStore
	var redisClient redis.UniversalClient // nil in lite mode
	var gatewayOpts []gateway.GatewayOption

	if cfg.IsLiteMode() {
		// ===== LITE MODE: embedded backends (SQLite + Badger) =====
		logging.Logger.Info().Msg("starting in AetherLite mode (embedded backends)")
		backends, err := initLiteBackends(ctx, cfg)
		if err != nil {
			logging.Logger.Fatal().Err(err).Msg("failed to initialize lite backends")
		}
		defer backends.Close()

		db = backends.db
		sessions = backends.sessions
		kvStore = backends.kvStore
		checkpointStore = backends.checkpoints
		msgRouter = backends.router
		taskStore = backends.taskStore
		gatewayOpts = backends.gatewayOpts
	} else {
		// ===== FULL MODE: PostgreSQL + Redis + RabbitMQ =====

		// Initialize PostgreSQL
		var dbErr error
		db, dbErr = initDatabase(ctx, cfg)
		if dbErr != nil {
			logging.Logger.Fatal().Err(dbErr).Msg("failed to initialize database")
		}
		if db != nil {
			defer db.Close()
		}

		// Production readiness check (skip in dev mode)
		if !*devMode {
			checker := readiness.NewChecker()
			results := checker.CheckConfig(cfg, *devMode)
			if db != nil {
				results = append(results, checker.CheckDatabase(db)...)
			}
			checker.PrintReport(results)
			if checker.HasCriticalFailures(results) {
				logging.Logger.Fatal().Msg("production readiness check failed — fix critical issues or start with --dev flag")
			}
		}

		// Initialize shared Redis connection pool
		if len(cfg.Redis.Cluster) == 0 {
			logging.Logger.Fatal().Msg("Redis addresses not configured")
		}
		redisClient = cfg.Redis.NewClient()
		defer redisClient.Close()
		logging.Logger.Debug().Strs("addrs", cfg.Redis.Cluster).Str("mode", cfg.Redis.GetMode()).Msg("Redis client initialized")

		// Initialize subsystems from shared Redis client
		redisSessions := state.NewSessionRegistryFromClient(redisClient)
		sessions = redisSessions
		var redisKVStore *kv.Store
		if kvDefaultTTL := cfg.KV.GetDefaultTTL(); kvDefaultTTL > 0 {
			redisKVStore = kv.NewStoreFromClientWithTTL(redisClient, kvDefaultTTL)
			logging.Logger.Debug().Dur("ttl", kvDefaultTTL).Msg("KV store default TTL configured")
		} else {
			redisKVStore = kv.NewStoreFromClient(redisClient)
			logging.Logger.Debug().Dur("ttl", cfg.KV.GetDefaultTTL()).Msg("KV store default TTL configured")
		}
		kvStore = redisKVStore
		checkpointStore = checkpoint.NewStoreFromClient(redisClient)
		logging.Logger.Debug().Msg("session registry, KV store, and checkpoint store initialized")

		// Initialize RabbitMQ router
		r, routerErr := router.NewRouter(cfg.RabbitMQ.StreamURL, cfg.RabbitMQ.GetStreamCapacityBytes())
		if routerErr != nil {
			logging.Logger.Fatal().Err(routerErr).Msg("failed to create router")
		}
		defer r.Close()
		msgRouter = r
		logging.Logger.Debug().Msg("RabbitMQ router initialized")

		// Initialize task store
		if db != nil {
			taskStore = tasks.NewTaskStore(db)
			logging.Logger.Debug().Msg("task store initialized")
		}
	}

	// Create gateway server with mTLS config from config file
	// The -tls flag or config-driven TLS overrides the config file setting
	mtlsRequired := cfg.Auth.MTLS.Required
	if *enableTLS {
		mtlsRequired = true // -tls flag forces mTLS to be required
	}

	mtlsMode := gateway.MTLSModeStrict
	if cfg.Auth.MTLS.Mode == "relaxed" {
		mtlsMode = gateway.MTLSModeRelaxed
	}

	mtlsConfig := gateway.MTLSConfig{
		Required: mtlsRequired,
		Mode:     mtlsMode,
	}

	// Initialize audit logger from unified config (YAML + env overrides already applied)
	auditConfig := buildAuditConfig(cfg)
	auditLogger := audit.NewAuditLogger(db, cfg.Gateway.GatewayID, auditConfig)
	defer auditLogger.Close()

	// Checkpoint default TTL
	checkpointDefaultTTL := cfg.Checkpoint.GetDefaultTTL()
	gatewayOpts = append(gatewayOpts, gateway.WithCheckpointDefaultTTL(checkpointDefaultTTL))
	if checkpointDefaultTTL > 0 {
		logging.Logger.Debug().Dur("ttl", checkpointDefaultTTL).Msg("checkpoint default TTL configured")
	} else {
		logging.Logger.Debug().Msg("checkpoint default TTL: no expiration")
	}

	// Quota management for multi-tenant deployments (skip in lite mode — already configured)
	if cfg.Quotas.Enabled && !cfg.IsLiteMode() {
		defaults := quota.DefaultQuotas{
			MaxConnectionsPerWorkspace: cfg.Quotas.MaxConnectionsPerWorkspace,
			MaxMessageRatePerIdentity:  cfg.Quotas.MaxMessageRatePerIdentity,
			MaxKVKeysPerNamespace:      cfg.Quotas.MaxKVKeysPerNamespace,
			MaxKVValueSize:             cfg.Quotas.MaxKVValueSize,
		}
		// Apply sensible defaults for unset values
		if defaults.MaxConnectionsPerWorkspace <= 0 {
			defaults.MaxConnectionsPerWorkspace = 1000
		}
		if defaults.MaxMessageRatePerIdentity <= 0 {
			defaults.MaxMessageRatePerIdentity = 100
		}
		if defaults.MaxKVKeysPerNamespace <= 0 {
			defaults.MaxKVKeysPerNamespace = 10000
		}
		if defaults.MaxKVValueSize <= 0 {
			defaults.MaxKVValueSize = 1048576 // 1MB
		}
		qm := quota.NewQuotaManager(redisClient, defaults)
		gatewayOpts = append(gatewayOpts, gateway.WithQuotaManager(qm))
		logging.Logger.Info().
			Int("max_connections_per_workspace", defaults.MaxConnectionsPerWorkspace).
			Float64("max_message_rate_per_identity", defaults.MaxMessageRatePerIdentity).
			Int("max_kv_keys_per_namespace", defaults.MaxKVKeysPerNamespace).
			Int("max_kv_value_size", defaults.MaxKVValueSize).
			Msg("quota management enabled")
	}

	// Task payload size limit (always enforced, independent of quota system)
	if cfg.Quotas.MaxTaskPayloadSize > 0 {
		gatewayOpts = append(gatewayOpts, gateway.WithMaxTaskPayloadSize(cfg.Quotas.MaxTaskPayloadSize))
	}

	// Proxy data-plane local bypass (Phase 6). Defaults to enabled; honour
	// the YAML override and the AETHER_PROXY_LOCAL_BYPASS_DISABLED env knob.
	gatewayOpts = append(gatewayOpts, gateway.WithProxyLocalBypassEnabled(cfg.Quotas.Proxy.IsLocalBypassEnabled()))

	// Proxy hop-depth cap (T40). Defaults to 8 when unset; rejects envelopes
	// whose proxy_chain_depth has reached the cap to break sandbox loops.
	gatewayOpts = append(gatewayOpts, gateway.WithProxyMaxChainDepth(cfg.Quotas.Proxy.GetMaxChainDepth()))

	// Orchestration services (skip in lite mode — already configured).
	// Hoisted so the auth setup below can wire the task-token authenticator
	// against orchestrationServices.TokenStore.
	var orchestrationServices *gateway.OrchestrationServices
	if !cfg.IsLiteMode() && db != nil && cfg.RabbitMQ.AMQPURL != "" {
		redisSessions, _ := sessions.(*state.SessionRegistry)
		svcs, err := gateway.InitializeOrchestrationServices(
			db,
			redisClient,
			cfg.RabbitMQ.AMQPURL,
			cfg.Postgres.DSN(),
			redisSessions,
		)
		if err != nil {
			logging.Logger.Warn().Err(err).Msg("failed to initialize orchestration services")
			logging.Logger.Warn().Msg("task assignment and orchestration features will be unavailable")
		} else {
			orchestrationServices = svcs
			gatewayOpts = append(gatewayOpts, gateway.WithOrchestrationServices(orchestrationServices))
			logging.Logger.Info().Msg("orchestration services initialized")
		}
	} else {
		logging.Logger.Debug().Msg("orchestration services not initialized (database or AMQP URL missing)")
	}

	// Authentication services
	if cfg.Auth.IsAuthModeEnabled("api_key") || cfg.Auth.IsAuthModeEnabled("oauth") {
		var authenticators []auth.Authenticator

		// Task token authenticator (precedes api_key/oauth per spec.md §3.2:
		// "mTLS identity > Task token validation > API key / OAuth"). When
		// orchestration services were not initialized the token store is
		// nil and the authenticator returns (nil, nil) for every request,
		// so the chain falls through cleanly.
		if orchestrationServices != nil && orchestrationServices.TokenStore != nil {
			authenticators = append(authenticators, auth.NewTaskTokenAuthenticator(orchestrationServices.TokenStore))
			logging.Logger.Info().Msg("Task token authentication enabled")
		}

		// API key authenticator (requires database)
		if cfg.Auth.IsAuthModeEnabled("api_key") && db != nil {
			apiTokenStore := auth.NewAPITokenStore(db)
			authenticators = append(authenticators, auth.NewAPIKeyAuthenticator(apiTokenStore))
			logging.Logger.Info().Msg("API key authentication enabled")
		}

		// OAuth authenticator
		if cfg.Auth.IsAuthModeEnabled("oauth") && len(cfg.Auth.OAuth.Providers) > 0 {
			var providers []auth.OAuthProviderConfig
			for _, p := range cfg.Auth.OAuth.Providers {
				providers = append(providers, auth.OAuthProviderConfig{
					Name:     p.Name,
					Issuer:   p.Issuer,
					JWKSURL:  p.JWKSURL,
					Audience: p.Audience,
					ClaimsMapping: auth.ClaimsMapping{
						PrincipalType: p.ClaimsMapping.PrincipalType,
						Workspace:     p.ClaimsMapping.Workspace,
						Identity:      p.ClaimsMapping.Identity,
					},
					DefaultPrincipal: p.DefaultPrincipal,
					DefaultWorkspace: p.DefaultWorkspace,
				})
			}
			oauthAuth := auth.NewOAuthAuthenticator(providers, cfg.Auth.OAuth.VerifySignature)
			authenticators = append(authenticators, oauthAuth)
			if !cfg.Auth.OAuth.ShouldVerifySignature() {
				logging.Logger.Warn().Msg("OAuth JWT signature verification is DISABLED (development mode)")
			}
			logging.Logger.Info().Int("providers", len(providers)).Msg("OAuth authentication enabled")
		}

		if len(authenticators) > 0 {
			compositeAuth := auth.NewCompositeAuthenticator(authenticators...)
			gatewayOpts = append(gatewayOpts, gateway.WithAuthenticator(compositeAuth))
			logging.Logger.Debug().Int("methods", len(authenticators)).Msg("composite authenticator initialized")
		}
	}

	// Cleanup service (added last so orchestration is available when it runs)
	cleanupConfig := &cleanup.Config{
		TaskPurgeInterval:      cfg.Cleanup.GetTaskPurgeInterval(),
		CompletedTaskRetention: cfg.Cleanup.GetCompletedTaskRetention(),
		FailedTaskRetention:    cfg.Cleanup.GetFailedTaskRetention(),
		CancelledTaskRetention: cfg.Cleanup.GetCancelledTaskRetention(),
		ReconciliationInterval: cfg.Cleanup.GetReconciliationInterval(),
	}
	gatewayOpts = append(gatewayOpts, gateway.WithCleanupService(cleanupConfig))

	// Circuit breaker for Redis/RabbitMQ protection
	if cfg.Gateway.CircuitBreaker.GetMaxFailures() > 0 || cfg.Gateway.CircuitBreaker.ResetTimeout != "" {
		cb := circuitbreaker.New("redis",
			circuitbreaker.WithMaxFailures(cfg.Gateway.CircuitBreaker.GetMaxFailures()),
			circuitbreaker.WithResetTimeout(cfg.Gateway.CircuitBreaker.GetResetTimeout()),
		)
		gatewayOpts = append(gatewayOpts, gateway.WithCircuitBreaker(cb))
	}

	// Per-client message rate limiting
	if cfg.Gateway.MessageRateLimit > 0 {
		burst := cfg.Gateway.MessageRateBurst
		if burst <= 0 {
			burst = int(cfg.Gateway.MessageRateLimit * 2)
		}
		gatewayOpts = append(gatewayOpts, gateway.WithMessageRateLimit(cfg.Gateway.MessageRateLimit, burst))
	}

	// Workspace-level message rate limiting (token bucket, in-memory)
	workspaceRL := quota.NewWorkspaceRateLimiter(cfg.Gateway.MessageRateLimit)
	gatewayOpts = append(gatewayOpts, gateway.WithWorkspaceRateLimiter(workspaceRL))

	// Per-principal rate limiter for foreign audit submissions (SubmitAuditEvent).
	// Default 100 events/sec/principal; tunable via AETHER_AUDIT_FOREIGN_RATE_PER_SEC.
	foreignAuditRL := quota.NewPrincipalRateLimiter(float64(config.EnvInt("AETHER_AUDIT_FOREIGN_RATE_PER_SEC", 100)))
	gatewayOpts = append(gatewayOpts, gateway.WithForeignAuditRateLimiter(foreignAuditRL))

	// Create the ACL service once so it is shared between the gateway server and the
	// state provider, avoiding duplicate Casbin enforcer instances with independent caches.
	//
	// Audit writes funnel through the shared auditLogger constructed above
	// (single batched writer goroutine), eliminating the SQLITE_BUSY WAL
	// contention we previously hit when audit.AuditLogger and the old
	// acl.AuditLogger each ran their own writer against
	// comprehensive_audit_log. In postgres deployments there is one DB
	// and one batcher, so the same constructor naturally works there too.
	var sharedACLService *acl.Service
	if db != nil {
		sharedACLService = acl.NewServiceWithSharedAudit(db, auditLogger, db, cfg.Gateway.GatewayID)
		gatewayOpts = append(gatewayOpts, gateway.WithACLService(sharedACLService))
		logging.Logger.Debug().Msg("shared ACL service initialized")
	}

	gatewayServer := gateway.NewGatewayServer(sessions, msgRouter, kvStore, checkpointStore, taskStore, db, cfg.Gateway.GatewayID, auditLogger, mtlsConfig, gatewayOpts...)
	defer gatewayServer.Stop()

	// Shared gRPC server hardening options applied to both TLS and non-TLS paths.
	// The otelgrpc stats handler creates a span per RPC that lasts for the
	// RPC's full lifetime — fine for unary, but Connect is a long-lived
	// bidi stream (hours per session) and the resulting span swallows every
	// per-message child span. Filter Connect out so per-message spans
	// (started manually inside the handler) become root spans of their own
	// traces with realistic durations.
	otelSkipLongStreams := otelgrpcfilters.None(
		otelgrpcfilters.FullMethodName("/aether.v1.AetherGateway/Connect"),
	)
	serverOpts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(otelSkipLongStreams))),
		grpc.MaxRecvMsgSize(4 * 1024 * 1024),  // 4MB
		grpc.MaxSendMsgSize(16 * 1024 * 1024), // 16MB
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

	// Create gRPC server with optional TLS via the shared helper. When TLS
	// is enabled the helper wires a dynamic-cert callback against
	// reloadableCfg so SIGHUP rotation lands on the next handshake without
	// a server restart. Shared with aetherlite to keep the two binaries
	// in lockstep.
	grpcServer, tlsActive, err := gateway.NewGRPCServerFromConfig(cfg, reloadableCfg, serverOpts...)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to create gRPC server")
	}
	if tlsActive {
		logging.Logger.Info().
			Str("cert", cfg.Gateway.TLS.CertFile).
			Str("key", cfg.Gateway.TLS.KeyFile).
			Str("ca", cfg.Gateway.TLS.CAFile).
			Str("client_auth", cfg.Gateway.TLS.ClientAuth).
			Msg("TLS enabled (dynamic certificate rotation active)")
	} else {
		logging.Logger.Debug().Msg("TLS disabled (insecure gRPC)")
	}

	// Log mTLS identity verification config
	if mtlsConfig.Required {
		logging.Logger.Debug().Str("mode", string(mtlsConfig.Mode)).Msg("mTLS identity verification required")
	} else {
		logging.Logger.Debug().Msg("mTLS identity verification disabled (using InitConnection identity)")
	}

	pb.RegisterAetherGatewayServer(grpcServer, gatewayServer)

	// Register gRPC health check service for Kubernetes probes
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("aether.v1.AetherGateway", healthpb.HealthCheckResponse_SERVING)

	// Start gRPC server
	addr := fmt.Sprintf(":%d", cfg.Gateway.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logging.Logger.Fatal().Err(err).Str("addr", addr).Msg("failed to listen")
	}

	// Start server in goroutine
	go func() {
		logging.Logger.Info().Str("addr", addr).Msg("Aether Gateway listening")
		if err := grpcServer.Serve(lis); err != nil {
			logging.Logger.Fatal().Err(err).Msg("failed to serve")
		}
	}()

	// Create shared state provider (used by both ops and admin servers)
	var agentRegistry *registry.AgentRegistry
	var profileMgr *registry.OrchestratorProfileManager

	if db != nil {
		agentRegistry = registry.NewAgentRegistry(db)
		if redisClient != nil {
			profileMgr = registry.NewOrchestratorProfileManagerWithRedis(db, redisClient)
		}
	}

	// State provider uses concrete types for admin operations.
	// In lite mode, type-assert what we can; admin features that need
	// Redis/RabbitMQ will gracefully degrade.
	var stateSessionRegistry *state.SessionRegistry
	if sr, ok := sessions.(*state.SessionRegistry); ok {
		stateSessionRegistry = sr
	}
	var stateKVStore *kv.Store
	if ks, ok := kvStore.(*kv.Store); ok {
		stateKVStore = ks
	}
	var stateRouter *router.Router
	if rr, ok := msgRouter.(*router.Router); ok {
		stateRouter = rr
	}
	stateProvider := gateway.NewGatewayStateProvider(
		cfg.Gateway.GatewayID,
		stateSessionRegistry,
		stateKVStore,
		taskStore,
		agentRegistry,
		profileMgr,
		sharedACLService,
		db,
		stateRouter,
	)
	stateProvider.SetGateway(gatewayServer)
	stateProvider.SetWorkspaceRateLimiter(workspaceRL)
	gatewayServer.SetAdminProvider(stateProvider)

	// Start ops server (health probes + Prometheus metrics) — always enabled
	opsServer := admin.NewOpsServer(cfg.Gateway.GetOpsPort(), stateProvider)
	opsServer.SetReady(true)
	go func() {
		if err := opsServer.Start(); err != nil && err != http.ErrServerClosed {
			logging.Logger.Fatal().Err(err).Msg("ops server error")
		}
	}()

	// Start Admin UI server
	var adminServer *admin.Server
	if cfg.Admin.Enabled {
		adminServer = admin.NewServer(admin.ServerConfig{
			Port:           cfg.Admin.Port,
			DevMode:        *devMode,
			DevPath:        *adminDevPath,
			CORSOrigin:     cfg.Admin.CORSOrigin,
			APIKey:         cfg.Admin.APIKey,
			GetAPIKey:      reloadableCfg.AdminAPIKey,
			InsecureNoAuth: *insecureAdmin || *devMode,
			TLSCertFile:    cfg.Admin.TLSCertFile,
			TLSKeyFile:     cfg.Admin.TLSKeyFile,
			RateLimit:      cfg.Admin.RateLimit,
			RateLimitBurst: cfg.Admin.RateLimitBurst,
		}, stateProvider)

		// Start admin server in goroutine
		go func() {
			if err := adminServer.Start(); err != nil && err != http.ErrServerClosed {
				logging.Logger.Fatal().Err(err).Msg("admin server error")
			}
		}()
	}

	// Wait for shutdown signal
	<-sigChan
	logging.Logger.Info().Msg("shutdown signal received, gracefully stopping")

	// Graceful shutdown
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
	// Send FORCE_DISCONNECT to all connected clients before gRPC stops accepting new streams.
	// This must happen before GracefulStop so clients receive the signal during the grace period.
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
	logging.Logger.Info().Msg("Aether Gateway stopped")
}
