package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"

	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/gateway"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/migrations"
	"google.golang.org/grpc"
)

// loadConfig loads configuration from file and applies environment overrides
func loadConfig() (*config.Config, error) {
	// If no config file is specified, require either --dev flag or fail.
	if *configFile == "" {
		if !*devMode {
			fmt.Fprintf(os.Stderr, "ERROR: --config is required; pass --dev with AETHER_ALLOW_DEV_MODE=true to use development defaults (NOT FOR PRODUCTION)\n")
			os.Exit(1)
		}
		if os.Getenv("AETHER_ALLOW_DEV_MODE") != "true" {
			log.Fatalf("FATAL: --dev mode requires AETHER_ALLOW_DEV_MODE=true environment variable to be set")
		}
		*configFile = "configs/dev.yaml"
	}

	// Check if config file exists
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		if !*devMode {
			log.Fatalf("Config file not found: %s — pass --dev to allow startup with development defaults (NOT FOR PRODUCTION)", *configFile)
		}
		if *devMode {
			if os.Getenv("AETHER_ALLOW_DEV_MODE") != "true" {
				log.Fatalf("FATAL: --dev mode requires AETHER_ALLOW_DEV_MODE=true environment variable to be set")
			}
		}

		// Return minimal default config only when -dev is explicitly passed
		log.Printf("WARNING: Config file %s not found, using DEVELOPMENT defaults -- NOT FOR PRODUCTION", *configFile)
		cfg := &config.Config{}
		cfg.Gateway.Port = 50051
		cfg.Gateway.GatewayID = "gateway-default"
		cfg.Admin.Enabled = true
		cfg.Admin.Port = 31880
		cfg.Admin.CORSOrigin = "*"
		cfg.LogLevel = "info"
		cfg.Audit.Enabled = true

		if *liteMode {
			// Lite mode: no external service defaults needed.
			cfg.Mode = "lite"
			log.Printf("========================================================")
			log.Printf("  DEV + LITE MODE ACTIVE — embedded backends in use:")
			log.Printf("  - SQLite + Badger in ./aether-lite-data")
			log.Printf("  - No Postgres, Redis, or RabbitMQ required")
			log.Printf("  DO NOT RUN WITH --dev IN PRODUCTION")
			log.Printf("========================================================")
			return cfg, nil
		}

		cfg.Postgres.Host = "localhost"
		cfg.Postgres.Port = 5432
		cfg.Postgres.Database = "aether"
		cfg.Postgres.SSLMode = "disable"

		// Read Postgres credentials from env vars; fall back to dev defaults with a warning.
		cfg.Postgres.User = os.Getenv("POSTGRES_USER")
		if cfg.Postgres.User == "" {
			cfg.Postgres.User = "aether"
			log.Printf("WARNING: POSTGRES_USER not set — using default 'aether' (dev mode only)")
		}
		cfg.Postgres.Password = os.Getenv("POSTGRES_PASSWORD")
		if cfg.Postgres.Password == "" {
			cfg.Postgres.Password = "aether_dev"
			log.Printf("WARNING: POSTGRES_PASSWORD not set — using default 'aether_dev' (dev mode only)")
		}

		cfg.Redis.Cluster = []string{"localhost:56379"}
		cfg.RabbitMQ.StreamURL = "rabbitmq-stream://guest:guest@localhost:55552"
		cfg.RabbitMQ.AMQPURL = "amqp://guest:guest@localhost:55672/"

		log.Printf("========================================================")
		log.Printf("  DEV MODE ACTIVE — SECURITY RELAXATIONS IN EFFECT:")
		log.Printf("  - Postgres SSLMode: disabled (localhost only)")
		log.Printf("  - CORS origin: * (all origins allowed)")
		log.Printf("  - RabbitMQ using guest credentials (localhost only)")
		log.Printf("  - Credentials sourced from env vars with insecure fallbacks")
		log.Printf("  DO NOT RUN WITH --dev IN PRODUCTION")
		log.Printf("========================================================")

		return cfg, nil
	}

	// Load config from file
	cfg, err := config.Load(*configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config from %s: %w", *configFile, err)
	}

	log.Printf("Configuration loaded from: %s", *configFile)
	return cfg, nil
}

// applyFlagOverrides applies CLI flag overrides to configuration
func applyFlagOverrides(cfg *config.Config) {
	if *liteMode {
		cfg.Mode = "lite"
	}
	if *port != 0 {
		cfg.Gateway.Port = *port
	}
	if *adminPort != 0 {
		cfg.Admin.Port = *adminPort
	}
	if *dbHost != "" {
		cfg.Postgres.Host = *dbHost
	}
	if *dbPort != 0 {
		cfg.Postgres.Port = *dbPort
	}
	if *dbUser != "" {
		cfg.Postgres.User = *dbUser
	}
	if *dbName != "" {
		cfg.Postgres.Database = *dbName
	}
	if *redisAddr != "" {
		cfg.Redis.Cluster = []string{*redisAddr}
	}
	if *streamURL != "" {
		cfg.RabbitMQ.StreamURL = *streamURL
	}
	if *amqpURL != "" {
		cfg.RabbitMQ.AMQPURL = *amqpURL
	}
}

// buildAuditConfig converts the unified config.AuditConfig into an audit.Config
func buildAuditConfig(cfg *config.Config) *audit.Config {
	auditCfg := audit.DefaultConfig()

	auditCfg.Enabled = cfg.Audit.Enabled
	if len(cfg.Audit.EventTypes) > 0 {
		auditCfg.EnabledEventTypes = cfg.Audit.EventTypes
	}
	if cfg.Audit.Verbosity != "" {
		auditCfg.VerbosityLevel = cfg.Audit.Verbosity
	}
	if cfg.Audit.BatchSize > 0 {
		auditCfg.BatchSize = cfg.Audit.BatchSize
	}
	if flushPeriod := cfg.Audit.GetFlushPeriod(); flushPeriod > 0 {
		auditCfg.FlushPeriod = flushPeriod
	}
	if cfg.Audit.RetentionDays > 0 {
		auditCfg.RetentionDays = cfg.Audit.RetentionDays
	}
	if cfg.Audit.ChannelBuffer > 0 {
		auditCfg.ChannelBuffer = cfg.Audit.ChannelBuffer
	}

	return auditCfg
}

// initDatabase initializes PostgreSQL connection and runs migrations
func initDatabase(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
	dsn := cfg.Postgres.DSN()
	logging.Logger.Debug().Msg("connecting to PostgreSQL")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(cfg.Postgres.MaxConnections)
	db.SetMaxIdleConns(cfg.Postgres.MaxIdleConnections)

	// Test database connection
	if err := db.PingContext(ctx); err != nil {
		logging.Logger.Warn().Err(err).Msg("cannot connect to PostgreSQL")
		logging.Logger.Warn().Msg("task persistence will be disabled")
		db.Close()
		return nil, nil
	}

	logging.Logger.Info().Msg("PostgreSQL connection established")

	// Run migrations from embedded SQL files
	if err := migrations.Run(ctx, db); err != nil {
		return nil, fmt.Errorf("database migration failed: %w", err)
	}
	return db, nil
}

// createTLSServer creates a gRPC server with TLS enabled and the provided server options.
// TLS parameters are read from the config (which may have been populated by CLI flags,
// env vars, or auto-TLS generation).
func createTLSServer(cfg *config.Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	certPath := cfg.Gateway.TLS.CertFile
	keyPath := cfg.Gateway.TLS.KeyFile
	caPath := cfg.Gateway.TLS.CAFile

	// Validate TLS parameters
	if certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("TLS enabled but missing required parameters (cert_file, key_file)")
	}

	// Check if certificate files exist
	if _, err := os.Stat(certPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("certificate file not found: %s", certPath)
	}
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("key file not found: %s", keyPath)
	}
	if caPath != "" {
		if _, err := os.Stat(caPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("CA file not found: %s", caPath)
		}
	}

	tlsConfig := gateway.TLSConfig{
		CertFile:   certPath,
		KeyFile:    keyPath,
		CAFile:     caPath,
		ClientAuth: gateway.ParseClientAuth(cfg.Gateway.TLS.ClientAuth),
	}

	return gateway.NewGRPCServerWithTLS(tlsConfig, opts...)
}

// redactURL masks credentials in a URL for safe logging.
func redactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<invalid-url>"
	}
	if u.User == nil {
		return u.String()
	}
	// Build manually to avoid URL-encoding the asterisks (e.g. %2A%2A%2A)
	return fmt.Sprintf("%s://%s:***@%s%s", u.Scheme, u.User.Username(), u.Host, u.RequestURI())
}
