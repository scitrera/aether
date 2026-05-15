package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	wfpg "github.com/scitrera/aether/internal/storage/workflow/postgres"
	wfsqlite "github.com/scitrera/aether/internal/storage/workflow/sqlite"
	versionpkg "github.com/scitrera/aether/internal/version"
	"github.com/scitrera/aether/internal/workflow"
	wfmigrations "github.com/scitrera/aether/internal/workflow/migrations"

	// Register the bare "sqlite" driver for SQLite mode.
	_ "modernc.org/sqlite"
)

var (
	version = versionpkg.Version
	banner  = `
 __        __         _     __ _
 \ \      / /__  _ __| | __/ _| | _____      __
  \ \ /\ / / _ \| '__| |/ / |_| |/ _ \ \ /\ / /
   \ V  V / (_) | |  |   <|  _| | (_) \ V  V /
    \_/\_/ \___/|_|  |_|\_\_| |_|\___/ \_/\_/

Aether Workflow Server v%s
`
)

var (
	configFile  = flag.String("config", "configs/workflow.yaml", "Path to configuration file")
	showVersion = flag.Bool("version", false, "Show version and exit")
	showHelp    = flag.Bool("help", false, "Show this help message")
	devMode     = flag.Bool("dev", false, "Allow startup with development defaults")
	liteMode    = flag.Bool("lite", false, "Run in lite mode: SQLite database, no Redis required")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, banner, version)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Aether Workflow Server provides event-driven rule routing,\n")
		fmt.Fprintf(os.Stderr, "DAG-based workflow execution, and recurring scheduled tasks.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}
	if *showVersion {
		fmt.Printf("Aether Workflow Server v%s\n", version)
		os.Exit(0)
	}

	log.Printf(banner, version)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	initLogger(cfg.Logging.Level, cfg.Logging.Format)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// The workflow package no longer opens databases or runs migrations.
	// Caller is responsible for opening the right DB (postgres or sqlite),
	// running migrations, and constructing the matching store impl.
	var (
		srv     *workflow.Server
		closeDB func()
	)
	if cfg.Mode == workflow.ModeLite {
		dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", cfg.SQLite.Path)
		db, openErr := sql.Open("sqlite", dsn)
		if openErr != nil {
			log.Fatalf("Failed to open SQLite database %s: %v", cfg.SQLite.Path, openErr)
		}
		// wfsqlite.New runs its own native migration set.
		store, storeErr := wfsqlite.New(db)
		if storeErr != nil {
			_ = db.Close()
			log.Fatalf("Failed to construct native sqlite workflow store: %v", storeErr)
		}
		srv, err = workflow.NewServer(cfg, store)
		if err != nil {
			_ = db.Close()
			log.Fatalf("Failed to create workflow server: %v", err)
		}
		closeDB = func() { _ = db.Close() }
	} else {
		db, openErr := sql.Open("postgres", cfg.Postgres.DSN())
		if openErr != nil {
			log.Fatalf("Failed to open PostgreSQL database: %v", openErr)
		}
		if cfg.Postgres.MaxConnections > 0 {
			db.SetMaxOpenConns(cfg.Postgres.MaxConnections)
		}
		if cfg.Postgres.MaxIdleConnections > 0 {
			db.SetMaxIdleConns(cfg.Postgres.MaxIdleConnections)
		}
		if pingErr := db.PingContext(ctx); pingErr != nil {
			_ = db.Close()
			log.Fatalf("Failed to ping PostgreSQL database: %v", pingErr)
		}
		if migErr := wfmigrations.Run(ctx, db); migErr != nil {
			_ = db.Close()
			log.Fatalf("Failed to run workflow PostgreSQL migrations: %v", migErr)
		}
		store := wfpg.New(db, false)
		srv, err = workflow.NewServer(cfg, store)
		if err != nil {
			_ = db.Close()
			log.Fatalf("Failed to create workflow server: %v", err)
		}
		closeDB = func() { _ = db.Close() }
	}
	defer closeDB()

	// Run server in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Run(ctx)
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down", sig)
		cancel()
	case err := <-errChan:
		if err != nil {
			log.Fatalf("Workflow server error: %v", err)
		}
	}

	// Give the server a few seconds to clean up
	<-time.After(3 * time.Second)
	log.Println("Aether Workflow Server stopped")
}

func loadConfig() (*workflow.Config, error) {
	var cfg *workflow.Config
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		if !*devMode {
			return nil, fmt.Errorf("config file not found: %s (pass -dev for defaults)", *configFile)
		}
		log.Printf("WARNING: Config file %s not found, using development defaults", *configFile)
		cfg = devDefaults()
	} else {
		var err error
		cfg, err = workflow.LoadConfig(*configFile)
		if err != nil {
			return nil, err
		}
	}
	if *liteMode {
		cfg.Mode = workflow.ModeLite
	}
	return cfg, nil
}

func devDefaults() *workflow.Config {
	cfg := &workflow.Config{}
	cfg.Aether.Address = "localhost:50051"
	cfg.Aether.Implementation = "aether-workflow"
	cfg.Aether.Workspace = "_system"
	cfg.Postgres.Host = "localhost"
	cfg.Postgres.Port = 5432
	cfg.Postgres.Database = "aether"
	cfg.Postgres.User = envOrDefault("POSTGRES_USER", "aether")
	cfg.Postgres.Password = envOrDefault("POSTGRES_PASSWORD", "aether_dev")
	cfg.Postgres.SSLMode = "disable"
	cfg.Redis.Cluster = []string{"localhost:6379"}
	cfg.Admin.Enabled = true
	cfg.Admin.Port = 31881
	cfg.Logging.Level = "info"
	return cfg
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func initLogger(level, format string) {
	var lvl zerolog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = zerolog.DebugLevel
	case "warn", "warning":
		lvl = zerolog.WarnLevel
	case "error":
		lvl = zerolog.ErrorLevel
	default:
		lvl = zerolog.InfoLevel
	}

	useConsole := format == "console"
	if format == "" {
		if fi, err := os.Stderr.Stat(); err == nil && (fi.Mode()&os.ModeCharDevice) != 0 {
			useConsole = true
		}
	}

	if useConsole {
		output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
		zerolog.DefaultContextLogger = nil
		logger := zerolog.New(output).With().Timestamp().Logger().Level(lvl)
		zerolog.DefaultContextLogger = &logger
	} else {
		logger := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(lvl)
		zerolog.DefaultContextLogger = &logger
	}
}
