package main

import (
	"context"
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
	"github.com/scitrera/aether/internal/msgbridge"
	versionpkg "github.com/scitrera/aether/internal/version"
)

var (
	version = versionpkg.Version
	banner  = `
 __  __           ____       _     _
|  \/  |___  __ _| __ ) _ __(_) __| | __ _  ___
| |\/| / __|/ _' |  _ \| '__| |/ _' |/ _' |/ _ \
| |  | \__ \ (_| | |_) | |  | | (_| | (_| |  __/
|_|  |_|___/\__, |____/|_|  |_|\__,_|\__, |\___|
             |___/                    |___/

Aether Messaging Bridge v%s
`
)

var (
	configFile  = flag.String("config", "configs/msgbridge.yaml", "Path to configuration file")
	showVersion = flag.Bool("version", false, "Show version and exit")
	showHelp    = flag.Bool("help", false, "Show this help message")
	devMode     = flag.Bool("dev", false, "Allow startup with development defaults")
	liteMode    = flag.Bool("lite", false, "Use SQLite instead of PostgreSQL (AetherLite mode)")
	sqlitePath  = flag.String("sqlite-path", "msgbridge.db", "Path to SQLite database file (used with --lite)")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, banner, version)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Aether Messaging Bridge bridges external messaging platforms\n")
		fmt.Fprintf(os.Stderr, "(Discord, Teams, Email) with the Aether gateway.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}
	if *showVersion {
		fmt.Printf("Aether Messaging Bridge v%s\n", version)
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

	srv, err := msgbridge.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create msgbridge server: %v", err)
	}

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
			log.Fatalf("Messaging bridge server error: %v", err)
		}
	}

	// Give the server a few seconds to clean up
	<-time.After(3 * time.Second)
	log.Println("Aether Messaging Bridge stopped")
}

func loadConfig() (*msgbridge.Config, error) {
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		if !*devMode {
			return nil, fmt.Errorf("config file not found: %s (pass -dev for defaults)", *configFile)
		}
		log.Printf("WARNING: Config file %s not found, using development defaults", *configFile)
		return devDefaults(), nil
	}
	cfg, err := msgbridge.LoadConfig(*configFile)
	if err != nil {
		return nil, err
	}
	// --lite flag overrides the config file mode setting.
	if *liteMode {
		cfg.Mode = "sqlite"
		if cfg.SQLite.Path == "" {
			cfg.SQLite.Path = *sqlitePath
		}
	}
	return cfg, nil
}

func devDefaults() *msgbridge.Config {
	cfg := &msgbridge.Config{}
	cfg.Aether.Address = "localhost:50051"
	cfg.Aether.Implementation = "aether-msgbridge"
	cfg.Aether.Specifier = "instance-1"
	if *liteMode {
		cfg.Mode = "sqlite"
		cfg.SQLite.Path = *sqlitePath
	} else {
		cfg.Postgres.Host = "localhost"
		cfg.Postgres.Port = 5432
		cfg.Postgres.Database = "aether"
		cfg.Postgres.User = envOrDefault("POSTGRES_USER", "aether")
		cfg.Postgres.Password = envOrDefault("POSTGRES_PASSWORD", "aether_dev")
		cfg.Postgres.SSLMode = "disable"
	}
	cfg.Admin.Enabled = true
	cfg.Admin.Port = 31882
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
