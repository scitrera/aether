// Package main implements a standalone production readiness checker for Aether.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/readiness"
	"github.com/scitrera/aether/pkg/crypto"
)

func main() {
	configFile := flag.String("config", "configs/dev.yaml", "Path to configuration file")
	secretsFile := flag.String("secrets-file", "/etc/aether/generated-secrets.yaml", "Path to generated secrets file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Initialize HMAC key if configured so the check reflects runtime state
	if cfg.Auth.TokenHMACKey != "" {
		crypto.InitTokenHMAC([]byte(cfg.Auth.TokenHMACKey))
	}

	checker := readiness.NewChecker()

	// devMode is always false for a standalone production check tool
	results := checker.CheckConfig(cfg, false)

	// Override secrets file path with the provided flag value
	results = append(results, readiness.CheckSecretsFilePath(*secretsFile))

	// Optional DB check
	db := connectDB(cfg)
	if db != nil {
		defer db.Close()
		results = append(results, checker.CheckDatabase(db)...)
	}

	checker.PrintReport(results)

	if checker.HasCriticalFailures(results) {
		fmt.Fprintln(os.Stderr, "ERROR: critical readiness checks failed")
		os.Exit(1)
	}
}

// connectDB attempts a lightweight PostgreSQL connection for readiness checks.
// Returns nil if postgres is not configured or unreachable (non-fatal for this tool).
func connectDB(cfg *config.Config) *sql.DB {
	if cfg.Postgres.Host == "" {
		return nil
	}
	db, err := sql.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		log.Printf("WARNING: failed to open database connection: %v", err)
		return nil
	}
	if err := db.PingContext(context.Background()); err != nil {
		log.Printf("WARNING: cannot reach PostgreSQL (%v) — skipping database checks", err)
		db.Close()
		return nil
	}
	return db
}
