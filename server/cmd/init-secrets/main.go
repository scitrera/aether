package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
	authsqlite "github.com/scitrera/aether/internal/auth/sqlite"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/secrets"
	aclsqlite "github.com/scitrera/aether/internal/storage/acl/sqlite"
	"github.com/scitrera/aether/pkg/certgen"
	"github.com/scitrera/aether/pkg/models"

	_ "modernc.org/sqlite" // register bare "sqlite" driver for aetherlite token/acl DBs
)

const defaultSecretsPath = "/etc/aether/generated-secrets.yaml"

// cnFilename returns a filesystem-safe form of an aether canonical name
// for use in cert/key filenames. The on-disk filename replaces "::" with
// "." so existing SDK clients (which historically resolved paths like
// sv.{impl}.{spec}-cert.pem) keep working after the topic-separator
// standardization. The CN inside the X.509 certificate is unchanged.
func cnFilename(cn string) string { return strings.ReplaceAll(cn, "::", ".") }

func main() {
	configFile := flag.String("config", "", "Path to main config file")
	output := flag.String("output", defaultSecretsPath, "Path to write generated secrets")
	createToken := flag.Bool("create-token", false, "Create an initial admin API token in the database")
	tokenName := flag.String("token-name", "admin-bootstrap", "Name for the initial token")
	principalType := flag.String("principal-type", "User", "Principal type for the token (User, Agent, Task, Service, Orchestrator, WorkflowEngine, MetricsBridge, Bridge)")
	accessLevel := flag.String("access-level", "ADMIN", "Access level for the token (NONE, READ, READWRITE, MANAGE, ADMIN, SUPERADMIN)")
	dataDir := flag.String("data-dir", config.EnvStr("AETHERLITE_DATA_DIR", "./aether-lite-data"), "AetherLite data directory holding tokens.db/acl.db; used for --create-token when PostgreSQL is not configured (env: AETHERLITE_DATA_DIR)")
	force := flag.Bool("force", false, "Regenerate even if secrets file already exists")
	printSecrets := flag.Bool("print", false, "Print generated values to stdout")

	// TLS certificate generation flags
	generateTLS := flag.Bool("generate-tls", false, "Generate CA + server TLS certificate")
	tlsDir := flag.String("tls-dir", "/etc/aether/secrets/tls", "Directory for TLS certificate artifacts")
	certValidity := flag.Duration("cert-validity", 8760*time.Hour, "Certificate validity period (default: 1 year)")
	tlsSANs := flag.String("tls-san", "", "Comma-separated additional DNS SANs for the server certificate")

	// Client certificate generation flags
	clientCert := flag.String("client-cert", "", "Generate client cert (agent, task, user, service, orchestrator, workflow-engine, metrics-bridge, anonymous)")
	workspace := flag.String("workspace", "", "Workspace for client cert identity")
	impl := flag.String("impl", "", "Implementation for client cert identity")
	spec := flag.String("spec", "", "Specifier for client cert identity")
	userID := flag.String("user-id", "", "User ID for user client cert")
	windowID := flag.String("window-id", "", "Window ID for user client cert")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Generate and manage Aether security bootstrap secrets.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Generate HMAC + admin key secrets:\n")
		fmt.Fprintf(os.Stderr, "  %s --print\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate CA + server TLS certificate:\n")
		fmt.Fprintf(os.Stderr, "  %s --generate-tls --tls-dir /tmp/aether-tls --print\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate with additional DNS SANs (e.g., for Docker container names):\n")
		fmt.Fprintf(os.Stderr, "  %s --generate-tls --tls-dir /tmp/aether-tls --tls-san ml-aether-gateway,gateway.local\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate anonymous mTLS client certificate:\n")
		fmt.Fprintf(os.Stderr, "  %s --client-cert anonymous --tls-dir /tmp/aether-tls\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate agent identity client certificate:\n")
		fmt.Fprintf(os.Stderr, "  %s --client-cert agent --workspace default --impl worker --spec v1 --tls-dir /tmp/aether-tls\n\n", os.Args[0])
	}
	flag.Parse()

	// Load config if provided
	var cfg *config.Config
	if *configFile != "" {
		var err error
		cfg, err = config.Load(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	} else {
		cfg = &config.Config{}
		cfg.ApplyEnvOverrides()
	}

	gs, err := secrets.EnsureSecrets(cfg, *output, *force)
	if err != nil {
		log.Fatalf("Failed to ensure secrets: %v", err)
	}

	log.Printf("Secrets ensured at: %s", *output)

	// Handle TLS certificate generation
	if *generateTLS {
		var extraSANs []string
		if *tlsSANs != "" {
			for _, s := range strings.Split(*tlsSANs, ",") {
				s = strings.TrimSpace(s)
				if s != "" {
					extraSANs = append(extraSANs, s)
				}
			}
		}
		if err := handleGenerateTLS(gs, *output, *tlsDir, *certValidity, *force, *printSecrets, extraSANs); err != nil {
			log.Fatalf("Failed to generate TLS certificates: %v", err)
		}
	}

	// Handle client certificate generation
	if *clientCert != "" {
		if err := handleClientCert(*clientCert, *tlsDir, *certValidity, *workspace, *impl, *spec, *userID, *windowID); err != nil {
			log.Fatalf("Failed to generate client certificate: %v", err)
		}
	}

	if *printSecrets {
		fmt.Printf("auth.token_hmac_key: %s\n", gs.Auth.TokenHMACKey)
		fmt.Printf("admin.api_key:       %s\n", gs.Admin.APIKey)
		if gs.TLS.CertFile != "" {
			fmt.Printf("tls.ca_cert_file:    %s\n", gs.TLS.CACertFile)
			fmt.Printf("tls.cert_file:       %s\n", gs.TLS.CertFile)
			fmt.Printf("tls.key_file:        %s\n", gs.TLS.KeyFile)
		}
	}

	if *createToken {
		level, err := secrets.ParseAccessLevel(*accessLevel)
		if err != nil {
			log.Fatalf("Invalid access level %q: %v", *accessLevel, err)
		}

		ctx := context.Background()

		var plaintext string
		if cfg.Postgres.Host != "" {
			// Full gateway topology: mint against PostgreSQL (existing behavior).
			plaintext, err = createTokenPostgres(ctx, cfg, *tokenName, models.PrincipalType(*principalType), level)
		} else {
			// aetherlite topology (single or cluster): no PostgreSQL. Mint
			// directly against the SQLite stores the gateway uses, so the CLI
			// works WITHOUT a running gateway.
			plaintext, err = createTokenSQLite(ctx, cfg, *dataDir, *tokenName, models.PrincipalType(*principalType), level)
		}
		if err != nil {
			log.Fatalf("Failed to create initial token: %v", err)
		}

		fmt.Printf("initial_api_token:   %s\n", plaintext)
		log.Printf("Initial API token '%s' created with %s access", *tokenName, *accessLevel)
	}
}

// createTokenPostgres mints an initial API token against the configured
// PostgreSQL database (full gateway topology).
func createTokenPostgres(ctx context.Context, cfg *config.Config, tokenName string, principalType models.PrincipalType, level int) (string, error) {
	db, err := sql.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		return "", fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return "", fmt.Errorf("connecting to database: %w", err)
	}

	return secrets.CreateInitialToken(ctx, db, cfg, tokenName, principalType, level)
}

// createTokenSQLite mints an initial API token against the SQLite stores under
// dataDir, exactly as aetherlite opens them (tokens.db + acl.db via the bare
// "sqlite" driver with WAL). This path is intended for when the aether gateway
// is DOWN: SQLite is single-writer and a running gateway holds the file. If the
// DB is locked, the error surfaced here suggests stopping the gateway.
func createTokenSQLite(ctx context.Context, cfg *config.Config, dataDir, tokenName string, principalType models.PrincipalType, level int) (string, error) {
	if dataDir == "" {
		return "", fmt.Errorf("--data-dir is required to create a token without PostgreSQL")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", fmt.Errorf("creating data directory %s: %w", dataDir, err)
	}

	// tokens.db holds the api_tokens table; authsqlite.New runs its migrations.
	tokensPath := filepath.Join(dataDir, "tokens.db")
	tokensDB, err := openSQLiteNative(ctx, tokensPath)
	if err != nil {
		return "", fmt.Errorf("opening tokens database %s (is the gateway running and holding it?): %w", tokensPath, err)
	}
	defer tokensDB.Close()

	tokenStore, err := authsqlite.New(tokensDB)
	if err != nil {
		return "", fmt.Errorf("constructing sqlite token store: %w", err)
	}

	// acl.db holds the ACL rules; aclsqlite.New runs its migrations and seeds
	// default fallback policies. We seed an explicit grant so the bootstrap
	// token's _system creator gets the requested (possibly elevated) access
	// level on all workspaces, matching the PostgreSQL path.
	aclPath := filepath.Join(dataDir, "acl.db")
	aclDB, err := openSQLiteNative(ctx, aclPath)
	if err != nil {
		return "", fmt.Errorf("opening acl database %s (is the gateway running and holding it?): %w", aclPath, err)
	}
	defer aclDB.Close()

	// audit.db is the audit sink the ACL store stamps decisions into; opening it
	// keeps parity with aetherlite's wiring. sharedAudit is nil — init-secrets
	// is a one-shot CLI and does not run the async audit batcher.
	auditPath := filepath.Join(dataDir, "audit.db")
	auditDB, err := openSQLiteNative(ctx, auditPath)
	if err != nil {
		return "", fmt.Errorf("opening audit database %s: %w", auditPath, err)
	}
	defer auditDB.Close()

	aclStore, err := aclsqlite.New(aclDB, nil, auditDB, cfg.Gateway.GatewayID)
	if err != nil {
		return "", fmt.Errorf("constructing sqlite acl store: %w", err)
	}
	defer aclStore.Close()

	grant := func(ctx context.Context, principalType, principalID, resourceType, resourceID string, accessLevel int, grantedBy, reason string, expiresAt *time.Time) error {
		_, gerr := aclStore.GrantAccess(ctx, principalType, principalID, resourceType, resourceID, accessLevel, grantedBy, reason, expiresAt)
		return gerr
	}

	return secrets.CreateInitialTokenWithStore(ctx, tokenStore, grant, cfg, tokenName, principalType, level)
}

// openSQLiteNative opens a SQLite database via the bare "sqlite" driver
// (modernc.org/sqlite) with the same pragmas aetherlite uses, so init-secrets
// reads/writes the on-disk files identically (see cmd/aetherlite/main.go).
func openSQLiteNative(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database %s: %w", path, err)
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("applying pragma %q on %s: %w", pragma, path, err)
		}
	}
	return db, nil
}

func handleGenerateTLS(gs *secrets.GeneratedSecrets, secretsPath, tlsDir string, validity time.Duration, force, printSecrets bool, extraSANs []string) error {
	caCertPath := filepath.Join(tlsDir, "ca-cert.pem")
	caKeyPath := filepath.Join(tlsDir, "ca-key.pem")
	serverCertPath := filepath.Join(tlsDir, "server-cert.pem")
	serverKeyPath := filepath.Join(tlsDir, "server-key.pem")

	// EnsureCA: load existing or generate new
	ca, err := certgen.EnsureCA(caCertPath, caKeyPath, certgen.CAOptions{
		Validity: 10 * 365 * 24 * time.Hour, // 10 years for CA
	})
	if err != nil {
		return fmt.Errorf("ensuring CA: %w", err)
	}
	log.Printf("CA certificate ready: %s", caCertPath)

	// Build DNS SANs: defaults + any extra SANs from --tls-san flag
	dnsNames := []string{"localhost", "aether-gateway", "aether-gateway.default.svc.cluster.local"}
	dnsNames = append(dnsNames, extraSANs...)

	// Generate server cert if not present (or force)
	if force || !fileExists(serverCertPath) || !fileExists(serverKeyPath) {
		bundle, genErr := ca.GenerateServerCert(certgen.ServerCertOptions{
			DNSNames: dnsNames,
			IPs:      []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
			Validity: validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating server certificate: %w", genErr)
		}
		if err := bundle.SaveToFiles(serverCertPath, serverKeyPath); err != nil {
			return fmt.Errorf("saving server certificate: %w", err)
		}
		log.Printf("Server certificate generated: %s", serverCertPath)
	} else {
		log.Printf("Server certificate already exists: %s", serverCertPath)
	}

	// Record TLS paths in generated secrets
	gs.TLS = secrets.TLSSecrets{
		CACertFile: caCertPath,
		CertFile:   serverCertPath,
		KeyFile:    serverKeyPath,
		Dir:        tlsDir,
	}

	// Re-save the secrets file with TLS paths
	if err := secrets.SaveGeneratedSecrets(secretsPath, gs); err != nil {
		return fmt.Errorf("updating secrets file with TLS paths: %w", err)
	}

	return nil
}

func handleClientCert(clientType, tlsDir string, validity time.Duration, workspace, impl, spec, userID, windowID string) error {
	caCertPath := filepath.Join(tlsDir, "ca-cert.pem")
	caKeyPath := filepath.Join(tlsDir, "ca-key.pem")

	// Load existing CA
	ca, err := certgen.LoadCA(caCertPath, caKeyPath)
	if err != nil {
		return fmt.Errorf("loading CA from %s (run --generate-tls first): %w", tlsDir, err)
	}

	clientsDir := filepath.Join(tlsDir, "clients")

	switch strings.ToLower(clientType) {
	case "anonymous":
		bundle, genErr := ca.GenerateAnonymousClientCert()
		if genErr != nil {
			return fmt.Errorf("generating anonymous client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, "anonymous-cert.pem")
		keyPath := filepath.Join(clientsDir, "anonymous-key.pem")
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("Anonymous client certificate generated: %s", certPath)

	case "agent":
		if workspace == "" || impl == "" || spec == "" {
			return fmt.Errorf("--client-cert agent requires --workspace, --impl, and --spec")
		}
		cn := fmt.Sprintf("ag::%s::%s::%s", workspace, impl, spec)
		bundle, genErr := ca.GenerateClientCert(certgen.ClientCertOptions{
			CommonName: cn,
			OrgUnit:    "Agent",
			Validity:   validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating agent client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cnFilename(cn)))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cnFilename(cn)))
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("Agent client certificate generated: %s (CN=%s)", certPath, cn)

	case "task":
		if workspace == "" || impl == "" {
			return fmt.Errorf("--client-cert task requires --workspace and --impl")
		}
		cn := fmt.Sprintf("ta::%s::%s", workspace, impl)
		if spec != "" {
			cn = fmt.Sprintf("tu::%s::%s::%s", workspace, impl, spec)
		}
		bundle, genErr := ca.GenerateClientCert(certgen.ClientCertOptions{
			CommonName: cn,
			OrgUnit:    "Task",
			Validity:   validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating task client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cnFilename(cn)))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cnFilename(cn)))
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("Task client certificate generated: %s (CN=%s)", certPath, cn)

	case "user":
		if userID == "" || windowID == "" {
			return fmt.Errorf("--client-cert user requires --user-id and --window-id")
		}
		cn := fmt.Sprintf("us::%s::%s", userID, windowID)
		bundle, genErr := ca.GenerateClientCert(certgen.ClientCertOptions{
			CommonName: cn,
			OrgUnit:    "User",
			Validity:   validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating user client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cnFilename(cn)))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cnFilename(cn)))
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("User client certificate generated: %s (CN=%s)", certPath, cn)

	case "orchestrator":
		if impl == "" {
			return fmt.Errorf("--client-cert orchestrator requires --impl")
		}
		cn := fmt.Sprintf("orc::%s", impl)
		if spec != "" {
			cn = fmt.Sprintf("orc::%s::%s", impl, spec)
		}
		bundle, genErr := ca.GenerateClientCert(certgen.ClientCertOptions{
			CommonName: cn,
			OrgUnit:    "Orchestrator",
			Validity:   validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating orchestrator client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cnFilename(cn)))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cnFilename(cn)))
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("Orchestrator client certificate generated: %s (CN=%s)", certPath, cn)

	case "workflow-engine":
		cn := "wfe::shard0"
		bundle, genErr := ca.GenerateClientCert(certgen.ClientCertOptions{
			CommonName: cn,
			OrgUnit:    "WorkflowEngine",
			Validity:   validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating workflow-engine client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, "wfe-cert.pem")
		keyPath := filepath.Join(clientsDir, "wfe-key.pem")
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("Workflow engine client certificate generated: %s (CN=%s)", certPath, cn)

	case "metrics-bridge":
		cn := "metrics::shard0"
		bundle, genErr := ca.GenerateClientCert(certgen.ClientCertOptions{
			CommonName: cn,
			OrgUnit:    "MetricsBridge",
			Validity:   validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating metrics-bridge client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, "metrics-cert.pem")
		keyPath := filepath.Join(clientsDir, "metrics-key.pem")
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("Metrics bridge client certificate generated: %s (CN=%s)", certPath, cn)

	case "service":
		if impl == "" || spec == "" {
			return fmt.Errorf("--client-cert service requires --impl and --spec")
		}
		cn := fmt.Sprintf("sv::%s::%s", impl, spec)
		bundle, genErr := ca.GenerateClientCert(certgen.ClientCertOptions{
			CommonName: cn,
			OrgUnit:    "Service",
			Validity:   validity,
		})
		if genErr != nil {
			return fmt.Errorf("generating service client cert: %w", genErr)
		}
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cnFilename(cn)))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cnFilename(cn)))
		if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
			return err
		}
		log.Printf("Service client certificate generated: %s (CN=%s)", certPath, cn)

	default:
		return fmt.Errorf("unknown client-cert type %q (valid: agent, task, user, service, orchestrator, workflow-engine, metrics-bridge, anonymous)", clientType)
	}

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
