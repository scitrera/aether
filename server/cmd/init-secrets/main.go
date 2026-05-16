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
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/secrets"
	"github.com/scitrera/aether/pkg/certgen"
	"github.com/scitrera/aether/pkg/models"
)

const defaultSecretsPath = "/etc/aether/generated-secrets.yaml"

func main() {
	configFile := flag.String("config", "", "Path to main config file")
	output := flag.String("output", defaultSecretsPath, "Path to write generated secrets")
	createToken := flag.Bool("create-token", false, "Create an initial admin API token in the database")
	tokenName := flag.String("token-name", "admin-bootstrap", "Name for the initial token")
	principalType := flag.String("principal-type", "User", "Principal type for the token (User, Agent, Task, Service, Orchestrator, WorkflowEngine, MetricsBridge, Bridge)")
	accessLevel := flag.String("access-level", "ADMIN", "Access level for the token (NONE, READ, READWRITE, MANAGE, ADMIN, SUPERADMIN)")
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
		if cfg.Postgres.Host == "" {
			log.Fatal("--create-token requires PostgreSQL configuration (set via --config or env vars)")
		}

		db, err := sql.Open("postgres", cfg.Postgres.DSN())
		if err != nil {
			log.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		ctx := context.Background()
		if err := db.PingContext(ctx); err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}

		level, err := secrets.ParseAccessLevel(*accessLevel)
		if err != nil {
			log.Fatalf("Invalid access level %q: %v", *accessLevel, err)
		}

		plaintext, err := secrets.CreateInitialToken(ctx, db, cfg, *tokenName, models.PrincipalType(*principalType), level)
		if err != nil {
			log.Fatalf("Failed to create initial token: %v", err)
		}

		fmt.Printf("initial_api_token:   %s\n", plaintext)
		log.Printf("Initial API token '%s' created with %s access", *tokenName, *accessLevel)
	}
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
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cn))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cn))
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
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cn))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cn))
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
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cn))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cn))
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
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cn))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cn))
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
		certPath := filepath.Join(clientsDir, fmt.Sprintf("%s-cert.pem", cn))
		keyPath := filepath.Join(clientsDir, fmt.Sprintf("%s-key.pem", cn))
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
