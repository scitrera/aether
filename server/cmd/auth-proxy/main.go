// auth-proxy is an authentication/authorization gateway for MemoryLayer.
//
// It validates credentials using Aether's auth system (API keys via
// PostgreSQL token store and/or OAuth/JWT) and evaluates workspace-level
// ACLs before forwarding requests to the MemoryLayer backend.
//
// Two operating modes are supported:
//
//   - proxy mode (default): reverse-proxies to MemoryLayer after injecting
//     trusted X-Auth-* headers. Suitable for dev and small deployments.
//   - verify mode: exposes /auth/verify for nginx auth_request or Envoy
//     ext_authz. Returns 200 with identity headers on success, 401/403 on
//     failure.
//
// Configuration is entirely via environment variables. See the authproxy
// package for the full list.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/secrets"
	versionpkg "github.com/scitrera/aether/internal/version"
	"github.com/scitrera/aether/pkg/authproxy"
	"github.com/scitrera/aether/pkg/crypto"
)

var (
	version = versionpkg.Version
	banner  = `
    _         _   _                ___
   / \  _   _| |_| |__           / _ \ _ __ _____  ___   _
  / _ \| | | | __| '_ \  _____  / /_)/ '__/ _ \ \/ / | | |
 / ___ \ |_| | |_| | | ||_____|| ___/| | | (_) >  <| |_| |
/_/   \_\__,_|\__|_| |_|       |_|   |_|  \___/_/\_\\__, |
                                                      |___/
Aether Auth Proxy v%s
`
)

func main() {
	log.Printf(banner, version)
	log.Println("Auth proxy starting...")

	// Load configuration from environment
	cfg, err := authproxy.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize structured logger
	logging.Init(cfg.LogLevel)

	// Initialize HMAC key for token hashing — must match the gateway's key.
	// Priority: AUTH_PROXY_TOKEN_HMAC_KEY env var > secrets file > none (bare SHA-256 fallback).
	if cfg.TokenHMACKey != "" {
		crypto.InitTokenHMAC([]byte(cfg.TokenHMACKey))
		log.Println("Token HMAC key initialized from environment")
	} else if cfg.SecretsFile != "" {
		gs, err := secrets.LoadGeneratedSecrets(cfg.SecretsFile)
		if err != nil {
			log.Printf("WARNING: could not load secrets file %s: %v", cfg.SecretsFile, err)
		} else if gs.Auth.TokenHMACKey != "" {
			crypto.InitTokenHMAC([]byte(gs.Auth.TokenHMACKey))
			cfg.TokenHMACKey = gs.Auth.TokenHMACKey
			log.Println("Token HMAC key initialized from secrets file")
		}
	}

	logging.Logger.Info().
		Str("mode", string(cfg.Mode)).
		Str("listen", cfg.ListenAddr).
		Str("tenant", cfg.TenantID).
		Str("log_level", cfg.LogLevel).
		Bool("hmac_configured", cfg.TokenHMACKey != "").
		Msg("configuration loaded")
	if cfg.Mode == authproxy.ModeProxy {
		logging.Logger.Info().Str("backend", cfg.BackendURL).Msg("proxy backend configured")
	}

	// Connect to PostgreSQL (shared with Aether gateway)
	db, err := initDatabase(cfg)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer db.Close()

	// Build authenticators
	authenticator, oauthAuth, entraAuth := buildAuthenticator(cfg, db)
	if oauthAuth != nil {
		defer oauthAuth.Close()
	}
	if entraAuth != nil {
		defer entraAuth.Close()
	}

	// Build ACL evaluator (Casbin-backed)
	evaluator, err := acl.NewCasbinEnforcer(db)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to create ACL enforcer")
	}

	// Build ACL service for OBO authority grant resolution. NewService does not
	// return an error; if the internal enforcer fails to init it falls back to
	// fail-closed for ACL checks but grant resolution (DB-direct) still works.
	aclService := acl.NewService(db, "authproxy")

	// Build the OSS-default IdentityResolver from environment.
	// AUTH_PROXY_AUTH_RULE_* and AUTH_PROXY_ALLOWED_EMAIL_DOMAINS configure
	// declarative auth checks (e.g. required Azure tid). The resolver also
	// emits the configured tenant id as DefaultTenantID for header injection.
	identityResolver, err := authproxy.LoadSingleTenantResolverFromEnv(cfg.TenantID)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to load identity resolver config")
	}

	// Create middleware with full plugin surface (OBO + identity resolver).
	middleware := authproxy.NewAuthMiddlewareFull(authenticator, evaluator, aclService, identityResolver, cfg.TenantID)

	// Create server
	server, err := authproxy.NewServer(cfg, middleware)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to create server")
	}

	// Optional browser-OAuth login flow. Disabled by default; enabled when
	// AUTH_PROXY_LOGIN_PROVIDERS is set. Adds /auth/login/<name>,
	// /auth/callback/<name>, /auth/logout, /auth/me, /auth/checkz routes
	// and registers a session authenticator on the composite chain.
	loginCfg, err := authproxy.LoadLoginConfigFromEnv()
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to load login config")
	}
	loginCleanup, err := authproxy.AttachLogin(context.Background(), loginCfg, server.Mux(), middleware, authenticator)
	if err != nil {
		logging.Logger.Fatal().Err(err).Msg("failed to attach login subsystem")
	}
	defer loginCleanup()

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			logging.Logger.Fatal().Err(err).Msg("server error")
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logging.Logger.Info().Msg("shutdown signal received, gracefully stopping")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Stop(shutdownCtx); err != nil {
		logging.Logger.Error().Err(err).Msg("server shutdown error")
	}

	logging.Logger.Info().Msg("auth proxy stopped")
}

// initDatabase connects to PostgreSQL and verifies connectivity.
func initDatabase(cfg *authproxy.Config) (*sql.DB, error) {
	logging.Logger.Info().Msg("connecting to PostgreSQL")

	db, err := sql.Open("postgres", cfg.DBURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Conservative pool settings for an auth proxy
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	logging.Logger.Info().Msg("PostgreSQL connection established")
	return db, nil
}

// buildAuthenticator constructs the composite authenticator based on
// configuration. It always includes API key auth (requires DB) and
// optionally adds OAuth/JWT and/or Azure Entra if configured.
//
// Returns the composite authenticator, the OAuthAuthenticator, and the
// AzureEntraAuthenticator (either may be nil) so the caller can close
// their JWKS refresh goroutines.
func buildAuthenticator(cfg *authproxy.Config, db *sql.DB) (*auth.CompositeAuthenticator, *auth.OAuthAuthenticator, *auth.AzureEntraAuthenticator) {
	var authenticators []auth.Authenticator
	var oauthAuth *auth.OAuthAuthenticator
	var entraAuth *auth.AzureEntraAuthenticator

	// API key authenticator (always enabled when DB is available)
	apiTokenStore := auth.NewAPITokenStore(db)
	authenticators = append(authenticators, auth.NewAPIKeyAuthenticator(apiTokenStore))
	logging.Logger.Info().Msg("API key authentication enabled")

	// OAuth authenticator (optional)
	if cfg.OAuthConfigured() {
		provider := auth.OAuthProviderConfig{
			Name:     "auth-proxy",
			Issuer:   cfg.OAuth.Issuer,
			JWKSURL:  cfg.OAuth.JWKSURL,
			Audience: cfg.OAuth.Audience,
			ClaimsMapping: auth.ClaimsMapping{
				PrincipalType: "principal_type",
				Workspace:     "workspace",
				Identity:      "sub",
			},
			DefaultPrincipal: "User",
			DefaultWorkspace: "",
		}

		verifySig := cfg.OAuth.VerifySignature
		oauthAuth = auth.NewOAuthAuthenticator(
			[]auth.OAuthProviderConfig{provider},
			&verifySig,
		)
		authenticators = append(authenticators, oauthAuth)

		if !cfg.OAuth.VerifySignature {
			logging.Logger.Warn().Msg("OAuth JWT signature verification is DISABLED (development mode)")
		}
		logging.Logger.Info().Str("issuer", cfg.OAuth.Issuer).Msg("OAuth authentication enabled")
	}

	// Azure Entra authenticator (optional)
	if cfg.EntraConfigured() {
		verifySig := cfg.Entra.VerifySignature
		entraAuth = auth.NewAzureEntraAuthenticator(
			cfg.Entra.TenantID,
			cfg.Entra.ClientID,
			cfg.Entra.AllowedTenants,
			&verifySig,
		)
		authenticators = append(authenticators, entraAuth)
		logging.Logger.Info().
			Str("tenant", cfg.Entra.TenantID).
			Strs("allowed_tenants", cfg.Entra.AllowedTenants).
			Msg("Azure Entra authentication enabled")
	}

	composite := auth.NewCompositeAuthenticator(authenticators...)
	logging.Logger.Info().Int("methods", len(authenticators)).Msg("composite authenticator initialized")

	return composite, oauthAuth, entraAuth
}
