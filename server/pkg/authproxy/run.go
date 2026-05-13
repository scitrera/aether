package authproxy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	"github.com/scitrera/aether/pkg/crypto"
)

// Option is a functional option for Run.
type Option func(*runOptions)

// runOptions captures the configurable behaviour of Run. Held internal so
// the option set can be extended without breaking callers.
type runOptions struct {
	identityResolver IdentityResolver
}

// WithIdentityResolver overrides the default IdentityResolver used by Run.
//
// The default is a SingleTenantResolver loaded from environment
// (AUTH_PROXY_AUTH_RULE_*, AUTH_PROXY_ALLOWED_EMAIL_DOMAINS); pass this
// option from external binaries (e.g. the Scitrera multi-tenant variant) to
// substitute their own.
func WithIdentityResolver(r IdentityResolver) Option {
	return func(o *runOptions) {
		o.identityResolver = r
	}
}

// Run wires up the auth-proxy from cfg, applies opts, and blocks until the
// process receives SIGINT/SIGTERM (or the supplied context is cancelled).
// It performs a graceful shutdown on signal and returns nil for clean exits
// or an error for fatal startup failures.
//
// Internally Run:
//
//   - opens the Postgres pool against cfg.DBURL
//   - initialises the token-HMAC key from env or cfg.SecretsFile
//   - builds the composite authenticator (api_key + optional OAuth + Entra)
//   - builds the Casbin ACL evaluator and OBO authority resolver
//   - constructs the AuthMiddleware with the chosen IdentityResolver
//   - mounts /healthz, /auth/verify, and (in proxy mode) the reverse proxy
//   - attaches the optional browser-OAuth login subsystem when
//     AUTH_PROXY_LOGIN_PROVIDERS is set
//   - registers signal handlers and blocks on Server.Start
//
// External binaries that need finer-grained control over a single step
// (e.g. injecting a custom authenticator on the composite chain) should
// compose the lower-level pieces directly using the internal/authproxy
// package — but this requires building inside the OSS module tree.
func Run(ctx context.Context, cfg *Config, opts ...Option) error {
	o := &runOptions{}
	for _, fn := range opts {
		fn(o)
	}

	logging.Init(cfg.LogLevel)

	if cfg.TokenHMACKey != "" {
		crypto.InitTokenHMAC([]byte(cfg.TokenHMACKey))
		logging.Logger.Info().Msg("token HMAC key initialized from environment")
	} else if cfg.SecretsFile != "" {
		gs, err := secrets.LoadGeneratedSecrets(cfg.SecretsFile)
		if err != nil {
			logging.Logger.Warn().Err(err).Str("file", cfg.SecretsFile).Msg("could not load secrets file")
		} else if gs.Auth.TokenHMACKey != "" {
			crypto.InitTokenHMAC([]byte(gs.Auth.TokenHMACKey))
			cfg.TokenHMACKey = gs.Auth.TokenHMACKey
			logging.Logger.Info().Msg("token HMAC key initialized from secrets file")
		}
	}

	logging.Logger.Info().
		Str("mode", string(cfg.Mode)).
		Str("listen", cfg.ListenAddr).
		Str("tenant", cfg.TenantID).
		Str("log_level", cfg.LogLevel).
		Bool("hmac_configured", cfg.TokenHMACKey != "").
		Msg("auth-proxy: configuration loaded")

	db, err := openDB(cfg)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer db.Close()

	composite, oauthAuth, entraAuth := buildStandardAuthenticator(cfg, db)
	if oauthAuth != nil {
		defer oauthAuth.Close()
	}
	if entraAuth != nil {
		defer entraAuth.Close()
	}

	evaluator, err := acl.NewCasbinEnforcer(db)
	if err != nil {
		return fmt.Errorf("create acl enforcer: %w", err)
	}
	aclService := acl.NewService(db, "authproxy")

	identityResolver := o.identityResolver
	if identityResolver == nil {
		st, err := LoadSingleTenantResolverFromEnv(cfg.TenantID)
		if err != nil {
			return fmt.Errorf("load default identity resolver: %w", err)
		}
		identityResolver = st
	}

	middleware := NewAuthMiddlewareFull(composite, evaluator, aclService, identityResolver, cfg.TenantID)

	server, err := NewServer(cfg, middleware)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	loginCfg, err := LoadLoginConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load login config: %w", err)
	}
	loginCleanup, err := AttachLogin(ctx, loginCfg, server.Mux(), middleware, composite)
	if err != nil {
		return fmt.Errorf("attach login: %w", err)
	}
	defer loginCleanup()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	startErr := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			startErr <- err
			return
		}
		startErr <- nil
	}()

	select {
	case <-sigChan:
		logging.Logger.Info().Msg("shutdown signal received")
	case <-ctx.Done():
		logging.Logger.Info().Msg("context cancelled, shutting down")
	case err := <-startErr:
		if err != nil {
			return fmt.Errorf("server start: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Stop(shutdownCtx); err != nil {
		logging.Logger.Error().Err(err).Msg("server shutdown error")
	}
	logging.Logger.Info().Msg("auth-proxy stopped")
	return nil
}

// openDB opens a Postgres pool against cfg.DBURL with conservative settings
// and verifies connectivity before returning.
func openDB(cfg *Config) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.DBURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return db, nil
}

// buildStandardAuthenticator constructs the OSS-default composite chain:
// api-key authentication (always), plus optional OAuth/JWT and Azure Entra
// when configured. Returned OAuth/Entra authenticators expose a Close()
// hook the caller must defer so background JWKS refresh goroutines exit
// cleanly.
func buildStandardAuthenticator(cfg *Config, db *sql.DB) (*auth.CompositeAuthenticator, *auth.OAuthAuthenticator, *auth.AzureEntraAuthenticator) {
	var authenticators []auth.Authenticator
	var oauthAuth *auth.OAuthAuthenticator
	var entraAuth *auth.AzureEntraAuthenticator

	apiTokenStore := auth.NewAPITokenStore(db)
	authenticators = append(authenticators, auth.NewAPIKeyAuthenticator(apiTokenStore))
	logging.Logger.Info().Msg("auth-proxy: API key authentication enabled")

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
		}
		v := cfg.OAuth.VerifySignature
		oauthAuth = auth.NewOAuthAuthenticator([]auth.OAuthProviderConfig{provider}, &v)
		authenticators = append(authenticators, oauthAuth)
		if !cfg.OAuth.VerifySignature {
			logging.Logger.Warn().Msg("OAuth JWT signature verification is DISABLED (development mode)")
		}
		logging.Logger.Info().Str("issuer", cfg.OAuth.Issuer).Msg("auth-proxy: OAuth authentication enabled")
	}

	if cfg.EntraConfigured() {
		v := cfg.Entra.VerifySignature
		entraAuth = auth.NewAzureEntraAuthenticator(cfg.Entra.TenantID, cfg.Entra.ClientID, cfg.Entra.AllowedTenants, &v)
		authenticators = append(authenticators, entraAuth)
		logging.Logger.Info().
			Str("tenant", cfg.Entra.TenantID).
			Strs("allowed_tenants", cfg.Entra.AllowedTenants).
			Msg("auth-proxy: Azure Entra authentication enabled")
	}

	composite := auth.NewCompositeAuthenticator(authenticators...)
	logging.Logger.Info().Int("methods", len(authenticators)).Msg("composite authenticator initialized")

	return composite, oauthAuth, entraAuth
}
