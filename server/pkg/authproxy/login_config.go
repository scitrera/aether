package authproxy

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/authproxy/login"
)

// LoginConfig drives the optional browser-OAuth login flow.
//
// When LoadLoginConfigFromEnv finds at least one provider configured (via
// AUTH_PROXY_LOGIN_PROVIDERS), the auth-proxy mounts /auth/login/<name>,
// /auth/callback/<name>, /auth/logout, /auth/me, and /auth/checkz. When
// no providers are configured, the auth-proxy is token-only (existing
// behaviour).
type LoginConfig struct {
	// Enabled gates the entire login subsystem.
	Enabled bool
	// Providers is the list of OIDC providers to mount.
	Providers []login.ProviderConfig
	// Cookies is the session-cookie configuration.
	Cookies login.CookieConfig
	// StoreKind selects between "redis" (default, opaque) and "jwt"
	// (signed-cookie). Both honour the same cookie config.
	StoreKind string
	// JWTSigningKey is required when StoreKind is "jwt"; HS256.
	JWTSigningKey []byte
	// RedisAddr is the Redis address for the opaque-id store. Falls back
	// to AUTH_PROXY_REDIS_ADDR if unset.
	RedisAddr string
	// RedisPassword and RedisDB are passed through to the redis client.
	RedisPassword string
	RedisDB       int
	// SessionPrefix is the Redis key prefix; default "auth-session:".
	SessionPrefix string
}

// LoadLoginConfigFromEnv reads AUTH_PROXY_LOGIN_* env vars into a LoginConfig.
//
// Recognised vars (when Enabled is true):
//
//   - AUTH_PROXY_LOGIN_PROVIDERS: comma-separated provider names (e.g. "azure,google")
//   - AUTH_PROXY_LOGIN_<NAME>_ISSUER, _CLIENT_ID, _CLIENT_SECRET, _REDIRECT_URL,
//     _SCOPES (csv), _ALLOWED_TENANTS (csv) — per-provider OIDC config
//   - AUTH_PROXY_SESSION_COOKIE_NAME, _COOKIE_DOMAIN, _COOKIE_SECURE (bool),
//     _COOKIE_SAMESITE ("lax"|"strict"|"none"), _TTL (duration, e.g. "24h")
//   - AUTH_PROXY_SESSION_STORE: "redis" (default) | "jwt"
//   - AUTH_PROXY_SESSION_JWT_SIGNING_KEY: at least 32 bytes when StoreKind is "jwt"
//   - AUTH_PROXY_SESSION_REDIS_ADDR / _PASSWORD / _DB / _PREFIX: Redis store
//
// Returns LoginConfig{Enabled: false} when no providers are configured.
func LoadLoginConfigFromEnv() (*LoginConfig, error) {
	names := splitAndTrim(os.Getenv("AUTH_PROXY_LOGIN_PROVIDERS"))
	if len(names) == 0 {
		return &LoginConfig{Enabled: false}, nil
	}

	cfg := &LoginConfig{Enabled: true, StoreKind: "redis"}

	for _, name := range names {
		key := strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
		pc := login.ProviderConfig{
			Name:             name,
			IssuerURL:        os.Getenv("AUTH_PROXY_LOGIN_" + key + "_ISSUER"),
			ClientID:         os.Getenv("AUTH_PROXY_LOGIN_" + key + "_CLIENT_ID"),
			ClientSecret:     os.Getenv("AUTH_PROXY_LOGIN_" + key + "_CLIENT_SECRET"),
			RedirectURL:      os.Getenv("AUTH_PROXY_LOGIN_" + key + "_REDIRECT_URL"),
			Scopes:           splitAndTrim(os.Getenv("AUTH_PROXY_LOGIN_" + key + "_SCOPES")),
			AllowedTenantIDs: splitAndTrim(os.Getenv("AUTH_PROXY_LOGIN_" + key + "_ALLOWED_TENANTS")),
		}
		cfg.Providers = append(cfg.Providers, pc)
	}

	cfg.Cookies = login.CookieConfig{
		Name:     getenv("AUTH_PROXY_SESSION_COOKIE_NAME", "aether_session"),
		Domain:   os.Getenv("AUTH_PROXY_SESSION_COOKIE_DOMAIN"),
		SameSite: parseSameSite(os.Getenv("AUTH_PROXY_SESSION_COOKIE_SAMESITE")),
		MaxAge:   parseDuration(os.Getenv("AUTH_PROXY_SESSION_TTL"), 24*time.Hour),
	}
	if v := os.Getenv("AUTH_PROXY_SESSION_COOKIE_SECURE"); v != "" {
		cfg.Cookies.Secure = parseBool(v, true)
	} else {
		cfg.Cookies.Secure = true
	}

	if v := os.Getenv("AUTH_PROXY_SESSION_STORE"); v != "" {
		cfg.StoreKind = strings.ToLower(strings.TrimSpace(v))
	}
	switch cfg.StoreKind {
	case "redis":
		cfg.RedisAddr = getenv("AUTH_PROXY_SESSION_REDIS_ADDR", os.Getenv("AUTH_PROXY_REDIS_ADDR"))
		cfg.RedisPassword = os.Getenv("AUTH_PROXY_SESSION_REDIS_PASSWORD")
		cfg.RedisDB = parseInt(os.Getenv("AUTH_PROXY_SESSION_REDIS_DB"), 0)
		cfg.SessionPrefix = getenv("AUTH_PROXY_SESSION_REDIS_PREFIX", "auth-session:")
		if cfg.RedisAddr == "" {
			return nil, fmt.Errorf("AUTH_PROXY_SESSION_STORE=redis requires AUTH_PROXY_SESSION_REDIS_ADDR or AUTH_PROXY_REDIS_ADDR")
		}
	case "jwt":
		key := os.Getenv("AUTH_PROXY_SESSION_JWT_SIGNING_KEY")
		if len(key) < 32 {
			return nil, fmt.Errorf("AUTH_PROXY_SESSION_STORE=jwt requires AUTH_PROXY_SESSION_JWT_SIGNING_KEY of at least 32 bytes")
		}
		cfg.JWTSigningKey = []byte(key)
	default:
		return nil, fmt.Errorf("unknown AUTH_PROXY_SESSION_STORE %q (want redis or jwt)", cfg.StoreKind)
	}

	return cfg, nil
}

// BuildSessionStore returns a SessionStore matching cfg.StoreKind. The Redis
// case opens a fresh client; callers retain ownership and should call
// .Close() during shutdown if they care to drain.
func (cfg *LoginConfig) BuildSessionStore() (login.SessionStore, *redis.Client, error) {
	switch cfg.StoreKind {
	case "redis":
		client := redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})
		// Probe the connection so misconfigurations surface at startup.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			return nil, nil, fmt.Errorf("redis ping: %w", err)
		}
		store := login.NewRedisOpaqueSessionStore(client, cfg.SessionPrefix)
		return store, client, nil
	case "jwt":
		store, err := login.NewSignedJWTSessionStore(cfg.JWTSigningKey, "aether-auth-proxy")
		if err != nil {
			return nil, nil, err
		}
		return store, nil, nil
	default:
		return nil, nil, fmt.Errorf("unknown StoreKind %q", cfg.StoreKind)
	}
}

// BuildRegistry performs OIDC discovery for each configured provider and
// returns a populated Registry. Discovery failures abort startup.
func (cfg *LoginConfig) BuildRegistry(ctx context.Context) (*login.Registry, error) {
	reg := login.NewRegistry()
	for _, pc := range cfg.Providers {
		prov, err := login.NewProvider(ctx, pc)
		if err != nil {
			return nil, err
		}
		reg.Register(prov)
		logging.Logger.Info().
			Str("provider", pc.Name).
			Str("issuer", pc.IssuerURL).
			Strs("scopes", prov.OAuth.Scopes).
			Msg("login: provider registered")
	}
	return reg, nil
}

// AttachLogin wires the login subsystem into the given mux + middleware:
// it builds the session store, registers providers, mounts handlers, and
// configures the AuthMiddleware to read the session cookie and to consult
// a SessionAuthenticator inside its composite chain.
//
// The returned cleanup function releases the underlying redis client (if
// any). Pass nil mux to skip route mounting (useful for tests).
func AttachLogin(
	ctx context.Context,
	cfg *LoginConfig,
	mux *http.ServeMux,
	mw *AuthMiddleware,
	composite *auth.CompositeAuthenticator,
) (cleanup func(), err error) {
	if cfg == nil || !cfg.Enabled {
		return func() {}, nil
	}

	store, redisClient, err := cfg.BuildSessionStore()
	if err != nil {
		return nil, fmt.Errorf("build session store: %w", err)
	}

	reg, err := cfg.BuildRegistry(ctx)
	if err != nil {
		if redisClient != nil {
			_ = redisClient.Close()
		}
		return nil, fmt.Errorf("build provider registry: %w", err)
	}

	handlers, err := login.NewHandlers(login.Options{
		Registry: reg,
		Store:    store,
		Cookies:  cfg.Cookies,
	})
	if err != nil {
		if redisClient != nil {
			_ = redisClient.Close()
		}
		return nil, fmt.Errorf("build login handlers: %w", err)
	}
	if mux != nil {
		handlers.Mount(mux)
	}

	// Tell the middleware to harvest the session cookie when present, and
	// register the session authenticator on the composite chain so the
	// session_token credential resolves to a verified identity.
	mw.SetSessionCookieName(cfg.Cookies.Name)
	composite.Add(auth.NewSessionAuthenticator(store))

	logging.Logger.Info().
		Str("store", store.Name()).
		Strs("providers", reg.Names()).
		Str("cookie", cfg.Cookies.Name).
		Msg("login: enabled")

	return func() {
		if redisClient != nil {
			_ = redisClient.Close()
		}
	}, nil
}

func splitAndTrim(s string) []string {
	if s == "" {
		return nil
	}
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

func parseBool(s string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	n := def
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return def
	}
	return n
}

func parseSameSite(s string) http.SameSite {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "strict":
		return http.SameSiteStrictMode
	case "none":
		return http.SameSiteNoneMode
	case "lax", "":
		return http.SameSiteLaxMode
	default:
		return http.SameSiteLaxMode
	}
}
