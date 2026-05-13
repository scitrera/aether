package authproxy

import (
	"fmt"
	"os"
	"strings"
)

// Mode selects the operating mode of the auth proxy.
type Mode string

const (
	// ModeProxy is the reverse-proxy mode: validate auth, inject headers,
	// forward to backend. Suitable for dev / small deployments.
	ModeProxy Mode = "proxy"

	// ModeVerify is the auth-verify mode: validate auth and return 200 with
	// headers (or 401/403). Designed for nginx auth_request or Envoy
	// ext_authz integration.
	ModeVerify Mode = "verify"
)

// Config holds the auth-proxy runtime configuration. External binaries
// (e.g. the Scitrera multi-tenant variant) populate this from their own
// env-loading paths and then call Run.
type Config struct {
	Mode         Mode
	ListenAddr   string
	BackendURL   string
	TenantID     string
	DBURL        string
	RedisAddr    string
	LogLevel     string
	TokenHMACKey string
	SecretsFile  string
	CORSOrigin   string
	TLSCertFile  string
	TLSKeyFile   string
	OAuth        OAuthConfig
	Entra        EntraConfig
}

// OAuthConfig holds optional OAuth/JWT bearer-token validation settings.
type OAuthConfig struct {
	Issuer          string
	JWKSURL         string
	Audience        string
	VerifySignature bool
}

// EntraConfig holds optional Microsoft Entra (Azure AD) JWT settings.
type EntraConfig struct {
	TenantID        string
	ClientID        string
	AllowedTenants  []string
	VerifySignature bool
}

// OAuthConfigured reports whether the OAuth bearer-token authenticator
// has the minimum env-driven config required to start.
func (c *Config) OAuthConfigured() bool {
	return c.OAuth.Issuer != "" || c.OAuth.JWKSURL != ""
}

// EntraConfigured reports whether the Azure Entra authenticator has its
// minimum env-driven config (tenant + client id) set.
func (c *Config) EntraConfigured() bool {
	return c.Entra.TenantID != "" && c.Entra.ClientID != ""
}

// LoadConfigFromEnv reads AUTH_PROXY_* environment variables into a Config
// with sane defaults. Returns an error if a required variable is missing or
// a JWT-verification override is enabled outside dev mode.
func LoadConfigFromEnv() (*Config, error) {
	cfg := &Config{
		Mode:       ModeProxy,
		ListenAddr: ":8080",
		BackendURL: "http://localhost:61001",
		TenantID:   "default",
		LogLevel:   "info",
		OAuth:      OAuthConfig{VerifySignature: true},
		Entra:      EntraConfig{VerifySignature: true},
	}

	if v := os.Getenv("AUTH_PROXY_MODE"); v != "" {
		switch strings.ToLower(v) {
		case "proxy":
			cfg.Mode = ModeProxy
		case "verify":
			cfg.Mode = ModeVerify
		default:
			return nil, fmt.Errorf("invalid AUTH_PROXY_MODE %q: must be \"proxy\" or \"verify\"", v)
		}
	}
	if v := os.Getenv("AUTH_PROXY_LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("AUTH_PROXY_BACKEND_URL"); v != "" {
		cfg.BackendURL = v
	}
	if v := os.Getenv("AUTH_PROXY_TENANT_ID"); v != "" {
		cfg.TenantID = v
	}
	if v := os.Getenv("AUTH_PROXY_DB_URL"); v != "" {
		cfg.DBURL = v
	}
	if v := os.Getenv("AUTH_PROXY_REDIS_ADDR"); v != "" {
		cfg.RedisAddr = v
	}
	if v := os.Getenv("AUTH_PROXY_LOG_LEVEL"); v != "" {
		cfg.LogLevel = strings.ToLower(v)
	}
	if v := os.Getenv("AUTH_PROXY_CORS_ORIGIN"); v != "" {
		cfg.CORSOrigin = v
	}
	if v := os.Getenv("AUTH_PROXY_TOKEN_HMAC_KEY"); v != "" {
		cfg.TokenHMACKey = v
	}
	if v := os.Getenv("AUTH_PROXY_SECRETS_FILE"); v != "" {
		cfg.SecretsFile = v
	}

	if v := os.Getenv("AUTH_PROXY_OAUTH_ISSUER"); v != "" {
		cfg.OAuth.Issuer = v
	}
	if v := os.Getenv("AUTH_PROXY_OAUTH_JWKS_URL"); v != "" {
		cfg.OAuth.JWKSURL = v
	}
	if v := os.Getenv("AUTH_PROXY_OAUTH_AUDIENCE"); v != "" {
		cfg.OAuth.Audience = v
	}
	if v := os.Getenv("AUTH_PROXY_OAUTH_VERIFY_SIGNATURE"); v != "" {
		switch strings.ToLower(v) {
		case "false", "0", "no":
			if os.Getenv("AETHER_DEV_MODE") != "true" {
				return nil, fmt.Errorf("JWT signature verification cannot be disabled outside dev mode; set AETHER_DEV_MODE=true to override")
			}
			cfg.OAuth.VerifySignature = false
		default:
			cfg.OAuth.VerifySignature = true
		}
	}

	if v := os.Getenv("AUTH_PROXY_ENTRA_TENANT_ID"); v != "" {
		cfg.Entra.TenantID = v
	}
	if v := os.Getenv("AUTH_PROXY_ENTRA_CLIENT_ID"); v != "" {
		cfg.Entra.ClientID = v
	}
	if v := os.Getenv("AUTH_PROXY_ENTRA_ALLOWED_TENANTS"); v != "" {
		for _, p := range strings.Split(v, ",") {
			if t := strings.TrimSpace(p); t != "" {
				cfg.Entra.AllowedTenants = append(cfg.Entra.AllowedTenants, t)
			}
		}
	}
	if v := os.Getenv("AUTH_PROXY_ENTRA_VERIFY_SIGNATURE"); v != "" {
		switch strings.ToLower(v) {
		case "false", "0", "no":
			if os.Getenv("AETHER_DEV_MODE") != "true" {
				return nil, fmt.Errorf("Azure Entra JWT signature verification cannot be disabled outside dev mode; set AETHER_DEV_MODE=true to override")
			}
			cfg.Entra.VerifySignature = false
		default:
			cfg.Entra.VerifySignature = true
		}
	}

	if v := os.Getenv("AUTH_PROXY_TLS_CERT_FILE"); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv("AUTH_PROXY_TLS_KEY_FILE"); v != "" {
		cfg.TLSKeyFile = v
	}

	if cfg.DBURL == "" {
		return nil, fmt.Errorf("AUTH_PROXY_DB_URL is required")
	}
	return cfg, nil
}
