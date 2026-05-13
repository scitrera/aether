package config

import (
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
)

// ReloadableConfig wraps sensitive config fields that can be hot-reloaded via SIGHUP.
//
// Hot-reloadable fields (SIGHUP triggers a re-read of the config file):
//   - Admin API key  (admin.api_key / AETHER_ADMIN_API_KEY)
//   - TLS certificate (gateway.tls.cert_file + gateway.tls.key_file)
//   - Token HMAC key (auth.token_hmac_key / AETHER_TOKEN_HMAC_KEY)
//
// NOT reloadable (requires restart):
//   - Redis connection settings
//   - RabbitMQ connection settings
//   - PostgreSQL connection settings
//   - Listen ports
//   - gRPC server options
type ReloadableConfig struct {
	mu         sync.RWMutex
	configPath string

	// Hot-reloadable fields stored atomically for lock-free reads.
	adminAPIKey  atomic.Value // string
	tlsCert      atomic.Value // *tls.Certificate
	tokenHMACKey atomic.Value // string
}

// NewReloadableConfig creates a new ReloadableConfig seeded from the provided Config.
// configPath is the path to the config file used for subsequent Reload calls.
func NewReloadableConfig(configPath string, initial *Config) *ReloadableConfig {
	rc := &ReloadableConfig{
		configPath: configPath,
	}
	rc.adminAPIKey.Store(initial.Admin.APIKey)
	rc.tokenHMACKey.Store(initial.Auth.TokenHMACKey)

	// Load initial TLS certificate if configured.
	if initial.Gateway.TLS.CertFile != "" && initial.Gateway.TLS.KeyFile != "" {
		if cert, err := tls.LoadX509KeyPair(initial.Gateway.TLS.CertFile, initial.Gateway.TLS.KeyFile); err == nil {
			rc.tlsCert.Store(&cert)
		}
		// Failure to load the initial cert is non-fatal; it is handled by the caller.
	}

	return rc
}

// Reload re-reads the config file and updates hot-reloadable fields.
// Returns a slice describing what was updated and any error encountered.
// On error the previously loaded values remain in effect.
func (rc *ReloadableConfig) Reload() ([]string, error) {
	if rc.configPath == "" {
		return nil, fmt.Errorf("no config path set; cannot reload")
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	cfg, err := Load(rc.configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var reloaded []string

	// Admin API key
	if newKey := cfg.Admin.APIKey; newKey != rc.adminAPIKey.Load().(string) {
		rc.adminAPIKey.Store(newKey)
		reloaded = append(reloaded, "admin_api_key")
	}

	// Token HMAC key
	if newKey := cfg.Auth.TokenHMACKey; newKey != rc.tokenHMACKey.Load().(string) {
		rc.tokenHMACKey.Store(newKey)
		reloaded = append(reloaded, "token_hmac_key")
	}

	// TLS certificate — only reload when both cert and key files are configured.
	if cfg.Gateway.TLS.CertFile != "" && cfg.Gateway.TLS.KeyFile != "" {
		cert, certErr := tls.LoadX509KeyPair(cfg.Gateway.TLS.CertFile, cfg.Gateway.TLS.KeyFile)
		if certErr != nil {
			return reloaded, fmt.Errorf("failed to reload TLS certificate: %w", certErr)
		}
		rc.tlsCert.Store(&cert)
		reloaded = append(reloaded, "tls_certificate")
	}

	return reloaded, nil
}

// AdminAPIKey returns the current admin API key.
func (rc *ReloadableConfig) AdminAPIKey() string {
	v := rc.adminAPIKey.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// TLSCertificate returns the current TLS certificate, or nil if none is loaded.
func (rc *ReloadableConfig) TLSCertificate() *tls.Certificate {
	v := rc.tlsCert.Load()
	if v == nil {
		return nil
	}
	return v.(*tls.Certificate)
}

// TokenHMACKey returns the current token HMAC key.
func (rc *ReloadableConfig) TokenHMACKey() string {
	v := rc.tokenHMACKey.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}
