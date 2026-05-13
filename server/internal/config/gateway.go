// Package config provides configuration management for the Aether gateway.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// Config represents the complete Aether configuration
type Config struct {
	Mode       string           `yaml:"mode"` // "full" (default) or "lite"
	Lite       LiteConfig       `yaml:"lite"`
	Gateway    GatewayConfig    `yaml:"gateway"`
	Admin      AdminConfig      `yaml:"admin"`
	Auth       AuthConfig       `yaml:"auth"`
	ACL        ACLConfig        `yaml:"acl"`
	Postgres   PostgresConfig   `yaml:"postgres"`
	Redis      RedisConfig      `yaml:"redis"`
	RabbitMQ   RabbitMQConfig   `yaml:"rabbitmq"`
	Audit      AuditConfig      `yaml:"audit"`
	Cleanup    CleanupConfig    `yaml:"cleanup"`
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	KV         KVConfig         `yaml:"kv"`
	Shutdown   ShutdownConfig   `yaml:"shutdown"`
	Quotas     QuotasConfig     `yaml:"quotas"`
	LogLevel   string           `yaml:"log_level"`
}

// IsLiteMode returns true when the gateway is configured for lite (embedded) mode.
func (c *Config) IsLiteMode() bool {
	return strings.EqualFold(c.Mode, "lite")
}

// LiteConfig holds configuration for AetherLite embedded mode.
type LiteConfig struct {
	// DataDir is the directory for SQLite database and Badger storage.
	// Defaults to "./aether-lite-data" when empty.
	DataDir string `yaml:"data_dir"`
}

// GetDataDir returns the effective data directory, defaulting to "./aether-lite-data".
func (l *LiteConfig) GetDataDir() string {
	if l.DataDir == "" {
		return "./aether-lite-data"
	}
	return l.DataDir
}

// ACLConfig contains ACL service settings.
type ACLConfig struct {
	// Required controls whether the gateway refuses to start (or rejects connections)
	// when the ACL service is unavailable (i.e. PostgreSQL is not configured).
	// Default: false (graceful degradation — ACL checks are skipped when service is nil).
	// Set to true in production to enforce that access control is always active.
	Required bool `yaml:"required"`
}

// GatewayTLSConfig contains gRPC TLS settings.
type GatewayTLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
	// ClientAuth controls client certificate requirement:
	// "require" = RequireAndVerifyClientCert (default)
	// "request" = VerifyClientCertIfGiven (allows connections with or without client cert)
	// "none" = NoClientCert (server TLS only, no mTLS)
	ClientAuth string `yaml:"client_auth"`
}

// GatewayConfig contains gateway server settings
type GatewayConfig struct {
	Port               int                  `yaml:"port"`
	OpsPort            int                  `yaml:"ops_port"` // Health probes + Prometheus metrics (default: 9090)
	GatewayID          string               `yaml:"gateway_id"`
	MessageRateLimit   float64              `yaml:"message_rate_limit"`   // Per-client messages/second (default: 100)
	MessageRateBurst   int                  `yaml:"message_rate_burst"`   // Per-client burst size (default: 200)
	DeliveryBufferSize int                  `yaml:"delivery_buffer_size"` // Per-client outbound message buffer (default: 256)
	CircuitBreaker     CircuitBreakerConfig `yaml:"circuit_breaker"`
	TLS                GatewayTLSConfig     `yaml:"tls"`
}

// GetOpsPort returns the ops port, defaulting to 9090.
func (g *GatewayConfig) GetOpsPort() int {
	if g.OpsPort <= 0 {
		return 9090
	}
	return g.OpsPort
}

// GetDeliveryBufferSize returns the configured delivery buffer size, defaulting to 256.
func (g *GatewayConfig) GetDeliveryBufferSize() int {
	if g.DeliveryBufferSize <= 0 {
		return 256
	}
	return g.DeliveryBufferSize
}

// CircuitBreakerConfig contains circuit breaker settings for Redis/RabbitMQ protection.
type CircuitBreakerConfig struct {
	// MaxFailures is the number of consecutive failures before the circuit opens.
	// Default: 5.
	MaxFailures int `yaml:"max_failures"`
	// ResetTimeout is how long to wait in open state before allowing a probe call.
	// Default: 30s.
	ResetTimeout string `yaml:"reset_timeout"`
}

// GetResetTimeout parses the reset timeout duration.
// Returns 30 seconds as default if not configured or invalid.
func (c *CircuitBreakerConfig) GetResetTimeout() time.Duration {
	return parseDurationOrDefault(c.ResetTimeout, 30*time.Second)
}

// GetMaxFailures returns the max failures, defaulting to 5 if not set.
func (c *CircuitBreakerConfig) GetMaxFailures() int {
	if c.MaxFailures <= 0 {
		return 5
	}
	return c.MaxFailures
}

// AdminConfig contains admin UI/API server settings
type AdminConfig struct {
	Enabled        bool    `yaml:"enabled"`
	Port           int     `yaml:"port"`
	CORSOrigin     string  `yaml:"cors_origin"`
	APIKey         string  `yaml:"api_key"` // Bearer token for admin API; if empty, admin API is unauthenticated (dev only)
	TLSCertFile    string  `yaml:"tls_cert_file"`
	TLSKeyFile     string  `yaml:"tls_key_file"`
	RateLimit      float64 `yaml:"rate_limit"`       // requests per second (default: 10)
	RateLimitBurst int     `yaml:"rate_limit_burst"` // burst size (default: 20)
}

// AuthConfig contains authentication settings
type AuthConfig struct {
	// Modes lists the enabled authentication modes in priority order.
	// Supported: "mtls", "task_token", "api_key", "oauth"
	// Default: ["mtls", "task_token"] (backwards compatible)
	Modes []string `yaml:"modes"`

	// MTLS contains mTLS identity verification settings
	MTLS MTLSConfig `yaml:"mtls"`

	// APIKey contains API key provider-specific settings (no enable flag — presence in modes enables it)
	APIKey APIKeyAuthConfig `yaml:"api_key"`

	// OAuth contains OAuth/JWT provider settings (no enable flag — presence in modes enables it)
	OAuth OAuthConfig `yaml:"oauth"`

	// TokenHMACKey is the server-side secret used for HMAC-SHA256 token hashing.
	// If unset, bare SHA-256 is used for backward compatibility.
	// Should be sourced from the AETHER_TOKEN_HMAC_KEY environment variable.
	// Key should be at least 32 bytes.
	TokenHMACKey string `yaml:"token_hmac_key"`
}

// IsAuthModeEnabled checks if a specific auth mode is enabled
func (a *AuthConfig) IsAuthModeEnabled(mode string) bool {
	// If no modes configured, default to mtls + task_token for backwards compatibility
	if len(a.Modes) == 0 {
		return mode == "mtls" || mode == "task_token"
	}
	for _, m := range a.Modes {
		if m == mode {
			return true
		}
	}
	return false
}

// MTLSConfig contains mTLS identity verification settings
type MTLSConfig struct {
	Required bool   `yaml:"required"`
	Mode     string `yaml:"mode"` // "strict" or "relaxed"
}

// APIKeyAuthConfig contains API key authentication settings
type APIKeyAuthConfig struct {
	// Provider-specific settings can be added here.
	// Enabling/disabling is controlled by auth.modes list.
}

// OAuthConfig contains OAuth/JWT provider settings
type OAuthConfig struct {
	Providers       []OAuthProvider `yaml:"providers"`
	VerifySignature *bool           `yaml:"verify_signature"` // Verify JWT signatures via JWKS; defaults to true
}

// ShouldVerifySignature returns whether JWT signature verification is enabled.
// Defaults to true if not explicitly set.
func (o *OAuthConfig) ShouldVerifySignature() bool {
	if o.VerifySignature == nil {
		return true
	}
	return *o.VerifySignature
}

// OAuthProvider configures a single OAuth/JWT provider
type OAuthProvider struct {
	Name             string             `yaml:"name"`     // e.g., "azure_ad", "google"
	Issuer           string             `yaml:"issuer"`   // Token issuer URL
	JWKSURL          string             `yaml:"jwks_url"` // JWKS endpoint for key validation
	Audience         string             `yaml:"audience"` // Expected audience claim
	ClaimsMapping    OAuthClaimsMapping `yaml:"claims_mapping"`
	DefaultPrincipal string             `yaml:"default_principal"` // Default principal type if not in claims
	DefaultWorkspace string             `yaml:"default_workspace"` // Default workspace if not in claims
}

// OAuthClaimsMapping maps JWT claims to Aether identity fields
type OAuthClaimsMapping struct {
	PrincipalType string `yaml:"principal_type"` // Claim name for principal type
	Workspace     string `yaml:"workspace"`      // Claim name for workspace
	Identity      string `yaml:"identity"`       // Claim name for identity string (e.g., "sub" or "preferred_username")
}

// PostgresConfig contains PostgreSQL connection settings
type PostgresConfig struct {
	Host               string `yaml:"host"`
	Port               int    `yaml:"port"`
	Database           string `yaml:"database"`
	User               string `yaml:"user"`
	Password           string `yaml:"password"`
	MaxConnections     int    `yaml:"max_connections"`
	MaxIdleConnections int    `yaml:"max_idle_connections"`
	SSLMode            string `yaml:"ssl_mode"`
}

// DSN returns the PostgreSQL connection string
func (p *PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.Database, p.SSLMode,
	)
}

// RedisConfig contains Redis cluster settings
type RedisConfig struct {
	Cluster  []string        `yaml:"cluster"`
	Password string          `yaml:"password"`
	DB       int             `yaml:"db"`
	Pool     RedisPoolConfig `yaml:"pool"`
	Mode     string          `yaml:"mode"` // "auto", "single", "cluster" — default "auto"
}

// GetMode returns the configured Redis mode, defaulting to "auto" if not set.
// In "auto" mode, single-node is used for 1 address and cluster for 2+.
func (r *RedisConfig) GetMode() string {
	if r.Mode == "" {
		return "auto"
	}
	return r.Mode
}

// RedisPoolConfig contains Redis connection pool tuning
type RedisPoolConfig struct {
	MinConnections      int    `yaml:"min_connections"`
	MaxConnections      int    `yaml:"max_connections"`
	HealthCheckInterval string `yaml:"health_check_interval"`
	IdleTimeout         string `yaml:"idle_timeout"`
}

// GetHealthCheckInterval parses the health check interval duration
func (r *RedisPoolConfig) GetHealthCheckInterval() time.Duration {
	return parseDurationOrDefault(r.HealthCheckInterval, 30*time.Second)
}

// GetIdleTimeout parses the idle timeout duration
func (r *RedisPoolConfig) GetIdleTimeout() time.Duration {
	return parseDurationOrDefault(r.IdleTimeout, 5*time.Minute)
}

// Addresses returns the Redis cluster addresses
func (r *RedisConfig) Addresses() []string {
	return r.Cluster
}

// NewClient creates a Redis client that respects the Mode setting.
//   - "single": always creates a standalone client (uses first address)
//   - "cluster": always creates a cluster client (discovers topology from seed addresses)
//   - "auto" (default): single if 1 address, cluster if 2+
func (r *RedisConfig) NewClient() redis.UniversalClient {
	mode := r.GetMode()
	addrs := r.Cluster

	switch mode {
	case "cluster":
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:           addrs,
			Password:        r.Password,
			PoolSize:        r.Pool.MaxConnections,
			MinIdleConns:    r.Pool.MinConnections,
			ConnMaxIdleTime: r.Pool.GetIdleTimeout(),
		})
	case "single":
		addr := addrs[0]
		return redis.NewClient(&redis.Options{
			Addr:            addr,
			Password:        r.Password,
			DB:              r.DB,
			PoolSize:        r.Pool.MaxConnections,
			MinIdleConns:    r.Pool.MinConnections,
			ConnMaxIdleTime: r.Pool.GetIdleTimeout(),
		})
	default: // "auto"
		return redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:           addrs,
			Password:        r.Password,
			DB:              r.DB,
			PoolSize:        r.Pool.MaxConnections,
			MinIdleConns:    r.Pool.MinConnections,
			ConnMaxIdleTime: r.Pool.GetIdleTimeout(),
		})
	}
}

// RabbitMQConfig contains RabbitMQ connection settings
type RabbitMQConfig struct {
	StreamURL           string `yaml:"stream_url"`
	AMQPURL             string `yaml:"amqp_url"`
	StreamCapacityBytes int64  `yaml:"stream_capacity_bytes"`
}

// GetStreamCapacityBytes returns the configured stream capacity in bytes.
// Defaults to 1GB (1_000_000_000) if not set or set to zero.
func (r *RabbitMQConfig) GetStreamCapacityBytes() int64 {
	if r.StreamCapacityBytes <= 0 {
		return 1_000_000_000
	}
	return r.StreamCapacityBytes
}

// AuditConfig contains audit logging settings.
// Environment variables (AUDIT_*) override these values.
type AuditConfig struct {
	Enabled       bool     `yaml:"enabled"`
	EventTypes    []string `yaml:"event_types"`
	Verbosity     string   `yaml:"verbosity"`
	BatchSize     int      `yaml:"batch_size"`
	FlushPeriod   string   `yaml:"flush_period"`
	RetentionDays int      `yaml:"retention_days"`
	ChannelBuffer int      `yaml:"channel_buffer"`
}

// GetFlushPeriod parses the flush period duration
func (a *AuditConfig) GetFlushPeriod() time.Duration {
	return parseDurationOrDefault(a.FlushPeriod, 5*time.Second)
}

// CleanupConfig contains cleanup job settings
type CleanupConfig struct {
	// TaskPurgeInterval is how often to run task purge (e.g., "24h").
	// Set to "0" to disable automatic purge.
	TaskPurgeInterval string `yaml:"task_purge_interval"`

	// CompletedTaskRetention is how long to keep completed tasks (e.g., "168h" = 7 days).
	CompletedTaskRetention string `yaml:"completed_task_retention"`

	// FailedTaskRetention is how long to keep failed tasks (e.g., "336h" = 14 days).
	FailedTaskRetention string `yaml:"failed_task_retention"`

	// CancelledTaskRetention is how long to keep cancelled tasks (e.g., "168h" = 7 days).
	CancelledTaskRetention string `yaml:"cancelled_task_retention"`

	// ReconciliationInterval is how often to run orphaned task reconciliation (e.g., "1m").
	// Set to "0" to disable automatic reconciliation.
	ReconciliationInterval string `yaml:"reconciliation_interval"`
}

// GetTaskPurgeInterval parses the task purge interval duration
func (c *CleanupConfig) GetTaskPurgeInterval() time.Duration {
	return parseDurationOrDefault(c.TaskPurgeInterval, 24*time.Hour)
}

// GetCompletedTaskRetention parses the completed task retention duration
func (c *CleanupConfig) GetCompletedTaskRetention() time.Duration {
	return parseDurationOrDefault(c.CompletedTaskRetention, 7*24*time.Hour)
}

// GetFailedTaskRetention parses the failed task retention duration
func (c *CleanupConfig) GetFailedTaskRetention() time.Duration {
	return parseDurationOrDefault(c.FailedTaskRetention, 14*24*time.Hour)
}

// GetCancelledTaskRetention parses the cancelled task retention duration
func (c *CleanupConfig) GetCancelledTaskRetention() time.Duration {
	return parseDurationOrDefault(c.CancelledTaskRetention, 7*24*time.Hour)
}

// GetReconciliationInterval parses the reconciliation interval duration
func (c *CleanupConfig) GetReconciliationInterval() time.Duration {
	return parseDurationOrDefault(c.ReconciliationInterval, 1*time.Minute)
}

// KVConfig contains KV store settings
type KVConfig struct {
	// DefaultTTL is the default TTL applied to KV keys when no TTL is specified.
	// Set to "0" for no default expiration. Examples: "24h", "168h" (7 days)
	DefaultTTL string `yaml:"default_ttl"`
	// MaxTTL is the maximum allowed TTL for any KV key. 0 = unlimited.
	MaxTTL string `yaml:"max_ttl"`
}

// GetDefaultTTL parses the default KV TTL duration.
// Returns 0 (no expiration) if not configured or invalid.
func (k *KVConfig) GetDefaultTTL() time.Duration {
	return parseDurationOrDefault(k.DefaultTTL, 0)
}

// GetMaxTTL parses the maximum KV TTL duration.
// Returns 0 (unlimited) if not configured or invalid.
func (k *KVConfig) GetMaxTTL() time.Duration {
	return parseDurationOrDefault(k.MaxTTL, 0)
}

// CheckpointConfig contains checkpoint settings
type CheckpointConfig struct {
	// DefaultTTL is the default TTL for checkpoints when client sends -1.
	// Set to "0" for no expiration. Examples: "3600s", "1h", "24h"
	DefaultTTL string `yaml:"default_ttl"`
}

// GetDefaultTTL parses the default checkpoint TTL duration.
// Returns 0 (no expiration) if not configured or invalid.
func (c *CheckpointConfig) GetDefaultTTL() time.Duration {
	return parseDurationOrDefault(c.DefaultTTL, 0)
}

// ShutdownConfig contains graceful shutdown settings
type ShutdownConfig struct {
	// GracefulTimeout is the maximum time to wait for graceful shutdown (e.g., "30s", "1m").
	// Set to "0" for immediate shutdown without grace period.
	GracefulTimeout string `yaml:"graceful_timeout"`
}

// GetGracefulTimeout parses the graceful shutdown timeout duration.
// Returns 30 seconds as default if not configured or invalid.
func (s *ShutdownConfig) GetGracefulTimeout() time.Duration {
	return parseDurationOrDefault(s.GracefulTimeout, 30*time.Second)
}

// QuotasConfig contains multi-tenant quota settings
type QuotasConfig struct {
	Enabled                    bool    `yaml:"enabled"`
	MaxConnectionsPerWorkspace int     `yaml:"max_connections_per_workspace"` // default: 1000
	MaxMessageRatePerIdentity  float64 `yaml:"max_message_rate_per_identity"` // default: 100/s
	MaxKVKeysPerNamespace      int     `yaml:"max_kv_keys_per_namespace"`     // default: 10000
	MaxKVValueSize             int     `yaml:"max_kv_value_size"`             // default: 1048576 (1MB)
	MaxTaskPayloadSize         int     `yaml:"max_task_payload_size"`         // default: 524288 (512KB)

	// Proxy / tunnel quotas (T4).
	Proxy ProxyQuotaConfig `yaml:"proxy"`
}

// ProxyQuotaConfig caps proxy/tunnel routing throughput per workspace.
// Zero values mean "use the gateway default".
type ProxyQuotaConfig struct {
	MaxConcurrentTunnelsPerWorkspace int   `yaml:"max_concurrent_tunnels_per_workspace"` // default: 256
	MaxRequestBodyBytes              int   `yaml:"max_request_body_bytes"`               // default: 8 MiB
	MaxTunnelBytes                   int64 `yaml:"max_tunnel_bytes"`                     // default: 0 (unlimited)

	// MaxChainDepth caps proxy/tunnel hop count along a chain (agent →
	// sandbox → agent → ...). The gateway rejects any ProxyHttpRequest or
	// TunnelOpen whose proxy_chain_depth >= this value, preventing routing
	// loops between sidecar terminators and relays. Each gateway hop
	// increments the field by 1 before forwarding. Default: 8.
	MaxChainDepth uint32 `yaml:"max_chain_depth"`

	// LocalBypassEnabled gates the single-node data-plane fast path: when the
	// caller and target sidecar are connected to the same gateway instance,
	// data-plane envelopes (TunnelData, TunnelAck, ProxyHttpBodyChunk) are
	// delivered directly between gRPC streams instead of round-tripping
	// through RabbitMQ. Control-plane envelopes (TunnelOpen, TunnelClose,
	// ProxyHttpRequest, ProxyHttpResponse, ProxyError) ALWAYS take the RMQ
	// path so audit fires unchanged. Defaults to true; set false (or set
	// AETHER_PROXY_LOCAL_BYPASS_DISABLED=1) to roll back in an incident.
	LocalBypassEnabled *bool `yaml:"local_bypass_enabled"`
}

// GetMaxChainDepth returns the configured proxy/tunnel chain-depth cap,
// defaulting to 8 when unset or zero.
func (p *ProxyQuotaConfig) GetMaxChainDepth() uint32 {
	if p.MaxChainDepth == 0 {
		return 8
	}
	return p.MaxChainDepth
}

// IsLocalBypassEnabled reports whether the proxy data-plane local bypass is
// enabled. Defaults to true when unset; the env override
// AETHER_PROXY_LOCAL_BYPASS_DISABLED=1 forces it off (emergency rollback).
func (p *ProxyQuotaConfig) IsLocalBypassEnabled() bool {
	if v := os.Getenv("AETHER_PROXY_LOCAL_BYPASS_DISABLED"); v == "1" || strings.EqualFold(v, "true") {
		return false
	}
	if p.LocalBypassEnabled == nil {
		return true
	}
	return *p.LocalBypassEnabled
}

// parseDurationOrDefault parses a duration string, returning the default if empty or invalid.
// When a non-empty string fails to parse, a warning is written to stderr because this function
// runs during config loading before the structured logger is initialized.
func parseDurationOrDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: invalid duration %q, using default %v: %v\n", s, def, err)
		return def
	}
	return d
}

// Validate checks the configuration for invalid or conflicting values.
// It collects all validation failures and returns them as a single combined error.
func (c *Config) Validate() error {
	var errs []string

	// --- Port ranges ---
	if c.Gateway.Port != 0 && (c.Gateway.Port < 1 || c.Gateway.Port > 65535) {
		errs = append(errs, fmt.Sprintf("gateway.port %d is out of range (1-65535)", c.Gateway.Port))
	}
	if c.Admin.Enabled && c.Admin.Port != 0 && (c.Admin.Port < 1 || c.Admin.Port > 65535) {
		errs = append(errs, fmt.Sprintf("admin.port %d is out of range (1-65535)", c.Admin.Port))
	}

	// --- PostgreSQL: if host is set, port and dbname must also be set ---
	// In lite mode, PostgreSQL is not used so these checks are skipped.
	if !c.IsLiteMode() && c.Postgres.Host != "" {
		if c.Postgres.Port == 0 {
			errs = append(errs, "postgres.port must be set when postgres.host is configured")
		}
		if c.Postgres.Database == "" {
			errs = append(errs, "postgres.database must be set when postgres.host is configured")
		}
	}

	// --- RabbitMQ stream URL ---
	// In lite mode, RabbitMQ is not used so URL format checks are skipped.
	if !c.IsLiteMode() {
		if c.RabbitMQ.StreamURL != "" {
			if !strings.HasPrefix(c.RabbitMQ.StreamURL, "rabbitmq-stream://") {
				errs = append(errs, fmt.Sprintf("rabbitmq.stream_url must start with rabbitmq-stream://, got: %q", c.RabbitMQ.StreamURL))
			}
		}

		// --- AMQP URL ---
		if c.RabbitMQ.AMQPURL != "" {
			if !strings.HasPrefix(c.RabbitMQ.AMQPURL, "amqp://") && !strings.HasPrefix(c.RabbitMQ.AMQPURL, "amqps://") {
				errs = append(errs, fmt.Sprintf("rabbitmq.amqp_url must start with amqp:// or amqps://, got: %q", c.RabbitMQ.AMQPURL))
			}
		}
	}

	// --- Admin TLS: if either cert or key is set, both must be set ---
	if (c.Admin.TLSCertFile != "") != (c.Admin.TLSKeyFile != "") {
		errs = append(errs, "admin TLS requires both tls_cert_file and tls_key_file to be set")
	}

	// --- Gateway TLS: if cert or key is set, both must be set ---
	if (c.Gateway.TLS.CertFile != "") != (c.Gateway.TLS.KeyFile != "") {
		errs = append(errs, "gateway TLS requires both tls.cert_file and tls.key_file to be set")
	}
	// --- Gateway TLS: client_auth must be a valid value ---
	if ca := c.Gateway.TLS.ClientAuth; ca != "" && ca != "require" && ca != "request" && ca != "none" {
		errs = append(errs, fmt.Sprintf("gateway tls.client_auth must be 'require', 'request', or 'none', got: %q", ca))
	}

	// --- Admin API key length ---
	if c.Admin.APIKey != "" && len(c.Admin.APIKey) < 16 {
		errs = append(errs, "admin.api_key must be at least 16 characters")
	}

	// --- OAuth providers: issuer must be non-empty ---
	if c.Auth.IsAuthModeEnabled("oauth") {
		for i, p := range c.Auth.OAuth.Providers {
			if p.Issuer == "" {
				errs = append(errs, fmt.Sprintf("auth.oauth.providers[%d] (%s): issuer must be non-empty", i, p.Name))
			}
		}
	}

	// --- verify_signature: false is only allowed in dev mode ---
	if !c.Auth.OAuth.ShouldVerifySignature() && os.Getenv("AETHER_DEV_MODE") != "true" {
		errs = append(errs, "verify_signature:false is not allowed outside dev mode")
	}

	// --- HMAC key validation ---
	// In lite mode, task tokens are not used so the HMAC key is not required.
	if !c.IsLiteMode() {
		if c.Auth.TokenHMACKey == "" && os.Getenv("AETHER_DEV_MODE") != "true" {
			errs = append(errs, "auth.token_hmac_key is required in production (set AETHER_DEV_MODE=true to bypass)")
		}
		// API-key auth with no HMAC key would store bare SHA-256 token hashes,
		// reducing token compromise to a hash-cracking exercise. Fail closed in
		// production; only allow the bare-SHA-256 fallback in dev mode.
		if c.Auth.IsAuthModeEnabled("api_key") && c.Auth.TokenHMACKey == "" {
			if os.Getenv("AETHER_DEV_MODE") == "true" {
				log.Warn().Msg("auth.token_hmac_key not set while api_key auth is enabled; token hashes use bare SHA-256 (dev mode)")
			} else {
				errs = append(errs, "auth.token_hmac_key is required when api_key auth is enabled (set AETHER_DEV_MODE=true to bypass for development)")
			}
		}
		if c.Auth.TokenHMACKey != "" && len(c.Auth.TokenHMACKey) < 32 {
			errs = append(errs, "auth.token_hmac_key should be at least 32 bytes for adequate security")
		}
	}

	// --- Admin CORS wildcard ---
	if c.Admin.CORSOrigin == "*" && os.Getenv("AETHER_ALLOW_DEV_MODE") != "true" && os.Getenv("AETHER_DEV_MODE") != "true" {
		errs = append(errs, "admin.cors_origin='*' is not allowed outside dev mode; set AETHER_ALLOW_DEV_MODE=true to override")
	}

	// --- Message rate limit ---
	if c.Gateway.MessageRateLimit < 0 {
		errs = append(errs, fmt.Sprintf("gateway.message_rate_limit must be > 0 if set, got: %g", c.Gateway.MessageRateLimit))
	}

	// --- ACL strict mode: requires PostgreSQL ---
	if c.ACL.Required && c.Postgres.Host == "" {
		errs = append(errs, "acl.required is true but postgres.host is not configured; ACL service requires a database")
	}

	// --- Checkpoint default TTL ---
	if ttl := c.Checkpoint.GetDefaultTTL(); ttl < 0 {
		errs = append(errs, fmt.Sprintf("checkpoint.default_ttl must be >= 0, got: %v", ttl))
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(errs, "\n  - "))
}

// Load reads and parses the configuration from the specified file path
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Apply environment variable overrides
	cfg.ApplyEnvOverrides()

	return &cfg, nil
}

// ApplyEnvOverrides applies environment variable overrides to the configuration.
func (c *Config) ApplyEnvOverrides() {
	// Gateway overrides
	if v := os.Getenv("PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Gateway.Port = port
		}
	}
	if v := os.Getenv("AETHER_GATEWAY_ID"); v != "" {
		c.Gateway.GatewayID = v
	}

	// Admin overrides
	if v := os.Getenv("AETHER_ADMIN_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Admin.Port = port
		}
	}
	if v := os.Getenv("AETHER_ADMIN_ENABLED"); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			c.Admin.Enabled = enabled
		}
	}
	if v := os.Getenv("AETHER_ADMIN_API_KEY"); v != "" {
		c.Admin.APIKey = v
	}
	if v := os.Getenv("AETHER_ADMIN_TLS_CERT_FILE"); v != "" {
		c.Admin.TLSCertFile = v
	}
	if v := os.Getenv("AETHER_ADMIN_TLS_KEY_FILE"); v != "" {
		c.Admin.TLSKeyFile = v
	}

	// Postgres overrides
	if v := os.Getenv("POSTGRES_HOST"); v != "" {
		c.Postgres.Host = v
	}
	if v := os.Getenv("POSTGRES_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Postgres.Port = port
		}
	}
	if v := os.Getenv("POSTGRES_USER"); v != "" {
		c.Postgres.User = v
	}
	if v := os.Getenv("POSTGRES_PASSWORD"); v != "" {
		c.Postgres.Password = v
	}
	if v := os.Getenv("POSTGRES_DATABASE"); v != "" {
		c.Postgres.Database = v
	}

	// Redis overrides
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		c.Redis.Cluster = []string{v}
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		c.Redis.Password = v
	}

	// RabbitMQ overrides
	if v := os.Getenv("STREAM_URL"); v != "" {
		c.RabbitMQ.StreamURL = v
	}
	if v := os.Getenv("AMQP_URL"); v != "" {
		c.RabbitMQ.AMQPURL = v
	}

	if v := os.Getenv("AETHER_TLS_CERT_FILE"); v != "" {
		c.Gateway.TLS.CertFile = v
	}
	if v := os.Getenv("AETHER_TLS_KEY_FILE"); v != "" {
		c.Gateway.TLS.KeyFile = v
	}
	if v := os.Getenv("AETHER_TLS_CA_FILE"); v != "" {
		c.Gateway.TLS.CAFile = v
	}
	if v := os.Getenv("AETHER_TLS_CLIENT_AUTH"); v != "" {
		c.Gateway.TLS.ClientAuth = v
	}

	// ACL overrides
	if v := os.Getenv("AETHER_ACL_REQUIRED"); v != "" {
		if required, err := strconv.ParseBool(v); err == nil {
			c.ACL.Required = required
		}
	}

	// Auth overrides
	if v := os.Getenv("AETHER_AUTH_MODES"); v != "" {
		c.Auth.Modes = strings.Split(v, ",")
	}
	if v := os.Getenv("AETHER_TOKEN_HMAC_KEY"); v != "" {
		c.Auth.TokenHMACKey = v
	}

	// Log level override
	if v := os.Getenv("AETHER_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}

	// Audit overrides (AETHER_AUDIT_* env vars take precedence over YAML)
	if v := os.Getenv("AETHER_AUDIT_ENABLED"); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			c.Audit.Enabled = enabled
		}
	}
	if v := os.Getenv("AETHER_AUDIT_BATCH_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil && size > 0 {
			c.Audit.BatchSize = size
		}
	}
	if v := os.Getenv("AETHER_AUDIT_FLUSH_PERIOD"); v != "" {
		c.Audit.FlushPeriod = v
	}
	if v := os.Getenv("AETHER_AUDIT_RETENTION_DAYS"); v != "" {
		if days, err := strconv.Atoi(v); err == nil && days > 0 {
			c.Audit.RetentionDays = days
		}
	}
	if v := os.Getenv("AETHER_AUDIT_CHANNEL_BUFFER"); v != "" {
		if size, err := strconv.Atoi(v); err == nil && size > 0 {
			c.Audit.ChannelBuffer = size
		}
	}
	if v := os.Getenv("AETHER_AUDIT_VERBOSITY_LEVEL"); v != "" {
		c.Audit.Verbosity = v
	}
	if v := os.Getenv("AETHER_AUDIT_EVENT_TYPES"); v != "" {
		c.Audit.EventTypes = strings.Split(v, ",")
	}
}

// LoadDefault loads configuration from the default path ./configs/dev.yaml
func LoadDefault() (*Config, error) {
	return Load("configs/dev.yaml")
}
