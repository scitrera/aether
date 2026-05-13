package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPostgresConfig_DSN(t *testing.T) {
	tests := []struct {
		name   string
		config PostgresConfig
		want   string
	}{
		{
			name: "basic DSN",
			config: PostgresConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "aether",
				Password: "secret",
				Database: "aether_db",
				SSLMode:  "disable",
			},
			want: "host=localhost port=5432 user=aether password=secret dbname=aether_db sslmode=disable",
		},
		{
			name: "remote host with SSL",
			config: PostgresConfig{
				Host:     "db.example.com",
				Port:     5433,
				User:     "admin",
				Password: "p@ssw0rd!",
				Database: "production",
				SSLMode:  "require",
			},
			want: "host=db.example.com port=5433 user=admin password=p@ssw0rd! dbname=production sslmode=require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.DSN()
			if got != tt.want {
				t.Errorf("DSN() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedisConfig_Addresses(t *testing.T) {
	tests := []struct {
		name   string
		config RedisConfig
		want   []string
	}{
		{
			name: "single address",
			config: RedisConfig{
				Cluster: []string{"localhost:6379"},
			},
			want: []string{"localhost:6379"},
		},
		{
			name: "multiple addresses",
			config: RedisConfig{
				Cluster: []string{"redis1:6379", "redis2:6379", "redis3:6379"},
			},
			want: []string{"redis1:6379", "redis2:6379", "redis3:6379"},
		},
		{
			name: "empty cluster",
			config: RedisConfig{
				Cluster: []string{},
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.Addresses()
			if len(got) != len(tt.want) {
				t.Errorf("Addresses() returned %d items, want %d", len(got), len(tt.want))
				return
			}
			for i, addr := range got {
				if addr != tt.want[i] {
					t.Errorf("Addresses()[%d] = %q, want %q", i, addr, tt.want[i])
				}
			}
		})
	}
}

func TestCleanupConfig_GetTaskPurgeInterval(t *testing.T) {
	tests := []struct {
		name   string
		config CleanupConfig
		want   time.Duration
	}{
		{
			name:   "explicit 12h",
			config: CleanupConfig{TaskPurgeInterval: "12h"},
			want:   12 * time.Hour,
		},
		{
			name:   "explicit 48h",
			config: CleanupConfig{TaskPurgeInterval: "48h"},
			want:   48 * time.Hour,
		},
		{
			name:   "empty defaults to 24h",
			config: CleanupConfig{TaskPurgeInterval: ""},
			want:   24 * time.Hour,
		},
		{
			name:   "invalid defaults to 24h",
			config: CleanupConfig{TaskPurgeInterval: "invalid"},
			want:   24 * time.Hour,
		},
		{
			name:   "zero disables",
			config: CleanupConfig{TaskPurgeInterval: "0"},
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetTaskPurgeInterval()
			if got != tt.want {
				t.Errorf("GetTaskPurgeInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCleanupConfig_GetCompletedTaskRetention(t *testing.T) {
	tests := []struct {
		name   string
		config CleanupConfig
		want   time.Duration
	}{
		{
			name:   "explicit 24h",
			config: CleanupConfig{CompletedTaskRetention: "24h"},
			want:   24 * time.Hour,
		},
		{
			name:   "explicit 168h (7 days)",
			config: CleanupConfig{CompletedTaskRetention: "168h"},
			want:   168 * time.Hour,
		},
		{
			name:   "empty defaults to 7 days",
			config: CleanupConfig{CompletedTaskRetention: ""},
			want:   7 * 24 * time.Hour,
		},
		{
			name:   "invalid defaults to 7 days",
			config: CleanupConfig{CompletedTaskRetention: "invalid"},
			want:   7 * 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetCompletedTaskRetention()
			if got != tt.want {
				t.Errorf("GetCompletedTaskRetention() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCleanupConfig_GetFailedTaskRetention(t *testing.T) {
	tests := []struct {
		name   string
		config CleanupConfig
		want   time.Duration
	}{
		{
			name:   "explicit 72h (3 days)",
			config: CleanupConfig{FailedTaskRetention: "72h"},
			want:   72 * time.Hour,
		},
		{
			name:   "empty defaults to 14 days",
			config: CleanupConfig{FailedTaskRetention: ""},
			want:   14 * 24 * time.Hour,
		},
		{
			name:   "invalid defaults to 14 days",
			config: CleanupConfig{FailedTaskRetention: "bad"},
			want:   14 * 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetFailedTaskRetention()
			if got != tt.want {
				t.Errorf("GetFailedTaskRetention() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCleanupConfig_GetCancelledTaskRetention(t *testing.T) {
	tests := []struct {
		name   string
		config CleanupConfig
		want   time.Duration
	}{
		{
			name:   "explicit 48h",
			config: CleanupConfig{CancelledTaskRetention: "48h"},
			want:   48 * time.Hour,
		},
		{
			name:   "empty defaults to 7 days",
			config: CleanupConfig{CancelledTaskRetention: ""},
			want:   7 * 24 * time.Hour,
		},
		{
			name:   "invalid defaults to 7 days",
			config: CleanupConfig{CancelledTaskRetention: "nope"},
			want:   7 * 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetCancelledTaskRetention()
			if got != tt.want {
				t.Errorf("GetCancelledTaskRetention() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCleanupConfig_GetReconciliationInterval(t *testing.T) {
	tests := []struct {
		name   string
		config CleanupConfig
		want   time.Duration
	}{
		{
			name:   "explicit 30s",
			config: CleanupConfig{ReconciliationInterval: "30s"},
			want:   30 * time.Second,
		},
		{
			name:   "explicit 5m",
			config: CleanupConfig{ReconciliationInterval: "5m"},
			want:   5 * time.Minute,
		},
		{
			name:   "empty defaults to 1 minute",
			config: CleanupConfig{ReconciliationInterval: ""},
			want:   1 * time.Minute,
		},
		{
			name:   "invalid defaults to 1 minute",
			config: CleanupConfig{ReconciliationInterval: "xyz"},
			want:   1 * time.Minute,
		},
		{
			name:   "zero disables",
			config: CleanupConfig{ReconciliationInterval: "0"},
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetReconciliationInterval()
			if got != tt.want {
				t.Errorf("GetReconciliationInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test.yaml")

	configContent := `
gateway:
  port: 50051
  gateway_id: test-gateway

admin:
  enabled: true
  port: 31880
  cors_origin: "*"

auth:
  modes: [mtls, task_token]
  mtls:
    required: true
    mode: strict

postgres:
  host: localhost
  port: 5432
  database: testdb
  user: testuser
  password: testpass
  ssl_mode: disable

redis:
  cluster:
    - localhost:6379
    - localhost:6380
  password: redispass
  db: 0

rabbitmq:
  stream_url: rabbitmq-stream://localhost:5552
  amqp_url: amqp://localhost:5672

audit:
  enabled: true
  event_types: [connection, auth]
  verbosity: medium
  batch_size: 50
  flush_period: "10s"
  retention_days: 30
  channel_buffer: 500

cleanup:
  task_purge_interval: 12h
  completed_task_retention: 48h
  failed_task_retention: 72h
  cancelled_task_retention: 24h
  reconciliation_interval: 2m

log_level: debug
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify gateway config
	if cfg.Gateway.Port != 50051 {
		t.Errorf("Gateway.Port = %d, want 50051", cfg.Gateway.Port)
	}
	if cfg.Gateway.GatewayID != "test-gateway" {
		t.Errorf("Gateway.GatewayID = %q, want %q", cfg.Gateway.GatewayID, "test-gateway")
	}

	// Verify admin config
	if !cfg.Admin.Enabled {
		t.Errorf("Admin.Enabled = false, want true")
	}
	if cfg.Admin.Port != 31880 {
		t.Errorf("Admin.Port = %d, want 31880", cfg.Admin.Port)
	}

	// Verify mTLS config (now under auth)
	if !cfg.Auth.MTLS.Required {
		t.Errorf("Auth.MTLS.Required = false, want true")
	}
	if cfg.Auth.MTLS.Mode != "strict" {
		t.Errorf("Auth.MTLS.Mode = %q, want %q", cfg.Auth.MTLS.Mode, "strict")
	}

	// Verify postgres config
	if cfg.Postgres.Host != "localhost" {
		t.Errorf("Postgres.Host = %q, want %q", cfg.Postgres.Host, "localhost")
	}
	if cfg.Postgres.Port != 5432 {
		t.Errorf("Postgres.Port = %d, want 5432", cfg.Postgres.Port)
	}

	// Verify redis config
	if len(cfg.Redis.Cluster) != 2 {
		t.Errorf("Redis.Cluster length = %d, want 2", len(cfg.Redis.Cluster))
	}

	// Verify audit config
	if !cfg.Audit.Enabled {
		t.Errorf("Audit.Enabled = false, want true")
	}
	if cfg.Audit.BatchSize != 50 {
		t.Errorf("Audit.BatchSize = %d, want 50", cfg.Audit.BatchSize)
	}
	if cfg.Audit.Verbosity != "medium" {
		t.Errorf("Audit.Verbosity = %q, want %q", cfg.Audit.Verbosity, "medium")
	}
	if cfg.Audit.RetentionDays != 30 {
		t.Errorf("Audit.RetentionDays = %d, want 30", cfg.Audit.RetentionDays)
	}

	// Verify cleanup config
	if cfg.Cleanup.GetTaskPurgeInterval() != 12*time.Hour {
		t.Errorf("Cleanup.GetTaskPurgeInterval() = %v, want 12h", cfg.Cleanup.GetTaskPurgeInterval())
	}
	if cfg.Cleanup.GetCompletedTaskRetention() != 48*time.Hour {
		t.Errorf("Cleanup.GetCompletedTaskRetention() = %v, want 48h", cfg.Cleanup.GetCompletedTaskRetention())
	}

	// Verify log level
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Errorf("Load() with nonexistent file should return error")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")

	// Write invalid YAML
	invalidYAML := `
gateway:
  port: [invalid array here
  not: valid: yaml: here
`
	if err := os.WriteFile(configPath, []byte(invalidYAML), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Errorf("Load() with invalid YAML should return error")
	}
}

func TestConfig_applyEnvOverrides(t *testing.T) {
	// Save original env values
	origEnvs := map[string]string{
		"PORT":              os.Getenv("PORT"),
		"AETHER_GATEWAY_ID": os.Getenv("AETHER_GATEWAY_ID"),
		"AETHER_ADMIN_PORT": os.Getenv("AETHER_ADMIN_PORT"),
		"POSTGRES_HOST":     os.Getenv("POSTGRES_HOST"),
		"POSTGRES_PORT":     os.Getenv("POSTGRES_PORT"),
		"REDIS_ADDR":        os.Getenv("REDIS_ADDR"),
		"STREAM_URL":        os.Getenv("STREAM_URL"),
		"AETHER_LOG_LEVEL":  os.Getenv("AETHER_LOG_LEVEL"),
	}

	// Cleanup after test
	defer func() {
		for k, v := range origEnvs {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	}()

	// Set test env vars
	os.Setenv("PORT", "9999")
	os.Setenv("AETHER_GATEWAY_ID", "env-gateway")
	os.Setenv("AETHER_ADMIN_PORT", "9090")
	os.Setenv("POSTGRES_HOST", "env-postgres")
	os.Setenv("POSTGRES_PORT", "5555")
	os.Setenv("REDIS_ADDR", "env-redis:6379")
	os.Setenv("STREAM_URL", "rabbitmq-stream://env-rmq:5552")
	os.Setenv("AETHER_LOG_LEVEL", "trace")

	// Create minimal config
	cfg := &Config{}
	cfg.ApplyEnvOverrides()

	// Verify overrides
	if cfg.Gateway.Port != 9999 {
		t.Errorf("Gateway.Port = %d, want 9999", cfg.Gateway.Port)
	}
	if cfg.Gateway.GatewayID != "env-gateway" {
		t.Errorf("Gateway.GatewayID = %q, want %q", cfg.Gateway.GatewayID, "env-gateway")
	}
	if cfg.Admin.Port != 9090 {
		t.Errorf("Admin.Port = %d, want 9090", cfg.Admin.Port)
	}
	if cfg.Postgres.Host != "env-postgres" {
		t.Errorf("Postgres.Host = %q, want %q", cfg.Postgres.Host, "env-postgres")
	}
	if cfg.Postgres.Port != 5555 {
		t.Errorf("Postgres.Port = %d, want 5555", cfg.Postgres.Port)
	}
	if len(cfg.Redis.Cluster) != 1 || cfg.Redis.Cluster[0] != "env-redis:6379" {
		t.Errorf("Redis.Cluster = %v, want [env-redis:6379]", cfg.Redis.Cluster)
	}
	if cfg.RabbitMQ.StreamURL != "rabbitmq-stream://env-rmq:5552" {
		t.Errorf("RabbitMQ.StreamURL = %q, want %q", cfg.RabbitMQ.StreamURL, "rabbitmq-stream://env-rmq:5552")
	}
	if cfg.LogLevel != "trace" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "trace")
	}
}

func TestConfig_applyEnvOverrides_InvalidPort(t *testing.T) {
	origPort := os.Getenv("PORT")
	defer func() {
		if origPort == "" {
			os.Unsetenv("PORT")
		} else {
			os.Setenv("PORT", origPort)
		}
	}()

	// Set invalid port
	os.Setenv("PORT", "not-a-number")

	cfg := &Config{
		Gateway: GatewayConfig{Port: 50051},
	}
	cfg.ApplyEnvOverrides()

	// Should keep original value when env var is invalid
	if cfg.Gateway.Port != 50051 {
		t.Errorf("Gateway.Port = %d, want 50051 (should keep original on invalid env)", cfg.Gateway.Port)
	}
}

func TestCheckpointConfig_GetDefaultTTL(t *testing.T) {
	tests := []struct {
		name   string
		config CheckpointConfig
		want   time.Duration
	}{
		{
			name:   "explicit 3600s (1 hour)",
			config: CheckpointConfig{DefaultTTL: "3600s"},
			want:   3600 * time.Second,
		},
		{
			name:   "explicit 1h",
			config: CheckpointConfig{DefaultTTL: "1h"},
			want:   time.Hour,
		},
		{
			name:   "explicit 24h",
			config: CheckpointConfig{DefaultTTL: "24h"},
			want:   24 * time.Hour,
		},
		{
			name:   "empty defaults to 0 (no expiration)",
			config: CheckpointConfig{DefaultTTL: ""},
			want:   0,
		},
		{
			name:   "invalid defaults to 0 (no expiration)",
			config: CheckpointConfig{DefaultTTL: "invalid"},
			want:   0,
		},
		{
			name:   "explicit 0 means no expiration",
			config: CheckpointConfig{DefaultTTL: "0"},
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetDefaultTTL()
			if got != tt.want {
				t.Errorf("GetDefaultTTL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	// Helper to build a minimal valid config
	validConfig := func() *Config {
		return &Config{
			Gateway: GatewayConfig{Port: 50051},
			Admin:   AdminConfig{Enabled: false},
			Redis:   RedisConfig{Cluster: []string{"localhost:6379"}},
			Auth:    AuthConfig{TokenHMACKey: "00000000000000000000000000000000"}, // 32-char key for tests
		}
	}

	t.Run("valid minimal config passes", func(t *testing.T) {
		cfg := validConfig()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error: %v", err)
		}
	})

	t.Run("zero port is allowed (not configured)", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.Port = 0
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for port 0: %v", err)
		}
	})

	t.Run("invalid gateway port below range", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.Port = -1
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for port -1, got nil")
		}
	})

	t.Run("invalid gateway port above range", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.Port = 99999
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for port 99999, got nil")
		}
	})

	t.Run("invalid admin port when admin enabled", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.Enabled = true
		cfg.Admin.Port = 70000
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for admin port 70000, got nil")
		}
	})

	t.Run("admin port out of range ignored when admin disabled", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.Enabled = false
		cfg.Admin.Port = 99999
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error when admin disabled: %v", err)
		}
	})

	t.Run("postgres host set but missing port", func(t *testing.T) {
		cfg := validConfig()
		cfg.Postgres.Host = "db.example.com"
		cfg.Postgres.Database = "mydb"
		cfg.Postgres.Port = 0
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for missing postgres port, got nil")
		}
	})

	t.Run("postgres host set but missing database", func(t *testing.T) {
		cfg := validConfig()
		cfg.Postgres.Host = "db.example.com"
		cfg.Postgres.Port = 5432
		cfg.Postgres.Database = ""
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for missing postgres database, got nil")
		}
	})

	t.Run("valid postgres config passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Postgres.Host = "db.example.com"
		cfg.Postgres.Port = 5432
		cfg.Postgres.Database = "aether"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for valid postgres config: %v", err)
		}
	})

	t.Run("invalid rabbitmq stream URL prefix", func(t *testing.T) {
		cfg := validConfig()
		cfg.RabbitMQ.StreamURL = "amqp://guest:guest@localhost:5552"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for bad stream URL prefix, got nil")
		}
	})

	t.Run("valid rabbitmq stream URL passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.RabbitMQ.StreamURL = "rabbitmq-stream://guest:guest@localhost:5552"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for valid stream URL: %v", err)
		}
	})

	t.Run("invalid AMQP URL prefix", func(t *testing.T) {
		cfg := validConfig()
		cfg.RabbitMQ.AMQPURL = "rabbitmq://guest:guest@localhost:5672"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for bad AMQP URL prefix, got nil")
		}
	})

	t.Run("valid amqp:// URL passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.RabbitMQ.AMQPURL = "amqp://guest:guest@localhost:5672/"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for valid amqp URL: %v", err)
		}
	})

	t.Run("valid amqps:// URL passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.RabbitMQ.AMQPURL = "amqps://user:pass@secure-rmq:5671/"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for valid amqps URL: %v", err)
		}
	})

	t.Run("TLS cert set without key fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.TLSCertFile = "/etc/certs/server.crt"
		cfg.Admin.TLSKeyFile = ""
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for TLS cert without key, got nil")
		}
	})

	t.Run("TLS key set without cert fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.TLSCertFile = ""
		cfg.Admin.TLSKeyFile = "/etc/certs/server.key"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for TLS key without cert, got nil")
		}
	})

	t.Run("TLS cert and key both set passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.TLSCertFile = "/etc/certs/server.crt"
		cfg.Admin.TLSKeyFile = "/etc/certs/server.key"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for valid TLS config: %v", err)
		}
	})

	t.Run("short admin API key fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.APIKey = "tooshort"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for short admin API key, got nil")
		}
	})

	t.Run("admin API key exactly 16 chars passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.APIKey = "1234567890123456"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for 16-char API key: %v", err)
		}
	})

	t.Run("empty admin API key passes (unauthenticated dev mode)", func(t *testing.T) {
		cfg := validConfig()
		cfg.Admin.APIKey = ""
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for empty API key: %v", err)
		}
	})

	t.Run("OAuth provider with empty issuer fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Auth.Modes = []string{"oauth"}
		cfg.Auth.OAuth.Providers = []OAuthProvider{
			{Name: "my-provider", Issuer: ""},
		}
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for OAuth provider with empty issuer, got nil")
		}
	})

	t.Run("OAuth provider with valid issuer passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Auth.Modes = []string{"oauth"}
		cfg.Auth.OAuth.Providers = []OAuthProvider{
			{Name: "azure", Issuer: "https://login.microsoftonline.com/tenant-id"},
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for valid OAuth provider: %v", err)
		}
	})

	t.Run("negative message rate limit fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.MessageRateLimit = -5
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for negative message rate limit, got nil")
		}
	})

	t.Run("zero message rate limit passes (disabled)", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.MessageRateLimit = 0
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for zero rate limit: %v", err)
		}
	})

	t.Run("multiple errors collected", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.Port = 99999
		cfg.Admin.APIKey = "short"
		cfg.RabbitMQ.AMQPURL = "http://wrong"
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() expected errors, got nil")
		}
		errStr := err.Error()
		// Should mention all three issues
		if !containsAll(errStr, "gateway.port", "admin.api_key", "rabbitmq.amqp_url") {
			t.Errorf("Validate() error should mention all failures, got: %v", err)
		}
	})
}

func TestGatewayTLSConfig_YAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "tls.yaml")

	configContent := `
gateway:
  port: 50051
  tls:
    cert_file: /etc/certs/server.crt
    key_file: /etc/certs/server.key
    ca_file: /etc/certs/ca.crt
    client_auth: request

auth:
  token_hmac_key: "00000000000000000000000000000000"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Gateway.TLS.CertFile != "/etc/certs/server.crt" {
		t.Errorf("TLS.CertFile = %q, want /etc/certs/server.crt", cfg.Gateway.TLS.CertFile)
	}
	if cfg.Gateway.TLS.KeyFile != "/etc/certs/server.key" {
		t.Errorf("TLS.KeyFile = %q, want /etc/certs/server.key", cfg.Gateway.TLS.KeyFile)
	}
	if cfg.Gateway.TLS.CAFile != "/etc/certs/ca.crt" {
		t.Errorf("TLS.CAFile = %q, want /etc/certs/ca.crt", cfg.Gateway.TLS.CAFile)
	}
	if cfg.Gateway.TLS.ClientAuth != "request" {
		t.Errorf("TLS.ClientAuth = %q, want request", cfg.Gateway.TLS.ClientAuth)
	}
}

func TestGatewayTLSConfig_EnvOverrides(t *testing.T) {
	origEnvs := map[string]string{
		"AETHER_TLS_CERT_FILE":   os.Getenv("AETHER_TLS_CERT_FILE"),
		"AETHER_TLS_KEY_FILE":    os.Getenv("AETHER_TLS_KEY_FILE"),
		"AETHER_TLS_CA_FILE":     os.Getenv("AETHER_TLS_CA_FILE"),
		"AETHER_TLS_CLIENT_AUTH": os.Getenv("AETHER_TLS_CLIENT_AUTH"),
	}
	defer func() {
		for k, v := range origEnvs {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	}()

	os.Setenv("AETHER_TLS_CERT_FILE", "/env/server.crt")
	os.Setenv("AETHER_TLS_KEY_FILE", "/env/server.key")
	os.Setenv("AETHER_TLS_CA_FILE", "/env/ca.crt")
	os.Setenv("AETHER_TLS_CLIENT_AUTH", "none")

	cfg := &Config{}
	cfg.ApplyEnvOverrides()

	if cfg.Gateway.TLS.CertFile != "/env/server.crt" {
		t.Errorf("TLS.CertFile = %q, want /env/server.crt", cfg.Gateway.TLS.CertFile)
	}
	if cfg.Gateway.TLS.KeyFile != "/env/server.key" {
		t.Errorf("TLS.KeyFile = %q, want /env/server.key", cfg.Gateway.TLS.KeyFile)
	}
	if cfg.Gateway.TLS.CAFile != "/env/ca.crt" {
		t.Errorf("TLS.CAFile = %q, want /env/ca.crt", cfg.Gateway.TLS.CAFile)
	}
	if cfg.Gateway.TLS.ClientAuth != "none" {
		t.Errorf("TLS.ClientAuth = %q, want none", cfg.Gateway.TLS.ClientAuth)
	}
}

func TestConfig_Validate_GatewayTLS(t *testing.T) {
	validConfig := func() *Config {
		return &Config{
			Gateway: GatewayConfig{Port: 50051},
			Auth:    AuthConfig{TokenHMACKey: "00000000000000000000000000000000"},
		}
	}

	t.Run("gateway TLS cert without key fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.CertFile = "/etc/certs/server.crt"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for TLS cert without key, got nil")
		}
	})

	t.Run("gateway TLS key without cert fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.KeyFile = "/etc/certs/server.key"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for TLS key without cert, got nil")
		}
	})

	t.Run("gateway TLS cert and key both set passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.CertFile = "/etc/certs/server.crt"
		cfg.Gateway.TLS.KeyFile = "/etc/certs/server.key"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for valid TLS config: %v", err)
		}
	})

	t.Run("gateway TLS invalid client_auth fails", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.CertFile = "/etc/certs/server.crt"
		cfg.Gateway.TLS.KeyFile = "/etc/certs/server.key"
		cfg.Gateway.TLS.ClientAuth = "invalid"
		if err := cfg.Validate(); err == nil {
			t.Error("Validate() expected error for invalid client_auth, got nil")
		}
	})

	t.Run("gateway TLS client_auth require passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.CertFile = "/etc/certs/server.crt"
		cfg.Gateway.TLS.KeyFile = "/etc/certs/server.key"
		cfg.Gateway.TLS.ClientAuth = "require"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for client_auth=require: %v", err)
		}
	})

	t.Run("gateway TLS client_auth request passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.CertFile = "/etc/certs/server.crt"
		cfg.Gateway.TLS.KeyFile = "/etc/certs/server.key"
		cfg.Gateway.TLS.ClientAuth = "request"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for client_auth=request: %v", err)
		}
	})

	t.Run("gateway TLS client_auth none passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.CertFile = "/etc/certs/server.crt"
		cfg.Gateway.TLS.KeyFile = "/etc/certs/server.key"
		cfg.Gateway.TLS.ClientAuth = "none"
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for client_auth=none: %v", err)
		}
	})

	t.Run("gateway TLS empty client_auth passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Gateway.TLS.CertFile = "/etc/certs/server.crt"
		cfg.Gateway.TLS.KeyFile = "/etc/certs/server.key"
		cfg.Gateway.TLS.ClientAuth = ""
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for empty client_auth: %v", err)
		}
	})
}

// containsAll returns true if s contains all of the given substrings.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestIsAuthModeEnabled(t *testing.T) {
	tests := []struct {
		name  string
		modes []string
		check string
		want  bool
	}{
		{
			name:  "empty modes defaults mtls",
			modes: nil,
			check: "mtls",
			want:  true,
		},
		{
			name:  "empty modes defaults task_token",
			modes: nil,
			check: "task_token",
			want:  true,
		},
		{
			name:  "empty modes rejects api_key",
			modes: nil,
			check: "api_key",
			want:  false,
		},
		{
			name:  "explicit api_key enabled",
			modes: []string{"mtls", "api_key"},
			check: "api_key",
			want:  true,
		},
		{
			name:  "explicit list excludes task_token",
			modes: []string{"mtls", "api_key"},
			check: "task_token",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth := &AuthConfig{Modes: tt.modes}
			got := auth.IsAuthModeEnabled(tt.check)
			if got != tt.want {
				t.Errorf("IsAuthModeEnabled(%q) = %v, want %v", tt.check, got, tt.want)
			}
		})
	}
}
