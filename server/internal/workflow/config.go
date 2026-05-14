package workflow

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

// ServerMode controls which backend dependencies are required.
type ServerMode string

const (
	// ModeStandard is the default mode using PostgreSQL + Redis.
	ModeStandard ServerMode = "standard"
	// ModeLite is the single-node mode using SQLite only (no Redis required).
	ModeLite ServerMode = "lite"
)

type Config struct {
	Mode     ServerMode     `yaml:"mode"`
	Aether   AetherConfig   `yaml:"aether"`
	Postgres PostgresConfig `yaml:"postgres"`
	SQLite   SQLiteConfig   `yaml:"sqlite"`
	Redis    RedisConfig    `yaml:"redis"`
	Workflow WorkflowConfig `yaml:"workflow"`
	Admin    AdminConfig    `yaml:"admin"`
	Logging  LoggingConfig  `yaml:"logging"`
}

type AdminConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	APIKey  string `yaml:"api_key"`
}

func (a *AdminConfig) GetPort() int {
	if a.Port <= 0 {
		return 31881
	}
	return a.Port
}

type AetherConfig struct {
	Address        string            `yaml:"address"`
	Implementation string            `yaml:"implementation"`
	Workspace      string            `yaml:"workspace"`
	TLS            TLSConfig         `yaml:"tls"`
	Credentials    CredentialsConfig `yaml:"credentials"`

	// InProcessConn, when non-nil, takes precedence over Address/TLS — the
	// workflow engine constructs its aether client from this pre-dialed
	// *grpc.ClientConn instead of dialing. Not yaml-serializable; set
	// programmatically by callers that embed the workflow engine in the
	// same process as the gateway (AetherLite). The conn typically points
	// at an in-process bufconn-backed gRPC server.
	InProcessConn *grpc.ClientConn `yaml:"-"`
}

type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
}

type CredentialsConfig struct {
	APIKey string `yaml:"api_key"`
}

type SQLiteConfig struct {
	Path string `yaml:"path"`
}

// DSN returns the SQLite data source name. Defaults to "workflow.db" if not set.
func (s *SQLiteConfig) DSN() string {
	if s.Path == "" {
		return "workflow.db"
	}
	return s.Path
}

type PostgresConfig struct {
	Host               string `yaml:"host"`
	Port               int    `yaml:"port"`
	Database           string `yaml:"database"`
	User               string `yaml:"user"`
	Password           string `yaml:"password"`
	SSLMode            string `yaml:"ssl_mode"`
	MaxConnections     int    `yaml:"max_connections"`
	MaxIdleConnections int    `yaml:"max_idle_connections"`
}

func (p *PostgresConfig) DSN() string {
	sslMode := p.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.Database, sslMode,
	)
}

type RedisConfig struct {
	Cluster  []string `yaml:"cluster"`
	Password string   `yaml:"password"`
}

type WorkflowConfig struct {
	RuleCacheTTL            string `yaml:"rule_cache_ttl"`
	RuleCacheSize           int    `yaml:"rule_cache_size"`
	SchedulerPollInterval   string `yaml:"scheduler_poll_interval"`
	DAGMonitorInterval      string `yaml:"dag_monitor_interval"`
	StepDefaultTimeout      string `yaml:"step_default_timeout"`
	DAGDefaultTimeout       string `yaml:"dag_default_timeout"`
	MaxConcurrentExecutions int    `yaml:"max_concurrent_executions"`
}

func (w *WorkflowConfig) GetRuleCacheTTL() time.Duration {
	return parseDuration(w.RuleCacheTTL, 2*time.Minute)
}

func (w *WorkflowConfig) GetRuleCacheSize() int {
	if w.RuleCacheSize <= 0 {
		return 2048
	}
	return w.RuleCacheSize
}

func (w *WorkflowConfig) GetSchedulerPollInterval() time.Duration {
	return parseDuration(w.SchedulerPollInterval, 1*time.Second)
}

func (w *WorkflowConfig) GetDAGMonitorInterval() time.Duration {
	return parseDuration(w.DAGMonitorInterval, 5*time.Second)
}

func (w *WorkflowConfig) GetStepDefaultTimeout() time.Duration {
	return parseDuration(w.StepDefaultTimeout, 5*time.Minute)
}

func (w *WorkflowConfig) GetDAGDefaultTimeout() time.Duration {
	return parseDuration(w.DAGDefaultTimeout, 1*time.Hour)
}

func (w *WorkflowConfig) GetMaxConcurrentExecutions() int {
	if w.MaxConcurrentExecutions <= 0 {
		return 100
	}
	return w.MaxConcurrentExecutions
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg.applyEnvOverrides()
	return &cfg, nil
}

func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("WORKFLOW_MODE"); v != "" {
		c.Mode = ServerMode(v)
	}
	if v := os.Getenv("SQLITE_PATH"); v != "" {
		c.SQLite.Path = v
	}
	if v := os.Getenv("AETHER_ADDRESS"); v != "" {
		c.Aether.Address = v
	}
	if v := os.Getenv("AETHER_WORKSPACE"); v != "" {
		c.Aether.Workspace = v
	}
	if v := os.Getenv("AETHER_API_KEY"); v != "" {
		c.Aether.Credentials.APIKey = v
	}
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
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		c.Redis.Cluster = []string{v}
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		c.Redis.Password = v
	}
	if v := os.Getenv("AETHER_LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("WORKFLOW_ADMIN_ENABLED"); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			c.Admin.Enabled = enabled
		}
	}
	if v := os.Getenv("WORKFLOW_ADMIN_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Admin.Port = port
		}
	}
	if v := os.Getenv("WORKFLOW_ADMIN_API_KEY"); v != "" {
		c.Admin.APIKey = v
	}
}

func (c *Config) Validate() error {
	var errs []string
	if c.Aether.Address == "" && c.Aether.InProcessConn == nil {
		errs = append(errs, "aether.address is required (or set aether.InProcessConn for embedded callers)")
	}
	if c.Mode != ModeLite {
		if c.Postgres.Host == "" {
			errs = append(errs, "postgres.host is required")
		}
		if c.Postgres.Port == 0 {
			errs = append(errs, "postgres.port is required")
		}
		if c.Postgres.Database == "" {
			errs = append(errs, "postgres.database is required")
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(errs, "\n  - "))
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
