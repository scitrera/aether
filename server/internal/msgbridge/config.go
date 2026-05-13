package msgbridge

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the messaging bridge server.
type Config struct {
	Mode      string          `yaml:"mode"` // "postgres" (default) or "sqlite"
	SQLite    SQLiteConfig    `yaml:"sqlite"`
	Aether    AetherConfig    `yaml:"aether"`
	Postgres  PostgresConfig  `yaml:"postgres"`
	Platforms PlatformsConfig `yaml:"platforms"`
	Admin     AdminConfig     `yaml:"admin"`
	Logging   LoggingConfig   `yaml:"logging"`
}

// IsLite reports whether the server is running in SQLite (lite) mode.
func (c *Config) IsLite() bool {
	return c.Mode == "sqlite"
}

// SQLiteConfig holds SQLite database settings.
type SQLiteConfig struct {
	Path string `yaml:"path"`
}

// AetherConfig holds connection settings for the Aether gateway.
type AetherConfig struct {
	Address        string            `yaml:"address"`
	Implementation string            `yaml:"implementation"`
	Specifier      string            `yaml:"specifier"`
	TLS            TLSConfig         `yaml:"tls"`
	Credentials    CredentialsConfig `yaml:"credentials"`
}

// TLSConfig holds TLS certificate paths.
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
}

// CredentialsConfig holds API key credentials.
type CredentialsConfig struct {
	APIKey string `yaml:"api_key"`
}

// PostgresConfig holds PostgreSQL connection settings.
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

// DSN returns the PostgreSQL connection string.
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

// PlatformsConfig holds configuration for each supported external messaging platform.
type PlatformsConfig struct {
	Discord DiscordConfig `yaml:"discord"`
	Teams   TeamsConfig   `yaml:"teams"`
	Email   EmailConfig   `yaml:"email"`
}

// DiscordConfig holds Discord bot settings.
type DiscordConfig struct {
	Enabled       bool   `yaml:"enabled"`
	BotToken      string `yaml:"bot_token"`
	ApplicationID string `yaml:"application_id"`
}

// TeamsConfig holds Microsoft Teams bot settings.
type TeamsConfig struct {
	Enabled     bool   `yaml:"enabled"`
	AppID       string `yaml:"app_id"`
	AppPassword string `yaml:"app_password"`
	TenantID    string `yaml:"tenant_id"`
	WebhookPort int    `yaml:"webhook_port"`
}

// GetWebhookPort returns the configured webhook port, defaulting to 8081.
func (t *TeamsConfig) GetWebhookPort() int {
	if t.WebhookPort <= 0 {
		return 8081
	}
	return t.WebhookPort
}

// EmailConfig holds email settings for outbound (SMTP) delivery.
// Inbound configuration will be added in a future release.
type EmailConfig struct {
	Enabled bool       `yaml:"enabled"`
	SMTP    SMTPConfig `yaml:"smtp"`
}

// SMTPConfig holds SMTP server settings.
type SMTPConfig struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	FromAddress string `yaml:"from_address"`
	TLS         bool   `yaml:"tls"`
}

// AdminConfig holds settings for the admin HTTP API.
type AdminConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	APIKey  string `yaml:"api_key"`
}

// GetPort returns the configured admin port, defaulting to 31882.
func (a *AdminConfig) GetPort() int {
	if a.Port <= 0 {
		return 31882
	}
	return a.Port
}

// LoggingConfig holds zerolog logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// LoadConfig reads and parses a YAML config file, then applies env var overrides.
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
	if v := os.Getenv("AETHER_ADDRESS"); v != "" {
		c.Aether.Address = v
	}
	if v := os.Getenv("AETHER_IMPLEMENTATION"); v != "" {
		c.Aether.Implementation = v
	}
	if v := os.Getenv("AETHER_SPECIFIER"); v != "" {
		c.Aether.Specifier = v
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
	if v := os.Getenv("DISCORD_BOT_TOKEN"); v != "" {
		c.Platforms.Discord.BotToken = v
	}
	if v := os.Getenv("DISCORD_APPLICATION_ID"); v != "" {
		c.Platforms.Discord.ApplicationID = v
	}
	if v := os.Getenv("TEAMS_APP_ID"); v != "" {
		c.Platforms.Teams.AppID = v
	}
	if v := os.Getenv("TEAMS_APP_PASSWORD"); v != "" {
		c.Platforms.Teams.AppPassword = v
	}
	if v := os.Getenv("TEAMS_TENANT_ID"); v != "" {
		c.Platforms.Teams.TenantID = v
	}
	if v := os.Getenv("TEAMS_WEBHOOK_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Platforms.Teams.WebhookPort = port
		}
	}
	if v := os.Getenv("SMTP_HOST"); v != "" {
		c.Platforms.Email.SMTP.Host = v
	}
	if v := os.Getenv("SMTP_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Platforms.Email.SMTP.Port = port
		}
	}
	if v := os.Getenv("SMTP_USERNAME"); v != "" {
		c.Platforms.Email.SMTP.Username = v
	}
	if v := os.Getenv("SMTP_PASSWORD"); v != "" {
		c.Platforms.Email.SMTP.Password = v
	}
	if v := os.Getenv("SMTP_FROM_ADDRESS"); v != "" {
		c.Platforms.Email.SMTP.FromAddress = v
	}
	if v := os.Getenv("AETHER_LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("MSGBRIDGE_ADMIN_ENABLED"); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			c.Admin.Enabled = enabled
		}
	}
	if v := os.Getenv("MSGBRIDGE_ADMIN_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Admin.Port = port
		}
	}
	if v := os.Getenv("MSGBRIDGE_ADMIN_API_KEY"); v != "" {
		c.Admin.APIKey = v
	}
}

// Validate checks that required configuration fields are set.
func (c *Config) Validate() error {
	var errs []string
	if c.Aether.Address == "" {
		errs = append(errs, "aether.address is required")
	}
	if c.Aether.Implementation == "" {
		errs = append(errs, "aether.implementation is required")
	}
	if c.Aether.Specifier == "" {
		errs = append(errs, "aether.specifier is required")
	}
	if !c.IsLite() {
		if c.Postgres.Host == "" {
			errs = append(errs, "postgres.host is required")
		}
		if c.Postgres.Port == 0 {
			errs = append(errs, "postgres.port is required")
		}
		if c.Postgres.Database == "" {
			errs = append(errs, "postgres.database is required")
		}
	} else {
		if c.SQLite.Path == "" {
			errs = append(errs, "sqlite.path is required in lite mode")
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(errs, "\n  - "))
}
