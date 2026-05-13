// Package testutil provides test utilities that connect to dev infrastructure.
// Configuration is read from environment variables with defaults matching scripts/setup_dev_infra.sh.
package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	_ "github.com/lib/pq"

	"github.com/scitrera/aether/migrations"
)

// Default configuration matching scripts/setup_dev_infra.sh
const (
	DefaultPostgresHost     = "localhost"
	DefaultPostgresPort     = 55432
	DefaultPostgresDB       = "aether"
	DefaultPostgresUser     = "aether"
	DefaultPostgresPassword = "aether_dev"

	DefaultRedisHost     = "localhost"
	DefaultRedisBasePort = 56379
	DefaultRedisCount    = 3

	DefaultRabbitMQHost       = "localhost"
	DefaultRabbitMQStreamPort = 55552
	DefaultRabbitMQAMQPPort   = 55672
	DefaultRabbitMQMgmtPort   = 61672
	DefaultRabbitMQUser       = "guest"
	DefaultRabbitMQPassword   = "guest"
)

// PostgresConfig holds PostgreSQL connection configuration.
type PostgresConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// DSN returns the PostgreSQL connection string.
func (c PostgresConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.User, c.Password, c.Host, c.Port, c.Database)
}

// GetPostgresConfig returns PostgreSQL configuration from environment variables.
func GetPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Host:     getEnv("POSTGRES_HOST", DefaultPostgresHost),
		Port:     getEnvInt("POSTGRES_PORT", DefaultPostgresPort),
		Database: getEnv("POSTGRES_DB", DefaultPostgresDB),
		User:     getEnv("POSTGRES_USER", DefaultPostgresUser),
		Password: getEnv("POSTGRES_PASSWORD", DefaultPostgresPassword),
	}
}

// RedisConfig holds Redis connection configuration for a single node.
type RedisConfig struct {
	Host string
	Port int
}

// Addr returns the Redis address string.
func (c RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// GetRedisConfigs returns Redis configuration for all nodes from environment variables.
func GetRedisConfigs() []RedisConfig {
	host := getEnv("REDIS_HOST", DefaultRedisHost)
	basePort := getEnvInt("REDIS_BASE_PORT", DefaultRedisBasePort)
	count := getEnvInt("REDIS_COUNT", DefaultRedisCount)

	configs := make([]RedisConfig, count)
	for i := 0; i < count; i++ {
		configs[i] = RedisConfig{
			Host: host,
			Port: basePort + i,
		}
	}
	return configs
}

// GetRedisAddrs returns all Redis addresses as strings.
func GetRedisAddrs() []string {
	configs := GetRedisConfigs()
	addrs := make([]string, len(configs))
	for i, c := range configs {
		addrs[i] = c.Addr()
	}
	return addrs
}

// RabbitMQConfig holds RabbitMQ connection configuration.
type RabbitMQConfig struct {
	Host       string
	StreamPort int
	AMQPPort   int
	MgmtPort   int
	User       string
	Password   string
}

// StreamURL returns the RabbitMQ Streams connection URL.
func (c RabbitMQConfig) StreamURL() string {
	return fmt.Sprintf("rabbitmq-stream://%s:%s@%s:%d",
		c.User, c.Password, c.Host, c.StreamPort)
}

// AMQPURL returns the RabbitMQ AMQP connection URL.
func (c RabbitMQConfig) AMQPURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d",
		c.User, c.Password, c.Host, c.AMQPPort)
}

// GetRabbitMQConfig returns RabbitMQ configuration from environment variables.
func GetRabbitMQConfig() RabbitMQConfig {
	return RabbitMQConfig{
		Host:       getEnv("RABBITMQ_HOST", DefaultRabbitMQHost),
		StreamPort: getEnvInt("RABBITMQ_STREAM_PORT", DefaultRabbitMQStreamPort),
		AMQPPort:   getEnvInt("RABBITMQ_AMQP_PORT", DefaultRabbitMQAMQPPort),
		MgmtPort:   getEnvInt("RABBITMQ_MGMT_PORT", DefaultRabbitMQMgmtPort),
		User:       getEnv("RABBITMQ_USER", DefaultRabbitMQUser),
		Password:   getEnv("RABBITMQ_PASSWORD", DefaultRabbitMQPassword),
	}
}

// TestDB wraps a database connection for testing purposes.
type TestDB struct {
	DB     *sql.DB
	Config PostgresConfig
}

// SetupTestDB connects to the dev PostgreSQL instance and runs migrations.
// It returns a TestDB and a cleanup function that truncates test tables.
func SetupTestDB(t *testing.T) (*TestDB, func()) {
	t.Helper()

	config := GetPostgresConfig()
	ctx := context.Background()

	db, err := sql.Open("postgres", config.DSN())
	if err != nil {
		t.Skipf("Failed to connect to test database (dev infrastructure may not be running): %v", err)
		return nil, func() {}
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("Failed to ping test database (dev infrastructure may not be running): %v", err)
		return nil, func() {}
	}

	// Run migrations to ensure schema is up to date
	if err := migrations.Run(ctx, db); err != nil {
		db.Close()
		t.Fatalf("Failed to run migrations: %v", err)
	}

	testDB := &TestDB{
		DB:     db,
		Config: config,
	}

	// Acquire advisory lock to prevent concurrent test packages from interfering.
	// This serializes DB access across parallel go test packages sharing the same database.
	_, err = db.ExecContext(ctx, "SELECT pg_advisory_lock(42424242)")
	if err != nil {
		db.Close()
		t.Fatalf("Failed to acquire advisory lock: %v", err)
	}

	// Cleanup function releases lock and truncates tables
	cleanup := func() {
		testDB.TruncateTestTables(t)
		// Best-effort advisory-lock release. If the unlock fails (e.g. the
		// session is already closing), the lock is released by Postgres on
		// connection teardown anyway.
		if _, err := db.ExecContext(ctx, "SELECT pg_advisory_unlock(42424242)"); err != nil {
			t.Logf("failed to release advisory lock: %v", err)
		}
	}

	// Truncate tables before tests to ensure clean state
	testDB.TruncateTestTables(t)

	t.Logf("Connected to test database: %s:%d/%s", config.Host, config.Port, config.Database)

	return testDB, cleanup
}

// TruncateTestTables removes all data from test tables.
// This is faster than recreating containers and allows test isolation.
func (tdb *TestDB) TruncateTestTables(t *testing.T) {
	t.Helper()

	// Tables to truncate in order (respecting foreign key constraints)
	// CASCADE handles dependent tables automatically
	tables := []string{
		"orchestrated_task_queue",
		"orchestrator_profiles",
		"agent_registry",
		"task_audit_events",
		"task_checkpoints",
		"task_assignments",
		"task_timers",
		"dlq",
		"tasks",
		"acl_audit_log",
		"acl_rules",
		"acl_fallback_policies",
		"workflow_traces",
		"sessions",
	}

	for _, table := range tables {
		_, err := tdb.DB.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
		if err != nil {
			// Table might not exist yet, which is fine
			t.Logf("Note: Could not truncate %s: %v", table, err)
		}
	}

	// Re-insert default ACL fallback policies (they get truncated)
	_, _ = tdb.DB.Exec(`
		INSERT INTO acl_fallback_policies (rule_category, fallback_access_level, updated_by)
		VALUES
			('user_workspace', 20, '_system'),
			('agent_workspace', 20, '_system'),
			('user_agent', 20, '_system'),
			('agent_agent', 20, '_system'),
			('task_workspace', 20, '_system'),
			('global_read', 20, '_system'),
			('orchestrator_system', 20, '_system')
		ON CONFLICT (rule_category) DO NOTHING
	`)

	// Re-insert default _global workspace ACL rule
	_, _ = tdb.DB.Exec(`
		INSERT INTO acl_rules (principal_type, principal_id, resource_type, resource_id, access_level, granted_by, reason)
		VALUES ('wildcard', '_any_authenticated', 'workspace', '_global', 20, '_system',
				'Default READ_WRITE access for all authenticated principals (development mode)')
		ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING
	`)
}

// Close closes the database connection.
func (tdb *TestDB) Close() error {
	if tdb.DB != nil {
		return tdb.DB.Close()
	}
	return nil
}

// Helper functions

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
