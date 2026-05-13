package workflow

import (
	"os"
	"testing"
	"time"
)

func TestWorkflowConfig_Defaults(t *testing.T) {
	cfg := WorkflowConfig{}

	if got := cfg.GetRuleCacheTTL(); got != 2*time.Minute {
		t.Errorf("GetRuleCacheTTL() = %v, want %v", got, 2*time.Minute)
	}
	if got := cfg.GetRuleCacheSize(); got != 2048 {
		t.Errorf("GetRuleCacheSize() = %d, want 2048", got)
	}
	if got := cfg.GetSchedulerPollInterval(); got != 1*time.Second {
		t.Errorf("GetSchedulerPollInterval() = %v, want %v", got, 1*time.Second)
	}
	if got := cfg.GetDAGMonitorInterval(); got != 5*time.Second {
		t.Errorf("GetDAGMonitorInterval() = %v, want %v", got, 5*time.Second)
	}
	if got := cfg.GetStepDefaultTimeout(); got != 5*time.Minute {
		t.Errorf("GetStepDefaultTimeout() = %v, want %v", got, 5*time.Minute)
	}
	if got := cfg.GetDAGDefaultTimeout(); got != 1*time.Hour {
		t.Errorf("GetDAGDefaultTimeout() = %v, want %v", got, 1*time.Hour)
	}
	if got := cfg.GetMaxConcurrentExecutions(); got != 100 {
		t.Errorf("GetMaxConcurrentExecutions() = %d, want 100", got)
	}
}

func TestWorkflowConfig_CustomValues(t *testing.T) {
	cfg := WorkflowConfig{
		RuleCacheTTL:            "5m",
		RuleCacheSize:           512,
		SchedulerPollInterval:   "500ms",
		DAGMonitorInterval:      "10s",
		StepDefaultTimeout:      "10m",
		DAGDefaultTimeout:       "2h",
		MaxConcurrentExecutions: 50,
	}

	if got := cfg.GetRuleCacheTTL(); got != 5*time.Minute {
		t.Errorf("GetRuleCacheTTL() = %v, want %v", got, 5*time.Minute)
	}
	if got := cfg.GetRuleCacheSize(); got != 512 {
		t.Errorf("GetRuleCacheSize() = %d, want 512", got)
	}
	if got := cfg.GetSchedulerPollInterval(); got != 500*time.Millisecond {
		t.Errorf("GetSchedulerPollInterval() = %v, want %v", got, 500*time.Millisecond)
	}
	if got := cfg.GetMaxConcurrentExecutions(); got != 50 {
		t.Errorf("GetMaxConcurrentExecutions() = %d, want 50", got)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				Aether:   AetherConfig{Address: "localhost:50051"},
				Postgres: PostgresConfig{Host: "localhost", Port: 5432, Database: "aether"},
			},
			wantErr: false,
		},
		{
			name: "missing aether address",
			cfg: Config{
				Postgres: PostgresConfig{Host: "localhost", Port: 5432, Database: "aether"},
			},
			wantErr: true,
		},
		{
			name: "missing postgres host",
			cfg: Config{
				Aether: AetherConfig{Address: "localhost:50051"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_EnvOverrides(t *testing.T) {
	cfg := Config{}

	os.Setenv("AETHER_ADDRESS", "remote:9090")
	os.Setenv("POSTGRES_HOST", "db.example.com")
	defer os.Unsetenv("AETHER_ADDRESS")
	defer os.Unsetenv("POSTGRES_HOST")

	cfg.applyEnvOverrides()

	if cfg.Aether.Address != "remote:9090" {
		t.Errorf("Aether.Address = %q, want %q", cfg.Aether.Address, "remote:9090")
	}
	if cfg.Postgres.Host != "db.example.com" {
		t.Errorf("Postgres.Host = %q, want %q", cfg.Postgres.Host, "db.example.com")
	}
}

func TestPostgresConfig_DSN(t *testing.T) {
	cfg := PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "pass",
		Database: "testdb",
		SSLMode:  "disable",
	}
	dsn := cfg.DSN()
	want := "host=localhost port=5432 user=user password=pass dbname=testdb sslmode=disable"
	if dsn != want {
		t.Errorf("DSN() = %q, want %q", dsn, want)
	}
}
