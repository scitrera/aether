package audit

import (
	"testing"
	"time"
)

func TestConfig_Validate_Default(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultConfig().Validate() = %v, want nil", err)
	}
}

func TestConfig_Validate_NilConfig(t *testing.T) {
	var cfg *Config
	if err := cfg.Validate(); err == nil {
		t.Error("nil config should fail validation")
	}
}

func TestConfig_Validate_InvalidVerbosity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.VerbosityLevel = "extreme"
	if err := cfg.Validate(); err == nil {
		t.Error("invalid verbosity should fail validation")
	}
}

func TestConfig_Validate_InvalidEventType(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnabledEventTypes = []string{EventTypeConnection, "bogus"}
	if err := cfg.Validate(); err == nil {
		t.Error("invalid event type should fail validation")
	}
}

func TestConfig_Validate_ZeroBatchSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = 0
	if err := cfg.Validate(); err == nil {
		t.Error("zero batch size should fail validation")
	}
}

func TestConfig_Validate_NegativeFlushPeriod(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FlushPeriod = -1
	if err := cfg.Validate(); err == nil {
		t.Error("negative flush period should fail validation")
	}
}

func TestConfig_Validate_ZeroRetention(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetentionDays = 0
	if err := cfg.Validate(); err == nil {
		t.Error("zero retention should fail validation")
	}
}

func TestConfig_Validate_ZeroChannelBuffer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ChannelBuffer = 0
	if err := cfg.Validate(); err == nil {
		t.Error("zero channel buffer should fail validation")
	}
}

func TestConfig_Clone(t *testing.T) {
	original := DefaultConfig()
	clone := original.Clone()

	if clone == original {
		t.Error("clone should be a different pointer")
	}
	if clone.Enabled != original.Enabled {
		t.Errorf("Enabled mismatch")
	}
	if clone.VerbosityLevel != original.VerbosityLevel {
		t.Errorf("VerbosityLevel mismatch")
	}
	if clone.BatchSize != original.BatchSize {
		t.Errorf("BatchSize mismatch")
	}

	// Mutating clone should not affect original
	clone.EnabledEventTypes = append(clone.EnabledEventTypes, "extra")
	if len(original.EnabledEventTypes) == len(clone.EnabledEventTypes) {
		t.Error("clone event types should be independent of original")
	}
}

func TestConfig_Clone_Nil(t *testing.T) {
	var cfg *Config
	if cfg.Clone() != nil {
		t.Error("Clone of nil should return nil")
	}
}

func TestConfig_MergeConfig(t *testing.T) {
	base := DefaultConfig()
	override := &Config{
		Enabled:           false,
		EnabledEventTypes: []string{EventTypeConnection},
		VerbosityLevel:    VerbosityHigh,
		BatchSize:         50,
		FlushPeriod:       10 * time.Second,
		RetentionDays:     30,
		ChannelBuffer:     500,
	}

	base.MergeConfig(override)

	if base.Enabled {
		t.Error("Enabled should be overridden to false")
	}
	if len(base.EnabledEventTypes) != 1 || base.EnabledEventTypes[0] != EventTypeConnection {
		t.Errorf("EnabledEventTypes = %v, want [connection]", base.EnabledEventTypes)
	}
	if base.VerbosityLevel != VerbosityHigh {
		t.Errorf("VerbosityLevel = %q, want high", base.VerbosityLevel)
	}
	if base.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want 50", base.BatchSize)
	}
	if base.FlushPeriod != 10*time.Second {
		t.Errorf("FlushPeriod = %v, want 10s", base.FlushPeriod)
	}
	if base.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", base.RetentionDays)
	}
	if base.ChannelBuffer != 500 {
		t.Errorf("ChannelBuffer = %d, want 500", base.ChannelBuffer)
	}
}

func TestConfig_MergeConfig_Nil(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MergeConfig(nil) // should not panic
	if cfg.VerbosityLevel != VerbosityLow {
		t.Error("merging nil should not change config")
	}
}

func TestConfig_EnableEventType(t *testing.T) {
	cfg := &Config{EnabledEventTypes: []string{EventTypeConnection}}

	if err := cfg.EnableEventType(EventTypeAuth); err != nil {
		t.Fatalf("EnableEventType() = %v", err)
	}
	if len(cfg.EnabledEventTypes) != 2 {
		t.Errorf("expected 2 event types, got %d", len(cfg.EnabledEventTypes))
	}

	// Enabling again should be idempotent
	if err := cfg.EnableEventType(EventTypeAuth); err != nil {
		t.Fatalf("EnableEventType() = %v", err)
	}
	if len(cfg.EnabledEventTypes) != 2 {
		t.Errorf("expected 2 event types (no duplicate), got %d", len(cfg.EnabledEventTypes))
	}
}

func TestConfig_EnableEventType_Invalid(t *testing.T) {
	cfg := &Config{}
	if err := cfg.EnableEventType("bogus"); err == nil {
		t.Error("expected error for invalid event type")
	}
}

func TestConfig_DisableEventType(t *testing.T) {
	cfg := DefaultConfig()
	original := len(cfg.EnabledEventTypes)

	cfg.DisableEventType(EventTypeMessage)

	if len(cfg.EnabledEventTypes) != original-1 {
		t.Errorf("expected %d event types, got %d", original-1, len(cfg.EnabledEventTypes))
	}
	for _, et := range cfg.EnabledEventTypes {
		if et == EventTypeMessage {
			t.Error("message should have been removed")
		}
	}
}

func TestConfig_SetVerbosityLevel(t *testing.T) {
	cfg := DefaultConfig()

	if err := cfg.SetVerbosityLevel(VerbosityHigh); err != nil {
		t.Fatalf("SetVerbosityLevel() = %v", err)
	}
	if cfg.VerbosityLevel != VerbosityHigh {
		t.Errorf("VerbosityLevel = %q, want high", cfg.VerbosityLevel)
	}
}

func TestConfig_SetVerbosityLevel_Invalid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.SetVerbosityLevel("extreme"); err == nil {
		t.Error("expected error for invalid verbosity")
	}
}

func TestConfig_String(t *testing.T) {
	cfg := DefaultConfig()
	s := cfg.String()

	if s == "" {
		t.Error("String() should not be empty")
	}
	if s == "Config(nil)" {
		t.Error("non-nil config should not return Config(nil)")
	}
}

func TestConfig_String_Nil(t *testing.T) {
	var cfg *Config
	if cfg.String() != "Config(nil)" {
		t.Errorf("nil config String() = %q, want Config(nil)", cfg.String())
	}
}

func TestConfig_GetRetentionDuration(t *testing.T) {
	cfg := &Config{RetentionDays: 30}
	want := 30 * 24 * time.Hour

	if got := cfg.GetRetentionDuration(); got != want {
		t.Errorf("GetRetentionDuration() = %v, want %v", got, want)
	}
}

func TestLoadConfigFromEnv_Defaults(t *testing.T) {
	cfg := LoadConfigFromEnv()
	if err := cfg.Validate(); err != nil {
		t.Errorf("default env config should be valid: %v", err)
	}
}

func TestLoadConfigFromEnv_AllEnvVars(t *testing.T) {
	t.Setenv("AETHER_AUDIT_ENABLED", "false")
	t.Setenv("AETHER_AUDIT_EVENT_TYPES", "connection,auth")
	t.Setenv("AETHER_AUDIT_VERBOSITY_LEVEL", "high")
	t.Setenv("AETHER_AUDIT_BATCH_SIZE", "50")
	t.Setenv("AETHER_AUDIT_FLUSH_PERIOD", "10s")
	t.Setenv("AETHER_AUDIT_RETENTION_DAYS", "30")
	t.Setenv("AETHER_AUDIT_CHANNEL_BUFFER", "500")

	cfg := LoadConfigFromEnv()

	if cfg.Enabled {
		t.Error("Enabled should be false")
	}
	if len(cfg.EnabledEventTypes) != 2 {
		t.Errorf("EnabledEventTypes length = %d, want 2", len(cfg.EnabledEventTypes))
	}
	if cfg.VerbosityLevel != VerbosityHigh {
		t.Errorf("VerbosityLevel = %q, want high", cfg.VerbosityLevel)
	}
	if cfg.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want 50", cfg.BatchSize)
	}
	if cfg.FlushPeriod != 10*time.Second {
		t.Errorf("FlushPeriod = %v, want 10s", cfg.FlushPeriod)
	}
	if cfg.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", cfg.RetentionDays)
	}
	if cfg.ChannelBuffer != 500 {
		t.Errorf("ChannelBuffer = %d, want 500", cfg.ChannelBuffer)
	}
}

func TestLoadConfigFromEnv_AllEventTypes(t *testing.T) {
	t.Setenv("AETHER_AUDIT_EVENT_TYPES", "all")

	cfg := LoadConfigFromEnv()

	if len(cfg.EnabledEventTypes) != 8 {
		t.Errorf("'all' should produce 8 event types, got %d", len(cfg.EnabledEventTypes))
	}
}

func TestLoadConfigFromEnv_InvalidEventTypes(t *testing.T) {
	t.Setenv("AETHER_AUDIT_EVENT_TYPES", "bogus,invalid")

	cfg := LoadConfigFromEnv()

	// Should fall back to default since no valid types
	if len(cfg.EnabledEventTypes) != 8 {
		t.Errorf("invalid types should keep default, got %d", len(cfg.EnabledEventTypes))
	}
}

func TestLoadConfigFromEnv_InvalidVerbosity(t *testing.T) {
	t.Setenv("AETHER_AUDIT_VERBOSITY_LEVEL", "extreme")

	cfg := LoadConfigFromEnv()

	// Should keep default
	if cfg.VerbosityLevel != VerbosityLow {
		t.Errorf("invalid verbosity should keep default, got %q", cfg.VerbosityLevel)
	}
}
