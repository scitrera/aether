package audit

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// LoadConfigFromEnv creates an audit configuration from environment variables
// Falls back to DefaultConfig() for any missing values
func LoadConfigFromEnv() *Config {
	cfg := DefaultConfig()

	// AETHER_AUDIT_ENABLED - enable/disable audit logging
	if v := os.Getenv("AETHER_AUDIT_ENABLED"); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			cfg.Enabled = enabled
		}
	}

	// AETHER_AUDIT_EVENT_TYPES - comma-separated list of event types to log
	// Example: "connection,auth,message,task" or "all" for all types
	if v := os.Getenv("AETHER_AUDIT_EVENT_TYPES"); v != "" {
		if strings.ToLower(v) == "all" {
			cfg.EnabledEventTypes = []string{
				EventTypeConnection,
				EventTypeAuth,
				EventTypeMessage,
				EventTypeKV,
				EventTypeTask,
				EventTypeAdmin,
				EventTypeACL,
				EventTypeAuthorization,
			}
		} else {
			types := strings.Split(v, ",")
			validTypes := []string{}
			for _, t := range types {
				trimmed := strings.TrimSpace(t)
				if ValidateEventType(trimmed) == nil {
					validTypes = append(validTypes, trimmed)
				}
			}
			if len(validTypes) > 0 {
				cfg.EnabledEventTypes = validTypes
			}
		}
	}

	// AETHER_AUDIT_VERBOSITY_LEVEL - verbosity level for message logging
	// Valid values: low, medium, high
	if v := os.Getenv("AETHER_AUDIT_VERBOSITY_LEVEL"); v != "" {
		if ValidateVerbosityLevel(v) == nil {
			cfg.VerbosityLevel = v
		}
	}

	// AETHER_AUDIT_BATCH_SIZE - number of events to batch before writing
	if v := os.Getenv("AETHER_AUDIT_BATCH_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil && size > 0 {
			cfg.BatchSize = size
		}
	}

	// AETHER_AUDIT_FLUSH_PERIOD - how often to flush batched events
	// Example: "5s", "10s", "1m"
	if v := os.Getenv("AETHER_AUDIT_FLUSH_PERIOD"); v != "" {
		if period, err := time.ParseDuration(v); err == nil && period > 0 {
			cfg.FlushPeriod = period
		}
	}

	// AETHER_AUDIT_RETENTION_DAYS - how long to retain audit logs
	if v := os.Getenv("AETHER_AUDIT_RETENTION_DAYS"); v != "" {
		if days, err := strconv.Atoi(v); err == nil && days > 0 {
			cfg.RetentionDays = days
		}
	}

	// AETHER_AUDIT_CHANNEL_BUFFER - size of async event channel buffer
	if v := os.Getenv("AETHER_AUDIT_CHANNEL_BUFFER"); v != "" {
		if size, err := strconv.Atoi(v); err == nil && size > 0 {
			cfg.ChannelBuffer = size
		}
	}

	return cfg
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}

	// Validate verbosity level
	if err := ValidateVerbosityLevel(c.VerbosityLevel); err != nil {
		return fmt.Errorf("invalid verbosity level %q: %w", c.VerbosityLevel, err)
	}

	// Validate event types
	for _, eventType := range c.EnabledEventTypes {
		if err := ValidateEventType(eventType); err != nil {
			return fmt.Errorf("invalid event type %q: %w", eventType, err)
		}
	}

	// Validate batch size
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", c.BatchSize)
	}

	// Validate flush period
	if c.FlushPeriod <= 0 {
		return fmt.Errorf("flush period must be positive, got %v", c.FlushPeriod)
	}

	// Validate retention days
	if c.RetentionDays <= 0 {
		return fmt.Errorf("retention days must be positive, got %d", c.RetentionDays)
	}

	// Validate channel buffer
	if c.ChannelBuffer <= 0 {
		return fmt.Errorf("channel buffer must be positive, got %d", c.ChannelBuffer)
	}

	return nil
}

// MergeConfig merges another config into this one, with other taking precedence
// This is useful for layering configurations (e.g., file config + env overrides)
func (c *Config) MergeConfig(other *Config) {
	if other == nil {
		return
	}

	// Merge enabled flag (explicit false in other config disables)
	c.Enabled = other.Enabled

	// Merge event types (if other specifies types, use those)
	if len(other.EnabledEventTypes) > 0 {
		c.EnabledEventTypes = make([]string, len(other.EnabledEventTypes))
		copy(c.EnabledEventTypes, other.EnabledEventTypes)
	}

	// Merge verbosity level (if other specifies, use that)
	if other.VerbosityLevel != "" && other.VerbosityLevel != DefaultVerbosityLevel {
		c.VerbosityLevel = other.VerbosityLevel
	}

	// Merge batch size (if other specifies non-default, use that)
	if other.BatchSize > 0 && other.BatchSize != DefaultBatchSize {
		c.BatchSize = other.BatchSize
	}

	// Merge flush period (if other specifies non-default, use that)
	if other.FlushPeriod > 0 && other.FlushPeriod != DefaultFlushPeriod {
		c.FlushPeriod = other.FlushPeriod
	}

	// Merge retention days (if other specifies non-default, use that)
	if other.RetentionDays > 0 && other.RetentionDays != DefaultRetentionDays {
		c.RetentionDays = other.RetentionDays
	}

	// Merge channel buffer (if other specifies non-default, use that)
	if other.ChannelBuffer > 0 && other.ChannelBuffer != DefaultChannelBuffer {
		c.ChannelBuffer = other.ChannelBuffer
	}
}

// EnableEventType adds an event type to the enabled list if not already present
func (c *Config) EnableEventType(eventType string) error {
	if err := ValidateEventType(eventType); err != nil {
		return err
	}

	// Check if already enabled
	for _, et := range c.EnabledEventTypes {
		if et == eventType {
			return nil // Already enabled
		}
	}

	c.EnabledEventTypes = append(c.EnabledEventTypes, eventType)
	return nil
}

// DisableEventType removes an event type from the enabled list
func (c *Config) DisableEventType(eventType string) {
	newTypes := []string{}
	for _, et := range c.EnabledEventTypes {
		if et != eventType {
			newTypes = append(newTypes, et)
		}
	}
	c.EnabledEventTypes = newTypes
}

// SetVerbosityLevel sets the message verbosity level with validation
func (c *Config) SetVerbosityLevel(level string) error {
	if err := ValidateVerbosityLevel(level); err != nil {
		return err
	}
	c.VerbosityLevel = level
	return nil
}

// Clone creates a deep copy of the configuration
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}

	clone := &Config{
		Enabled:        c.Enabled,
		VerbosityLevel: c.VerbosityLevel,
		BatchSize:      c.BatchSize,
		FlushPeriod:    c.FlushPeriod,
		RetentionDays:  c.RetentionDays,
		ChannelBuffer:  c.ChannelBuffer,
	}

	// Deep copy event types
	if len(c.EnabledEventTypes) > 0 {
		clone.EnabledEventTypes = make([]string, len(c.EnabledEventTypes))
		copy(clone.EnabledEventTypes, c.EnabledEventTypes)
	}

	return clone
}

// String returns a human-readable representation of the configuration
func (c *Config) String() string {
	if c == nil {
		return "Config(nil)"
	}

	enabledTypes := "all"
	if len(c.EnabledEventTypes) > 0 {
		enabledTypes = strings.Join(c.EnabledEventTypes, ", ")
	}

	return fmt.Sprintf(
		"Config(enabled=%v, types=[%s], verbosity=%s, batch=%d, flush=%v, retention=%dd, buffer=%d)",
		c.Enabled,
		enabledTypes,
		c.VerbosityLevel,
		c.BatchSize,
		c.FlushPeriod,
		c.RetentionDays,
		c.ChannelBuffer,
	)
}

// GetRetentionDuration returns the retention period as a time.Duration
func (c *Config) GetRetentionDuration() time.Duration {
	return time.Duration(c.RetentionDays) * 24 * time.Hour
}
