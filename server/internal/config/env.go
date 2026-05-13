package config

import (
	"os"
	"strconv"
	"time"
)

// Helpers for reading typed values from environment variables, suitable for
// use as `flag.X(...)` defaults in command-line entry points:
//
//	port = flag.Int("port", config.EnvInt("MY_PORT", 50051), "...")
//
// Precedence at the call site becomes:
//   explicit CLI flag > matching env var > compiled-in default.
//
// Lookups happen at package-init time (when the flag block initializes), so
// later runtime changes to the environment have no effect.

// EnvStr returns the value of the named environment variable, or def if the
// variable is unset or empty.
func EnvStr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// EnvInt returns the int value of the named environment variable, or def if
// the variable is unset, empty, or cannot be parsed by strconv.Atoi.
func EnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// EnvBool returns the bool value of the named environment variable, or def if
// the variable is unset, empty, or cannot be parsed by strconv.ParseBool.
// Accepts the standard truthy/falsy forms ("1"/"0", "t"/"f", "true"/"false",
// case-insensitive).
func EnvBool(name string, def bool) bool {
	if v := os.Getenv(name); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// EnvDuration returns the time.Duration value of the named environment
// variable, or def if the variable is unset, empty, or cannot be parsed by
// time.ParseDuration. Accepts forms like "500ms", "30s", "5m", "2h".
func EnvDuration(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
