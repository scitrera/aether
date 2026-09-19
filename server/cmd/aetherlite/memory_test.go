package main

import (
	"math"
	"runtime/debug"
	"testing"
)

func TestConfigureMemoryLimit(t *testing.T) {
	// These settings are process-wide; do not run in parallel.
	previous := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })
	for _, tc := range []struct {
		name    string
		env     string
		initial int64
		want    int64
	}{
		{"default", "", math.MaxInt64, 1 << 30},
		{"lower override", "256MiB", 256 << 20, 256 << 20},
		{"higher override", "2GiB", 2 << 30, 2 << 30},
		{"disabled", "off", math.MaxInt64, math.MaxInt64},
		{"zero", "0", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOMEMLIMIT", tc.env)
			// Simulate the runtime's startup parsing; changing the environment
			// alone does not change an already running process's limit.
			debug.SetMemoryLimit(tc.initial)
			if got := configureMemoryLimit(); got != tc.want {
				t.Fatalf("limit = %d, want %d", got, tc.want)
			}
		})
	}
}
