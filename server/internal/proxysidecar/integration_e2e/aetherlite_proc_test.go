//go:build e2e

// TestMain owns the lifecycle of the shared aetherlite subprocess
// used by every e2e test in this package. The actual spawn/build
// helpers live in aetherlite_proc.go (non-_test) so they're visible
// to harness.go.

package integration_e2e

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedAetherlite != nil {
		sharedAetherlite.stop()
		sharedAetherlite = nil
	}
	os.Exit(code)
}
