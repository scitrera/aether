package workflow

import (
	"testing"
)

// TestNewServer_rejectsNilStore locks the §14.1 contract: passing a nil
// store to the workflow.NewServer constructor is a class-A configuration
// error and must surface at startup rather than as a runtime nil-deref.
// All other store-injection happy paths are covered cross-backend in
// internal/storage/workflow/conformance_test.go's TestNewServer_NativeInjection.
func TestNewServer_rejectsNilStore(t *testing.T) {
	cfg := &Config{Mode: ModeLite}
	srv, err := NewServer(cfg, nil)
	if err == nil {
		t.Fatal("NewServer(cfg, nil) should return an error")
	}
	if srv != nil {
		t.Fatal("NewServer(cfg, nil) should return nil Server")
	}
}
