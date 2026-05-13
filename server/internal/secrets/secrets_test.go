package secrets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/aether/internal/config"
)

func TestGenerateAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "secrets.yaml")

	cfg := &config.Config{}
	gs, err := EnsureSecrets(cfg, outPath, false)
	if err != nil {
		t.Fatalf("EnsureSecrets: %v", err)
	}

	if gs.Auth.TokenHMACKey == "" {
		t.Fatal("expected non-empty HMAC key")
	}
	if gs.Admin.APIKey == "" {
		t.Fatal("expected non-empty admin API key")
	}

	// Config should now have the values
	if cfg.Auth.TokenHMACKey != gs.Auth.TokenHMACKey {
		t.Errorf("config HMAC key = %q, want %q", cfg.Auth.TokenHMACKey, gs.Auth.TokenHMACKey)
	}
	if cfg.Admin.APIKey != gs.Admin.APIKey {
		t.Errorf("config admin key = %q, want %q", cfg.Admin.APIKey, gs.Admin.APIKey)
	}

	// File should exist with restrictive permissions
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat secrets file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("file permissions = %o, want 0600", perm)
	}

	// Load should return same values
	loaded, err := LoadGeneratedSecrets(outPath)
	if err != nil {
		t.Fatalf("LoadGeneratedSecrets: %v", err)
	}
	if loaded.Auth.TokenHMACKey != gs.Auth.TokenHMACKey {
		t.Errorf("loaded HMAC key mismatch")
	}
	if loaded.Admin.APIKey != gs.Admin.APIKey {
		t.Errorf("loaded admin key mismatch")
	}
}

func TestIdempotency(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "secrets.yaml")

	cfg := &config.Config{}
	gs1, err := EnsureSecrets(cfg, outPath, false)
	if err != nil {
		t.Fatalf("first EnsureSecrets: %v", err)
	}

	// Second call with fresh config should load existing file, not regenerate
	cfg2 := &config.Config{}
	gs2, err := EnsureSecrets(cfg2, outPath, false)
	if err != nil {
		t.Fatalf("second EnsureSecrets: %v", err)
	}

	if gs2.Auth.TokenHMACKey != gs1.Auth.TokenHMACKey {
		t.Error("HMAC key changed on second run")
	}
	if gs2.Admin.APIKey != gs1.Admin.APIKey {
		t.Error("admin key changed on second run")
	}
}

func TestForceRegenerate(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "secrets.yaml")

	cfg := &config.Config{}
	gs1, err := EnsureSecrets(cfg, outPath, false)
	if err != nil {
		t.Fatalf("first EnsureSecrets: %v", err)
	}

	cfg2 := &config.Config{}
	gs2, err := EnsureSecrets(cfg2, outPath, true)
	if err != nil {
		t.Fatalf("force EnsureSecrets: %v", err)
	}

	if gs2.Auth.TokenHMACKey == gs1.Auth.TokenHMACKey {
		t.Error("expected HMAC key to change with --force")
	}
	if gs2.Admin.APIKey == gs1.Admin.APIKey {
		t.Error("expected admin key to change with --force")
	}
}

func TestApplyToConfigDoesNotOverride(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.TokenHMACKey = "existing-hmac"
	cfg.Admin.APIKey = "existing-admin-key"

	gs := &GeneratedSecrets{
		Auth:  AuthSecrets{TokenHMACKey: "generated-hmac"},
		Admin: AdminSecrets{APIKey: "generated-admin-key"},
	}

	ApplyToConfig(cfg, gs)

	if cfg.Auth.TokenHMACKey != "existing-hmac" {
		t.Errorf("HMAC key overwritten: got %q", cfg.Auth.TokenHMACKey)
	}
	if cfg.Admin.APIKey != "existing-admin-key" {
		t.Errorf("admin key overwritten: got %q", cfg.Admin.APIKey)
	}
}

func TestConfigPrecedenceOverSecretsFile(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "secrets.yaml")

	// Pre-populate config with values from env/config file
	cfg := &config.Config{}
	cfg.Auth.TokenHMACKey = "from-config-file"
	cfg.Admin.APIKey = "from-env-variable-admin"

	gs, err := EnsureSecrets(cfg, outPath, false)
	if err != nil {
		t.Fatalf("EnsureSecrets: %v", err)
	}

	// Should not have generated — config values take precedence
	if gs.Auth.TokenHMACKey != "from-config-file" {
		t.Errorf("expected config value, got %q", gs.Auth.TokenHMACKey)
	}
	if gs.Admin.APIKey != "from-env-variable-admin" {
		t.Errorf("expected env value, got %q", gs.Admin.APIKey)
	}

	// File should not be created when nothing needed generating
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Error("secrets file should not exist when config already has all values")
	}
}

func TestLoadNonexistentFile(t *testing.T) {
	_, err := LoadGeneratedSecrets("/nonexistent/path/secrets.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}
