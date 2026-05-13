package readiness

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/pkg/crypto"
)

// makeConfig returns a minimal config with all fields zero/empty.
func makeConfig() *config.Config {
	return &config.Config{}
}

// --- HasCriticalFailures ---

func TestHasCriticalFailures_NoneWhenEmpty(t *testing.T) {
	c := NewChecker()
	if c.HasCriticalFailures(nil) {
		t.Error("HasCriticalFailures(nil) = true, want false")
	}
}

func TestHasCriticalFailures_FalseWhenAllPass(t *testing.T) {
	c := NewChecker()
	results := []CheckResult{
		{Name: "a", Passed: true, Severity: "critical"},
		{Name: "b", Passed: true, Severity: "warning"},
	}
	if c.HasCriticalFailures(results) {
		t.Error("HasCriticalFailures() = true, want false when all pass")
	}
}

func TestHasCriticalFailures_TrueWhenCriticalFails(t *testing.T) {
	c := NewChecker()
	results := []CheckResult{
		{Name: "a", Passed: false, Severity: "critical"},
		{Name: "b", Passed: true, Severity: "warning"},
	}
	if !c.HasCriticalFailures(results) {
		t.Error("HasCriticalFailures() = false, want true when a critical check fails")
	}
}

func TestHasCriticalFailures_IgnoresFailedWarnings(t *testing.T) {
	c := NewChecker()
	results := []CheckResult{
		{Name: "a", Passed: false, Severity: "warning"},
		{Name: "b", Passed: true, Severity: "critical"},
	}
	if c.HasCriticalFailures(results) {
		t.Error("HasCriticalFailures() = true, want false when only warnings fail")
	}
}

// --- checkHMACInitialized (via CheckConfig) ---

func TestCheckConfig_HMACNotInitialized(t *testing.T) {
	// Ensure HMAC is not initialized for this sub-test by using a fresh state.
	// crypto.IsHMACInitialized reflects package-level state; we cannot reset it,
	// so we check the result value against the actual state and verify consistency.
	c := NewChecker()
	cfg := makeConfig()
	results := c.CheckConfig(cfg, false)

	var hmacResult *CheckResult
	for i := range results {
		if results[i].Name == "HMAC Key Set" {
			hmacResult = &results[i]
			break
		}
	}
	if hmacResult == nil {
		t.Fatal("CheckConfig() did not include 'HMAC Key Set' result")
	}
	if hmacResult.Severity != "critical" {
		t.Errorf("HMAC Key Set severity = %q, want 'critical'", hmacResult.Severity)
	}
	// Result must match actual HMAC state.
	if hmacResult.Passed != crypto.IsHMACInitialized() {
		t.Errorf("HMAC Key Set Passed = %v but IsHMACInitialized() = %v", hmacResult.Passed, crypto.IsHMACInitialized())
	}
}

func TestCheckConfig_HMACInitialized(t *testing.T) {
	crypto.InitTokenHMAC([]byte("test-hmac-key-for-readiness-check"))

	c := NewChecker()
	cfg := makeConfig()
	results := c.CheckConfig(cfg, false)

	for _, r := range results {
		if r.Name == "HMAC Key Set" {
			if !r.Passed {
				t.Error("HMAC Key Set: Passed = false after InitTokenHMAC, want true")
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'HMAC Key Set' result")
}

// --- checkTLSEnabled ---

func TestCheckConfig_TLSNotEnabled(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	results := c.CheckConfig(cfg, false)

	for _, r := range results {
		if r.Name == "TLS Enabled" {
			if r.Passed {
				t.Error("TLS Enabled: Passed = true with no cert/key, want false")
			}
			if r.Severity != "critical" {
				t.Errorf("TLS Enabled severity = %q, want 'critical'", r.Severity)
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'TLS Enabled' result")
}

func TestCheckConfig_TLSEnabled(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	cfg.Gateway.TLS.CertFile = "/etc/tls/server.crt"
	cfg.Gateway.TLS.KeyFile = "/etc/tls/server.key"
	results := c.CheckConfig(cfg, false)

	for _, r := range results {
		if r.Name == "TLS Enabled" {
			if !r.Passed {
				t.Error("TLS Enabled: Passed = false with cert+key set, want true")
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'TLS Enabled' result")
}

// --- checkAdminAPIKey ---

func TestCheckConfig_AdminAPIKeyMissing(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	results := c.CheckConfig(cfg, false)

	for _, r := range results {
		if r.Name == "Admin API Key Set" {
			if r.Passed {
				t.Error("Admin API Key Set: Passed = true with no key, want false")
			}
			if r.Severity != "critical" {
				t.Errorf("Admin API Key Set severity = %q, want 'critical'", r.Severity)
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'Admin API Key Set' result")
}

func TestCheckConfig_AdminAPIKeyPresent(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	cfg.Admin.APIKey = "super-secret-key"
	results := c.CheckConfig(cfg, false)

	for _, r := range results {
		if r.Name == "Admin API Key Set" {
			if !r.Passed {
				t.Error("Admin API Key Set: Passed = false with key set, want true")
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'Admin API Key Set' result")
}

// --- checkDevModeDisabled ---

func TestCheckConfig_DevModeEnabled(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	results := c.CheckConfig(cfg, true /* devMode */)

	for _, r := range results {
		if r.Name == "Dev Mode Disabled" {
			if r.Passed {
				t.Error("Dev Mode Disabled: Passed = true in dev mode, want false")
			}
			if r.Severity != "warning" {
				t.Errorf("Dev Mode Disabled severity = %q, want 'warning'", r.Severity)
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'Dev Mode Disabled' result")
}

func TestCheckConfig_DevModeDisabled(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	results := c.CheckConfig(cfg, false /* devMode */)

	for _, r := range results {
		if r.Name == "Dev Mode Disabled" {
			if !r.Passed {
				t.Error("Dev Mode Disabled: Passed = false when dev mode is off, want true")
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'Dev Mode Disabled' result")
}

// --- checkCORSNotWildcard ---

func TestCheckConfig_CORSWildcard(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	cfg.Admin.CORSOrigin = "*"
	results := c.CheckConfig(cfg, false)

	for _, r := range results {
		if r.Name == "CORS Not Wildcard" {
			if r.Passed {
				t.Error("CORS Not Wildcard: Passed = true for wildcard origin, want false")
			}
			if r.Severity != "warning" {
				t.Errorf("CORS Not Wildcard severity = %q, want 'warning'", r.Severity)
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'CORS Not Wildcard' result")
}

func TestCheckConfig_CORSSpecific(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	cfg.Admin.CORSOrigin = "https://app.example.com"
	results := c.CheckConfig(cfg, false)

	for _, r := range results {
		if r.Name == "CORS Not Wildcard" {
			if !r.Passed {
				t.Error("CORS Not Wildcard: Passed = false for specific origin, want true")
			}
			return
		}
	}
	t.Fatal("CheckConfig() did not include 'CORS Not Wildcard' result")
}

// --- CheckSecretsFilePath ---

func TestCheckSecretsFilePath_FileExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secrets.yaml")
	if err := os.WriteFile(path, []byte("key: value\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r := CheckSecretsFilePath(path)
	if !r.Passed {
		t.Errorf("CheckSecretsFilePath(%q): Passed = false, want true (file exists)", path)
	}
	if r.Severity != "warning" {
		t.Errorf("CheckSecretsFilePath severity = %q, want 'warning'", r.Severity)
	}
}

func TestCheckSecretsFilePath_FileMissing(t *testing.T) {
	r := CheckSecretsFilePath("/nonexistent/path/secrets.yaml")
	if r.Passed {
		t.Error("CheckSecretsFilePath(missing): Passed = true, want false")
	}
}

// --- CheckDatabase with nil ---

func TestCheckDatabase_NilDB(t *testing.T) {
	c := NewChecker()
	results := c.CheckDatabase(nil)
	if len(results) != 0 {
		t.Errorf("CheckDatabase(nil) returned %d results, want 0", len(results))
	}
}

// --- CheckConfig returns expected number of checks ---

func TestCheckConfig_ReturnsExpectedCheckCount(t *testing.T) {
	c := NewChecker()
	cfg := makeConfig()
	results := c.CheckConfig(cfg, false)
	// Currently: HMAC, TLS, AdminAPIKey, DevMode, CORS, SecretsFile = 6
	if len(results) != 6 {
		t.Errorf("CheckConfig() returned %d results, want 6", len(results))
	}
}
