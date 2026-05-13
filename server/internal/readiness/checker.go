// Package readiness provides production readiness checks for the Aether gateway.
package readiness

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/pkg/crypto"
)

// CheckResult holds the outcome of a single readiness check.
type CheckResult struct {
	Name     string
	Passed   bool
	Message  string
	Severity string // "critical", "warning", "info"
}

// Checker runs production readiness checks against configuration and infrastructure.
type Checker struct{}

// NewChecker returns a new Checker.
func NewChecker() *Checker {
	return &Checker{}
}

// CheckConfig runs all configuration-level readiness checks.
// devMode is passed explicitly because it may come from a CLI flag rather than config.
func (c *Checker) CheckConfig(cfg *config.Config, devMode bool) []CheckResult {
	var results []CheckResult

	// 1. HMAC key initialized
	results = append(results, checkHMACInitialized())

	// 2. TLS enabled
	results = append(results, checkTLSEnabled(cfg))

	// 3. Admin API key set
	results = append(results, checkAdminAPIKey(cfg))

	// 4. Dev mode disabled
	results = append(results, checkDevModeDisabled(devMode))

	// 5. CORS not wildcard
	results = append(results, checkCORSNotWildcard(cfg))

	// 6. Secrets file exists
	results = append(results, checkSecretsFile(cfg))

	return results
}

// CheckDatabase runs database-level readiness checks. Safe to call with nil db — all checks are skipped.
func (c *Checker) CheckDatabase(db *sql.DB) []CheckResult {
	if db == nil {
		return nil
	}
	return []CheckResult{checkACLFallbackDenyByDefault(db)}
}

// HasCriticalFailures returns true if any result has Severity "critical" and Passed == false.
func (c *Checker) HasCriticalFailures(results []CheckResult) bool {
	for _, r := range results {
		if r.Severity == "critical" && !r.Passed {
			return true
		}
	}
	return false
}

// PrintReport prints a human-readable readiness report to stdout.
func (c *Checker) PrintReport(results []CheckResult) {
	fmt.Println("=== Production Readiness Report ===")
	passed := 0
	for _, r := range results {
		status := "PASS"
		if !r.Passed {
			status = "FAIL"
		} else {
			passed++
		}
		fmt.Printf("  [%s] [%-8s] %s: %s\n", status, r.Severity, r.Name, r.Message)
	}
	fmt.Printf("  %d/%d checks passed\n", passed, len(results))
	fmt.Println("===================================")
}

// --- individual checks ---

func checkHMACInitialized() CheckResult {
	if crypto.IsHMACInitialized() {
		return CheckResult{
			Name:     "HMAC Key Set",
			Passed:   true,
			Message:  "token HMAC key is initialized",
			Severity: "critical",
		}
	}
	return CheckResult{
		Name:     "HMAC Key Set",
		Passed:   false,
		Message:  "token HMAC key is not initialized — set auth.token_hmac_key or AETHER_TOKEN_HMAC_KEY",
		Severity: "critical",
	}
}

func checkTLSEnabled(cfg *config.Config) CheckResult {
	if cfg.Gateway.TLS.CertFile != "" && cfg.Gateway.TLS.KeyFile != "" {
		return CheckResult{
			Name:     "TLS Enabled",
			Passed:   true,
			Message:  fmt.Sprintf("TLS configured with cert %s", cfg.Gateway.TLS.CertFile),
			Severity: "critical",
		}
	}
	return CheckResult{
		Name:     "TLS Enabled",
		Passed:   false,
		Message:  "gateway TLS is not configured — set gateway.tls.cert_file and gateway.tls.key_file",
		Severity: "critical",
	}
}

func checkAdminAPIKey(cfg *config.Config) CheckResult {
	if cfg.Admin.APIKey != "" {
		return CheckResult{
			Name:     "Admin API Key Set",
			Passed:   true,
			Message:  "admin API key is configured",
			Severity: "critical",
		}
	}
	return CheckResult{
		Name:     "Admin API Key Set",
		Passed:   false,
		Message:  "admin API key is not set — set admin.api_key or AETHER_ADMIN_API_KEY; admin API is unauthenticated",
		Severity: "critical",
	}
}

func checkDevModeDisabled(devMode bool) CheckResult {
	if !devMode {
		return CheckResult{
			Name:     "Dev Mode Disabled",
			Passed:   true,
			Message:  "dev mode is off",
			Severity: "warning",
		}
	}
	return CheckResult{
		Name:     "Dev Mode Disabled",
		Passed:   false,
		Message:  "server is running in dev mode — not suitable for production",
		Severity: "warning",
	}
}

func checkCORSNotWildcard(cfg *config.Config) CheckResult {
	if cfg.Admin.CORSOrigin != "*" {
		return CheckResult{
			Name:     "CORS Not Wildcard",
			Passed:   true,
			Message:  fmt.Sprintf("admin CORS origin is %q", cfg.Admin.CORSOrigin),
			Severity: "warning",
		}
	}
	return CheckResult{
		Name:     "CORS Not Wildcard",
		Passed:   false,
		Message:  "admin CORS origin is set to \"*\" — restrict to specific origins in production",
		Severity: "warning",
	}
}

func checkSecretsFile(cfg *config.Config) CheckResult {
	// The secrets file path comes from the CLI flag; we check the common default
	// location as a heuristic since we don't have the flag value here.
	// Callers that have the actual path should call checkSecretsFilePath directly.
	return checkSecretsFilePath("/etc/aether/generated-secrets.yaml")
}

// CheckSecretsFilePath checks whether the given secrets file path exists.
// Use this variant when the caller has the actual secrets file path from CLI flags.
func CheckSecretsFilePath(path string) CheckResult {
	return checkSecretsFilePath(path)
}

func checkSecretsFilePath(path string) CheckResult {
	if _, err := os.Stat(path); err == nil {
		return CheckResult{
			Name:     "Secrets File Exists",
			Passed:   true,
			Message:  fmt.Sprintf("secrets file found at %s", path),
			Severity: "warning",
		}
	}
	return CheckResult{
		Name:     "Secrets File Exists",
		Passed:   false,
		Message:  fmt.Sprintf("secrets file not found at %s — run init-secrets or verify path", path),
		Severity: "warning",
	}
}

func checkACLFallbackDenyByDefault(db *sql.DB) CheckResult {
	// Query all fallback policies. If any has fallback_access_level > AccessNone, flag it.
	query := `SELECT rule_category, fallback_access_level FROM acl_fallback_policies`
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return CheckResult{
			Name:     "ACL Fallback Deny-by-Default",
			Passed:   false,
			Message:  fmt.Sprintf("failed to query fallback policies: %v", err),
			Severity: "warning",
		}
	}
	defer rows.Close()

	var permissive []string
	for rows.Next() {
		var category string
		var level int
		if err := rows.Scan(&category, &level); err != nil {
			continue
		}
		if level > acl.AccessNone {
			permissive = append(permissive, fmt.Sprintf("%s=%s", category, acl.AccessLevelName(level)))
		}
	}
	if err := rows.Err(); err != nil {
		return CheckResult{
			Name:     "ACL Fallback Deny-by-Default",
			Passed:   false,
			Message:  fmt.Sprintf("error reading fallback policy rows: %v", err),
			Severity: "warning",
		}
	}

	if len(permissive) == 0 {
		return CheckResult{
			Name:     "ACL Fallback Deny-by-Default",
			Passed:   true,
			Message:  "all ACL fallback policies deny by default",
			Severity: "warning",
		}
	}
	return CheckResult{
		Name:     "ACL Fallback Deny-by-Default",
		Passed:   false,
		Message:  fmt.Sprintf("some fallback policies are permissive: %v", permissive),
		Severity: "warning",
	}
}
