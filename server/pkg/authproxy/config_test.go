package authproxy

import (
	"os"
	"testing"
)

func setEnvs(t *testing.T, envs map[string]string) {
	t.Helper()
	for k, v := range envs {
		t.Setenv(k, v)
	}
}

func TestLoadConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv("AUTH_PROXY_DB_URL", "postgres://localhost/aether")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Mode != ModeProxy {
		t.Errorf("expected default mode proxy, got %s", cfg.Mode)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("expected default listen :8080, got %s", cfg.ListenAddr)
	}
	if cfg.BackendURL != "http://localhost:61001" {
		t.Errorf("expected default backend URL, got %s", cfg.BackendURL)
	}
	if cfg.TenantID != "default" {
		t.Errorf("expected default tenant, got %s", cfg.TenantID)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected default log level info, got %s", cfg.LogLevel)
	}
	if !cfg.OAuth.VerifySignature {
		t.Error("expected default VerifySignature=true")
	}
}

func TestLoadConfigFromEnv_MissingDBURL(t *testing.T) {
	// Clear any existing value
	os.Unsetenv("AUTH_PROXY_DB_URL")

	_, err := LoadConfigFromEnv()
	if err == nil {
		t.Fatal("expected error for missing DB URL")
	}
}

func TestLoadConfigFromEnv_AllValues(t *testing.T) {
	setEnvs(t, map[string]string{
		"AUTH_PROXY_MODE":                   "verify",
		"AUTH_PROXY_LISTEN_ADDR":            ":9090",
		"AUTH_PROXY_BACKEND_URL":            "http://backend:8000",
		"AUTH_PROXY_TENANT_ID":              "my-tenant",
		"AUTH_PROXY_DB_URL":                 "postgres://db/aether",
		"AUTH_PROXY_REDIS_ADDR":             "redis:6379",
		"AUTH_PROXY_LOG_LEVEL":              "DEBUG",
		"AUTH_PROXY_OAUTH_ISSUER":           "https://auth.example.com",
		"AUTH_PROXY_OAUTH_JWKS_URL":         "https://auth.example.com/.well-known/jwks.json",
		"AUTH_PROXY_OAUTH_AUDIENCE":         "my-api",
		"AUTH_PROXY_OAUTH_VERIFY_SIGNATURE": "false",
		"AETHER_DEV_MODE":                   "true",
	})

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Mode != ModeVerify {
		t.Errorf("expected mode verify, got %s", cfg.Mode)
	}
	if cfg.ListenAddr != ":9090" {
		t.Errorf("expected :9090, got %s", cfg.ListenAddr)
	}
	if cfg.BackendURL != "http://backend:8000" {
		t.Errorf("expected backend URL, got %s", cfg.BackendURL)
	}
	if cfg.TenantID != "my-tenant" {
		t.Errorf("expected my-tenant, got %s", cfg.TenantID)
	}
	if cfg.DBURL != "postgres://db/aether" {
		t.Errorf("expected DB URL, got %s", cfg.DBURL)
	}
	if cfg.RedisAddr != "redis:6379" {
		t.Errorf("expected redis addr, got %s", cfg.RedisAddr)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected debug (lowered), got %s", cfg.LogLevel)
	}
	if cfg.OAuth.Issuer != "https://auth.example.com" {
		t.Errorf("expected issuer, got %s", cfg.OAuth.Issuer)
	}
	if cfg.OAuth.JWKSURL != "https://auth.example.com/.well-known/jwks.json" {
		t.Errorf("expected JWKS URL, got %s", cfg.OAuth.JWKSURL)
	}
	if cfg.OAuth.Audience != "my-api" {
		t.Errorf("expected audience, got %s", cfg.OAuth.Audience)
	}
	if cfg.OAuth.VerifySignature {
		t.Error("expected VerifySignature=false")
	}
}

func TestLoadConfigFromEnv_InvalidMode(t *testing.T) {
	setEnvs(t, map[string]string{
		"AUTH_PROXY_DB_URL": "postgres://localhost/aether",
		"AUTH_PROXY_MODE":   "invalid",
	})

	_, err := LoadConfigFromEnv()
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestOAuthConfigured(t *testing.T) {
	tests := []struct {
		name   string
		oauth  OAuthConfig
		expect bool
	}{
		{"empty", OAuthConfig{}, false},
		{"issuer only", OAuthConfig{Issuer: "https://auth.example.com"}, true},
		{"jwks only", OAuthConfig{JWKSURL: "https://auth.example.com/jwks"}, true},
		{"both", OAuthConfig{Issuer: "https://auth.example.com", JWKSURL: "https://auth.example.com/jwks"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{OAuth: tt.oauth}
			if got := cfg.OAuthConfigured(); got != tt.expect {
				t.Errorf("OAuthConfigured() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestEntraConfigured(t *testing.T) {
	tests := []struct {
		name   string
		entra  EntraConfig
		expect bool
	}{
		{"empty", EntraConfig{}, false},
		{"tenant only", EntraConfig{TenantID: "abc"}, false},
		{"client only", EntraConfig{ClientID: "xyz"}, false},
		{"both set", EntraConfig{TenantID: "abc", ClientID: "xyz"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Entra: tt.entra}
			if got := cfg.EntraConfigured(); got != tt.expect {
				t.Errorf("EntraConfigured() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestLoadConfigFromEnv_EntraValues(t *testing.T) {
	setEnvs(t, map[string]string{
		"AUTH_PROXY_DB_URL":                 "postgres://db/aether",
		"AUTH_PROXY_ENTRA_TENANT_ID":        "my-tenant-id",
		"AUTH_PROXY_ENTRA_CLIENT_ID":        "my-client-id",
		"AUTH_PROXY_ENTRA_ALLOWED_TENANTS":  "tenant-a, tenant-b , tenant-c",
		"AUTH_PROXY_ENTRA_VERIFY_SIGNATURE": "false",
		"AETHER_DEV_MODE":                   "true",
	})

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Entra.TenantID != "my-tenant-id" {
		t.Errorf("Entra.TenantID = %q, want my-tenant-id", cfg.Entra.TenantID)
	}
	if cfg.Entra.ClientID != "my-client-id" {
		t.Errorf("Entra.ClientID = %q, want my-client-id", cfg.Entra.ClientID)
	}
	if len(cfg.Entra.AllowedTenants) != 3 {
		t.Fatalf("Entra.AllowedTenants length = %d, want 3", len(cfg.Entra.AllowedTenants))
	}
	if cfg.Entra.AllowedTenants[0] != "tenant-a" {
		t.Errorf("AllowedTenants[0] = %q, want tenant-a", cfg.Entra.AllowedTenants[0])
	}
	if cfg.Entra.AllowedTenants[1] != "tenant-b" {
		t.Errorf("AllowedTenants[1] = %q, want tenant-b", cfg.Entra.AllowedTenants[1])
	}
	if cfg.Entra.AllowedTenants[2] != "tenant-c" {
		t.Errorf("AllowedTenants[2] = %q, want tenant-c", cfg.Entra.AllowedTenants[2])
	}
	if cfg.Entra.VerifySignature {
		t.Error("Entra.VerifySignature = true, want false")
	}
}

func TestLoadConfigFromEnv_EntraDefaults(t *testing.T) {
	t.Setenv("AUTH_PROXY_DB_URL", "postgres://db/aether")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.Entra.VerifySignature {
		t.Error("expected Entra.VerifySignature=true by default")
	}
	if cfg.Entra.TenantID != "" {
		t.Errorf("expected empty Entra.TenantID by default, got %q", cfg.Entra.TenantID)
	}
	if cfg.EntraConfigured() {
		t.Error("expected EntraConfigured()=false when not set")
	}
}

func TestLoadConfigFromEnv_EntraVerifySignature_OutsideDevMode(t *testing.T) {
	setEnvs(t, map[string]string{
		"AUTH_PROXY_DB_URL":                 "postgres://db/aether",
		"AUTH_PROXY_ENTRA_VERIFY_SIGNATURE": "false",
		// AETHER_DEV_MODE intentionally not set
	})
	os.Unsetenv("AETHER_DEV_MODE")

	_, err := LoadConfigFromEnv()
	if err == nil {
		t.Fatal("expected error when disabling Entra signature verification outside dev mode")
	}
}

func TestLoadConfigFromEnv_EntraAllowedTenants_Empty(t *testing.T) {
	setEnvs(t, map[string]string{
		"AUTH_PROXY_DB_URL":                "postgres://db/aether",
		"AUTH_PROXY_ENTRA_TENANT_ID":       "tid",
		"AUTH_PROXY_ENTRA_CLIENT_ID":       "cid",
		"AUTH_PROXY_ENTRA_ALLOWED_TENANTS": "",
	})

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Entra.AllowedTenants) != 0 {
		t.Errorf("expected empty AllowedTenants for empty env var, got %v", cfg.Entra.AllowedTenants)
	}
}
