package main

import (
	"github.com/scitrera/aether/server/internal/config"
	"testing"
)

func TestLiteAdminServerConfigPreservesSecuritySettings(t *testing.T) {
	oldDev, oldInsecure := *devMode, *insecureAdmin
	t.Cleanup(func() { *devMode, *insecureAdmin = oldDev, oldInsecure })
	*devMode, *insecureAdmin = false, false
	cfg := &config.Config{Admin: config.AdminConfig{
		Port: 31880, APIKey: "test-only-admin-key", TLSCertFile: "admin.crt", TLSKeyFile: "admin.key",
		CORSOrigin: "https://localhost", RateLimit: 3.5, RateLimitBurst: 7,
	}}
	got := liteAdminServerConfig(cfg)
	if got.APIKey != cfg.Admin.APIKey || got.TLSCertFile != cfg.Admin.TLSCertFile || got.TLSKeyFile != cfg.Admin.TLSKeyFile {
		t.Fatal("admin authentication or TLS configuration was lost")
	}
	if got.Port != cfg.Admin.Port || got.CORSOrigin != cfg.Admin.CORSOrigin || got.RateLimit != 3.5 || got.RateLimitBurst != 7 {
		t.Fatal("admin listener configuration was lost")
	}
	if got.InsecureNoAuth || got.DevMode {
		t.Fatal("secure configuration unexpectedly enabled insecure mode")
	}
	*insecureAdmin = true
	if !liteAdminServerConfig(cfg).InsecureNoAuth {
		t.Fatal("explicit insecure-admin flag was not preserved")
	}
	*insecureAdmin, *devMode = false, true
	got = liteAdminServerConfig(cfg)
	if !got.InsecureNoAuth || !got.DevMode {
		t.Fatal("explicit development mode was not preserved")
	}
}
