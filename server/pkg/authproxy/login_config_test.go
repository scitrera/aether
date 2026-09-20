package authproxy

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/scitrera/aether/server/pkg/authproxy/login"
)

func clearLoginEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"LOGIN_PROVIDERS", "SESSION_STORE", "SESSION_TTL", "SESSION_REDIS_ADDR", "REDIS_ADDR", "SESSION_REDIS_USERNAME", "SESSION_REDIS_PASSWORD", "SESSION_REDIS_DB", "SESSION_REDIS_PREFIX", "SESSION_SENTINEL_MASTER", "SESSION_SENTINEL_ADDRS", "SESSION_SENTINEL_USERNAME", "SESSION_SENTINEL_PASSWORD"} {
		t.Setenv("AUTH_PROXY_"+key, "")
	}
	t.Setenv("AUTH_PROXY_LOGIN_PROVIDERS", "fixture")
}

func TestLoginConfigSentinel(t *testing.T) {
	clearLoginEnv(t)
	t.Setenv("AUTH_PROXY_SESSION_SENTINEL_MASTER", "sessions")
	t.Setenv("AUTH_PROXY_SESSION_SENTINEL_ADDRS", " first:26379, ,second:26379 ")
	t.Setenv("AUTH_PROXY_SESSION_SENTINEL_USERNAME", "observer")
	t.Setenv("AUTH_PROXY_SESSION_SENTINEL_PASSWORD", "sentinel-secret")
	t.Setenv("AUTH_PROXY_SESSION_REDIS_USERNAME", "sessions-user")
	t.Setenv("AUTH_PROXY_SESSION_REDIS_PASSWORD", "data-secret")
	t.Setenv("AUTH_PROXY_SESSION_TTL", "48h")
	cfg, err := LoadLoginConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SentinelMaster != "sessions" || len(cfg.SentinelAddrs) != 2 || cfg.SentinelAddrs[0] != "first:26379" {
		t.Fatalf("incorrect discovery configuration")
	}
	if cfg.SentinelUsername != "observer" || cfg.SentinelPassword != "sentinel-secret" || cfg.RedisUsername != "sessions-user" || cfg.RedisPassword != "data-secret" {
		t.Fatal("credentials were not separated")
	}
	if cfg.Cookies.MaxAge != 48*time.Hour {
		t.Fatal("incorrect browser lifetime")
	}
}

func TestLoginConfigRejectsPartialSentinel(t *testing.T) {
	for _, key := range []string{"MASTER", "ADDRS"} {
		t.Run(key, func(t *testing.T) {
			clearLoginEnv(t)
			t.Setenv("AUTH_PROXY_SESSION_REDIS_ADDR", "fallback:6379")
			t.Setenv("AUTH_PROXY_SESSION_SENTINEL_"+key, "configured")
			if _, err := LoadLoginConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "Sentinel requires both") {
				t.Fatalf("expected explicit configuration error: %v", err)
			}
		})
	}
}

func TestDirectSessionStoreStillExpiresAndRevokes(t *testing.T) {
	clearLoginEnv(t)
	r := miniredis.RunT(t)
	t.Setenv("AUTH_PROXY_REDIS_ADDR", r.Addr())
	cfg, err := LoadLoginConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	store, client, err := cfg.BuildSessionStore()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	id, err := store.New(ctx, &login.SessionData{UserID: "synthetic", IssuedAt: time.Now(), ExpiresAt: time.Now().Add(48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.Get(ctx, id); err != nil || got == nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Get(ctx, id); err != nil || got != nil {
		t.Fatalf("revoked session survived: %v", err)
	}
	id, err = store.New(ctx, &login.SessionData{UserID: "synthetic", IssuedAt: time.Now(), ExpiresAt: time.Now().Add(48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	r.FastForward(49 * time.Hour)
	if got, err := store.Get(ctx, id); err != nil || got != nil {
		t.Fatalf("expired session survived: %v", err)
	}
}
