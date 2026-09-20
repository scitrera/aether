package authproxy

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/server/pkg/authproxy/login"
)

// Run against an isolated fixture with two data servers and three Sentinel voters.
// Never point AUTH_PROXY_TEST_SENTINELS at a live session store: this test promotes
// the replica and creates/deletes synthetic session records.
func TestSentinelSessionFailover(t *testing.T) {
	addrs := os.Getenv("AUTH_PROXY_TEST_SENTINELS")
	if addrs == "" {
		t.Skip("isolated Sentinel fixture not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := &LoginConfig{StoreKind: "redis", SentinelMaster: "test-sessions", SentinelAddrs: strings.Split(addrs, ","), SessionPrefix: "test-session:"}
	store, client, err := cfg.BuildSessionStore()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	observer := redis.NewSentinelClient(&redis.Options{Addr: cfg.SentinelAddrs[0]})
	defer observer.Close()
	before, err := observer.GetMasterAddrByName(ctx, cfg.SentinelMaster).Result()
	if err != nil {
		t.Fatal(err)
	}
	id, err := store.New(ctx, &login.SessionData{UserID: "synthetic-failover", IssuedAt: time.Now(), ExpiresAt: time.Now().Add(48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := client.Wait(ctx, 1, 5*time.Second).Result(); err != nil || n != 1 {
		t.Fatalf("replica did not acknowledge session: %v", err)
	}
	if err := observer.Failover(ctx, cfg.SentinelMaster).Err(); err != nil {
		t.Fatal(err)
	}
	promoted := false
	for ctx.Err() == nil {
		after, err := observer.GetMasterAddrByName(ctx, cfg.SentinelMaster).Result()
		if err == nil && strings.Join(after, ":") != strings.Join(before, ":") {
			if got, err := store.Get(ctx, id); err == nil && got != nil && got.UserID == "synthetic-failover" {
				if err := store.Delete(ctx, id); err == nil {
					promoted = true
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !promoted {
		t.Fatal("client did not reconnect to promoted primary with its session intact")
	}
	if got, err := store.Get(ctx, id); err != nil || got != nil {
		t.Fatalf("logout did not survive promotion: %v", err)
	}
	id, err = store.New(ctx, &login.SessionData{UserID: "synthetic-after-failover", IssuedAt: time.Now(), ExpiresAt: time.Now().Add(48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Delete(context.Background(), id)
	if got, err := store.Get(ctx, id); err != nil || got == nil {
		t.Fatalf("new login failed after promotion: %v", err)
	}
}
