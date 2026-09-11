package login

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func managementStore(t *testing.T) (*RedisOpaqueSessionStore, *redis.Client) {
	t.Helper()
	addr := os.Getenv("AUTH_TEST_REDIS_ADDR")
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	prefix := "test-session:" + digest(t.Name()+time.Now().String()) + ":"
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := client.Scan(context.Background(), cursor, prefix+"*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				client.Del(context.Background(), keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		client.Close()
	})
	return NewRedisOpaqueSessionStore(client, prefix), client
}

func sessionFixture() *SessionData {
	return &SessionData{UserID: "Person@Example.com", Email: " Person@Example.com ", Provider: "google", Claims: map[string]any{"secret": "never-list-claims"}, IssuedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour).UTC()}
}

func TestSessionManagement(t *testing.T) {
	ctx := context.Background()
	s, client := managementStore(t)
	otherReplica := NewRedisOpaqueSessionStore(client, s.prefix)
	id, err := s.New(ctx, sessionFixture())
	if err != nil {
		t.Fatal(err)
	}
	page, err := otherReplica.ListSessions(ctx, "person@example.com", 10, 0)
	if err != nil || len(page.Sessions) != 1 {
		t.Fatalf("list: %+v %v", page, err)
	}
	handle := page.Sessions[0].ID
	encoded, _ := json.Marshal(page)
	if strings.Contains(string(encoded), id) || strings.Contains(string(encoded), "never-list-claims") {
		t.Fatal("credential/claim leak")
	}
	if result, err := s.Get(ctx, handle); err != nil || result != nil {
		t.Fatal("management ID authenticated")
	}
	if err := otherReplica.RevokeSession(ctx, "another@example.com", handle); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Get(ctx, id); err != nil || result == nil {
		t.Fatal("cross-user revocation")
	}
	if err := otherReplica.RevokeSession(ctx, "person@example.com", handle); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Get(ctx, id); err != nil || result != nil {
		t.Fatal("revoked session authenticated")
	}
	if err := otherReplica.RevokeSession(ctx, "person@example.com", handle); err != nil {
		t.Fatal("not idempotent", err)
	}
	page, err = s.ListSessions(ctx, "person@example.com", 10, 0)
	if err != nil || len(page.Sessions) != 0 {
		t.Fatal("revoked session listed")
	}
}

func TestSessionBulkRevocationLegacyAndNewLogin(t *testing.T) {
	ctx := context.Background()
	s, client := managementStore(t)
	legacy, _ := newOpaqueID(32)
	unseenLegacy, _ := newOpaqueID(32)
	payload, _ := json.Marshal(sessionFixture())
	for _, id := range []string{legacy, unseenLegacy} {
		if err := client.Set(ctx, s.prefix+id, payload, time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := s.Get(ctx, legacy); err != nil || result == nil {
		t.Fatalf("legacy migration: %v", err)
	}
	page, err := s.ListSessions(ctx, "person@example.com", 10, 0)
	if err != nil || len(page.Sessions) != 1 {
		t.Fatal("legacy not indexed", err)
	}
	id, err := s.New(ctx, sessionFixture())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAllSessions(ctx, "person@example.com"); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{id, legacy, unseenLegacy} {
		if result, err := s.Get(ctx, token); err != nil || result != nil {
			t.Fatalf("revoked token survived: %v", err)
		}
	}
	page, err = s.ListSessions(ctx, "person@example.com", 10, 0)
	if err != nil || len(page.Sessions) != 0 {
		t.Fatal("bulk revoked sessions listed")
	}
	id, err = s.New(ctx, sessionFixture())
	if err != nil {
		t.Fatal(err)
	}
	if result, err := s.Get(ctx, id); err != nil || result == nil {
		t.Fatalf("subsequent login denied: %v", err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Get(ctx, id); err != nil || result != nil {
		t.Fatal("logout failed")
	}
}

func TestSessionPaginationExpiryAndIndexLifetime(t *testing.T) {
	ctx := context.Background()
	s, client := managementStore(t)
	data := sessionFixture()
	for i := range 3 {
		data.ExpiresAt = time.Now().Add(time.Duration(i+1) * time.Hour)
		if _, err := s.New(ctx, data); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.ListSessions(ctx, "person@example.com", 2, 0)
	if err != nil || len(page.Sessions) != 2 || !page.HasMore {
		t.Fatal("first page", page, err)
	}
	page, err = s.ListSessions(ctx, "person@example.com", 2, 2)
	if err != nil || len(page.Sessions) != 1 || page.HasMore {
		t.Fatal("last page", page, err)
	}
	index := s.subjectKey("person@example.com") + ":index:0"
	if ttl := client.PTTL(ctx, index).Val(); ttl < 2*time.Hour {
		t.Fatal("shortened index lifetime", ttl)
	}
	// Model elapsed time without sleeps: rewrite one record's expiry and score.
	key := s.sessionKey(page.Sessions[0].ID)
	raw, err := client.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var record storedSession
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	record.ExpiresAt = time.Now().Add(-time.Hour)
	raw, _ = json.Marshal(record)
	client.Set(ctx, key, raw, time.Hour)
	client.ZAdd(ctx, index, redis.Z{Score: float64(record.ExpiresAt.UnixMilli()), Member: page.Sessions[0].ID})
	page, err = s.ListSessions(ctx, "person@example.com", 10, 0)
	if err != nil || len(page.Sessions) != 2 {
		t.Fatal("expired session listed", page, err)
	}
	if _, err := s.New(ctx, &SessionData{ExpiresAt: time.Now().Add(-time.Hour)}); err == nil {
		t.Fatal("accepted expired session")
	}
	if _, err := s.New(ctx, nil); err == nil {
		t.Fatal("accepted nil session")
	}
}

func TestSessionConcurrentCreationAndRevocation(t *testing.T) {
	ctx := context.Background()
	s, _ := managementStore(t)
	var wg sync.WaitGroup
	ids := make(chan string, 30)
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := s.New(ctx, sessionFixture())
			if err != nil {
				t.Error(err)
				return
			}
			ids <- id
			if err := s.RevokeAllSessions(ctx, "person@example.com"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(ids)
	if err := s.RevokeAllSessions(ctx, "person@example.com"); err != nil {
		t.Fatal(err)
	}
	for id := range ids {
		if result, err := s.Get(ctx, id); err != nil || result != nil {
			t.Fatal("session survived final revocation", err)
		}
	}
}

func TestSessionRedisFailureAndPrefixIsolation(t *testing.T) {
	ctx := context.Background()
	s, client := managementStore(t)
	id, err := s.New(ctx, sessionFixture())
	if err != nil {
		t.Fatal(err)
	}
	other := NewRedisOpaqueSessionStore(client, s.prefix+"other:")
	if err := other.RevokeAllSessions(ctx, "person@example.com"); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Get(ctx, id); err != nil || result == nil {
		t.Fatal("prefix isolation failed")
	}
	client.Close()
	if result, err := s.Get(ctx, id); err == nil || result != nil {
		t.Fatal("store failure accepted session")
	}
	if _, err := s.ListSessions(ctx, "person@example.com", 10, 0); err == nil {
		t.Fatal("store failure reported empty inventory")
	}
	if err := s.RevokeAllSessions(ctx, "person@example.com"); err == nil {
		t.Fatal("store failure reported success")
	}
}

func TestSessionSubjectsAndNonexpiringSessions(t *testing.T) {
	ctx := context.Background()
	s, client := managementStore(t)
	data := sessionFixture()
	data.Email = " Person@BÜCHER.example. "
	data.ExpiresAt = time.Time{}
	id, err := s.New(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.ListSessions(ctx, "person@xn--bcher-kva.example", 10, 0)
	if err != nil || len(page.Sessions) != 1 {
		t.Fatal("IDNA subject mismatch", page, err)
	}
	// Adding a finite session must not expire an index containing a timeless one.
	data.ExpiresAt = time.Now().Add(time.Hour)
	if _, err := s.New(ctx, data); err != nil {
		t.Fatal(err)
	}
	if ttl := client.PTTL(ctx, s.subjectKey("person@xn--bcher-kva.example")+":index:0").Val(); ttl != -1 {
		t.Fatal("nonexpiring index acquired a TTL", ttl)
	}
	if err := s.RevokeAllSessions(ctx, "person@xn--bcher-kva.example"); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Get(ctx, id); err != nil || result != nil {
		t.Fatal("nonexpiring revocation failed", err)
	}
	data.Email = ""
	data.UserID = "CaseSensitiveSubject"
	if _, err := s.New(ctx, data); err != nil {
		t.Fatal(err)
	}
	page, err = s.ListSessions(ctx, "casesensitivesubject", 10, 0)
	if err != nil || len(page.Sessions) != 0 {
		t.Fatal("folded opaque subject")
	}
	page, err = s.ListSessions(ctx, "CaseSensitiveSubject", 10, 0)
	if err != nil || len(page.Sessions) != 1 {
		t.Fatal("missing opaque subject")
	}
}

func TestSessionPreservesClaimJSON(t *testing.T) {
	s, _ := managementStore(t)
	data := sessionFixture()
	data.Claims = map[string]any{"roles": []any{}, "metadata": map[string]any{}, "nested": map[string]any{"groups": []any{}}}
	id, err := s.New(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(context.Background(), id)
	if err != nil || got == nil {
		t.Fatal(err)
	}
	expected, _ := json.Marshal(data.Claims)
	actual, _ := json.Marshal(got.Claims)
	if string(expected) != string(actual) {
		t.Fatalf("changed claim types: want %s got %s", expected, actual)
	}
}
