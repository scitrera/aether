package login

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestSessionCreationPrunesExpiredIndexEntries(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "new"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, client := managementStore(t)
			data := sessionFixture()
			oldID, err := s.New(ctx, data)
			if err != nil {
				t.Fatal(err)
			}
			data.ExpiresAt = time.Now().Add(3 * time.Hour)
			liveID, err := s.New(ctx, data)
			if err != nil {
				t.Fatal(err)
			}
			index := s.subjectKey("person@example.com") + ":index:0"
			// Model Redis expiry without sleeps: the record disappears while its
			// expired member remains in an index kept alive by another session.
			if err := client.Del(ctx, s.sessionKey(digest(oldID))).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.ZAdd(ctx, index, redis.Z{Score: float64(time.Now().Add(-time.Hour).UnixMilli()), Member: digest(oldID)}).Err(); err != nil {
				t.Fatal(err)
			}
			data.ExpiresAt = time.Now().Add(time.Hour)
			if legacy {
				id, err := newOpaqueID(32)
				if err != nil {
					t.Fatal(err)
				}
				payload, err := json.Marshal(data)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Set(ctx, s.prefix+id, payload, time.Hour).Err(); err != nil {
					t.Fatal(err)
				}
				if got, err := s.Get(ctx, id); err != nil || got == nil {
					t.Fatalf("legacy migration: session=%+v err=%v", got, err)
				}
			} else if _, err := s.New(ctx, data); err != nil {
				t.Fatal(err)
			}
			// Do not call ListSessions: normal login traffic must reclaim entries
			// even when the optional management API is never used.
			ids, err := client.ZRange(ctx, index, 0, -1).Result()
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != 2 {
				t.Fatalf("index has %d entries for 2 live sessions", len(ids))
			}
			for _, id := range ids {
				if id == digest(oldID) {
					t.Fatal("expired index entry retained")
				}
			}
			if ttl, err := client.PTTL(ctx, index).Result(); err != nil || ttl < 2*time.Hour {
				t.Fatalf("longer-lived session lost index lifetime: ttl=%v err=%v", ttl, err)
			}
			if got, err := s.Get(ctx, liveID); err != nil || got == nil {
				t.Fatalf("live session lost: session=%+v err=%v", got, err)
			}
		})
	}
}

func TestSessionIndexExpiryAfterNonexpiringRemoval(t *testing.T) {
	for _, operation := range []string{"logout", "revoke"} {
		for _, remaining := range []string{"none", "finite", "nonexpiring"} {
			t.Run(operation+"/"+remaining, func(t *testing.T) {
				ctx := context.Background()
				s, client := managementStore(t)
				data := sessionFixture()
				data.ExpiresAt = time.Time{}
				id, err := s.New(ctx, data)
				if err != nil {
					t.Fatal(err)
				}
				var liveIDs []string
				if remaining != "none" {
					for _, expiry := range []time.Time{time.Now().Add(time.Hour), time.Now().Add(2 * time.Hour)} {
						data.ExpiresAt = expiry
						if remaining == "nonexpiring" {
							data.ExpiresAt = time.Time{}
						}
						liveID, err := s.New(ctx, data)
						if err != nil {
							t.Fatal(err)
						}
						liveIDs = append(liveIDs, liveID)
					}
				}
				if operation == "logout" {
					err = s.Delete(ctx, id)
				} else {
					err = s.RevokeSession(ctx, "person@example.com", digest(id))
				}
				if err != nil {
					t.Fatal(err)
				}
				index := s.subjectKey("person@example.com") + ":index:0"
				ttl, err := client.PTTL(ctx, index).Result()
				if err != nil {
					t.Fatal(err)
				}
				switch remaining {
				case "none":
					if ttl != -2 {
						t.Fatalf("empty index retained: ttl=%v", ttl)
					}
				case "finite":
					if ttl < time.Hour || ttl > 2*time.Hour {
						t.Fatalf("index must expire with its longest-lived remaining session: ttl=%v", ttl)
					}
				case "nonexpiring":
					if ttl != -1 {
						t.Fatalf("remaining nonexpiring sessions lost index lifetime: ttl=%v", ttl)
					}
				}
				if got, err := s.Get(ctx, id); err != nil || got != nil {
					t.Fatalf("removed session survived: session=%+v err=%v", got, err)
				}
				for _, liveID := range liveIDs {
					if got, err := s.Get(ctx, liveID); err != nil || got == nil {
						t.Fatalf("live session lost: session=%+v err=%v", got, err)
					}
				}
			})
		}
	}
}
