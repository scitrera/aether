package login

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSignedJWTSessionStore_Roundtrip(t *testing.T) {
	store, err := NewSignedJWTSessionStore([]byte(strings.Repeat("k", 32)), "test-issuer")
	if err != nil {
		t.Fatalf("NewSignedJWTSessionStore: %v", err)
	}

	in := &SessionData{
		UserID:    "alice@scitrera.com",
		Email:     "alice@scitrera.com",
		Name:      "Alice",
		Provider:  "azure",
		Claims:    map[string]any{"oid": "abc", "tid": "xyz"},
		IssuedAt:  time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}
	id, err := store.New(context.Background(), in)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty signed token")
	}

	out, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if out == nil {
		t.Fatal("expected session to be returned")
	}
	if out.UserID != in.UserID || out.Email != in.Email || out.Provider != in.Provider {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", out, in)
	}
	if got := out.Claims["tid"]; got != "xyz" {
		t.Errorf("claims tid: got %v, want xyz", got)
	}
}

func TestSignedJWTSessionStore_RejectsTamperedToken(t *testing.T) {
	store, _ := NewSignedJWTSessionStore([]byte(strings.Repeat("k", 32)), "test")
	id, _ := store.New(context.Background(), &SessionData{UserID: "u", ExpiresAt: time.Now().Add(time.Hour)})
	tampered := id[:len(id)-4] + "xxxx"
	out, err := store.Get(context.Background(), tampered)
	if err != nil {
		t.Fatalf("Get tampered: unexpected error %v", err)
	}
	if out != nil {
		t.Fatal("tampered token must yield nil session")
	}
}

func TestSignedJWTSessionStore_ExpiredToken(t *testing.T) {
	store, _ := NewSignedJWTSessionStore([]byte(strings.Repeat("k", 32)), "test")
	in := &SessionData{
		UserID:    "u",
		ExpiresAt: time.Now().Add(-time.Hour), // already expired
	}
	// New() rejects past-dated ExpiresAt for the Redis store via TTL math but
	// the JWT store assigns ExpiresAt to NumericDate; so we expect the verify
	// step in Get() to refuse the token.
	id, err := store.New(context.Background(), in)
	if err != nil {
		t.Fatalf("New on expired data: %v", err)
	}
	out, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get expired: %v", err)
	}
	if out != nil {
		t.Fatal("expired token must yield nil session")
	}
}

func TestSignedJWTSessionStore_RejectsShortKey(t *testing.T) {
	if _, err := NewSignedJWTSessionStore([]byte("short"), "test"); err == nil {
		t.Fatal("expected error for short key")
	}
}

func TestStatePayload_Roundtrip(t *testing.T) {
	cases := []struct {
		name  string
		state string
		next  string
	}{
		{"with next", "abc123", "/dashboard"},
		{"empty next", "abc123", ""},
		{"complex next", "stateNonce", "/path?q=1&r=2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := encodeStatePayload(tc.state, tc.next)
			gotState, gotNext := decodeStatePayload(payload)
			if gotState != tc.state {
				t.Errorf("state: got %q, want %q", gotState, tc.state)
			}
			if gotNext != tc.next {
				t.Errorf("next: got %q, want %q", gotNext, tc.next)
			}
		})
	}
}

func TestSessionData_IsExpired(t *testing.T) {
	if (&SessionData{}).IsExpired() {
		t.Error("zero ExpiresAt must not be expired")
	}
	if !(&SessionData{ExpiresAt: time.Now().Add(-time.Second)}).IsExpired() {
		t.Error("past ExpiresAt must be expired")
	}
	if (&SessionData{ExpiresAt: time.Now().Add(time.Hour)}).IsExpired() {
		t.Error("future ExpiresAt must not be expired")
	}
}
