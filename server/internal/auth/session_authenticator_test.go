package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scitrera/aether/pkg/authproxy/login"
)

// fakeStore is an in-memory SessionStore for unit tests.
type fakeStore struct {
	sessions map[string]*login.SessionData
	getErr   error
}

func (s *fakeStore) Name() string { return "fake" }
func (s *fakeStore) New(ctx context.Context, data *login.SessionData) (string, error) {
	id := "fake-id-" + data.UserID
	s.sessions[id] = data
	return id, nil
}
func (s *fakeStore) Get(ctx context.Context, id string) (*login.SessionData, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if d, ok := s.sessions[id]; ok {
		return d, nil
	}
	return nil, nil
}
func (s *fakeStore) Delete(ctx context.Context, id string) error {
	delete(s.sessions, id)
	return nil
}

func TestSessionAuthenticator_NoCredential_Skip(t *testing.T) {
	a := NewSessionAuthenticator(&fakeStore{sessions: map[string]*login.SessionData{}})
	got, err := a.Authenticate(context.Background(), map[string]string{})
	if err != nil {
		t.Fatalf("expected nil err for missing cred, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil result for missing cred, got %+v", got)
	}
}

func TestSessionAuthenticator_ValidSession(t *testing.T) {
	store := &fakeStore{sessions: map[string]*login.SessionData{}}
	store.sessions["sid-1"] = &login.SessionData{
		UserID:    "alice@scitrera.com",
		Email:     "alice@scitrera.com",
		Name:      "Alice",
		Provider:  "azure",
		Claims:    map[string]any{"oid": "azure-oid", "tid": "xyz", "name": "Alice"},
		ExpiresAt: time.Now().Add(time.Hour),
	}

	a := NewSessionAuthenticator(store)
	res, err := a.Authenticate(context.Background(), map[string]string{
		CredKeySession: "sid-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil || !res.Authenticated {
		t.Fatalf("expected authenticated result, got %+v", res)
	}
	if res.Method != "session" {
		t.Errorf("Method: got %q, want session", res.Method)
	}
	if res.Identity.ID != "alice@scitrera.com" {
		t.Errorf("Identity.ID: got %q", res.Identity.ID)
	}
	if got := res.Metadata["tid"]; got != "xyz" {
		t.Errorf("Metadata.tid: got %v, want xyz", got)
	}
	if got := res.Metadata["email"]; got != "alice@scitrera.com" {
		t.Errorf("Metadata.email: got %v", got)
	}
	if got := res.Metadata["provider"]; got != "azure" {
		t.Errorf("Metadata.provider: got %v", got)
	}
}

func TestSessionAuthenticator_UnknownSession(t *testing.T) {
	store := &fakeStore{sessions: map[string]*login.SessionData{}}
	a := NewSessionAuthenticator(store)
	res, err := a.Authenticate(context.Background(), map[string]string{
		CredKeySession: "ghost",
	})
	if err == nil {
		t.Fatal("expected error for unknown session id")
	}
	if res != nil {
		t.Fatalf("expected nil result on unknown session, got %+v", res)
	}
}

func TestSessionAuthenticator_StoreError(t *testing.T) {
	store := &fakeStore{sessions: map[string]*login.SessionData{}, getErr: errors.New("redis down")}
	a := NewSessionAuthenticator(store)
	_, err := a.Authenticate(context.Background(), map[string]string{
		CredKeySession: "sid-1",
	})
	if err == nil {
		t.Fatal("expected error when store returns error")
	}
}

func TestSessionAuthenticator_NilStore(t *testing.T) {
	a := NewSessionAuthenticator(nil)
	_, err := a.Authenticate(context.Background(), map[string]string{CredKeySession: "x"})
	if err == nil {
		t.Fatal("expected error when store is nil")
	}
}
