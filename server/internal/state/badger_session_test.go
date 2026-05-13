package state

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/pkg/models"
)

func newBadgerSessionRegistry(t *testing.T) *BadgerSessionRegistry {
	t.Helper()
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewBadgerSessionRegistry(db)
}

func testIdentity() models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "test",
		Implementation: "impl",
		Specifier:      "spec",
	}
}

func TestBadgerSession_AcquireAndRelease(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	// Fresh acquire — should succeed.
	acquired, resumed, forced, err := reg.AcquireOrResumeLock(ctx, id, "sess1", "", 0)
	if err != nil {
		t.Fatalf("AcquireOrResumeLock: %v", err)
	}
	if !acquired {
		t.Error("expected acquired=true for first lock")
	}
	if resumed {
		t.Error("expected resumed=false for first lock")
	}
	if forced {
		t.Error("expected forced=false for first lock")
	}

	// Same identity, different sessionID — lock already held, should fail.
	acquired2, _, _, err := reg.AcquireOrResumeLock(ctx, id, "sess2", "", 0)
	if err != nil {
		t.Fatalf("AcquireOrResumeLock second: %v", err)
	}
	if acquired2 {
		t.Error("expected acquired=false when lock held by another session")
	}

	// Release lock.
	if err := reg.ReleaseLock(ctx, id, "sess1"); err != nil {
		t.Fatalf("ReleaseLock: %v", err)
	}

	// Acquire again — lock released, should succeed.
	acquired3, _, _, err := reg.AcquireOrResumeLock(ctx, id, "sess3", "", 0)
	if err != nil {
		t.Fatalf("AcquireOrResumeLock after release: %v", err)
	}
	if !acquired3 {
		t.Error("expected acquired=true after lock released")
	}
}

func TestBadgerSession_Resume(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	// Acquire with sess1.
	acquired, _, _, err := reg.AcquireOrResumeLock(ctx, id, "sess1", "", 0)
	if err != nil {
		t.Fatalf("AcquireOrResumeLock: %v", err)
	}
	if !acquired {
		t.Fatal("expected initial acquire to succeed")
	}

	// Resume with same identity and resumeSessionID="sess1".
	acquired2, resumed, _, err := reg.AcquireOrResumeLock(ctx, id, "sess2", "sess1", 0)
	if err != nil {
		t.Fatalf("AcquireOrResumeLock resume: %v", err)
	}
	if !acquired2 {
		t.Error("expected acquired=true on resume")
	}
	if !resumed {
		t.Error("expected resumed=true on resume")
	}
}

func TestBadgerSession_IsActive(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()
	idStr := id.String()

	// Before acquire — not active.
	active, err := reg.IsActive(ctx, idStr)
	if err != nil {
		t.Fatalf("IsActive before acquire: %v", err)
	}
	if active {
		t.Error("expected IsActive=false before acquire")
	}

	// Acquire — now active.
	if _, _, _, err := reg.AcquireOrResumeLock(ctx, id, "sess1", "", 0); err != nil {
		t.Fatalf("AcquireOrResumeLock: %v", err)
	}
	active, err = reg.IsActive(ctx, idStr)
	if err != nil {
		t.Fatalf("IsActive after acquire: %v", err)
	}
	if !active {
		t.Error("expected IsActive=true after acquire")
	}

	// Release — no longer active.
	if err := reg.ReleaseLock(ctx, id, "sess1"); err != nil {
		t.Fatalf("ReleaseLock: %v", err)
	}
	active, err = reg.IsActive(ctx, idStr)
	if err != nil {
		t.Fatalf("IsActive after release: %v", err)
	}
	if active {
		t.Error("expected IsActive=false after release")
	}
}

func TestBadgerSession_RefreshLock(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	// Acquire lock.
	if _, _, _, err := reg.AcquireOrResumeLock(ctx, id, "sess1", "", 0); err != nil {
		t.Fatalf("AcquireOrResumeLock: %v", err)
	}

	// RefreshLock — we own the lock, should return true.
	refreshed, err := reg.RefreshLock(ctx, id, "sess1")
	if err != nil {
		t.Fatalf("RefreshLock: %v", err)
	}
	if !refreshed {
		t.Error("expected RefreshLock=true while owning lock")
	}

	// Release lock.
	if err := reg.ReleaseLock(ctx, id, "sess1"); err != nil {
		t.Fatalf("ReleaseLock: %v", err)
	}

	// RefreshLock — lock gone, should return false.
	refreshed, err = reg.RefreshLock(ctx, id, "sess1")
	if err != nil {
		t.Fatalf("RefreshLock after release: %v", err)
	}
	if refreshed {
		t.Error("expected RefreshLock=false after release")
	}
}

func TestBadgerSession_RegisterUnregisterSession(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	// RegisterSession — no error.
	if err := reg.RegisterSession(ctx, id, "sess1", "test-gateway-1"); err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}

	// RefreshSession — no error.
	if err := reg.RefreshSession(ctx, "sess1"); err != nil {
		t.Fatalf("RefreshSession: %v", err)
	}

	// UnregisterSession — no error.
	if err := reg.UnregisterSession(ctx, "sess1"); err != nil {
		t.Fatalf("UnregisterSession: %v", err)
	}
}

func TestBadgerSession_GetSessionIdentity(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	if err := reg.RegisterSession(ctx, id, "sess1", "test-gateway-1"); err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}

	got, err := reg.GetSessionIdentity(ctx, "sess1")
	if err != nil {
		t.Fatalf("GetSessionIdentity: %v", err)
	}
	if got.String() != id.String() {
		t.Fatalf("GetSessionIdentity() = %q, want %q", got.String(), id.String())
	}
}
