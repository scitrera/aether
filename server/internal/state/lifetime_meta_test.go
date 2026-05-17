package state

import (
	"context"
	"testing"
	"time"
)

// These tests cover the session-lifetime + client-version fields added
// per the InitConnection versioning spec. They use the Badger backend
// because it has no external dependencies; the JetStream and Redis
// equivalents live alongside the existing infrastructure-bound suites.

func TestBadgerSession_FreshConnectStampsInitialAndZeroReconnects(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	before := time.Now().UnixMilli()
	r, err := reg.AcquireOrResumeLock(ctx, id, "sess1", "", 0, ConnectMeta{
		ClientVersion: "1.2.3",
		ClientSDK:     "go",
	})
	if err != nil {
		t.Fatalf("AcquireOrResumeLock: %v", err)
	}
	after := time.Now().UnixMilli()

	if !r.Acquired {
		t.Fatal("expected Acquired=true on fresh connect")
	}
	if r.Resumed || r.Forced {
		t.Fatalf("unexpected resume/force flags on fresh connect: %+v", r)
	}
	if r.ReconnectionCount != 0 {
		t.Errorf("ReconnectionCount = %d, want 0 on fresh connect", r.ReconnectionCount)
	}
	if r.InitialConnectionUnixMs < before || r.InitialConnectionUnixMs > after {
		t.Errorf("InitialConnectionUnixMs %d outside expected [%d, %d]", r.InitialConnectionUnixMs, before, after)
	}
}

func TestBadgerSession_ResumePreservesInitialAndIncrementsCount(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	r1, err := reg.AcquireOrResumeLock(ctx, id, "sess1", "", 0, ConnectMeta{})
	if err != nil || !r1.Acquired {
		t.Fatalf("initial acquire: r=%+v err=%v", r1, err)
	}
	originalInitial := r1.InitialConnectionUnixMs

	// Small sleep so any time-based bug would surface as a different value.
	time.Sleep(5 * time.Millisecond)

	r2, err := reg.AcquireOrResumeLock(ctx, id, "sess2", "sess1", 0, ConnectMeta{})
	if err != nil || !r2.Acquired || !r2.Resumed {
		t.Fatalf("resume: r=%+v err=%v", r2, err)
	}
	if r2.InitialConnectionUnixMs != originalInitial {
		t.Errorf("InitialConnectionUnixMs after resume = %d, want %d (preserved)", r2.InitialConnectionUnixMs, originalInitial)
	}
	if r2.ReconnectionCount != 1 {
		t.Errorf("ReconnectionCount after 1st resume = %d, want 1", r2.ReconnectionCount)
	}

	// Second resume increments to 2.
	r3, err := reg.AcquireOrResumeLock(ctx, id, "sess3", "sess2", 0, ConnectMeta{})
	if err != nil || !r3.Acquired || !r3.Resumed {
		t.Fatalf("second resume: r=%+v err=%v", r3, err)
	}
	if r3.InitialConnectionUnixMs != originalInitial {
		t.Errorf("InitialConnectionUnixMs after 2nd resume = %d, want %d (preserved)", r3.InitialConnectionUnixMs, originalInitial)
	}
	if r3.ReconnectionCount != 2 {
		t.Errorf("ReconnectionCount after 2nd resume = %d, want 2", r3.ReconnectionCount)
	}
}

func TestBadgerSession_ForceTakeoverResetsLifetimeFields(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	r1, err := reg.AcquireOrResumeLock(ctx, id, "sess-victim", "", 0, ConnectMeta{})
	if err != nil || !r1.Acquired {
		t.Fatalf("initial acquire: r=%+v err=%v", r1, err)
	}
	originalInitial := r1.InitialConnectionUnixMs

	// Force takeover: threshold larger than full LockTTL guarantees the
	// "remaining TTL < threshold" branch fires regardless of timing.
	time.Sleep(5 * time.Millisecond)
	r2, err := reg.AcquireOrResumeLock(ctx, id, "sess-taker", "", (LockTTL * 2).Milliseconds(), ConnectMeta{})
	if err != nil {
		t.Fatalf("force takeover: %v", err)
	}
	if !r2.Acquired || !r2.Forced || r2.Resumed {
		t.Fatalf("expected forced takeover (acquired, forced, !resumed), got %+v", r2)
	}
	if r2.ReconnectionCount != 0 {
		t.Errorf("ReconnectionCount after force takeover = %d, want 0 (reset)", r2.ReconnectionCount)
	}
	if r2.InitialConnectionUnixMs == originalInitial {
		t.Errorf("InitialConnectionUnixMs after force takeover should be fresh, but matches original %d", originalInitial)
	}
}

func TestBadgerSession_MissingClientVersionLeavesFieldsEmpty(t *testing.T) {
	ctx := context.Background()
	reg := newBadgerSessionRegistry(t)
	id := testIdentity()

	// Empty ConnectMeta simulates an older SDK that doesn't send version
	// fields. AcquireOrResumeLock must still succeed and the lifetime
	// fields must still populate.
	r, err := reg.AcquireOrResumeLock(ctx, id, "sess-anon", "", 0, ConnectMeta{})
	if err != nil || !r.Acquired {
		t.Fatalf("acquire with empty meta: r=%+v err=%v", r, err)
	}
	if r.InitialConnectionUnixMs == 0 {
		t.Error("InitialConnectionUnixMs must be set even when client meta is empty")
	}
}
