package state

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/pkg/models"
)

// startTestJetStream boots an in-process NATS server with JetStream enabled
// on an OS-assigned port and returns a connected JetStream context. Cleanup
// is registered via t.Cleanup.
func startTestJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	opts := &natsserver.Options{
		Host:               "127.0.0.1",
		Port:               -1,
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 64 * 1024 * 1024,
		JetStreamMaxStore:  256 * 1024 * 1024,
		NoSigs:             true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server new: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not become ready")
	}
	conn, err := natsgo.Connect("", natsgo.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		t.Fatalf("jetstream new: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Drain()
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return js
}

// newTestJetStreamSession creates a JetStreamSession against a fresh
// embedded JetStream context.
func newTestJetStreamSession(t *testing.T) *JetStreamSession {
	t.Helper()
	js := startTestJetStream(t)
	s, err := NewJetStreamSession(context.Background(), js, JetStreamSessionConfig{Replicas: 1})
	if err != nil {
		t.Fatalf("new jetstream session: %v", err)
	}
	return s
}

func testAgentIdentity(workspace, impl, spec string) models.Identity {
	return models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      workspace,
		Implementation: impl,
		Specifier:      spec,
	}
}

func testServiceIdentity(impl, spec string) models.Identity {
	return models.Identity{
		Type:           models.PrincipalService,
		Implementation: impl,
		Specifier:      spec,
	}
}

// ---------------------------------------------------------------------------
// Acquire / Release happy path
// ---------------------------------------------------------------------------

func TestJetStreamSession_LockAcquireRelease_HappyPath(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws", "impl", "spec")
	acquired, resumed, forced, err := acquireLegacy(s, ctx, id, "sess-1", "", 0)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !acquired || resumed || forced {
		t.Fatalf("expected acquired=true resumed=false forced=false, got (%v,%v,%v)", acquired, resumed, forced)
	}

	active, err := s.IsActive(ctx, id.String())
	if err != nil {
		t.Fatalf("IsActive: %v", err)
	}
	if !active {
		t.Fatal("expected IsActive=true after acquire")
	}

	if err := s.ReleaseLock(ctx, id, "sess-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	active, err = s.IsActive(ctx, id.String())
	if err != nil {
		t.Fatalf("IsActive post-release: %v", err)
	}
	if active {
		t.Fatal("expected IsActive=false after release")
	}
}

// ---------------------------------------------------------------------------
// Duplicate acquire rejection
// ---------------------------------------------------------------------------

func TestJetStreamSession_LockAcquire_Duplicate_Rejected(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws", "impl", "dup")
	if acquired, _, _, err := acquireLegacy(s, ctx, id, "sess-A", "", 0); err != nil || !acquired {
		t.Fatalf("first acquire must succeed: acquired=%v err=%v", acquired, err)
	}

	acquired, resumed, forced, err := acquireLegacy(s, ctx, id, "sess-B", "", 0)
	if err != nil {
		t.Fatalf("second acquire returned error: %v", err)
	}
	if acquired || resumed || forced {
		t.Fatalf("expected duplicate acquire to be rejected, got (acquired=%v resumed=%v forced=%v)", acquired, resumed, forced)
	}
}

// ---------------------------------------------------------------------------
// Resume with matching session ID
// ---------------------------------------------------------------------------

func TestJetStreamSession_LockResume_SameSessionID(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws", "impl", "resume")
	if acquired, _, _, err := acquireLegacy(s, ctx, id, "sess-orig", "", 0); err != nil || !acquired {
		t.Fatalf("initial acquire: acquired=%v err=%v", acquired, err)
	}

	// Caller "remembers" sess-orig and reconnects with a new sessionID.
	acquired, resumed, forced, err := acquireLegacy(s, ctx, id, "sess-new", "sess-orig", 0)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !acquired || !resumed || forced {
		t.Fatalf("expected acquired=true resumed=true forced=false, got (%v,%v,%v)", acquired, resumed, forced)
	}
}

// ---------------------------------------------------------------------------
// Force takeover when remaining TTL is below threshold
// ---------------------------------------------------------------------------

func TestJetStreamSession_LockForceTakeover_BelowThreshold(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws", "impl", "force")
	if acquired, _, _, err := acquireLegacy(s, ctx, id, "sess-orig", "", 0); err != nil || !acquired {
		t.Fatalf("initial acquire: acquired=%v err=%v", acquired, err)
	}

	// LockTTL is 30s; pass a threshold larger than the remaining TTL to trigger
	// the force-takeover branch deterministically without sleeping.
	thresholdMs := LockTTL.Milliseconds() + 5000
	acquired, resumed, forced, err := acquireLegacy(s, ctx, id, "sess-new", "", thresholdMs)
	if err != nil {
		t.Fatalf("force takeover: %v", err)
	}
	if !acquired || resumed || !forced {
		t.Fatalf("expected acquired=true resumed=false forced=true, got (%v,%v,%v)", acquired, resumed, forced)
	}
}

// ---------------------------------------------------------------------------
// RefreshLock preserves identity and extends TTL
// ---------------------------------------------------------------------------

func TestJetStreamSession_RefreshLock_PreservesIdentity_ExtendsTTL(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws", "impl", "refresh")
	if acquired, _, _, err := acquireLegacy(s, ctx, id, "sess-r", "", 0); err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}

	// Read stored expiry before refresh.
	entry, err := s.locks.Get(ctx, encodeKVKey(id.String()))
	if err != nil {
		t.Fatalf("get pre-refresh: %v", err)
	}
	beforeLV, err := decodeLockValue(entry.Value())
	if err != nil {
		t.Fatalf("decode pre-refresh: %v", err)
	}

	// Sleep just long enough for the wall clock to advance past the
	// 1-millisecond resolution we encode in expires_unix_ms.
	time.Sleep(5 * time.Millisecond)

	ok, err := s.RefreshLock(ctx, id, "sess-r")
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if !ok {
		t.Fatal("expected RefreshLock=true for owned lock")
	}

	entry2, err := s.locks.Get(ctx, encodeKVKey(id.String()))
	if err != nil {
		t.Fatalf("get post-refresh: %v", err)
	}
	afterLV, err := decodeLockValue(entry2.Value())
	if err != nil {
		t.Fatalf("decode post-refresh: %v", err)
	}
	if afterLV.SessionID != "sess-r" {
		t.Fatalf("session ID corrupted: got %q want %q", afterLV.SessionID, "sess-r")
	}
	if afterLV.ExpiresUnixMs <= beforeLV.ExpiresUnixMs {
		t.Fatalf("expected expires_unix_ms to increase: before=%d after=%d", beforeLV.ExpiresUnixMs, afterLV.ExpiresUnixMs)
	}

	// Refreshing with a wrong session must fail (return ok=false, no error).
	ok, err = s.RefreshLock(ctx, id, "sess-other")
	if err != nil {
		t.Fatalf("refresh wrong session: %v", err)
	}
	if ok {
		t.Fatal("expected RefreshLock=false when caller no longer owns the lock")
	}
}

// ---------------------------------------------------------------------------
// ReleaseLock rejects wrong owner
// ---------------------------------------------------------------------------

func TestJetStreamSession_ReleaseLock_WrongOwner_Rejected(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws", "impl", "rel-wrong")
	if acquired, _, _, err := acquireLegacy(s, ctx, id, "sess-owner", "", 0); err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}

	// Attempt release with mismatching sessionID — must be a no-op (no error).
	if err := s.ReleaseLock(ctx, id, "sess-stranger"); err != nil {
		t.Fatalf("release wrong owner: %v", err)
	}

	// Lock should still be held.
	active, err := s.IsActive(ctx, id.String())
	if err != nil {
		t.Fatalf("IsActive: %v", err)
	}
	if !active {
		t.Fatal("expected lock to remain after wrong-owner release attempt")
	}

	// Correct owner can still release.
	if err := s.ReleaseLock(ctx, id, "sess-owner"); err != nil {
		t.Fatalf("release correct owner: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Session register / lookup round-trip
// ---------------------------------------------------------------------------

func TestJetStreamSession_RegisterSession_LookupRoundTrip(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws-a", "implX", "spec1")

	// Acquire lock so GetSessionGateway can resolve.
	if _, _, _, err := acquireLegacy(s, ctx, id, "sess-1", "", 0); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if err := s.RegisterSession(ctx, id, "sess-1", "gw-7"); err != nil {
		t.Fatalf("register: %v", err)
	}

	got, err := s.GetSessionIdentity(ctx, "sess-1")
	if err != nil {
		t.Fatalf("get identity: %v", err)
	}
	if got.String() != id.String() {
		t.Fatalf("identity mismatch: got %q want %q", got.String(), id.String())
	}

	// GetSessionGateway uses lock.session_id to find the session record. The
	// AcquireOrResumeLock impl sets lock.session_id to the same string the
	// caller will then pass to RegisterSession, so this round-trips.
	gw, err := s.GetSessionGateway(ctx, id)
	if err != nil {
		t.Fatalf("get gateway: %v", err)
	}
	if gw != "gw-7" {
		t.Fatalf("gateway_id mismatch: got %q want %q", gw, "gw-7")
	}

	if err := s.UnregisterSession(ctx, "sess-1"); err != nil {
		t.Fatalf("unregister: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tunnel pin lifecycle
// ---------------------------------------------------------------------------

func TestJetStreamSession_TunnelPin_SetGetRefreshDelete(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	tunnelID := "tnl-abc-123"
	svc := "sv::impl::node-1"

	if err := s.SetTunnelPin(ctx, tunnelID, svc, 5*time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := s.GetTunnelPin(ctx, tunnelID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != svc {
		t.Fatalf("get mismatch: got %q want %q", got, svc)
	}

	// Refresh extends the expiry; reading should still return the value.
	if err := s.RefreshTunnelPin(ctx, tunnelID, 10*time.Minute); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, err = s.GetTunnelPin(ctx, tunnelID)
	if err != nil {
		t.Fatalf("get post-refresh: %v", err)
	}
	if got != svc {
		t.Fatalf("get post-refresh mismatch: got %q want %q", got, svc)
	}

	// Refresh on missing pin is a no-op (no error).
	if err := s.RefreshTunnelPin(ctx, "missing-tunnel", time.Minute); err != nil {
		t.Fatalf("refresh missing: %v", err)
	}

	if err := s.DeleteTunnelPin(ctx, tunnelID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err = s.GetTunnelPin(ctx, tunnelID)
	if err != nil {
		t.Fatalf("get post-delete: %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty pin after delete, got %q", got)
	}

	// Delete is idempotent.
	if err := s.DeleteTunnelPin(ctx, tunnelID); err != nil {
		t.Fatalf("delete idempotent: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Request pin lifecycle
// ---------------------------------------------------------------------------

func TestJetStreamSession_RequestPin_SetGetRefreshDelete(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	requestID := "req-xyz-9"
	pin := "ag::ws::impl::spec|sv::impl::node-1"

	if err := s.SetRequestPin(ctx, requestID, pin, 30*time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.GetRequestPin(ctx, requestID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != pin {
		t.Fatalf("get mismatch: got %q want %q", got, pin)
	}

	if err := s.RefreshRequestPin(ctx, requestID, 60*time.Second); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, err = s.GetRequestPin(ctx, requestID)
	if err != nil {
		t.Fatalf("get post-refresh: %v", err)
	}
	if got != pin {
		t.Fatalf("get post-refresh mismatch: got %q want %q", got, pin)
	}

	if err := s.RefreshRequestPin(ctx, "missing-req", time.Second); err != nil {
		t.Fatalf("refresh missing: %v", err)
	}

	if err := s.DeleteRequestPin(ctx, requestID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err = s.GetRequestPin(ctx, requestID)
	if err != nil {
		t.Fatalf("get post-delete: %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty pin after delete, got %q", got)
	}

	if err := s.DeleteRequestPin(ctx, requestID); err != nil {
		t.Fatalf("delete idempotent: %v", err)
	}
}

// ---------------------------------------------------------------------------
// FindHealthyServiceInstances filters expired/foreign locks
// ---------------------------------------------------------------------------

func TestJetStreamSession_FindHealthyServiceInstances_FiltersExpired(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	// Two healthy service instances of impl=worker
	sv1 := testServiceIdentity("worker", "node-1")
	sv2 := testServiceIdentity("worker", "node-2")
	// One healthy service of a different impl that must NOT be returned.
	other := testServiceIdentity("other", "node-9")
	// One agent of impl=worker — sv prefix filter must reject this.
	agent := testAgentIdentity("ws", "worker", "agent-1")

	for _, id := range []models.Identity{sv1, sv2, other, agent} {
		if acq, _, _, err := acquireLegacy(s, ctx, id, id.String(), "", 0); err != nil || !acq {
			t.Fatalf("acquire %s: acquired=%v err=%v", id.String(), acq, err)
		}
	}

	// Inject a logically-expired sv::worker::stale entry by writing directly
	// to the locks bucket with an expired expires_unix_ms.
	stale := testServiceIdentity("worker", "stale")
	expiredLV := lockValue{
		GatewayID:      "stale",
		SessionID:      "stale",
		AcquiredUnixMs: nowUnixMs() - 60000,
		ExpiresUnixMs:  nowUnixMs() - 1000,
	}
	encoded, err := encodeLockValue(expiredLV)
	if err != nil {
		t.Fatalf("encode expired: %v", err)
	}
	if _, err := s.locks.Put(ctx, encodeKVKey(stale.String()), encoded); err != nil {
		t.Fatalf("put expired: %v", err)
	}

	out, err := s.FindHealthyServiceInstances(ctx, "worker", 0)
	if err != nil {
		t.Fatalf("find: %v", err)
	}

	// Build a set for assertions.
	have := make(map[string]bool, len(out))
	for _, v := range out {
		have[v] = true
	}

	if !have[sv1.String()] || !have[sv2.String()] {
		t.Fatalf("expected sv1 and sv2 in results, got %v", out)
	}
	if have[other.String()] {
		t.Fatalf("expected impl=other to be filtered out, got %v", out)
	}
	if have[agent.String()] {
		t.Fatalf("expected agent identity to be filtered out, got %v", out)
	}
	if have[stale.String()] {
		t.Fatalf("expected expired sv::worker::stale to be filtered out, got %v", out)
	}

	// With minRemaining well above LockTTL, no entry should qualify.
	out, err = s.FindHealthyServiceInstances(ctx, "worker", LockTTL+time.Hour)
	if err != nil {
		t.Fatalf("find with high minRemaining: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected zero healthy with high minRemaining, got %v", out)
	}
}

// ---------------------------------------------------------------------------
// Concurrent lock race — exactly one winner
// ---------------------------------------------------------------------------

func TestJetStreamSession_ConcurrentLockRace_ExactlyOneWinner(t *testing.T) {
	s := newTestJetStreamSession(t)
	ctx := context.Background()

	id := testAgentIdentity("ws", "impl", "race")

	const N = 16
	var wg sync.WaitGroup
	wg.Add(N)

	var winners atomic.Int32
	var errs atomic.Int32
	startCh := make(chan struct{})

	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-startCh // unblock all at once
			sessionID := "sess-racer-"
			// append index without using fmt for speed
			for _, c := range []byte{byte('0' + (i / 10)), byte('0' + (i % 10))} {
				sessionID += string(c)
			}
			acquired, _, _, err := acquireLegacy(s, ctx, id, sessionID, "", 0)
			if err != nil {
				errs.Add(1)
				return
			}
			if acquired {
				winners.Add(1)
			}
		}()
	}

	close(startCh)
	wg.Wait()

	if e := errs.Load(); e != 0 {
		t.Fatalf("unexpected errors during race: %d", e)
	}
	if got := winners.Load(); got != 1 {
		t.Fatalf("expected exactly one winner, got %d", got)
	}
}
