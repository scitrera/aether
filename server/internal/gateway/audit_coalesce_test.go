package gateway

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/audit"
)

// coalescableEvent builds a successful OpMessageReceived event for the given
// sender/target — the canonical high-volume coalescable case.
func coalescableEvent(sender, target string) *audit.AuditEvent {
	return audit.NewMessageEvent("agent", sender, audit.OpMessageReceived, target, "ws", uuid.Nil, true, "", nil)
}

func TestAuditCoalescer_FirstPassesRepeatDropped(t *testing.T) {
	c := newAuditCoalescer(time.Minute)
	ev := coalescableEvent("ag::ws::a::1", "ag::ws::b::1")

	if !c.shouldLog(ev) {
		t.Fatal("first event in window should pass")
	}
	if c.shouldLog(ev) {
		t.Fatal("second identical event in window should be dropped")
	}
	if c.shouldLog(ev) {
		t.Fatal("third identical event in window should be dropped")
	}
}

func TestAuditCoalescer_DifferentKeyPasses(t *testing.T) {
	c := newAuditCoalescer(time.Minute)

	// Different sender.
	if !c.shouldLog(coalescableEvent("ag::ws::a::1", "ag::ws::b::1")) {
		t.Fatal("first key should pass")
	}
	if !c.shouldLog(coalescableEvent("ag::ws::a::2", "ag::ws::b::1")) {
		t.Fatal("different sender should pass")
	}
	// Different target.
	if !c.shouldLog(coalescableEvent("ag::ws::a::1", "ag::ws::b::2")) {
		t.Fatal("different target should pass")
	}
	// Different op (but still coalescable) — distinct key.
	ev := audit.NewMessageEvent("agent", "ag::ws::a::1", audit.OpMessageRouted, "ag::ws::b::1", "ws", uuid.Nil, true, "", nil)
	if !c.shouldLog(ev) {
		t.Fatal("different op should pass")
	}
}

func TestAuditCoalescer_WindowExpiryReadmits(t *testing.T) {
	c := newAuditCoalescer(20 * time.Millisecond)
	ev := coalescableEvent("ag::ws::a::1", "ag::ws::b::1")

	if !c.shouldLog(ev) {
		t.Fatal("first event should pass")
	}
	if c.shouldLog(ev) {
		t.Fatal("second within-window event should be dropped")
	}

	time.Sleep(40 * time.Millisecond)

	if !c.shouldLog(ev) {
		t.Fatal("event after window expiry should be re-admitted")
	}
	// And immediately suppressed again in the fresh window.
	if c.shouldLog(ev) {
		t.Fatal("repeat in the fresh window should be dropped")
	}
}

func TestAuditCoalescer_NonCoalescableOpsAlwaysPass(t *testing.T) {
	c := newAuditCoalescer(time.Minute)

	// Tunnel + non-message ops are never coalesced.
	nonCoalescable := []string{
		audit.OpTunnelOpened,
		audit.OpTunnelClosed,
		audit.OpMessageDelivered,
		audit.OpKVPut,
		audit.OpTaskCreate,
		audit.OpAuthMTLSSuccess,
		audit.OpConnectionEstablished,
	}
	for _, op := range nonCoalescable {
		ev := audit.NewMessageEvent("agent", "ag::ws::a::1", op, "ag::ws::b::1", "ws", uuid.Nil, true, "", nil)
		for i := 0; i < 5; i++ {
			if !c.shouldLog(ev) {
				t.Fatalf("op %q must always pass (iteration %d)", op, i)
			}
		}
	}
}

func TestAuditCoalescer_FailureAlwaysPasses(t *testing.T) {
	c := newAuditCoalescer(time.Minute)

	// success=false on an otherwise-coalescable op must always be audited.
	ev := audit.NewMessageEvent("agent", "ag::ws::a::1", audit.OpMessageReceived, "ag::ws::b::1", "ws", uuid.Nil, false, "boom", nil)
	for i := 0; i < 5; i++ {
		if !c.shouldLog(ev) {
			t.Fatalf("failure event must always pass (iteration %d)", i)
		}
	}
}

func TestAuditCoalescer_DisabledWindowIsPassthrough(t *testing.T) {
	// window <= 0 -> nil coalescer -> passthrough (every event logged).
	if c := newAuditCoalescer(0); c != nil {
		t.Fatal("window 0 should produce a nil coalescer")
	}
	if c := newAuditCoalescer(-1 * time.Second); c != nil {
		t.Fatal("negative window should produce a nil coalescer")
	}

	var c *auditCoalescer // nil
	ev := coalescableEvent("ag::ws::a::1", "ag::ws::b::1")
	for i := 0; i < 5; i++ {
		if !c.shouldLog(ev) {
			t.Fatalf("nil coalescer must pass every event (iteration %d)", i)
		}
	}
}

func TestAuditCoalescer_BoundedUnderManyKeys(t *testing.T) {
	c := newAuditCoalescer(time.Hour) // long window so nothing expires

	// Push far more distinct keys than the total cap so eviction must kick in.
	total := coalesceShardCount * coalesceMaxEntriesPerShard * 3
	for i := 0; i < total; i++ {
		ev := coalescableEvent(fmt.Sprintf("ag::ws::a::%d", i), "ag::ws::b::1")
		c.shouldLog(ev)
	}

	// Every shard must be at or under the per-shard cap — proof the map cannot
	// grow without bound.
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n := len(sh.last)
		sh.mu.Unlock()
		if n > coalesceMaxEntriesPerShard {
			t.Fatalf("shard %d holds %d entries, exceeds cap %d", i, n, coalesceMaxEntriesPerShard)
		}
	}
}

func TestAuditCoalescer_ConcurrentAccess(t *testing.T) {
	c := newAuditCoalescer(time.Minute)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				ev := coalescableEvent(fmt.Sprintf("ag::ws::a::%d", (g*7+i)%50), "ag::ws::b::1")
				c.shouldLog(ev)
			}
		}(g)
	}
	wg.Wait()
	// Success = no race/panic (run under -race). No assertion needed beyond
	// completion.
}

// The gateway-level tests below reuse captureAuditStore (defined in
// denial_audit_test.go) — a minimal auditstore.Store that records LogEvent
// calls — to observe auditLog's coalescing gate end-to-end.

func TestGatewayAuditLog_SuppressesRepeatWithinWindow(t *testing.T) {
	store := &captureAuditStore{}
	s := &GatewayServer{
		auditLogger:    store,
		auditCoalescer: newAuditCoalescer(time.Minute),
	}
	ctx := context.Background()

	ev := coalescableEvent("ag::ws::a::1", "ag::ws::b::1")
	s.auditLog(ctx, ev)
	s.auditLog(ctx, ev)
	s.auditLog(ctx, ev)

	if got := len(store.captured()); got != 1 {
		t.Fatalf("auditLog wrote %d events, want 1 (2 suppressed)", got)
	}

	// A failure event for the same key is always written.
	fail := audit.NewMessageEvent("agent", "ag::ws::a::1", audit.OpMessageReceived, "ag::ws::b::1", "ws", uuid.Nil, false, "boom", nil)
	s.auditLog(ctx, fail)
	if got := len(store.captured()); got != 2 {
		t.Fatalf("auditLog wrote %d events, want 2 (failure not suppressed)", got)
	}
}

func TestGatewayAuditLog_DisabledCoalescerLogsEvery(t *testing.T) {
	store := &captureAuditStore{}
	s := &GatewayServer{
		auditLogger:    store,
		auditCoalescer: nil, // coalescing disabled
	}
	ctx := context.Background()

	ev := coalescableEvent("ag::ws::a::1", "ag::ws::b::1")
	s.auditLog(ctx, ev)
	s.auditLog(ctx, ev)
	s.auditLog(ctx, ev)

	if got := len(store.captured()); got != 3 {
		t.Fatalf("auditLog wrote %d events, want 3 (no coalescing)", got)
	}
}
