package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/pkg/models"
)

// TestClusterIntegration_IdentityLock_ExactlyOneWinner asserts that when N
// goroutines (each holding a JetStreamSession bound to a different cluster
// node's JetStream context) race to AcquireOrResumeLock for the same identity,
// EXACTLY ONE wins (acquired=true) and the other N-1 see acquired=false with
// no error.
//
// This is the cluster-scope generalisation of the unit test in
// internal/state/jetstream_session_test.go which only races goroutines
// against a single JetStream context. The cross-node version exercises the
// full Raft-style replication path: every Create attempt is replicated to the
// JetStream meta-leader, and only one wins the kv.Create call.
func TestClusterIntegration_IdentityLock_ExactlyOneWinner(t *testing.T) {
	c := setupCluster3(t)

	const N = 20
	sessions := make([]*state.JetStreamSession, N)
	// Round-robin goroutines across the 3 nodes so contention is genuinely
	// cross-node rather than all happening on one server.
	for i := 0; i < N; i++ {
		nodeIdx := i % len(c.Nodes)
		js := c.Node(nodeIdx).JetStream()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		s, err := state.NewJetStreamSession(ctx, js, state.JetStreamSessionConfig{Replicas: 3})
		cancel()
		if err != nil {
			t.Fatalf("NewJetStreamSession on node %d: %v", nodeIdx, err)
		}
		sessions[i] = s
	}

	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "chaos-ws",
		Implementation: "racer",
		Specifier:      "v1",
	}

	// Use a barrier so every goroutine kicks AcquireOrResumeLock at the same
	// wall-clock instant — gives the broadest contention window.
	var start sync.WaitGroup
	start.Add(1)

	var (
		acquired atomic.Int64
		rejected atomic.Int64
		errs     atomic.Int64
		wg       sync.WaitGroup
	)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int, s *state.JetStreamSession) {
			defer wg.Done()
			start.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			sessionID := fmt.Sprintf("sess-racer-%02d", i)
			r, err := s.AcquireOrResumeLock(ctx, identity, sessionID, "", 0, state.ConnectMeta{})
			ok := r.Acquired
			if err != nil {
				t.Logf("goroutine %d AcquireOrResumeLock error: %v", i, err)
				errs.Add(1)
				return
			}
			if ok {
				acquired.Add(1)
			} else {
				rejected.Add(1)
			}
		}(i, sessions[i])
	}

	start.Done()
	wg.Wait()

	if got := acquired.Load(); got != 1 {
		t.Fatalf("expected exactly 1 acquired, got %d (rejected=%d errs=%d)",
			got, rejected.Load(), errs.Load())
	}
	if got := rejected.Load(); got != int64(N-1) {
		t.Fatalf("expected %d rejected, got %d (acquired=%d errs=%d)",
			N-1, got, acquired.Load(), errs.Load())
	}
	if got := errs.Load(); got != 0 {
		t.Fatalf("expected 0 hard errors during contention, got %d", got)
	}
}

// TestClusterIntegration_IdentityLock_NodeKillMidAcquire models the failure
// mode where a holder vanishes (node killed) and another node must take over
// once the logical TTL expires. We use force-takeover (with a threshold longer
// than the remaining TTL) instead of waiting LockTTL because LockTTL is 30s in
// production and waiting that out in a CI test would be wasteful.
//
// Sequence:
//  1. Acquire lock via a JetStreamSession on node-1 (call it the "victim").
//  2. Attempt acquire from node-0's session → expect rejected (lock healthy).
//  3. Stop node-1 (simulating a crash). The lock value is still in the KV
//     bucket on the surviving replicas, but no one is refreshing it.
//  4. From node-0, retry AcquireOrResumeLock with a large
//     forceTakeoverThreshold so the surviving session sees "remaining TTL <
//     threshold" and takes ownership via CAS.
//  5. Assert the retry returned acquired=true forced=true.
func TestClusterIntegration_IdentityLock_NodeKillMidAcquire(t *testing.T) {
	c := setupCluster3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build sessions on node-0 (survivor) and node-1 (victim). Both use
	// Replicas=3 so the lock bucket survives node-1 going away.
	survivorJS := c.Node(0).JetStream()
	victimJS := c.Node(1).JetStream()

	survivor, err := state.NewJetStreamSession(ctx, survivorJS, state.JetStreamSessionConfig{Replicas: 3})
	if err != nil {
		t.Fatalf("survivor session: %v", err)
	}
	victim, err := state.NewJetStreamSession(ctx, victimJS, state.JetStreamSessionConfig{Replicas: 3})
	if err != nil {
		t.Fatalf("victim session: %v", err)
	}

	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "kill-ws",
		Implementation: "killable",
		Specifier:      "v1",
	}

	// Step 1: victim acquires.
	rv, err := victim.AcquireOrResumeLock(ctx, identity, "sess-victim", "", 0, state.ConnectMeta{})
	ok := rv.Acquired
	if err != nil {
		t.Fatalf("victim acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected victim to acquire the lock")
	}

	// Step 2: survivor's first attempt should be rejected because the
	// victim is still holding a healthy lease.
	rs, err := survivor.AcquireOrResumeLock(ctx, identity, "sess-survivor", "", 0, state.ConnectMeta{})
	ok = rs.Acquired
	_ = err
	if err != nil {
		t.Fatalf("survivor pre-kill acquire: %v", err)
	}
	if ok {
		t.Fatal("expected survivor pre-kill acquire to be rejected (victim holds healthy lock)")
	}

	// Step 3: kill the victim. The lock entry remains in the KV bucket on
	// the surviving replicas; its expires_unix_ms is still in the future,
	// but no one will refresh it.
	c.Node(1).Stop()

	// Allow the cluster's view of online peers to update. NATS gossip is
	// fast in-process; 1s is plenty.
	time.Sleep(1 * time.Second)

	// Step 4: survivor retries with a large forceTakeoverThreshold so the
	// in-process expiry check fires "remaining TTL < threshold" and it
	// takes over via CAS. The default LockTTL in state is on the order of
	// tens of seconds; we pass a threshold larger than that to force the
	// branch deterministically without waiting.
	var (
		acquired bool
		forced   bool
	)
	if got := waitUntil(15*time.Second, 100*time.Millisecond, func() bool {
		r, err := survivor.AcquireOrResumeLock(ctx, identity, "sess-survivor", "", 24*60*60*1000, state.ConnectMeta{})
		ok, f := r.Acquired, r.Forced
		if err != nil {
			t.Logf("survivor retry error (will retry): %v", err)
			return false
		}
		acquired, forced = ok, f
		return acquired
	}); !got {
		t.Fatalf("survivor did not take over within timeout (acquired=%v forced=%v)", acquired, forced)
	}
	if !forced {
		t.Fatalf("expected forced=true on takeover, got forced=false (acquired=%v)", acquired)
	}
}
