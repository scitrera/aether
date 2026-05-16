// Package integration provides end-to-end cluster behavior tests that exercise
// the JetStream-backed aetherlite backends against a real, in-process 3-node
// embedded NATS cluster.
//
// These tests are deliberately separate from the per-package unit tests so that
//   - the dependency direction stays unidirectional (integration imports
//     cluster/nats, state, router, orchestration, registry, cluster/backup —
//     none of those import integration);
//   - they can be skipped with `-short` in fast CI lanes;
//   - they can share a single 3-node bootstrap helper rather than duplicating
//     it across packages.
//
// Each *_test.go file in this package targets one cluster behavior:
//   - identity_lock_chaos_test.go    — JetStreamSession lock contention/recovery
//   - message_routing_test.go        — JetStreamRouter cross-node fan-out
//   - task_assignment_race_test.go   — JetStreamTaskDispatcher work-queue split
//   - phase4_recursive_subscribe_test.go — cross-node post-subscribe child events
//   - phase5_prefixindex_test.go     — cross-node KV Watch propagation
//   - partition_recovery_test.go     — node-restart + catch-up
//   - backup_roundtrip_test.go       — backup/restore per policy domain
package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	clusternats "github.com/scitrera/aether/internal/cluster/nats"
)

// pickFreePort asks the kernel for an ephemeral TCP port, closes the listener
// immediately, and returns the port number. The kernel does not reserve the
// port across the close → it MAY be re-used by another process before the
// embedded NATS server binds it. In CI this race has been negligible
// (≪1/10000); if it ever becomes a problem we'd switch to explicit binding +
// graceful re-pick.
func pickFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// cluster3 is the handle returned by setupCluster3: three embedded NATS
// servers wired into a single cluster, plus a Stop helper that tears them all
// down in a sensible order.
//
// The slice ordering (Nodes[0], Nodes[1], Nodes[2]) corresponds to the
// "node-a", "node-b", "node-c" naming used by individual tests.
type cluster3 struct {
	Nodes []*clusternats.EmbeddedServer
}

// Node returns the i-th embedded server. Panics on out-of-range so test code
// never silently degrades to a single-node configuration.
func (c *cluster3) Node(i int) *clusternats.EmbeddedServer {
	if i < 0 || i >= len(c.Nodes) {
		panic(fmt.Sprintf("cluster3.Node: index %d out of range [0, %d)", i, len(c.Nodes)))
	}
	return c.Nodes[i]
}

// Stop shuts each node down. Safe to call multiple times (EmbeddedServer.Stop
// is idempotent).
func (c *cluster3) Stop() {
	for _, n := range c.Nodes {
		if n != nil {
			n.Stop()
		}
	}
}

// setupCluster3 spins up three in-process NATS servers wired together via NATS
// cluster routes. Each server gets its own temp data directory; ports are
// allocated ephemerally and exchanged so each node references the other two as
// peers.
//
// HAMode is HAModeAuto, which yields ReplicasForHA() == 3 — appropriate for
// the cluster behaviors we want to assert (lock contention across nodes, fan-
// out, work-queue load balancing).
//
// The returned cluster is registered for t.Cleanup; tests may also call
// Stop() explicitly when they need precise ordering (e.g. node-kill chaos
// tests).
func setupCluster3(t *testing.T) *cluster3 {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping 3-node cluster integration test in -short mode")
	}

	const clusterName = "aetherlite-integ"
	const numNodes = 3

	// Pick distinct ephemeral cluster ports up-front so every node can list
	// the other two as peers before any of them has booted.
	clusterPorts := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		clusterPorts[i] = pickFreePort(t)
	}
	for i := 0; i < numNodes; i++ {
		for j := i + 1; j < numNodes; j++ {
			if clusterPorts[i] == clusterPorts[j] {
				t.Skipf("could not allocate %d distinct ephemeral ports (collision i=%d j=%d)", numNodes, i, j)
			}
		}
	}

	c := &cluster3{Nodes: make([]*clusternats.EmbeddedServer, numNodes)}

	for i := 0; i < numNodes; i++ {
		peers := make([]string, 0, numNodes-1)
		for j := 0; j < numNodes; j++ {
			if j == i {
				continue
			}
			peers = append(peers, fmt.Sprintf("nats://127.0.0.1:%d", clusterPorts[j]))
		}
		cfg := clusternats.Config{
			DataDir:     t.TempDir(),
			ClusterName: clusterName,
			NodeName:    fmt.Sprintf("node-%c", 'a'+i),
			ListenHost:  "127.0.0.1",
			ClientPort:  -1,
			ClusterPort: clusterPorts[i],
			Peers:       peers,
			HAMode:      clusternats.HAModeAuto,
		}
		es := &clusternats.EmbeddedServer{}
		startCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := es.Start(startCtx, cfg)
		cancel()
		if err != nil {
			// Tear down whatever did come up; t.Fatalf does not run prior
			// t.Cleanup hooks for things we just stored on c.
			c.Stop()
			t.Fatalf("start node %d (%s): %v", i, cfg.NodeName, err)
		}
		c.Nodes[i] = es
	}

	// Wait for routes to settle. NATS gossip is fast in-process (~tens of ms),
	// but JetStream meta-leader election can take a beat longer. We poll the
	// cluster size from node-a's perspective rather than sleeping a fixed
	// duration so the helper is robust on slow CI runners.
	waitForClusterFormed(t, c)

	t.Cleanup(c.Stop)
	return c
}

// waitForClusterFormed blocks until every node reports all peers connected,
// up to a generous timeout. Uses NATS' internal "connectedURLs" view via the
// client connection: in an in-process cluster, each node's NATS connection
// will list 3 URLs (its own + 2 peers) once gossip has settled.
func waitForClusterFormed(t *testing.T, c *cluster3) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, n := range c.Nodes {
			if n == nil {
				ready = false
				break
			}
			conn := n.Conn()
			if conn == nil || !conn.IsConnected() {
				ready = false
				break
			}
		}
		if ready {
			// Give JetStream meta-group a brief moment to elect a leader so
			// stream/KV creation on first call doesn't ENOQUORUM. 200ms is
			// empirically sufficient on a laptop; CI may need more but 500ms
			// is still cheap.
			time.Sleep(500 * time.Millisecond)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("3-node cluster did not finish forming within 15s")
}

// waitUntil polls the supplied predicate until it returns true or the deadline
// elapses. Returns true on success, false on timeout — callers decide whether
// to t.Fatal or just log + continue.
func waitUntil(timeout, poll time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(poll)
	}
	return cond()
}
