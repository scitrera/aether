package integration

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/registry"
)

// TestClusterIntegration_Phase5_PrefixIndex_LiveUpdateCrossNode is the
// HEADLINE test for Phase 5's "broadcast gap closed" property: a
// PrefixIndex Watch on node-0 must observe an agent published via node-1's
// KV bucket within ~100ms (we allow 1s of slack for cross-node Raft commit +
// gossip).
//
// Sequence:
//  1. Bring up the 3-node cluster.
//  2. Open the aether_registry KV bucket from node-0's JetStream context with
//     Replicas=3 so updates propagate to every node.
//  3. Start a PrefixIndex Watch on node-0 (the read side).
//  4. PublishAgent via node-1's JetStream context (the write side).
//  5. Assert node-0's PrefixIndex.Lookup returns the new prefix within 1s.
//
// A failure here means the cross-node propagation path is broken — Phase 5
// would regress to per-gateway-local state, and ACL attribution would drift
// across the cluster.
func TestClusterIntegration_Phase5_PrefixIndex_LiveUpdateCrossNode(t *testing.T) {
	c := setupCluster3(t)

	bootstrapCtx, bootstrapCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer bootstrapCancel()

	// Open the registry bucket on BOTH nodes. CreateOrUpdateKeyValue is
	// idempotent; opening it from each node ensures both have a handle, and
	// keeps Replicas consistent at 3.
	kvReader, err := registry.CreateOrOpenRegistryBucket(bootstrapCtx, c.Node(0).JetStream(), 3)
	if err != nil {
		t.Fatalf("open registry bucket on node-0: %v", err)
	}
	kvWriter, err := registry.CreateOrOpenRegistryBucket(bootstrapCtx, c.Node(1).JetStream(), 3)
	if err != nil {
		t.Fatalf("open registry bucket on node-1: %v", err)
	}

	// PrefixIndex on the reader side starts its Watch goroutine against the
	// reader's KV handle.
	idx := registry.NewPrefixIndex()
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	logger := slog.Default()
	if err := idx.StartJetStreamWatch(watchCtx, kvReader, logger); err != nil {
		t.Fatalf("StartJetStreamWatch: %v", err)
	}

	// Confirm the bucket starts empty for the prefix we care about.
	if _, _, ok := idx.Lookup("docmgmt/document/abc"); ok {
		t.Fatal("PrefixIndex unexpectedly returned a match before any agent was published")
	}

	const implementation = "com.example.docmgmt"
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer publishCancel()
	reg := &registry.AgentRegistration{
		Implementation: implementation,
		ResourceSchema: []registry.AgentResourceSchemaEntry{
			{ResourceTypePrefix: "docmgmt/"},
			{ResourceTypePrefix: "chat/"},
		},
	}
	if err := registry.PublishAgent(publishCtx, kvWriter, reg); err != nil {
		t.Fatalf("PublishAgent on node-1: %v", err)
	}

	// Now poll the reader's PrefixIndex; the Watch goroutine should pick the
	// KV update up via cross-node replication and fan-out within ~100ms.
	// Allow up to 2s of slack for slow CI runners.
	const target = "docmgmt/document/abc"
	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		impl, _, ok := idx.Lookup(target)
		return ok && impl == implementation
	}) {
		impl, prefix, ok := idx.Lookup(target)
		t.Fatalf("cross-node PrefixIndex did not converge within 2s: lookup(%q)=(impl=%q prefix=%q ok=%v)",
			target, impl, prefix, ok)
	}

	// Sanity: the chat/ prefix should also resolve.
	if impl, _, ok := idx.Lookup("chat/messages"); !ok || impl != implementation {
		t.Errorf("chat/ prefix did not resolve: impl=%q ok=%v", impl, ok)
	}

	// Sanity: DeleteAgent should propagate too.
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deleteCancel()
	if err := registry.DeleteAgent(deleteCtx, kvWriter, implementation); err != nil {
		t.Fatalf("DeleteAgent: %v", err)
	}
	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		_, _, ok := idx.Lookup(target)
		return !ok
	}) {
		impl, prefix, ok := idx.Lookup(target)
		t.Fatalf("DeleteAgent did not propagate to reader's PrefixIndex within 2s: impl=%q prefix=%q ok=%v",
			impl, prefix, ok)
	}
}
