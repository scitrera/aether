package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/scitrera/aether/sdk/go/coord"
)

// TestCoordLeaderElector_AcquiresLeadership verifies a single elector acquires
// leadership over a shared locker.
func TestCoordLeaderElector_AcquiresLeadership(t *testing.T) {
	lk := coord.NewMemoryLocker()
	le := NewCoordLeaderElector(lk, workflowLeaderKey, "instance-a")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	le.Start(ctx)

	if !waitForLeader(2*time.Second, le.IsLeader) {
		t.Fatal("instance-a did not become leader within 2s")
	}
	_ = le.Shutdown(context.Background())
}

// TestCoordLeaderElector_SingleLeaderAndFailover verifies that two electors
// sharing a locker never both lead, and that the survivor takes over after the
// leader steps down.
func TestCoordLeaderElector_SingleLeaderAndFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing-based leader failover test in -short mode")
	}
	lk := coord.NewMemoryLocker()
	a := NewCoordLeaderElector(lk, workflowLeaderKey, "instance-a")
	b := NewCoordLeaderElector(lk, workflowLeaderKey, "instance-b")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Start(ctx)
	if !waitForLeader(2*time.Second, a.IsLeader) {
		t.Fatal("instance-a did not become leader")
	}
	b.Start(ctx)
	// b must not seize leadership while a holds it.
	time.Sleep(300 * time.Millisecond)
	if b.IsLeader() {
		t.Fatal("instance-b became leader while instance-a holds it")
	}

	// a steps down → b takes over (Shutdown eagerly releases the lease).
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatalf("a.Shutdown: %v", err)
	}
	if !waitForLeader(workflowLeaderTTL+5*time.Second, b.IsLeader) {
		t.Fatal("instance-b did not take over after instance-a stepped down")
	}
	_ = b.Shutdown(context.Background())
}

func waitForLeader(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}
