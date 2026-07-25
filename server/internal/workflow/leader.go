package workflow

import (
	"context"
	"time"

	"github.com/scitrera/aether/server/internal/kv"
	"github.com/scitrera/aether/sdk/go/coord"
)

// LeaderElector elects a single active workflow-server instance among replicas
// so only one runs the scheduler and DAG monitor at a time.
//
// The election runs on the shared coord primitive (atomic SetNX/CompareAndSet
// lease lock) over a coord.Locker. In production the locker is the
// WorkflowEngine client's KV locker, so leadership is coordinated through the
// gateway's KV store — Redis in standard mode, Badger/JetStream in aetherlite —
// without the workflow server holding its own Redis connection. The same logic
// therefore yields a correct single leader in every deployment mode.
type LeaderElector interface {
	// Start launches the election loop in the background. Idempotent.
	Start(ctx context.Context)
	// Shutdown stops the election loop and best-effort releases leadership so a
	// successor can take over without waiting for the lease to expire.
	Shutdown(ctx context.Context) error
	// IsLeader reports whether this instance currently holds leadership.
	IsLeader() bool
}

const (
	// workflowLeaderTTL is the leadership lease lifetime. A crashed leader is
	// replaced after at most this long.
	workflowLeaderTTL = 30 * time.Second
	// workflowLeaderRenew is how often the leader renews its lease.
	workflowLeaderRenew = 10 * time.Second
)

// workflowLeaderKey is the coordination key the election lock lives under, in
// the reserved infra-coordination namespace (so the gateway grants the
// WorkflowEngine access via its infra fast-path). Used on the shared global
// scope so every replica rendezvous on the same key.
var workflowLeaderKey = kv.ReservedCoordKeyPrefix + "workflow/leader"

// NewCoordLeaderElector builds a leader elector over any coord.Locker (and thus
// any KV backend). instanceID is folded into the fencing token for log
// attribution; a random suffix guarantees per-replica uniqueness.
func NewCoordLeaderElector(locker coord.Locker, key, instanceID string) LeaderElector {
	owner := instanceID + ":" + coord.NewOwnerID()
	return coord.NewLeaderElection(locker, key,
		coord.WithLeaderLeaseTTL(workflowLeaderTTL),
		coord.WithLeaderRenewInterval(workflowLeaderRenew),
		coord.WithLeaderOwnerID(owner),
	)
}
