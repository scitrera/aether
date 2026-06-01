package workflow

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/scitrera/aether/internal/coordkv"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/sdk/go/coord"
)

// LeaderElector elects a single active workflow-server instance among replicas
// so only one runs the scheduler and DAG monitor at a time.
//
// Implementations: a coord-backed elector (standard mode, over a shared KV
// backend) and a no-op single-node elector (lite mode). The election now runs
// on the shared coord primitive (atomic SetNX/CompareAndSet lease lock), so the
// same battle-tested logic backs every deployment mode rather than a
// Redis-only SET NX path.
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

// NewRedisLeaderElector builds a leader elector for the standard (Redis)
// deployment mode. Despite the name (retained for call-site compatibility), the
// election runs on the shared coord primitive over a Redis-backed KV store —
// the same code path that backs Badger/JetStream — so behaviour is uniform
// across backends. instanceID is folded into the fencing token for log
// attribution; a random suffix guarantees per-replica uniqueness.
func NewRedisLeaderElector(client redis.UniversalClient, key, instanceID string) LeaderElector {
	locker := coordkv.NewGlobal(kv.NewStoreFromClient(client))
	return NewCoordLeaderElector(locker, key, instanceID)
}

// NewCoordLeaderElector builds a leader elector over any coord.Locker (and thus
// any KV backend). Exposed for tests and for non-Redis deployments.
func NewCoordLeaderElector(locker coord.Locker, key, instanceID string) LeaderElector {
	owner := instanceID + ":" + coord.NewOwnerID()
	return coord.NewLeaderElection(locker, key,
		coord.WithLeaderLeaseTTL(workflowLeaderTTL),
		coord.WithLeaderRenewInterval(workflowLeaderRenew),
		coord.WithLeaderOwnerID(owner),
	)
}
