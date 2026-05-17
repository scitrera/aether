package gateway

import (
	"context"
	"time"

	"github.com/scitrera/aether/internal/checkpoint"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/pkg/models"
)

// SessionManager abstracts session registry operations used by the gateway.
type SessionManager interface {
	AcquireOrResumeLock(ctx context.Context, identity models.Identity, sessionID, resumeSessionID string, forceTakeoverThresholdMs int64, meta state.ConnectMeta) (state.ConnectResult, error)
	ReleaseLock(ctx context.Context, identity models.Identity, sessionID string) error
	RefreshLock(ctx context.Context, identity models.Identity, sessionID string) (bool, error)
	RefreshLockAndSession(ctx context.Context, identity models.Identity, sessionID string) (bool, error)
	RegisterSession(ctx context.Context, identity models.Identity, sessionID, gatewayID string) error
	GetSessionIdentity(ctx context.Context, sessionID string) (models.Identity, error)
	// GetSessionGateway returns the gateway_id of the gateway hosting the
	// given principal's connection. Returns "" with nil error when the
	// principal is offline. Used by Phase-7 cross-gateway forwarding to
	// discover the peer hosting a given target principal.
	GetSessionGateway(ctx context.Context, identity models.Identity) (string, error)
	UnregisterSession(ctx context.Context, sessionID string) error
	RefreshSession(ctx context.Context, sessionID string) error
	IsActive(ctx context.Context, identity string) (bool, error)

	// FindHealthyServiceInstances returns the identity strings of connected
	// `sv::{impl}::*` instances cluster-wide. Implementations should exclude
	// instances whose lock TTL has decayed below minRemaining (i.e. the holder
	// is likely dead and won't refresh in time). Returns an empty slice when
	// no candidate exists. Used by proxy/tunnel wildcard resolution.
	FindHealthyServiceInstances(ctx context.Context, impl string, minRemaining time.Duration) ([]string, error)

	// SetTunnelPin records that tunnelID is bound to concrete service identity.
	// Implementations MUST set a TTL >= ttl. Pins are refreshed on each
	// data/ack frame to keep the binding alive for the life of the tunnel.
	SetTunnelPin(ctx context.Context, tunnelID, serviceIdentity string, ttl time.Duration) error
	// GetTunnelPin returns the concrete service identity bound to tunnelID,
	// or empty string when no pin exists.
	GetTunnelPin(ctx context.Context, tunnelID string) (string, error)
	// RefreshTunnelPin extends the TTL of an existing pin. No-op when the pin
	// no longer exists.
	RefreshTunnelPin(ctx context.Context, tunnelID string, ttl time.Duration) error
	// DeleteTunnelPin removes a tunnel pin. Idempotent.
	DeleteTunnelPin(ctx context.Context, tunnelID string) error

	// SetRequestPin records that requestID is bound to an opaque pin value
	// (typically caller|service identities). Mirrors SetTunnelPin for proxy
	// HTTP request stickiness across body chunks and the response.
	SetRequestPin(ctx context.Context, requestID, pinValue string, ttl time.Duration) error
	// GetRequestPin returns the bound pin value, or "" when no pin exists.
	GetRequestPin(ctx context.Context, requestID string) (string, error)
	// RefreshRequestPin extends the TTL of an existing request pin. No-op
	// when the pin no longer exists.
	RefreshRequestPin(ctx context.Context, requestID string, ttl time.Duration) error
	// DeleteRequestPin removes a request pin. Idempotent.
	DeleteRequestPin(ctx context.Context, requestID string) error
}

// MessageRouter abstracts message routing operations used by the gateway.
type MessageRouter interface {
	Publish(ctx context.Context, topic string, payload []byte) error
	Subscribe(topic string, handler func([]byte)) (func(), error)
	SubscribeExclusive(topic string, consumerName string, handler func([]byte)) (func(), error)
	SubscribeExclusiveFromNow(topic string, consumerName string, handler func([]byte)) (func(), error)
	// SubscribeExclusiveFromTimestamp creates an exclusive subscription that resumes from
	// a stored offset if available; otherwise, when startTimestampMs > 0, it starts from
	// messages at-or-after that unix-millisecond timestamp. When 0 (or unsupported by
	// the router backend), falls back to existing replay semantics. Used for cold-starting
	// pool-dispatched agents.
	SubscribeExclusiveFromTimestamp(topic string, consumerName string, startTimestampMs int64, handler func([]byte)) (func(), error)
}

// KVReadWriter abstracts KV store operations used by the gateway and KVHandler.
type KVReadWriter interface {
	Get(ctx context.Context, agent models.Identity, scope kv.KVScope, key string, userID string, workspace string) (string, error)
	Set(ctx context.Context, agent models.Identity, scope kv.KVScope, key string, value string, userID string, workspace string, ttl time.Duration) error
	Delete(ctx context.Context, agent models.Identity, scope kv.KVScope, key string, userID string, workspace string) error
	List(ctx context.Context, agent models.Identity, scope kv.KVScope, userID string, workspace string) (map[string]string, error)
	ListPaginated(ctx context.Context, agent models.Identity, scope kv.KVScope, userID string, workspace string, opts *kv.ListOptions) (*kv.ListResult, error)
	Increment(ctx context.Context, agent models.Identity, scope kv.KVScope, key string, userID string, workspace string) (int64, error)
	Decrement(ctx context.Context, agent models.Identity, scope kv.KVScope, key string, userID string, workspace string) (int64, error)
	IncrementIf(ctx context.Context, agent models.Identity, scope kv.KVScope, key string, userID string, workspace string, delta int64, ceiling int64) (int64, bool, error)
	DecrementIf(ctx context.Context, agent models.Identity, scope kv.KVScope, key string, userID string, workspace string, delta int64, floor int64) (int64, bool, error)
}

// CheckpointManager abstracts checkpoint store operations used by the gateway.
type CheckpointManager interface {
	Save(ctx context.Context, identity models.Identity, key string, data []byte, ttl time.Duration) error
	Load(ctx context.Context, identity models.Identity, key string) (*checkpoint.Checkpoint, error)
	Delete(ctx context.Context, identity models.Identity, key string) error
	List(ctx context.Context, identity models.Identity) ([]string, error)
}

// QuotaChecker abstracts quota enforcement operations used by the gateway.
// The full QuotaManager (quota package) satisfies this interface; in-process
// implementations can provide lightweight alternatives for lite mode.
type QuotaChecker interface {
	CheckAndIncrementConnections(ctx context.Context, workspace string) error
	DecrementConnections(ctx context.Context, workspace string) error
	CheckMessageQuota(ctx context.Context, workspace, identity string) error
	CheckKVValueSize(ctx context.Context, workspace string, valueSize int) error
}
