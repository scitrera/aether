// Package quota provides connection and message rate quota management
// for multi-tenant Aether deployments.
package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/logging"
	pkgerrors "github.com/scitrera/aether/pkg/errors"
)

// Redis key prefixes for quota tracking.
const (
	keyPrefixConn     = "quota:conn:"
	keyPrefixMsgRate  = "quota:msgrate:"
	keyPrefixOverride = "quota:override:"

	// connCountTTL is the TTL for connection counters. Counters are refreshed
	// by gateway heartbeats; a generous TTL ensures stale counters expire even
	// if decrement is missed during a crash.
	connCountTTL = 10 * time.Minute

	// msgRateWindow is the sliding window duration for message rate limiting.
	msgRateWindow = 1 * time.Second
)

// luaCheckAndIncrement atomically checks the current connection count against the
// limit and increments it only if the limit is not exceeded.
// KEYS[1] = counter key, ARGV[1] = limit, ARGV[2] = TTL in seconds
// Returns: current count after increment on success, or -1 if limit exceeded.
var luaCheckAndIncrement = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1])) or 0
local limit = tonumber(ARGV[1])
if current >= limit then
    return -1
end
local new = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return new
`)

// luaIncrWithTTL atomically increments a key and sets TTL only on first creation,
// preventing key leaks if the process crashes between INCR and EXPIRE.
// KEYS[1] = counter key, ARGV[1] = TTL in seconds
// Returns: new count after increment.
var luaIncrWithTTL = redis.NewScript(`
local new = redis.call('INCR', KEYS[1])
if new == 1 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
end
return new
`)

// DefaultQuotas holds system-wide default quota values.
type DefaultQuotas struct {
	MaxConnectionsPerWorkspace int     `yaml:"max_connections_per_workspace"` // default: 1000
	MaxMessageRatePerIdentity  float64 `yaml:"max_message_rate_per_identity"` // default: 100/s
	MaxKVKeysPerNamespace      int     `yaml:"max_kv_keys_per_namespace"`     // default: 10000
	MaxKVValueSize             int     `yaml:"max_kv_value_size"`             // default: 1048576 (1MB)
}

// WorkspaceQuota holds per-workspace quota overrides.
type WorkspaceQuota struct {
	Workspace                 string  `json:"workspace"`
	MaxConnections            int     `json:"max_connections"`
	MaxMessageRatePerIdentity float64 `json:"max_message_rate_per_identity"`
	MaxKVKeys                 int     `json:"max_kv_keys"`
	MaxKVValueSize            int     `json:"max_kv_value_size"`
}

// QuotaManager manages connection, message rate, and KV quotas.
type QuotaManager struct {
	redis    redis.UniversalClient
	defaults DefaultQuotas
	mu       sync.RWMutex
	// cached overrides reduces Redis reads for hot-path quota checks
	overrides map[string]*WorkspaceQuota
}

// NewQuotaManager creates a QuotaManager with the given Redis client and default quotas.
func NewQuotaManager(redisClient redis.UniversalClient, defaults DefaultQuotas) *QuotaManager {
	return &QuotaManager{
		redis:     redisClient,
		defaults:  defaults,
		overrides: make(map[string]*WorkspaceQuota),
	}
}

// CheckAndIncrementConnections atomically checks and increments the connection count
// for a workspace using a Lua script, eliminating the TOCTOU race between a
// separate check and increment. Returns QuotaExceededError if the limit is reached.
func (qm *QuotaManager) CheckAndIncrementConnections(ctx context.Context, workspace string) error {
	limit := qm.getConnectionLimit(ctx, workspace)
	if limit <= 0 {
		// Unlimited — just increment with TTL refresh.
		key := keyPrefixConn + workspace
		pipe := qm.redis.Pipeline()
		pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, connCountTTL)
		if _, err := pipe.Exec(ctx); err != nil {
			logging.Logger.Warn().Err(err).Str("workspace", workspace).Msg("failed to increment connection count (unlimited)")
		}
		return nil
	}

	key := keyPrefixConn + workspace
	ttlSeconds := int64(connCountTTL.Seconds())
	result, err := luaCheckAndIncrement.Run(ctx, qm.redis, []string{key}, limit, ttlSeconds).Int64()
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Msg("failed to check-and-increment connection count, allowing connection")
		return nil // fail open on Redis errors
	}
	if result == -1 {
		return &pkgerrors.QuotaExceededError{
			Resource:  "connections",
			Workspace: workspace,
			Current:   limit,
			Limit:     limit,
		}
	}
	return nil
}

// checkConnectionQuota verifies that the workspace has not exceeded its connection limit.
//
// Deprecated: Use CheckAndIncrementConnections instead. This method has a TOCTOU race
// between check and increment and is retained only for internal testing.
func (qm *QuotaManager) checkConnectionQuota(ctx context.Context, workspace string) error {
	limit := qm.getConnectionLimit(ctx, workspace)
	if limit <= 0 {
		return nil // unlimited
	}

	key := keyPrefixConn + workspace
	count, err := qm.redis.Get(ctx, key).Int()
	if err != nil && err != redis.Nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Msg("failed to check connection quota, allowing connection")
		return nil // fail open on Redis errors
	}

	if count >= limit {
		return &pkgerrors.QuotaExceededError{
			Resource:  "connections",
			Workspace: workspace,
			Current:   count,
			Limit:     limit,
		}
	}
	return nil
}

// incrementConnections atomically increments the connection count for a workspace.
//
// Deprecated: Use CheckAndIncrementConnections instead. This method has a TOCTOU race
// when combined with checkConnectionQuota and is retained only for internal testing.
func (qm *QuotaManager) incrementConnections(ctx context.Context, workspace string) error {
	key := keyPrefixConn + workspace
	pipe := qm.redis.Pipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, connCountTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Msg("failed to increment connection count")
	}
	return err
}

// DecrementConnections atomically decrements the connection count for a workspace.
func (qm *QuotaManager) DecrementConnections(ctx context.Context, workspace string) error {
	key := keyPrefixConn + workspace
	result, err := qm.redis.Decr(ctx, key).Result()
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Msg("failed to decrement connection count")
		return err
	}
	// Clamp to zero to handle race conditions or missed increments.
	if result < 0 {
		if setErr := qm.redis.Set(ctx, key, 0, connCountTTL).Err(); setErr != nil {
			logging.Logger.Warn().Err(setErr).Str("workspace", workspace).Msg("failed to clamp connection count to zero")
		}
	}
	return nil
}

// CheckMessageQuota verifies that the identity has not exceeded its per-second message
// rate limit. Uses an atomic Lua script to increment and set TTL in one operation,
// preventing key leaks if the process crashes between INCR and EXPIRE.
func (qm *QuotaManager) CheckMessageQuota(ctx context.Context, workspace, identity string) error {
	limit := qm.getMessageRateLimit(ctx, workspace)
	if limit <= 0 {
		return nil // unlimited
	}

	key := keyPrefixMsgRate + workspace + ":" + identity
	ttlSeconds := int64(msgRateWindow.Seconds())
	count, err := luaIncrWithTTL.Run(ctx, qm.redis, []string{key}, ttlSeconds).Int64()
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Str("identity", identity).Msg("failed to check message quota, allowing message")
		return nil // fail open
	}

	if count > int64(limit) {
		return &pkgerrors.QuotaExceededError{
			Resource:  "message_rate",
			Workspace: workspace,
			Identity:  identity,
			Current:   int(count),
			Limit:     int(limit),
		}
	}
	return nil
}

// CheckKVQuota verifies that the namespace has not exceeded its key count limit.
func (qm *QuotaManager) CheckKVQuota(ctx context.Context, workspace, namespace string, currentKeyCount int) error {
	limit := qm.getKVKeysLimit(ctx, workspace)
	if limit <= 0 {
		return nil // unlimited
	}

	if currentKeyCount >= limit {
		return &pkgerrors.QuotaExceededError{
			Resource:  "kv_keys",
			Workspace: workspace,
			Current:   currentKeyCount,
			Limit:     limit,
		}
	}
	return nil
}

// CheckKVValueSize verifies that a KV value does not exceed the size limit.
func (qm *QuotaManager) CheckKVValueSize(ctx context.Context, workspace string, valueSize int) error {
	limit := qm.getKVValueSizeLimit(ctx, workspace)
	if limit <= 0 {
		return nil // unlimited
	}

	if valueSize > limit {
		return &pkgerrors.QuotaExceededError{
			Resource:  "kv_value_size",
			Workspace: workspace,
			Current:   valueSize,
			Limit:     limit,
		}
	}
	return nil
}

// SetWorkspaceQuota stores a per-workspace quota override in Redis and updates the local cache.
func (qm *QuotaManager) SetWorkspaceQuota(ctx context.Context, workspace string, quota WorkspaceQuota) error {
	quota.Workspace = workspace
	data, err := json.Marshal(quota)
	if err != nil {
		return fmt.Errorf("failed to marshal workspace quota: %w", err)
	}

	key := keyPrefixOverride + workspace
	if err := qm.redis.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to store workspace quota: %w", err)
	}

	qm.mu.Lock()
	qm.overrides[workspace] = &quota
	qm.mu.Unlock()

	return nil
}

// GetWorkspaceQuota retrieves the effective quota for a workspace.
// Returns workspace-specific overrides if set, otherwise falls back to defaults.
func (qm *QuotaManager) GetWorkspaceQuota(ctx context.Context, workspace string) (*WorkspaceQuota, error) {
	// Check local cache first
	qm.mu.RLock()
	cached, ok := qm.overrides[workspace]
	qm.mu.RUnlock()
	if ok {
		return cached, nil
	}

	// Check Redis
	key := keyPrefixOverride + workspace
	data, err := qm.redis.Get(ctx, key).Bytes()
	if err == redis.Nil {
		// No override — return defaults as a WorkspaceQuota
		return &WorkspaceQuota{
			Workspace:                 workspace,
			MaxConnections:            qm.defaults.MaxConnectionsPerWorkspace,
			MaxMessageRatePerIdentity: qm.defaults.MaxMessageRatePerIdentity,
			MaxKVKeys:                 qm.defaults.MaxKVKeysPerNamespace,
			MaxKVValueSize:            qm.defaults.MaxKVValueSize,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace quota: %w", err)
	}

	var quota WorkspaceQuota
	if err := json.Unmarshal(data, &quota); err != nil {
		return nil, fmt.Errorf("failed to unmarshal workspace quota: %w", err)
	}

	// Cache it
	qm.mu.Lock()
	qm.overrides[workspace] = &quota
	qm.mu.Unlock()

	return &quota, nil
}

// getEffectiveQuota returns the WorkspaceQuota for a workspace, using the local cache
// when available and falling back to Redis. Returns nil on error (callers use defaults).
func (qm *QuotaManager) getEffectiveQuota(ctx context.Context, workspace string) *WorkspaceQuota {
	qm.mu.RLock()
	cached, ok := qm.overrides[workspace]
	qm.mu.RUnlock()
	if ok {
		return cached
	}

	q, err := qm.GetWorkspaceQuota(ctx, workspace)
	if err != nil {
		return nil
	}
	return q
}

// getConnectionLimit returns the effective connection limit for a workspace.
func (qm *QuotaManager) getConnectionLimit(ctx context.Context, workspace string) int {
	q := qm.getEffectiveQuota(ctx, workspace)
	if q == nil || q.MaxConnections <= 0 {
		return qm.defaults.MaxConnectionsPerWorkspace
	}
	return q.MaxConnections
}

// getMessageRateLimit returns the effective message rate limit for a workspace.
func (qm *QuotaManager) getMessageRateLimit(ctx context.Context, workspace string) float64 {
	q := qm.getEffectiveQuota(ctx, workspace)
	if q == nil || q.MaxMessageRatePerIdentity <= 0 {
		return qm.defaults.MaxMessageRatePerIdentity
	}
	return q.MaxMessageRatePerIdentity
}

// getKVKeysLimit returns the effective KV keys limit for a workspace.
func (qm *QuotaManager) getKVKeysLimit(ctx context.Context, workspace string) int {
	q := qm.getEffectiveQuota(ctx, workspace)
	if q == nil || q.MaxKVKeys <= 0 {
		return qm.defaults.MaxKVKeysPerNamespace
	}
	return q.MaxKVKeys
}

// getKVValueSizeLimit returns the effective KV value size limit for a workspace.
func (qm *QuotaManager) getKVValueSizeLimit(ctx context.Context, workspace string) int {
	q := qm.getEffectiveQuota(ctx, workspace)
	if q == nil || q.MaxKVValueSize <= 0 {
		return qm.defaults.MaxKVValueSize
	}
	return q.MaxKVValueSize
}
