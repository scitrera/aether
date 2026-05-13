package state

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/redisutil"
	"go.opentelemetry.io/otel/attribute"
)

// tunnelPinKeyPrefix is the Redis key prefix for proxy tunnel stickiness pins.
// A pin maps tunnel_id → concrete sv::{impl}::{specifier} identity, so follow-on
// TunnelData / TunnelClose frames hit the same sidecar that handled TunnelOpen.
const tunnelPinKeyPrefix = "tunnel-pin:"

// requestPinKeyPrefix is the Redis key prefix for proxy HTTP request stickiness
// pins. A pin maps request_id → caller|service identities, so follow-on
// ProxyHttpBodyChunk / ProxyHttpResponse frames find both the originating
// caller and the assigned service instance.
const requestPinKeyPrefix = "request-pin:"

func tunnelPinKey(tunnelID string) string   { return tunnelPinKeyPrefix + tunnelID }
func requestPinKey(requestID string) string { return requestPinKeyPrefix + requestID }

const (
	// LockTTL is the time-to-live for session locks.
	// Locks must be refreshed before this expires to remain valid.
	// If the gateway crashes, locks will auto-expire after this duration.
	LockTTL = 30 * time.Second

	// LockRefreshInterval is how often locks should be refreshed.
	// Should be less than LockTTL to ensure locks don't expire while connection is alive.
	LockRefreshInterval = 10 * time.Second
)

type SessionRegistry struct {
	redis redis.UniversalClient
}

// NewSessionRegistryFromClient creates a session registry from an existing Redis client
func NewSessionRegistryFromClient(client redis.UniversalClient) *SessionRegistry {
	return &SessionRegistry{redis: client}
}

// GetRedisClient returns the underlying Redis client.
// Used by admin health checks and session metadata queries.
func (s *SessionRegistry) GetRedisClient() redis.UniversalClient {
	return s.redis
}

func (s *SessionRegistry) AcquireLock(ctx context.Context, identity models.Identity, sessionID string) (bool, error) {
	key := fmt.Sprintf("lock:%s", identity.String())
	// Use sessionID as value to identify who holds the lock.
	// We use SetNX for exclusivity with a TTL.
	// The lock has a TTL so that if the gateway crashes, the lock will auto-expire.
	// The gateway must periodically call RefreshLock to keep the lock alive.
	success, err := s.redis.SetNX(ctx, key, sessionID, LockTTL).Result()
	if err != nil {
		return false, err
	}
	return success, nil
}

// AcquireOrResumeLock attempts to acquire a lock, allowing takeover if the existing
// lock has a matching session ID (reconnection scenario) or if the existing lock
// appears to be held by a dead client (TTL decayed below forceTakeoverThresholdMs).
//
// Returns (acquired, resumed, forced, error) where:
//   - acquired: true if lock was acquired (fresh, resumed, or forced)
//   - resumed: true if this was a session resume (existing lock had matching sessionID)
//   - forced: true if the lock was force-taken from a dead holder (TTL below threshold)
func (s *SessionRegistry) AcquireOrResumeLock(ctx context.Context, identity models.Identity, sessionID, resumeSessionID string, forceTakeoverThresholdMs int64) (bool, bool, bool, error) {
	key := fmt.Sprintf("lock:%s", identity.String())

	// Use a unified Lua script that handles all cases atomically:
	// 1. No lock exists → acquire
	// 2. Lock matches resume session ID → resume
	// 3. Lock held by another session but TTL decayed below threshold → force takeover (dead holder)
	// 4. Lock held by another session with healthy TTL → reject
	script := `
		local current = redis.call("get", KEYS[1])
		if current == false then
			-- No lock exists, create new one
			redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
			return {1, 0, 0}  -- acquired, not resumed, not forced
		elseif ARGV[3] ~= "" and current == ARGV[3] then
			-- Lock exists with matching resume session ID, take over
			redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
			return {1, 1, 0}  -- acquired, resumed, not forced
		else
			-- Lock held by different session — check if stale
			local ttl = redis.call("pttl", KEYS[1])
			if ttl > 0 and ttl < tonumber(ARGV[4]) then
				-- TTL below threshold: holder missed refresh cycles, likely dead
				redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
				return {1, 0, 1}  -- acquired, not resumed, forced
			end
			return {0, 0, 0}  -- rejected
		end
	`
	result, err := s.redis.Eval(ctx, script, []string{key}, sessionID, LockTTL.Milliseconds(), resumeSessionID, forceTakeoverThresholdMs).Result()
	if err != nil {
		return false, false, false, err
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) < 3 {
		return false, false, false, fmt.Errorf("unexpected Lua script result type: %T", result)
	}
	acquiredVal, ok1 := arr[0].(int64)
	resumedVal, ok2 := arr[1].(int64)
	forcedVal, ok3 := arr[2].(int64)
	if !ok1 || !ok2 || !ok3 {
		return false, false, false, fmt.Errorf("unexpected Lua script result values: %v", arr)
	}
	return acquiredVal == 1, resumedVal == 1, forcedVal == 1, nil
}

// RefreshLock extends the TTL of an existing lock.
// Returns true if the lock was refreshed (we still own it), false if not.
// This should be called periodically (every LockRefreshInterval) while the connection is alive.
func (s *SessionRegistry) RefreshLock(ctx context.Context, identity models.Identity, sessionID string) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "session.RefreshLock")
	defer span.End()
	span.SetAttributes(attribute.String("identity", identity.String()))

	key := fmt.Sprintf("lock:%s", identity.String())
	// Only refresh if we still own the lock
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
	result, err := s.redis.Eval(ctx, script, []string{key}, sessionID, LockTTL.Milliseconds()).Result()
	if err != nil {
		return false, err
	}
	return result.(int64) == 1, nil
}

func (s *SessionRegistry) ReleaseLock(ctx context.Context, identity models.Identity, sessionID string) error {
	ctx, span := tracing.Tracer.Start(ctx, "session.ReleaseLock")
	defer span.End()
	span.SetAttributes(attribute.String("identity", identity.String()))

	key := fmt.Sprintf("lock:%s", identity.String())
	// Only release if we own it
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	_, err := s.redis.Eval(ctx, script, []string{key}, sessionID).Result()
	return err
}

func (s *SessionRegistry) IsActive(ctx context.Context, identity string) (bool, error) {
	key := fmt.Sprintf("lock:%s", identity)
	exists, err := s.redis.Exists(ctx, key).Result()
	return exists > 0, err
}

// CleanupStaleLocks removes all lock keys that have no TTL (created before TTL was added).
// This should be called on gateway startup to clean up legacy locks.
// Returns the number of stale locks removed.
func (s *SessionRegistry) CleanupStaleLocks(ctx context.Context) (int, error) {
	// Find all lock keys using SCAN (production-safe, non-blocking)
	keys, err := redisutil.ScanKeys(ctx, s.redis, "lock:*")
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, key := range keys {
		// Check TTL - if -1 (no expiration), it's a stale lock
		ttl, err := s.redis.TTL(ctx, key).Result()
		if err != nil {
			continue
		}
		// TTL returns -1 for keys with no expiration, -2 for non-existent keys
		if ttl == -1 {
			if err := s.redis.Del(ctx, key).Err(); err == nil {
				removed++
			}
		}
	}

	return removed, nil
}

func (s *SessionRegistry) RegisterSession(ctx context.Context, identity models.Identity, sessionID, gatewayID string) error {
	ctx, span := tracing.Tracer.Start(ctx, "session.RegisterSession")
	defer span.End()
	span.SetAttributes(
		attribute.String("identity", identity.String()),
		attribute.String("session_id", sessionID),
		attribute.String("gateway_id", gatewayID),
	)

	key := fmt.Sprintf("session:%s", sessionID)
	pipe := s.redis.Pipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"identity":   identity.String(),
		"start":      time.Now().Unix(),
		"gateway_id": gatewayID,
	})
	pipe.Expire(ctx, key, LockTTL) // TTL matches lock TTL; refreshed alongside lock
	_, err := pipe.Exec(ctx)
	return err
}

// luaGetSessionGateway atomically reads the lock value (sessionID) for an
// identity and then reads the gateway_id field from the corresponding session
// HASH. KEYS[1] = lock key. Returns the gateway_id string, or "" when either
// the lock is missing (principal offline) or the session HASH lacks a
// gateway_id field.
var luaGetSessionGateway = redis.NewScript(`
	local sid = redis.call("get", KEYS[1])
	if not sid or sid == false then
		return ""
	end
	local gw = redis.call("hget", "session:" .. sid, "gateway_id")
	if not gw or gw == false then
		return ""
	end
	return gw
`)

// GetSessionGateway returns the gateway_id of the gateway hosting the given
// principal's connection. Returns "" with nil error when the principal is
// offline (lock absent) or when the session HASH has no gateway_id (legacy
// sessions registered before the field was added).
func (s *SessionRegistry) GetSessionGateway(ctx context.Context, identity models.Identity) (string, error) {
	ctx, span := tracing.Tracer.Start(ctx, "session.GetSessionGateway")
	defer span.End()
	span.SetAttributes(attribute.String("identity", identity.String()))

	lockKey := fmt.Sprintf("lock:%s", identity.String())
	result, err := luaGetSessionGateway.Run(ctx, s.redis, []string{lockKey}).Text()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	return result, nil
}

// GetSessionIdentity resolves the principal identity stored for a session.
func (s *SessionRegistry) GetSessionIdentity(ctx context.Context, sessionID string) (models.Identity, error) {
	key := fmt.Sprintf("session:%s", sessionID)
	identityStr, err := s.redis.HGet(ctx, key, "identity").Result()
	if err != nil {
		return models.Identity{}, err
	}

	return parseStoredSessionIdentity(identityStr)
}

// RefreshSession extends the TTL of session metadata (call alongside RefreshLock).
func (s *SessionRegistry) RefreshSession(ctx context.Context, sessionID string) error {
	key := fmt.Sprintf("session:%s", sessionID)
	return s.redis.Expire(ctx, key, LockTTL).Err()
}

// luaRefreshLockAndSession atomically refreshes both the lock TTL and session TTL
// in a single Redis round-trip, replacing the previous two-call sequence.
// KEYS[1] = lock key, KEYS[2] = session key
// ARGV[1] = expected lock value (sessionID), ARGV[2] = lock TTL ms, ARGV[3] = session TTL seconds
// Returns 1 if the lock was refreshed (we still own it), 0 otherwise.
var luaRefreshLockAndSession = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		redis.call("pexpire", KEYS[1], ARGV[2])
		if redis.call("exists", KEYS[2]) == 1 then
			redis.call("expire", KEYS[2], ARGV[3])
		end
		return 1
	else
		return 0
	end
`)

// RefreshLockAndSession atomically refreshes both the distributed lock TTL and
// the session metadata TTL in a single Lua script execution, replacing the
// previous two sequential Redis round-trips.
// Returns true if the lock was refreshed (we still own it), false if not.
func (s *SessionRegistry) RefreshLockAndSession(ctx context.Context, identity models.Identity, sessionID string) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "session.RefreshLockAndSession")
	defer span.End()
	span.SetAttributes(
		attribute.String("identity", identity.String()),
		attribute.String("session_id", sessionID),
	)

	lockKey := fmt.Sprintf("lock:%s", identity.String())
	sessionKey := fmt.Sprintf("session:%s", sessionID)

	result, err := luaRefreshLockAndSession.Run(
		ctx, s.redis,
		[]string{lockKey, sessionKey},
		sessionID,
		LockTTL.Milliseconds(),
		int64(LockTTL.Seconds()),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *SessionRegistry) UnregisterSession(ctx context.Context, sessionID string) error {
	key := fmt.Sprintf("session:%s", sessionID)
	return s.redis.Del(ctx, key).Err()
}

// IsOnline checks if an identity has an active connection (lock exists)
func (s *SessionRegistry) IsOnline(identity models.Identity) bool {
	ctx := context.Background()
	key := fmt.Sprintf("lock:%s", identity.String())
	exists, err := s.redis.Exists(ctx, key).Result()
	if err != nil {
		return false
	}
	return exists > 0
}

// FindHealthyServiceInstances scans cluster-wide locks for sv::{impl}::* keys
// and returns the identity strings of healthy candidates — those whose lock
// TTL has at least minRemaining left. minRemaining ≤ 0 disables the TTL
// filter (any present lock counts as healthy). Returns an empty slice when
// no candidate exists. Used by proxy/tunnel wildcard resolution to load
// balance across connected sidecar instances.
func (s *SessionRegistry) FindHealthyServiceInstances(ctx context.Context, impl string, minRemaining time.Duration) ([]string, error) {
	pattern := "lock:sv" + models.IdentitySep + impl + models.IdentitySep + "*"
	keys, err := redisutil.ScanKeys(ctx, s.redis, pattern)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		identityStr := strings.TrimPrefix(k, "lock:")
		if identityStr == "" {
			continue
		}
		if minRemaining > 0 {
			ttl, err := s.redis.PTTL(ctx, k).Result()
			if err != nil {
				continue
			}
			// ttl < 0 means key missing (-2) or no expiration (-1); both are skipped:
			// missing = race with delete, no expiration = malformed legacy lock.
			if ttl < 0 || ttl < minRemaining {
				continue
			}
		}
		out = append(out, identityStr)
	}
	return out, nil
}

// SetTunnelPin records a pin tunnelID → serviceIdentity with the given TTL.
// Uses SET (not SETNX) so an in-place rebind on a TunnelOpen-after-failure
// path can replace a stale pin transparently.
func (s *SessionRegistry) SetTunnelPin(ctx context.Context, tunnelID, serviceIdentity string, ttl time.Duration) error {
	if tunnelID == "" || serviceIdentity == "" {
		return fmt.Errorf("tunnelID and serviceIdentity must be non-empty")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return s.redis.Set(ctx, tunnelPinKey(tunnelID), serviceIdentity, ttl).Err()
}

// GetTunnelPin returns the concrete service identity bound to tunnelID.
// Returns empty string with nil error when the pin is absent or expired.
func (s *SessionRegistry) GetTunnelPin(ctx context.Context, tunnelID string) (string, error) {
	if tunnelID == "" {
		return "", nil
	}
	val, err := s.redis.Get(ctx, tunnelPinKey(tunnelID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// RefreshTunnelPin extends the TTL of an existing pin. No-op when missing —
// callers are expected to recover via PEER_RESET when GetTunnelPin returns "".
func (s *SessionRegistry) RefreshTunnelPin(ctx context.Context, tunnelID string, ttl time.Duration) error {
	if tunnelID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	// PERSIST-then-EXPIRE would race; EXPIRE alone is fine for "extend if exists".
	_, err := s.redis.Expire(ctx, tunnelPinKey(tunnelID), ttl).Result()
	return err
}

// DeleteTunnelPin removes a tunnel pin. Idempotent.
func (s *SessionRegistry) DeleteTunnelPin(ctx context.Context, tunnelID string) error {
	if tunnelID == "" {
		return nil
	}
	return s.redis.Del(ctx, tunnelPinKey(tunnelID)).Err()
}

// SetRequestPin records a pin requestID → caller|service identities with the
// given TTL. Used to route ProxyHttpBodyChunk and ProxyHttpResponse follow-on
// frames to the correct counterparty after the parent ProxyHttpRequest has
// resolved a wildcard target to a concrete service instance.
func (s *SessionRegistry) SetRequestPin(ctx context.Context, requestID, pinValue string, ttl time.Duration) error {
	if requestID == "" || pinValue == "" {
		return fmt.Errorf("requestID and pinValue must be non-empty")
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return s.redis.Set(ctx, requestPinKey(requestID), pinValue, ttl).Err()
}

// GetRequestPin returns the encoded caller|service pin value bound to
// requestID, or empty string when the pin is absent or expired.
func (s *SessionRegistry) GetRequestPin(ctx context.Context, requestID string) (string, error) {
	if requestID == "" {
		return "", nil
	}
	val, err := s.redis.Get(ctx, requestPinKey(requestID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// RefreshRequestPin extends the TTL of an existing pin. No-op when missing.
func (s *SessionRegistry) RefreshRequestPin(ctx context.Context, requestID string, ttl time.Duration) error {
	if requestID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	_, err := s.redis.Expire(ctx, requestPinKey(requestID), ttl).Result()
	return err
}

// DeleteRequestPin removes a request pin. Idempotent.
func (s *SessionRegistry) DeleteRequestPin(ctx context.Context, requestID string) error {
	if requestID == "" {
		return nil
	}
	return s.redis.Del(ctx, requestPinKey(requestID)).Err()
}

func parseStoredSessionIdentity(identityStr string) (models.Identity, error) {
	if identity, err := models.ParseIdentity(identityStr); err == nil {
		return identity, nil
	}

	parts := strings.Split(identityStr, models.IdentitySep)
	if len(parts) < 2 {
		return models.Identity{}, fmt.Errorf("invalid stored identity %q", identityStr)
	}

	switch parts[0] {
	case "orc":
		identity := models.Identity{
			Type:           models.PrincipalOrchestrator,
			Implementation: parts[1],
		}
		if len(parts) > 2 {
			identity.Specifier = strings.Join(parts[2:], models.IdentitySep)
		}
		return identity, nil
	case "wfe":
		return models.Identity{
			Type:           models.PrincipalWorkflowEngine,
			Implementation: strings.Join(parts[1:], "."),
		}, nil
	case "metrics":
		return models.Identity{
			Type:           models.PrincipalMetricsBridge,
			Implementation: strings.Join(parts[1:], "."),
		}, nil
	default:
		return models.Identity{}, fmt.Errorf("unsupported stored identity %q", identityStr)
	}
}
