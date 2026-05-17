package state

import (
	"context"
	"encoding/json"
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

// lockMetaKeyPrefix is the Redis key prefix for the JSON sidecar that
// holds session-lifetime + client-version metadata for each identity
// lock. Written atomically with the lock value via Lua; TTL matches the
// lock so it dies with the connection.
const lockMetaKeyPrefix = "lockmeta:"

func tunnelPinKey(tunnelID string) string   { return tunnelPinKeyPrefix + tunnelID }
func requestPinKey(requestID string) string { return requestPinKeyPrefix + requestID }
func lockMetaKey(identity string) string    { return lockMetaKeyPrefix + identity }

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

// luaAcquireOrResumeLock atomically acquires/resumes/force-takes the
// identity lock and updates the sidecar `lockmeta:{identity}` JSON
// holding session-lifetime + client-version metadata.
//
// KEYS[1] = lock:{identity}, KEYS[2] = lockmeta:{identity}
// ARGV[1] = new sessionID
// ARGV[2] = lock TTL ms
// ARGV[3] = resume sessionID (or "")
// ARGV[4] = force-takeover threshold ms
// ARGV[5] = now (unix ms)
// ARGV[6] = client meta JSON; the script merges lifetime fields and
//
//	re-writes the sidecar.
//
// Returns: {acquired, resumed, forced, initial_connection_unix_ms,
//
//	reconnection_count}.
var luaAcquireOrResumeLock = redis.NewScript(`
	local current = redis.call("get", KEYS[1])
	local now = tonumber(ARGV[5])
	local function writeFresh()
		local meta = cjson.decode(ARGV[6])
		meta.session_id = ARGV[1]
		meta.acquired_unix_ms = now
		meta.initial_connection_unix_ms = now
		meta.reconnection_count = 0
		redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
		redis.call("set", KEYS[2], cjson.encode(meta), "PX", ARGV[2])
		return {now, 0}
	end
	if current == false then
		local r = writeFresh()
		return {1, 0, 0, r[1], r[2]}
	end
	if ARGV[3] ~= "" and current == ARGV[3] then
		-- Resume: preserve initial connect time, bump reconnect count.
		local initial = now
		local count = 1
		local existing = redis.call("get", KEYS[2])
		if existing ~= false then
			local ok, cur = pcall(cjson.decode, existing)
			if ok and type(cur) == "table" then
				if cur.initial_connection_unix_ms then initial = cur.initial_connection_unix_ms end
				if cur.reconnection_count then count = cur.reconnection_count + 1 end
			end
		end
		local meta = cjson.decode(ARGV[6])
		meta.session_id = ARGV[1]
		meta.acquired_unix_ms = now
		meta.initial_connection_unix_ms = initial
		meta.reconnection_count = count
		redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
		redis.call("set", KEYS[2], cjson.encode(meta), "PX", ARGV[2])
		return {1, 1, 0, initial, count}
	end
	local ttl = redis.call("pttl", KEYS[1])
	if ttl > 0 and ttl < tonumber(ARGV[4]) then
		-- Force takeover: treat as fresh (prior holder considered dead).
		local r = writeFresh()
		return {1, 0, 1, r[1], r[2]}
	end
	return {0, 0, 0, 0, 0}
`)

// AcquireOrResumeLock attempts to acquire a lock, allowing takeover if the existing
// lock has a matching session ID (reconnection scenario) or if the existing lock
// appears to be held by a dead client (TTL decayed below forceTakeoverThresholdMs).
//
// Returns ConnectResult containing acquisition status and session-lifetime
// fields. The lifetime fields (InitialConnectionUnixMs, ReconnectionCount)
// are preserved across resume_session_id takeovers; force takeover resets
// them since the prior holder is considered dead.
func (s *SessionRegistry) AcquireOrResumeLock(ctx context.Context, identity models.Identity, sessionID, resumeSessionID string, forceTakeoverThresholdMs int64, meta ConnectMeta) (ConnectResult, error) {
	lockKey := fmt.Sprintf("lock:%s", identity.String())
	metaKey := lockMetaKey(identity.String())

	// Build the meta JSON: cjson cannot manufacture struct shape from
	// the Go side without going through encoding/json. We send a
	// pre-shaped JSON object; the Lua script merges lifetime fields on
	// top of it.
	metaPayload := map[string]interface{}{}
	if meta.ClientVersion != "" {
		metaPayload["client_version"] = meta.ClientVersion
	}
	if meta.ClientSDK != "" {
		metaPayload["client_sdk"] = meta.ClientSDK
	}
	if meta.ClientBuildInfo != nil {
		metaPayload["client_build_info"] = meta.ClientBuildInfo
	}
	metaJSON, err := json.Marshal(metaPayload)
	if err != nil {
		return ConnectResult{}, fmt.Errorf("marshal connect meta: %w", err)
	}
	// cjson.decode rejects empty arrays vs objects ambiguously; for an
	// empty map encoding/json emits "{}" which decodes cleanly to a Lua
	// table, so no special-casing required.

	nowMs := time.Now().UnixMilli()

	result, err := luaAcquireOrResumeLock.Run(
		ctx, s.redis,
		[]string{lockKey, metaKey},
		sessionID,
		LockTTL.Milliseconds(),
		resumeSessionID,
		forceTakeoverThresholdMs,
		nowMs,
		string(metaJSON),
	).Result()
	if err != nil {
		return ConnectResult{}, err
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) < 5 {
		return ConnectResult{}, fmt.Errorf("unexpected Lua script result type: %T", result)
	}
	acquiredVal, ok1 := arr[0].(int64)
	resumedVal, ok2 := arr[1].(int64)
	forcedVal, ok3 := arr[2].(int64)
	initialMs, ok4 := arr[3].(int64)
	count, ok5 := arr[4].(int64)
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return ConnectResult{}, fmt.Errorf("unexpected Lua script result values: %v", arr)
	}
	return ConnectResult{
		Acquired:                acquiredVal == 1,
		Resumed:                 resumedVal == 1,
		Forced:                  forcedVal == 1,
		InitialConnectionUnixMs: initialMs,
		ReconnectionCount:       int32(count),
	}, nil
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

	lockKey := fmt.Sprintf("lock:%s", identity.String())
	metaKey := lockMetaKey(identity.String())
	// Only release if we own the lock; the sidecar lockmeta entry is
	// dropped alongside so admin queries don't observe orphan metadata
	// once a session ends.
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			redis.call("del", KEYS[1])
			redis.call("del", KEYS[2])
			return 1
		else
			return 0
		end
	`
	_, err := s.redis.Eval(ctx, script, []string{lockKey, metaKey}, sessionID).Result()
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

// luaRefreshLockAndSession atomically refreshes the lock TTL, the session
// HASH TTL, and the lockmeta sidecar TTL in a single Redis round-trip.
// KEYS[1] = lock key, KEYS[2] = session key, KEYS[3] = lockmeta key
// ARGV[1] = expected lock value (sessionID), ARGV[2] = lock TTL ms, ARGV[3] = session TTL seconds
// Returns 1 if the lock was refreshed (we still own it), 0 otherwise.
var luaRefreshLockAndSession = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		redis.call("pexpire", KEYS[1], ARGV[2])
		if redis.call("exists", KEYS[2]) == 1 then
			redis.call("expire", KEYS[2], ARGV[3])
		end
		if redis.call("exists", KEYS[3]) == 1 then
			redis.call("pexpire", KEYS[3], ARGV[2])
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
	metaKey := lockMetaKey(identity.String())

	result, err := luaRefreshLockAndSession.Run(
		ctx, s.redis,
		[]string{lockKey, sessionKey, metaKey},
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
		// Singleton invariant: WFE identity always collapses regardless of
		// the trailing tokens (e.g. legacy "wfe::{impl}" forms in old
		// stored sessions).
		return models.Identity{Type: models.PrincipalWorkflowEngine}, nil
	case "metrics":
		// Singleton invariant: MetricsBridge mirrors WFE — Implementation
		// is ignored, identity always collapses to the singleton.
		return models.Identity{Type: models.PrincipalMetricsBridge}, nil
	default:
		return models.Identity{}, fmt.Errorf("unsupported stored identity %q", identityStr)
	}
}
