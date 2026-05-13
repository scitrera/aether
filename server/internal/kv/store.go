package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/crypto"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/redisutil"
	"go.opentelemetry.io/otel/attribute"
)

// EncryptedKeyPrefix is the prefix for keys that should be encrypted at rest.
// When a key starts with this prefix, the value will be encrypted before storage
// and decrypted upon retrieval.
const EncryptedKeyPrefix = "enc:"

// ErrKeyNotFound is returned when a key does not exist in the store.
var ErrKeyNotFound = errors.New("key not found")

// Store provides agent-centric KV storage with multi-scope support.
// The Store does not own the Redis client lifecycle - the client is managed
// by the application (typically in main.go). Do not call Close() on a Store,
// as it would close the shared client used by other components.
type Store struct {
	client     redis.UniversalClient
	defaultTTL time.Duration
	cb         *circuitbreaker.CircuitBreaker // optional; nil means no protection
	encryption *crypto.EncryptionService      // optional; nil means no encryption
}

// NewStoreFromClient creates a new KV store from an existing Redis client.
// No default TTL is applied (keys without TTL persist indefinitely).
func NewStoreFromClient(client redis.UniversalClient) *Store {
	return &Store{client: client}
}

// NewStoreFromClientWithTTL creates a new KV store from an existing Redis client
// with a default TTL applied to keys that do not specify one.
func NewStoreFromClientWithTTL(client redis.UniversalClient, defaultTTL time.Duration) *Store {
	return &Store{client: client, defaultTTL: defaultTTL}
}

// WithCircuitBreaker injects an optional circuit breaker into the KV store.
// When set, all Redis operations are wrapped with circuit breaker protection.
// Pass nil to disable circuit breaker protection (backward compatible default).
func (s *Store) WithCircuitBreaker(cb *circuitbreaker.CircuitBreaker) *Store {
	s.cb = cb
	return s
}

// WithEncryption injects an optional encryption service into the KV store.
// When set, keys prefixed with "enc:" will have their values encrypted at rest.
// Pass nil to disable encryption (backward compatible default).
func (s *Store) WithEncryption(enc *crypto.EncryptionService) *Store {
	s.encryption = enc
	return s
}

// isEncryptedKey returns true if the key should be encrypted.
// Keys starting with EncryptedKeyPrefix ("enc:") are encrypted.
func (s *Store) isEncryptedKey(key string) bool {
	return strings.HasPrefix(key, EncryptedKeyPrefix)
}

// stripEncryptedPrefix returns the key without the encryption prefix.
// If the key doesn't have the prefix, it returns the key unchanged.
func (s *Store) stripEncryptedPrefix(key string) string {
	return strings.TrimPrefix(key, EncryptedKeyPrefix)
}

// encryptValue encrypts the value if encryption is configured and the key is encrypted.
// Returns the value unchanged if encryption is not configured.
func (s *Store) encryptValue(key, value string) (string, error) {
	if s.encryption == nil || !s.isEncryptedKey(key) {
		return value, nil
	}
	return s.encryption.Encrypt(value)
}

// decryptValue decrypts the value if encryption is configured and the key is encrypted.
// Returns the value unchanged if encryption is not configured.
func (s *Store) decryptValue(key, value string) (string, error) {
	if s.encryption == nil || !s.isEncryptedKey(key) {
		return value, nil
	}
	return s.encryption.Decrypt(value)
}

// execCB executes fn through the circuit breaker if one is configured,
// or calls fn directly otherwise.
func (s *Store) execCB(fn func() error) error {
	if s.cb == nil {
		return fn()
	}
	return s.cb.Execute(fn)
}

// Ping checks Redis connectivity
func (s *Store) Ping(ctx context.Context) error {
	return s.execCB(func() error {
		return s.client.Ping(ctx).Err()
	})
}

// Get retrieves a value from the agent's KV store in the specified scope
func (s *Store) Get(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (string, error) {
	ctx, span := tracing.Tracer.Start(ctx, "kv.Get")
	defer span.End()
	span.SetAttributes(attribute.String("key", key), attribute.String("scope", string(scope)))

	if err := validateKey(key); err != nil {
		return "", err
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	var val string
	err := s.execCB(func() error {
		var redisErr error
		val, redisErr = s.client.Get(ctx, fullKey).Result()
		if redisErr == redis.Nil {
			return fmt.Errorf("%w: %s", ErrKeyNotFound, key)
		}
		return redisErr
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", err
		}
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	// Decrypt value if the key is encrypted
	return s.decryptValue(key, val)
}

// Set stores a value in the agent's KV store in the specified scope
func (s *Store) Set(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	value string,
	userID string,
	workspace string,
	ttl time.Duration,
) error {
	ctx, span := tracing.Tracer.Start(ctx, "kv.Set")
	defer span.End()
	span.SetAttributes(attribute.String("key", key), attribute.String("scope", string(scope)))

	if err := validateKey(key); err != nil {
		return err
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	// Encrypt value if the key is encrypted
	encryptedValue, err := s.encryptValue(key, value)
	if err != nil {
		return fmt.Errorf("failed to encrypt value for key %s: %w", key, err)
	}

	effectiveTTL := ttl
	if effectiveTTL <= 0 && s.defaultTTL > 0 {
		effectiveTTL = s.defaultTTL
	}
	if err := s.execCB(func() error {
		return s.client.Set(ctx, fullKey, encryptedValue, effectiveTTL).Err()
	}); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// Delete removes a key from the agent's KV store
func (s *Store) Delete(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) error {
	ctx, span := tracing.Tracer.Start(ctx, "kv.Delete")
	defer span.End()
	span.SetAttributes(attribute.String("key", key), attribute.String("scope", string(scope)))

	if err := validateKey(key); err != nil {
		return err
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	if err := s.execCB(func() error {
		return s.client.Del(ctx, fullKey).Err()
	}); err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}

	return nil
}

// DefaultListLimit is the maximum number of keys returned by List when no options are specified.
const DefaultListLimit = 100

// ListOptions controls pagination for List operations.
// A nil *ListOptions is equivalent to &ListOptions{Limit: DefaultListLimit}.
type ListOptions struct {
	// Cursor is the Redis SCAN cursor returned by a previous ListPaginated call.
	// Pass "" (or "0") to start from the beginning.
	Cursor string
	// Limit is the maximum number of keys to return in a single call.
	// If <= 0, DefaultListLimit is used.
	Limit int
}

// ListResult is the return type for ListPaginated.
type ListResult struct {
	Items      map[string]string
	NextCursor string
	HasMore    bool
}

// ListPaginated returns up to opts.Limit keys in a namespace with their values,
// using Redis SCAN cursors for safe iteration over large key-spaces.
// Use the returned NextCursor in a subsequent call to page through all results.
func (s *Store) ListPaginated(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	userID string,
	workspace string,
	opts *ListOptions,
) (*ListResult, error) {
	ctx, span := tracing.Tracer.Start(ctx, "kv.ListPaginated")
	defer span.End()
	span.SetAttributes(attribute.String("scope", string(scope)))

	limit := DefaultListLimit
	var startCursor uint64
	if opts != nil {
		if opts.Limit > 0 {
			limit = opts.Limit
		}
		if opts.Cursor != "" && opts.Cursor != "0" {
			if _, err := fmt.Sscanf(opts.Cursor, "%d", &startCursor); err != nil {
				return nil, fmt.Errorf("invalid cursor: %w", err)
			}
		}
	}

	namespace := BuildNamespace(agent, scope, userID, workspace)
	pattern := fmt.Sprintf("%s:*", namespace)
	prefix := namespace + ":"

	var collected []string
	cursor := startCursor
	for {
		var batch []string
		var nextCursor uint64
		if err := s.execCB(func() error {
			var scanErr error
			batch, nextCursor, scanErr = s.client.Scan(ctx, cursor, pattern, int64(limit)).Result()
			return scanErr
		}); err != nil {
			return nil, fmt.Errorf("failed to scan keys: %w", err)
		}
		collected = append(collected, batch...)
		cursor = nextCursor
		if cursor == 0 || len(collected) >= limit {
			break
		}
	}

	// Trim to limit and handle cursor correctly
	hasMore := false
	var nextCursorStr string
	if len(collected) > limit {
		collected = collected[:limit]
		hasMore = true
		// Use the last non-zero cursor for pagination
		if cursor != 0 {
			nextCursorStr = fmt.Sprintf("%d", cursor)
		}
		// If cursor == 0, scan completed but we have excess keys — hasMore is
		// already true; nextCursorStr stays empty to signal restart from 0.
	} else if cursor != 0 {
		hasMore = true
		nextCursorStr = fmt.Sprintf("%d", cursor)
	}

	items := make(map[string]string, len(collected))
	if len(collected) == 0 {
		return &ListResult{Items: items, NextCursor: nextCursorStr, HasMore: hasMore}, nil
	}

	var vals []interface{}
	if err := s.execCB(func() error {
		var mgetErr error
		vals, mgetErr = s.client.MGet(ctx, collected...).Result()
		return mgetErr
	}); err != nil {
		return nil, fmt.Errorf("failed to get values: %w", err)
	}
	for i, k := range collected {
		if vals[i] != nil {
			items[k[len(prefix):]] = vals[i].(string)
		}
	}

	return &ListResult{Items: items, NextCursor: nextCursorStr, HasMore: hasMore}, nil
}

// List returns all keys in a namespace with their values.
// It is capped at DefaultListLimit keys to prevent Redis OOM under load.
// For full iteration over large namespaces use ListPaginated.
func (s *Store) List(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	userID string,
	workspace string,
) (map[string]string, error) {
	ctx, span := tracing.Tracer.Start(ctx, "kv.List")
	defer span.End()
	span.SetAttributes(attribute.String("scope", string(scope)))

	res, err := s.ListPaginated(ctx, agent, scope, userID, workspace, &ListOptions{Limit: DefaultListLimit})
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

// ListKeys returns only the keys in a namespace (without values)
func (s *Store) ListKeys(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	userID string,
	workspace string,
) ([]string, error) {
	namespace := BuildNamespace(agent, scope, userID, workspace)
	pattern := fmt.Sprintf("%s:*", namespace)

	keys, err := redisutil.ScanKeysLimit(ctx, s.client, pattern, 10000)
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	// Strip namespace prefix from keys
	result := make([]string, len(keys))
	prefix := namespace + ":"
	for i, k := range keys {
		result[i] = k[len(prefix):]
	}

	return result, nil
}

// Exists checks if a key exists in the specified scope
func (s *Store) Exists(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "kv.Exists")
	defer span.End()
	span.SetAttributes(attribute.String("key", key), attribute.String("scope", string(scope)))

	if err := validateKey(key); err != nil {
		return false, err
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	var n int64
	if err := s.execCB(func() error {
		var existsErr error
		n, existsErr = s.client.Exists(ctx, fullKey).Result()
		return existsErr
	}); err != nil {
		return false, fmt.Errorf("failed to check key existence: %w", err)
	}

	return n > 0, nil
}

// GetTTL returns the remaining TTL for a key
func (s *Store) GetTTL(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (time.Duration, error) {
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	var duration time.Duration
	if err := s.execCB(func() error {
		var ttlErr error
		duration, ttlErr = s.client.TTL(ctx, fullKey).Result()
		return ttlErr
	}); err != nil {
		return 0, fmt.Errorf("failed to get TTL: %w", err)
	}

	return duration, nil
}

// SetTTL sets or updates the TTL for an existing key
func (s *Store) SetTTL(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	ttl time.Duration,
) error {
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	if err := s.execCB(func() error {
		return s.client.Expire(ctx, fullKey, ttl).Err()
	}); err != nil {
		return fmt.Errorf("failed to set TTL: %w", err)
	}

	return nil
}

// RemoveTTL removes the TTL from a key (makes it persistent)
func (s *Store) RemoveTTL(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) error {
	if err := validateKey(key); err != nil {
		return err
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	if err := s.execCB(func() error {
		return s.client.Persist(ctx, fullKey).Err()
	}); err != nil {
		return fmt.Errorf("failed to remove TTL: %w", err)
	}

	return nil
}

// Increment atomically increments a key's value
func (s *Store) Increment(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	var val int64
	if err := s.execCB(func() error {
		var incrErr error
		val, incrErr = s.client.Incr(ctx, fullKey).Result()
		return incrErr
	}); err != nil {
		return 0, fmt.Errorf("failed to increment key %s: %w", key, err)
	}

	return val, nil
}

// Decrement atomically decrements a key's value
func (s *Store) Decrement(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	var val int64
	if err := s.execCB(func() error {
		var decrErr error
		val, decrErr = s.client.Decr(ctx, fullKey).Result()
		return decrErr
	}); err != nil {
		return 0, fmt.Errorf("failed to decrement key %s: %w", key, err)
	}

	return val, nil
}

// incrementIfLuaScript atomically increments a counter by `delta` only
// when the resulting value would NOT exceed `ceiling`. Returns:
//
//	{1, newValue}  on applied
//	{0, currentValue} on rejected (would have exceeded ceiling)
//
// Missing keys are treated as 0. Non-numeric existing values cause a
// Redis error (propagated up to the caller as a regular error).
var incrementIfLuaScript = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local delta = tonumber(ARGV[1])
local ceiling = tonumber(ARGV[2])
local proposed = cur + delta
if proposed > ceiling then
  return {0, cur}
end
local newval = redis.call('INCRBY', KEYS[1], delta)
return {1, newval}
`)

// decrementIfLuaScript atomically decrements a counter by `delta` only
// when the resulting value would NOT fall below `floor`. Returns:
//
//	{1, newValue}  on applied
//	{0, currentValue} on rejected (would have fallen below floor)
var decrementIfLuaScript = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local delta = tonumber(ARGV[1])
local floor = tonumber(ARGV[2])
local proposed = cur - delta
if proposed < floor then
  return {0, cur}
end
local newval = redis.call('DECRBY', KEYS[1], delta)
return {1, newval}
`)

// IncrementIf atomically increments a counter by `delta` if and only if
// the resulting value would not exceed `ceiling`. Returns the (possibly
// unchanged) current value plus a boolean indicating whether the
// mutation was applied. `delta` should be > 0; passing 0 is a no-op
// equivalent to GET.
func (s *Store) IncrementIf(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	ceiling int64,
) (int64, bool, error) {
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	if delta < 0 {
		return 0, false, fmt.Errorf("IncrementIf delta must be non-negative, got %d", delta)
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	return s.runGuardedCounter(ctx, incrementIfLuaScript, fullKey, delta, ceiling)
}

// DecrementIf atomically decrements a counter by `delta` if and only if
// the resulting value would not drop below `floor`. Returns the
// (possibly unchanged) current value plus a boolean indicating whether
// the mutation was applied. `delta` should be > 0.
func (s *Store) DecrementIf(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	floor int64,
) (int64, bool, error) {
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	if delta < 0 {
		return 0, false, fmt.Errorf("DecrementIf delta must be non-negative, got %d", delta)
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullKey := fmt.Sprintf("%s:%s", namespace, key)

	return s.runGuardedCounter(ctx, decrementIfLuaScript, fullKey, delta, floor)
}

// runGuardedCounter runs a counter Lua script and decodes the
// {applied, value} result. Centralizes circuit-breaker wrapping and
// result-shape validation for IncrementIf/DecrementIf.
func (s *Store) runGuardedCounter(ctx context.Context, script *redis.Script, fullKey string, delta, guard int64) (int64, bool, error) {
	var raw interface{}
	if err := s.execCB(func() error {
		var runErr error
		raw, runErr = script.Run(ctx, s.client, []string{fullKey}, delta, guard).Result()
		return runErr
	}); err != nil {
		return 0, false, fmt.Errorf("guarded counter on %s failed: %w", fullKey, err)
	}
	arr, ok := raw.([]interface{})
	if !ok || len(arr) != 2 {
		return 0, false, fmt.Errorf("unexpected guarded counter result shape: %T", raw)
	}
	appliedRaw, valueRaw := arr[0], arr[1]
	applied, _ := appliedRaw.(int64)
	value, _ := valueRaw.(int64)
	return value, applied == 1, nil
}

// GetJSON retrieves and unmarshals a JSON value
func (s *Store) GetJSON(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	dest interface{},
) error {
	data, err := s.Get(ctx, agent, scope, key, userID, workspace)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(data), dest)
}

// SetJSON marshals and stores a JSON value
func (s *Store) SetJSON(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	value interface{},
	userID string,
	workspace string,
	ttl time.Duration,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return s.Set(ctx, agent, scope, key, string(data), userID, workspace, ttl)
}

// DeleteByPattern deletes all keys matching a pattern within a scope
func (s *Store) DeleteByPattern(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	pattern string,
	userID string,
	workspace string,
) (int64, error) {
	namespace := BuildNamespace(agent, scope, userID, workspace)
	fullPattern := fmt.Sprintf("%s:%s", namespace, pattern)

	keys, err := redisutil.ScanKeys(ctx, s.client, fullPattern)
	if err != nil {
		return 0, fmt.Errorf("failed to find keys: %w", err)
	}

	if len(keys) == 0 {
		return 0, nil
	}

	if err := s.execCB(func() error {
		return s.client.Del(ctx, keys...).Err()
	}); err != nil {
		return 0, fmt.Errorf("failed to delete keys: %w", err)
	}

	return int64(len(keys)), nil
}

// GetMultiple retrieves multiple keys at once
func (s *Store) GetMultiple(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	keys []string,
	userID string,
	workspace string,
) (map[string]string, error) {
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return nil, err
		}
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)

	result := make(map[string]string)
	pipe := s.client.Pipeline()

	cmds := make(map[string]*redis.StringCmd)
	for _, key := range keys {
		fullKey := fmt.Sprintf("%s:%s", namespace, key)
		cmds[key] = pipe.Get(ctx, fullKey)
	}

	// Execute pipeline through the circuit breaker; individual command errors
	// are checked per-key below. pipe.Exec returns redis.Nil if any key was
	// missing, which is expected.
	s.execCB(func() error { pipe.Exec(ctx); return nil }) //nolint:errcheck

	for key, cmd := range cmds {
		val, err := cmd.Result()
		if err != nil {
			if err == redis.Nil {
				continue // key doesn't exist, skip
			}
			log.Warn().Err(err).Str("key", key).Msg("error retrieving key in GetMultiple")
			continue
		}
		result[key] = val
	}

	return result, nil
}

// SetMultiple sets multiple key-value pairs at once
func (s *Store) SetMultiple(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	values map[string]string,
	userID string,
	workspace string,
	ttl time.Duration,
) error {
	for key := range values {
		if err := validateKey(key); err != nil {
			return err
		}
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)

	effectiveTTL := ttl
	if effectiveTTL <= 0 && s.defaultTTL > 0 {
		effectiveTTL = s.defaultTTL
	}
	pipe := s.client.Pipeline()
	for key, value := range values {
		fullKey := fmt.Sprintf("%s:%s", namespace, key)
		pipe.Set(ctx, fullKey, value, effectiveTTL)
	}

	if err := s.execCB(func() error {
		_, execErr := pipe.Exec(ctx)
		return execErr
	}); err != nil {
		return fmt.Errorf("failed to set multiple keys: %w", err)
	}

	return nil
}

// DeleteMultiple deletes multiple keys at once
func (s *Store) DeleteMultiple(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	keys []string,
	userID string,
	workspace string,
) error {
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return err
		}
	}
	namespace := BuildNamespace(agent, scope, userID, workspace)

	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = fmt.Sprintf("%s:%s", namespace, key)
	}

	if err := s.execCB(func() error {
		return s.client.Del(ctx, fullKeys...).Err()
	}); err != nil {
		return fmt.Errorf("failed to delete multiple keys: %w", err)
	}

	return nil
}

// HealthCheck checks if the Redis connection is healthy
func (s *Store) HealthCheck(ctx context.Context) error {
	return s.execCB(func() error {
		return s.client.Ping(ctx).Err()
	})
}

// validateKey checks that a user-supplied key is safe to use.
// It rejects empty keys and keys exceeding 256 characters.
//
// Note: ':' is permitted in user keys. Although ':' is the Redis namespace separator
// used by BuildNamespace (e.g. "kv:agent:{impl}.{spec}:{scope}:{userkey}"), allowing
// it in user keys is safe because:
//   - The user key is always the final segment, appended via fmt.Sprintf("%s:%s", namespace, key)
//   - ParseNamespace only operates on the namespace prefix, never on full Redis keys
//   - List/scan operations strip the namespace prefix by length, not by splitting on ':'
//   - BuildNamespace independently sanitizes its own components (impl, spec, userID, workspace)
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if len(key) > 256 {
		return fmt.Errorf("key exceeds maximum length of 256 characters")
	}
	return nil
}
