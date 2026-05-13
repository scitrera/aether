// Package checkpoint provides persistent storage for agent/task state checkpoints.
// Checkpoints allow agents and tasks to save arbitrary state data that survives
// restarts, enabling graceful recovery and state persistence.
//
// This is separate from RabbitMQ's message offset tracking - checkpoints are for
// application-specific state, while offsets handle message stream position.
package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/redisutil"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// DefaultKey is used when no checkpoint key is specified
	DefaultKey = "default"

	// KeyPrefix is the Redis key prefix for checkpoints
	KeyPrefix = "checkpoint"
)

// Checkpoint represents a stored checkpoint with metadata
type Checkpoint struct {
	Data      []byte    `json:"data"`
	SavedAt   time.Time `json:"saved_at"`
	Identity  string    `json:"identity"`
	Key       string    `json:"key"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// Store provides Redis-backed checkpoint storage.
// The Store does not own the Redis client lifecycle - the client is managed
// by the application (typically in main.go). Do not call Close() on a Store,
// as it would close the shared client used by other components.
type Store struct {
	client redis.UniversalClient
	cb     *circuitbreaker.CircuitBreaker // optional; nil means no protection
}

// NewStoreFromClient creates a checkpoint store from an existing Redis client
func NewStoreFromClient(client redis.UniversalClient) *Store {
	return &Store{client: client}
}

// WithCircuitBreaker injects an optional circuit breaker into the checkpoint store.
// When set, all Redis operations are wrapped with circuit breaker protection.
// Pass nil to disable circuit breaker protection (backward compatible default).
func (s *Store) WithCircuitBreaker(cb *circuitbreaker.CircuitBreaker) *Store {
	s.cb = cb
	return s
}

// execCB executes fn through the circuit breaker if one is configured,
// or calls fn directly otherwise.
func (s *Store) execCB(fn func() error) error {
	if s.cb == nil {
		return fn()
	}
	return s.cb.Execute(fn)
}

// buildKey constructs the Redis key for a checkpoint
func buildKey(identity models.Identity, key string) string {
	if key == "" {
		key = DefaultKey
	}
	return fmt.Sprintf("%s:%s:%s", KeyPrefix, identity.String(), key)
}

// buildPatternKey constructs a pattern for listing checkpoints
func buildPatternKey(identity models.Identity) string {
	return fmt.Sprintf("%s:%s:*", KeyPrefix, identity.String())
}

// Save stores a checkpoint for an identity
func (s *Store) Save(ctx context.Context, identity models.Identity, key string, data []byte, ttl time.Duration) error {
	ctx, span := tracing.Tracer.Start(ctx, "checkpoint.Save")
	defer span.End()
	span.SetAttributes(
		attribute.String("identity", identity.String()),
		attribute.String("key", key),
		attribute.Int("data_size", len(data)),
	)

	if key == "" {
		key = DefaultKey
	}

	checkpoint := Checkpoint{
		Data:     data,
		SavedAt:  time.Now(),
		Identity: identity.String(),
		Key:      key,
	}

	if ttl > 0 {
		checkpoint.ExpiresAt = time.Now().Add(ttl)
	}

	jsonData, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	redisKey := buildKey(identity, key)
	if err = s.execCB(func() error {
		if ttl > 0 {
			return s.client.Set(ctx, redisKey, jsonData, ttl).Err()
		}
		return s.client.Set(ctx, redisKey, jsonData, 0).Err()
	}); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return nil
}

// Load retrieves a checkpoint for an identity
func (s *Store) Load(ctx context.Context, identity models.Identity, key string) (*Checkpoint, error) {
	ctx, span := tracing.Tracer.Start(ctx, "checkpoint.Load")
	defer span.End()
	span.SetAttributes(
		attribute.String("identity", identity.String()),
		attribute.String("key", key),
	)

	if key == "" {
		key = DefaultKey
	}

	redisKey := buildKey(identity, key)
	var jsonData string
	if err := s.execCB(func() error {
		var getErr error
		jsonData, getErr = s.client.Get(ctx, redisKey).Result()
		if getErr == redis.Nil {
			return nil // treat not-found as success; jsonData stays empty
		}
		return getErr
	}); err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}
	if jsonData == "" {
		return nil, nil // Not found
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal([]byte(jsonData), &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &checkpoint, nil
}

// Delete removes a checkpoint for an identity
func (s *Store) Delete(ctx context.Context, identity models.Identity, key string) error {
	ctx, span := tracing.Tracer.Start(ctx, "checkpoint.Delete")
	defer span.End()
	span.SetAttributes(
		attribute.String("identity", identity.String()),
		attribute.String("key", key),
	)

	if key == "" {
		key = DefaultKey
	}

	redisKey := buildKey(identity, key)
	if err := s.execCB(func() error {
		return s.client.Del(ctx, redisKey).Err()
	}); err != nil {
		return fmt.Errorf("failed to delete checkpoint: %w", err)
	}

	return nil
}

// List returns all checkpoint keys for an identity
func (s *Store) List(ctx context.Context, identity models.Identity) ([]string, error) {
	ctx, span := tracing.Tracer.Start(ctx, "checkpoint.List")
	defer span.End()
	span.SetAttributes(attribute.String("identity", identity.String()))

	pattern := buildPatternKey(identity)
	keys, err := redisutil.ScanKeys(ctx, s.client, pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}

	// Extract the checkpoint key portion from each Redis key
	prefix := fmt.Sprintf("%s:%s:", KeyPrefix, identity.String())
	result := make([]string, len(keys))
	for i, k := range keys {
		result[i] = k[len(prefix):]
	}

	return result, nil
}

// Exists checks if a checkpoint exists
func (s *Store) Exists(ctx context.Context, identity models.Identity, key string) (bool, error) {
	if key == "" {
		key = DefaultKey
	}

	redisKey := buildKey(identity, key)
	var n int64
	if err := s.execCB(func() error {
		var existsErr error
		n, existsErr = s.client.Exists(ctx, redisKey).Result()
		return existsErr
	}); err != nil {
		return false, fmt.Errorf("failed to check checkpoint existence: %w", err)
	}

	return n > 0, nil
}

// DeleteAll removes all checkpoints for an identity
func (s *Store) DeleteAll(ctx context.Context, identity models.Identity) (int64, error) {
	pattern := buildPatternKey(identity)
	keys, err := redisutil.ScanKeys(ctx, s.client, pattern)
	if err != nil {
		return 0, fmt.Errorf("failed to list checkpoints for deletion: %w", err)
	}

	if len(keys) == 0 {
		return 0, nil
	}

	var n int64
	if err := s.execCB(func() error {
		var delErr error
		n, delErr = s.client.Del(ctx, keys...).Result()
		return delErr
	}); err != nil {
		return 0, fmt.Errorf("failed to delete checkpoints: %w", err)
	}

	return n, nil
}

// HealthCheck verifies Redis connectivity
func (s *Store) HealthCheck(ctx context.Context) error {
	return s.execCB(func() error {
		return s.client.Ping(ctx).Err()
	})
}
