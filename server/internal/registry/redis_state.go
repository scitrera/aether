package registry

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RedisProfileStateStore implements ProfileStateStore on top of a
// redis.UniversalClient. Used by the full gateway path where Redis is
// already the shared state substrate.
type RedisProfileStateStore struct {
	client redis.UniversalClient
}

// NewRedisProfileStateStore wraps the given redis client. Caller retains
// ownership of the underlying connection.
func NewRedisProfileStateStore(client redis.UniversalClient) *RedisProfileStateStore {
	return &RedisProfileStateStore{client: client}
}

// Incr atomically increments the integer value at key and returns the new
// value. Matches Redis INCR semantics: missing keys are treated as 0.
func (s *RedisProfileStateStore) Incr(ctx context.Context, key string) (int64, error) {
	return s.client.Incr(ctx, key).Result()
}
