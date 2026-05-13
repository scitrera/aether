package workflow

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// LeaderElector is the interface for distributed leader election.
type LeaderElector interface {
	TryAcquire(ctx context.Context) bool
	Release(ctx context.Context)
	IsLeader() bool
}

// RedisLeaderElector uses Redis SET NX to ensure only one workflow server
// instance runs the scheduler and DAG monitor at a time.
type RedisLeaderElector struct {
	client redis.UniversalClient
	key    string
	id     string
	ttl    time.Duration
	isLead bool
}

func NewRedisLeaderElector(client redis.UniversalClient, key, instanceID string) *RedisLeaderElector {
	return &RedisLeaderElector{
		client: client,
		key:    key,
		id:     instanceID,
		ttl:    30 * time.Second,
	}
}

// IsLeader returns whether this instance currently holds the leader lock.
func (l *RedisLeaderElector) IsLeader() bool {
	return l.isLead
}

// TryAcquire attempts to acquire or refresh the leader lock.
// Returns true if this instance is (or became) the leader.
func (l *RedisLeaderElector) TryAcquire(ctx context.Context) bool {
	if l.isLead {
		// Refresh existing lock
		ok, err := l.client.SetArgs(ctx, l.key, l.id, redis.SetArgs{
			Mode: "XX",
			TTL:  l.ttl,
		}).Result()
		if err != nil || ok != "OK" {
			// Lock lost or expired; try to re-acquire
			l.isLead = false
			return l.trySetNX(ctx)
		}
		return true
	}
	return l.trySetNX(ctx)
}

func (l *RedisLeaderElector) trySetNX(ctx context.Context) bool {
	ok, err := l.client.SetNX(ctx, l.key, l.id, l.ttl).Result()
	if err != nil {
		log.Warn().Err(err).Msg("leader election SetNX failed")
		return false
	}
	l.isLead = ok
	if ok {
		log.Info().Str("instance", l.id).Msg("acquired workflow leader lock")
	}
	return ok
}

// Release explicitly releases the leader lock.
func (l *RedisLeaderElector) Release(ctx context.Context) {
	if !l.isLead {
		return
	}
	// Only delete if we own it
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`)
	script.Run(ctx, l.client, []string{l.key}, l.id)
	l.isLead = false
	log.Info().Str("instance", l.id).Msg("released workflow leader lock")
}

// RunRefreshLoop periodically refreshes the leader lock.
// It stops when ctx is cancelled.
func (l *RedisLeaderElector) RunRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(l.ttl / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.Release(context.Background())
			return
		case <-ticker.C:
			l.TryAcquire(ctx)
		}
	}
}
