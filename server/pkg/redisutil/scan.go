package redisutil

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// ScanKeys iterates over Redis keys matching a pattern using SCAN.
// This is production-safe (non-blocking) unlike KEYS.
// It collects ALL matching keys - for bounded scans use ScanKeysLimit.
func ScanKeys(ctx context.Context, client redis.Cmdable, pattern string) ([]string, error) {
	return ScanKeysLimit(ctx, client, pattern, 0)
}

// ScanKeysLimit iterates over Redis keys matching a pattern using SCAN,
// collecting at most maxKeys results. If maxKeys <= 0, all keys are collected.
func ScanKeysLimit(ctx context.Context, client redis.Cmdable, pattern string, maxKeys int) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		var batch []string
		var err error
		batch, cursor, err = client.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		if maxKeys > 0 && len(keys) >= maxKeys {
			keys = keys[:maxKeys]
			break
		}
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}
