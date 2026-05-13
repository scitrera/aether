package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/kv"
)

// =============================================================================
// KV Store
// =============================================================================

func (p *GatewayStateProvider) GetKVKeys(ctx context.Context, scope, prefix string) ([]string, error) {
	if p.kvStore == nil {
		return nil, fmt.Errorf("kv store not available")
	}

	// Use admin identity for full access
	kvScope := kv.ScopeFromString(scope)

	result, err := p.kvStore.List(ctx, adminIdentity, kvScope, "", "")
	if err != nil {
		return nil, err
	}

	var keys []string
	for k := range result {
		// Apply prefix filter if specified
		if prefix == "" || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (p *GatewayStateProvider) GetKVValue(ctx context.Context, scope, key string) (*admin.KVEntry, error) {
	if p.kvStore == nil {
		return nil, fmt.Errorf("kv store not available")
	}

	kvScope := kv.ScopeFromString(scope)

	value, err := p.kvStore.Get(ctx, adminIdentity, kvScope, key, "", "")
	if err != nil {
		return nil, err
	}

	return &admin.KVEntry{
		Key:   key,
		Value: value,
		Scope: scope,
	}, nil
}

func (p *GatewayStateProvider) SetKVValue(ctx context.Context, scope, key, value string, ttl int64) error {
	if p.kvStore == nil {
		return fmt.Errorf("kv store not available")
	}

	kvScope := kv.ScopeFromString(scope)

	// Convert ttl from seconds to time.Duration
	ttlDuration := time.Duration(ttl) * time.Second

	return p.kvStore.Set(ctx, adminIdentity, kvScope, key, value, "", "", ttlDuration)
}

func (p *GatewayStateProvider) DeleteKVKey(ctx context.Context, scope, key string) error {
	if p.kvStore == nil {
		return fmt.Errorf("kv store not available")
	}

	kvScope := kv.ScopeFromString(scope)

	return p.kvStore.Delete(ctx, adminIdentity, kvScope, key, "", "")
}
