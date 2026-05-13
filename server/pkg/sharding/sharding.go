// Package sharding centralises the fan-in topic sharding scheme used by the
// Workflow Engine (event::receiver{N}) and Metrics Bridge (metric::receiver{N})
// subscription topologies.
//
// Today the system runs with a single shard (TotalShards()==1). The functions
// here are intentionally introduced as stubs so that the gateway, identity
// model, and SDKs can route through a single decision point. When multi-shard
// fan-in goes live, only ShardForWorkspace and TotalShards need to change.
package sharding

import (
	"strconv"
	"strings"
)

// IdentitySep mirrors models.IdentitySep without taking the import dependency
// (this package is imported by pkg/models, so the dependency would otherwise
// be circular).
const identitySep = "::"

// ShardForWorkspace returns the shard index (0-based) for the given workspace.
// Stub: always returns 0. Future versions will hash workspace into a pool of
// totalShards shards (likely fnv64) once multi-shard fan-in goes live.
func ShardForWorkspace(workspace string, totalShards int) int {
	// TODO(sharding-v2): replace with stable hash when totalShards > 1.
	_ = workspace
	_ = totalShards
	return 0
}

// ReceiverTopic builds the topic name for a fan-in shard:
// "{prefix}::receiver{n}". Prefix is typically "event" or "metric".
func ReceiverTopic(prefix string, shard int) string {
	return prefix + identitySep + "receiver" + strconv.Itoa(shard)
}

// IsReceiverTopic reports whether topic matches "{prefix}::receiver{N}" for any
// prefix and any non-negative shard index. Used by workspace-extraction logic
// to treat fan-in topics as workspace-agnostic.
func IsReceiverTopic(topic string) bool {
	parts := strings.Split(topic, identitySep)
	if len(parts) < 2 {
		return false
	}
	return strings.HasPrefix(parts[1], "receiver")
}

// TotalShards returns the configured number of fan-in shards. Currently fixed
// at 1; future: read from AETHER_FANIN_SHARDS env or gateway config.
func TotalShards() int {
	// TODO(sharding-v2): plumb from config / env.
	return 1
}
