package kv_test

import (
	"github.com/scitrera/aether/server/internal/gateway"
	"github.com/scitrera/aether/server/internal/kv"
)

// Compile-time check: JetStreamKVStore satisfies the gateway KVReadWriter interface.
var _ gateway.KVReadWriter = (*kv.JetStreamKVStore)(nil)
