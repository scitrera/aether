package kv_test

import (
	"github.com/scitrera/aether/server/internal/gateway"
	"github.com/scitrera/aether/server/internal/kv"
)

// Compile-time check: BadgerKVStore satisfies the gateway KVReadWriter interface.
var _ gateway.KVReadWriter = (*kv.BadgerKVStore)(nil)
