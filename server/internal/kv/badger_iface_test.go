package kv_test

import (
	"github.com/scitrera/aether/internal/gateway"
	"github.com/scitrera/aether/internal/kv"
)

// Compile-time check: BadgerKVStore satisfies the gateway KVReadWriter interface.
var _ gateway.KVReadWriter = (*kv.BadgerKVStore)(nil)
