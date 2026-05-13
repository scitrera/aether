package state_test

import (
	"github.com/scitrera/aether/internal/gateway"
	"github.com/scitrera/aether/internal/state"
)

// Compile-time checks: Badger implementations satisfy gateway interfaces.
var _ gateway.SessionManager = (*state.BadgerSessionRegistry)(nil)
var _ state.TokenStore = (*state.BadgerTokenStore)(nil)
