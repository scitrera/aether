package router_test

import (
	"github.com/scitrera/aether/server/internal/gateway"
	"github.com/scitrera/aether/server/internal/router"
)

// Compile-time check: BadgerRouter satisfies the gateway MessageRouter interface.
var _ gateway.MessageRouter = (*router.BadgerRouter)(nil)
