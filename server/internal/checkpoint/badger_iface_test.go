package checkpoint_test

import (
	"github.com/scitrera/aether/server/internal/checkpoint"
	"github.com/scitrera/aether/server/internal/gateway"
)

// Compile-time check: BadgerCheckpointStore satisfies the gateway CheckpointManager interface.
var _ gateway.CheckpointManager = (*checkpoint.BadgerCheckpointStore)(nil)
