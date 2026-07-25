package checkpoint_test

import (
	"github.com/scitrera/aether/server/internal/checkpoint"
	"github.com/scitrera/aether/server/internal/gateway"
)

// Compile-time check: JetStreamCheckpointStore satisfies the gateway CheckpointManager interface.
var _ gateway.CheckpointManager = (*checkpoint.JetStreamCheckpointStore)(nil)
