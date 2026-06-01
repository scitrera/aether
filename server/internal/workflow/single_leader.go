package workflow

import "context"

// SingleNodeLeaderElector is a no-op leader elector for lite/single-node mode.
// It always considers itself the leader, requiring no shared KV backend.
type SingleNodeLeaderElector struct{}

func NewSingleNodeLeaderElector() *SingleNodeLeaderElector {
	return &SingleNodeLeaderElector{}
}

func (s *SingleNodeLeaderElector) Start(_ context.Context) {}

func (s *SingleNodeLeaderElector) Shutdown(_ context.Context) error { return nil }

func (s *SingleNodeLeaderElector) IsLeader() bool { return true }
