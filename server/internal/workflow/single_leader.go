package workflow

import "context"

// SingleNodeLeaderElector is a no-op leader elector for lite/single-node mode.
// It always considers itself the leader, requiring no Redis dependency.
type SingleNodeLeaderElector struct{}

func NewSingleNodeLeaderElector() *SingleNodeLeaderElector {
	return &SingleNodeLeaderElector{}
}

func (s *SingleNodeLeaderElector) TryAcquire(_ context.Context) bool { return true }
func (s *SingleNodeLeaderElector) Release(_ context.Context)         {}
func (s *SingleNodeLeaderElector) IsLeader() bool                    { return true }
