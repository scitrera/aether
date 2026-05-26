// Package gateway helpers for translating between the proto RetryPolicy
// representation and the storage-layer tasks.RetryPolicy. The two are
// intentionally separate types: pb.RetryPolicy is the wire shape and lives
// in api/proto, while tasks.RetryPolicy is the JSON-persistable form used
// inside the task store and consumed by FailTask / rescheduleFn. Keeping
// them decoupled lets non-server packages (aetherlite, webhookservice)
// import the storage form without pulling in the proto package.
package gateway

import (
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/tasks"
)

// retryPolicyFromProto converts an incoming proto policy to the storage
// form. Returns nil when the caller didn't set a policy (preserving the
// legacy "no policy = immediate re-pend" behavior).
func retryPolicyFromProto(p *pb.RetryPolicy) *tasks.RetryPolicy {
	if p == nil {
		return nil
	}
	return &tasks.RetryPolicy{
		MaxAttempts:          p.GetMaxAttempts(),
		Backoff:              tasks.BackoffStrategy(p.GetBackoff()),
		InitialDelayMs:       p.GetInitialDelayMs(),
		MaxDelayMs:           p.GetMaxDelayMs(),
		JitterFactor:         p.GetJitterFactor(),
		ScheduleMs:           append([]int64(nil), p.GetScheduleMs()...),
		RetryableStatusCodes: append([]int32(nil), p.GetRetryableStatusCodes()...),
		HonorRetryAfter:      p.GetHonorRetryAfter(),
	}
}
