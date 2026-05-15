package gateway

import (
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/tasks"
)

// waitReasonToStatus maps the wire-level WaitReason enum to the target
// TaskStatus a PAUSE/WAIT_FOR op should transition the task into.
// WAIT_REASON_UNSPECIFIED defaults to TASK_STATUS_WAITING_INPUT, matching the
// Phase 1 plan's "default = waiting_input" stance for unqualified pauses.
func waitReasonToStatus(reason pb.WaitReason) tasks.TaskStatus {
	switch reason {
	case pb.WaitReason_WAIT_REASON_INPUT:
		return tasks.TaskStatusWaitingInput
	case pb.WaitReason_WAIT_REASON_AUTHORITY:
		return tasks.TaskStatusWaitingAuthority
	case pb.WaitReason_WAIT_REASON_DEPENDENCY:
		return tasks.TaskStatusWaitingDependency
	case pb.WaitReason_WAIT_REASON_HIBERNATION:
		return tasks.TaskStatusHibernated
	default:
		return tasks.TaskStatusWaitingInput
	}
}

// taskStatusToWaitReason is the inverse of waitReasonToStatus. Returns
// WAIT_REASON_UNSPECIFIED for any non-waiting state.
func taskStatusToWaitReason(status tasks.TaskStatus) pb.WaitReason {
	switch status {
	case tasks.TaskStatusWaitingInput:
		return pb.WaitReason_WAIT_REASON_INPUT
	case tasks.TaskStatusWaitingAuthority:
		return pb.WaitReason_WAIT_REASON_AUTHORITY
	case tasks.TaskStatusWaitingDependency:
		return pb.WaitReason_WAIT_REASON_DEPENDENCY
	case tasks.TaskStatusHibernated:
		return pb.WaitReason_WAIT_REASON_HIBERNATION
	default:
		return pb.WaitReason_WAIT_REASON_UNSPECIFIED
	}
}

// protoWaitSpecToTasks converts a wire-level *pb.WaitSpec into the internal
// *tasks.WaitSpec form persisted by the task store. A nil input yields nil.
func protoWaitSpecToTasks(spec *pb.WaitSpec) *tasks.WaitSpec {
	if spec == nil {
		return nil
	}
	out := &tasks.WaitSpec{
		ExpectedPrincipal:   spec.GetExpectedPrincipal(),
		InputMatch:          spec.GetInputMatch(),
		AuthorityRequestID:  spec.GetAuthorityRequestId(),
		DependsOn:           spec.GetDependsOn(),
		WakeOnAny:           spec.GetWakeOnAny(),
		TimeoutMs:           spec.GetTimeoutMs(),
		ScheduledWakeUnixMs: spec.GetScheduledWakeUnixMs(),
	}
	switch spec.GetReason() {
	case pb.WaitReason_WAIT_REASON_INPUT:
		out.Reason = tasks.WaitReasonInput
	case pb.WaitReason_WAIT_REASON_AUTHORITY:
		out.Reason = tasks.WaitReasonAuthority
	case pb.WaitReason_WAIT_REASON_DEPENDENCY:
		out.Reason = tasks.WaitReasonDependency
	case pb.WaitReason_WAIT_REASON_HIBERNATION:
		out.Reason = tasks.WaitReasonHibernation
	}
	return out
}
