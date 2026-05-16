package gateway

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
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
	if hib := spec.GetHibernation(); hib != nil {
		out.Hibernation = &tasks.HibernationDescriptor{
			CheckpointKey:    hib.GetCheckpointKey(),
			ResumeSessionID:  hib.GetResumeSessionId(),
			WakeEventTypes:   hib.GetWakeEventTypes(),
			EscalationPolicy: hib.GetEscalationPolicy(),
		}
	}
	return out
}

// validateHibernationPrecondition enforces the Phase 3 invariant that a task
// transitioning to HIBERNATED must already have a checkpoint saved under the
// descriptor's key, scoped to the task's assignee identity. Checkpoints in the
// gateway are identity-scoped; the worker SAVE'd it via the checkpoint store
// before requesting the PAUSE op. Returns nil when the precondition is met (or
// when there is no checkpoint manager configured — degrade gracefully rather
// than block hibernation entirely; the lite/in-process gateway may not wire a
// checkpoint backend in every test path).
//
// Implementation note: we use checkpointManager.Load and discard the payload
// rather than adding an Exists method. The checkpoint store wraps Get with a
// circuit breaker; for the rare hibernation transition the extra payload fetch
// is negligible compared to the value of reusing the existing interface.
func (s *GatewayServer) validateHibernationPrecondition(ctx context.Context, task *tasks.Task, spec *tasks.WaitSpec) error {
	if spec == nil || spec.Hibernation == nil {
		return errors.New("hibernation requires wait_spec.hibernation with a checkpoint_key")
	}
	if spec.Hibernation.CheckpointKey == "" {
		return errors.New("hibernation requires a non-empty wait_spec.hibernation.checkpoint_key")
	}
	if s.checkpoints == nil {
		// No checkpoint manager wired (e.g. minimal in-process test gateway).
		// Stage B will add end-to-end validation; for Stage A, accept the
		// descriptor as-is so the data model continues to flow.
		return nil
	}
	if task == nil || task.AssignedTo == "" {
		return errors.New("hibernation requires the task to have an assignee identity for checkpoint scoping")
	}
	identity, err := models.ParseIdentity(task.AssignedTo)
	if err != nil {
		return fmt.Errorf("hibernation precondition: failed to parse assignee identity %q: %w", task.AssignedTo, err)
	}
	cp, err := s.checkpoints.Load(ctx, identity, spec.Hibernation.CheckpointKey)
	if err != nil {
		return fmt.Errorf("hibernation precondition: checkpoint load failed: %w", err)
	}
	if cp == nil {
		return fmt.Errorf("hibernation requires a saved checkpoint with key %q for identity %s",
			spec.Hibernation.CheckpointKey, task.AssignedTo)
	}
	return nil
}

// notifyTaskHibernated sends a DownstreamMessage_TaskHibernated to the worker
// assigned to a task that just transitioned to HIBERNATED. Best-effort: if the
// worker has already disconnected (or the gateway never tracked a session for
// the assignee), the call is a no-op. The disconnect_reaper now skips
// HIBERNATED tasks (Stage A), so an unanswered TaskHibernated is safe — the
// worker's session will be reaped via normal session-expiry semantics whether
// or not it receives this signal.
//
// The descriptor is echoed back so the worker can verify it's hibernating
// against the expected checkpoint key. Empty descriptor is permitted (the
// PAUSE precondition would have rejected an empty checkpoint_key already, so
// a nil descriptor here is purely defensive).
func (s *GatewayServer) notifyTaskHibernated(ctx context.Context, task *tasks.Task, spec *tasks.WaitSpec) {
	if task == nil || task.AssignedTo == "" {
		return
	}
	assigneeIdentity, err := models.ParseIdentity(task.AssignedTo)
	if err != nil {
		logging.Logger.Debug().
			Err(err).
			Str("task_id", task.TaskID).
			Str("assignee", task.AssignedTo).
			Msg("notifyTaskHibernated: failed to parse assignee identity; skipping signal")
		return
	}
	client := s.getClientByIdentity(assigneeIdentity)
	if client == nil {
		logging.Logger.Debug().
			Str("task_id", task.TaskID).
			Str("assignee", task.AssignedTo).
			Msg("notifyTaskHibernated: no live session for assignee; skipping signal")
		return
	}

	msg := &pb.TaskHibernated{TaskId: task.TaskID}
	if spec != nil && spec.Hibernation != nil {
		msg.Descriptor_ = &pb.HibernationDescriptor{
			CheckpointKey:    spec.Hibernation.CheckpointKey,
			ResumeSessionId:  spec.Hibernation.ResumeSessionID,
			WakeEventTypes:   spec.Hibernation.WakeEventTypes,
			EscalationPolicy: spec.Hibernation.EscalationPolicy,
		}
	}
	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskHibernated{TaskHibernated: msg},
	}); err != nil {
		logging.Logger.Debug().
			Err(err).
			Str("task_id", task.TaskID).
			Str("assignee", task.AssignedTo).
			Msg("notifyTaskHibernated: SafeSend failed (worker likely already disconnected)")
		return
	}
	logging.Logger.Info().
		Str("task_id", task.TaskID).
		Str("assignee", task.AssignedTo).
		Msg("notifyTaskHibernated: signaled worker to disconnect cleanly")
	_ = ctx // ctx accepted for parity with sibling notify helpers; SafeSend has its own write timeout.
}
