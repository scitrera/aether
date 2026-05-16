// Phase 4 Stage B: task-scoped event emission.
//
// TaskEventPublisher is the narrow surface the TaskAssignmentService needs to
// fan task lifecycle transitions onto the per-task event topic
// (tk::{workspace}::{task_id}::events). The publisher implementation lives in
// internal/gateway alongside the router; orchestration depends only on the
// proto type and the interface here. A nil publisher is treated as "no-op" so
// existing callers (tests, lite mode without a gateway) keep working unchanged.

package orchestration

import (
	"context"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/tasks"
)

// TaskEventPublisher publishes task-scoped events to the per-task topic.
// The gateway provides a router-backed implementation; tests can substitute a
// recording stub.
type TaskEventPublisher interface {
	PublishTaskEvent(ctx context.Context, workspace, taskID string, event *pb.TaskEvent) error
}

// SetEventPublisher injects a TaskEventPublisher. Pass nil to disable (the
// default). Idempotent — last writer wins.
func (tas *TaskAssignmentService) SetEventPublisher(p TaskEventPublisher) {
	tas.eventPub = p
}

// taskStatusToProto maps the Go-side TaskStatus to the wire enum. The Go-side
// enum is finer than the wire enum (pending/assigned/starting collapse to
// QUEUED on the wire); callers downstream only need the coarse wire state.
func taskStatusToProto(s tasks.TaskStatus) pb.TaskStatus {
	switch s {
	case tasks.TaskStatusPending, tasks.TaskStatusAssigned, tasks.TaskStatusStarting:
		return pb.TaskStatus_TASK_STATUS_QUEUED
	case tasks.TaskStatusRunning:
		return pb.TaskStatus_TASK_STATUS_RUNNING
	case tasks.TaskStatusCompleted:
		return pb.TaskStatus_TASK_STATUS_COMPLETED
	case tasks.TaskStatusFailed:
		return pb.TaskStatus_TASK_STATUS_FAILED
	case tasks.TaskStatusCancelled:
		return pb.TaskStatus_TASK_STATUS_CANCELLED
	case tasks.TaskStatusWaitingInput:
		return pb.TaskStatus_TASK_STATUS_WAITING_INPUT
	case tasks.TaskStatusWaitingAuthority:
		return pb.TaskStatus_TASK_STATUS_WAITING_AUTHORITY
	case tasks.TaskStatusWaitingDependency:
		return pb.TaskStatus_TASK_STATUS_WAITING_DEPENDENCY
	case tasks.TaskStatusHibernated:
		return pb.TaskStatus_TASK_STATUS_HIBERNATED
	case tasks.TaskStatusRejected:
		return pb.TaskStatus_TASK_STATUS_REJECTED
	default:
		return pb.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

// publishStatusChange best-effort emits a TaskStatusChangedEvent on the task's
// event topic. Best-effort: errors are logged and ignored so a publish failure
// never blocks the lifecycle transition that has already committed to the
// store. Called after every lifecycle method on TaskAssignmentService.
func (tas *TaskAssignmentService) publishStatusChange(ctx context.Context, taskID, workspace, parentTaskID string, from, to tasks.TaskStatus, reason string) {
	if tas.eventPub == nil || taskID == "" || workspace == "" {
		return
	}
	evt := &pb.TaskEvent{
		TaskId:          taskID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       workspace,
		ParentTaskId:    parentTaskID,
		Event: &pb.TaskEvent_StatusChanged{
			StatusChanged: &pb.TaskStatusChangedEvent{
				FromStatus: taskStatusToProto(from),
				ToStatus:   taskStatusToProto(to),
				Reason:     reason,
			},
		},
	}
	if err := tas.eventPub.PublishTaskEvent(ctx, workspace, taskID, evt); err != nil {
		logging.Logger.Debug().Err(err).Str("task_id", taskID).Str("workspace", workspace).
			Str("from", string(from)).Str("to", string(to)).Msg("publishStatusChange: event publish failed (non-fatal)")
	}
}

// publishChildLifecycle best-effort emits a TaskChildLifecycleEvent on the
// parent task's event topic. classifier is one of "spawned" / "transitioned"
// / "completed".
func (tas *TaskAssignmentService) publishChildLifecycle(ctx context.Context, parentTaskID, parentWorkspace, childTaskID string, childStatus tasks.TaskStatus, classifier string) {
	if tas.eventPub == nil || parentTaskID == "" || parentWorkspace == "" || childTaskID == "" {
		return
	}
	evt := &pb.TaskEvent{
		TaskId:          parentTaskID,
		EmittedAtUnixMs: time.Now().UnixMilli(),
		Workspace:       parentWorkspace,
		Event: &pb.TaskEvent_ChildLifecycle{
			ChildLifecycle: &pb.TaskChildLifecycleEvent{
				ChildTaskId: childTaskID,
				ChildStatus: taskStatusToProto(childStatus),
				Lifecycle:   classifier,
			},
		},
	}
	if err := tas.eventPub.PublishTaskEvent(ctx, parentWorkspace, parentTaskID, evt); err != nil {
		logging.Logger.Debug().Err(err).Str("parent_task_id", parentTaskID).Str("child_task_id", childTaskID).
			Msg("publishChildLifecycle: event publish failed (non-fatal)")
	}
}

// emitTransitionEvent is the convenience wrapper invoked from each lifecycle
// method. pre is the task snapshot loaded BEFORE the transition (so
// from_status is meaningful); when pre is nil the helper short-circuits since
// we don't know the workspace. After publishing the parent-task event, if the
// transition is terminal AND pre has a non-empty ParentTaskID, also emit a
// child-lifecycle event so the parent's subscriber learns of the child's
// terminal state without needing to subscribe to every potential child.
func (tas *TaskAssignmentService) emitTransitionEvent(ctx context.Context, pre *tasks.ExtendedTask, taskID string, to tasks.TaskStatus, reason string) {
	if pre == nil || tas.eventPub == nil {
		return
	}
	tas.publishStatusChange(ctx, taskID, pre.Workspace, pre.ParentTaskID, pre.Status, to, reason)
	// Child-lifecycle relay: terminal transitions surface to the parent's
	// per-task subscription so a recursive subscriber sees child completion
	// without depending on a separate child subscription.
	if pre.ParentTaskID != "" && (tasks.IsTerminal(to) || tasks.IsWaiting(to)) {
		classifier := "transitioned"
		if tasks.IsTerminal(to) {
			classifier = "completed"
		}
		tas.publishChildLifecycle(ctx, pre.ParentTaskID, pre.Workspace, taskID, to, classifier)
	}
}

// loadTransitionMetadata fetches the current task row so lifecycle methods can
// derive from-status, workspace, and parent_task_id without each caller paying
// for a duplicate GetTask. Returns nil + nil err when the task is missing —
// callers treat that as "no event to publish".
func (tas *TaskAssignmentService) loadTransitionMetadata(ctx context.Context, taskID string) *tasks.ExtendedTask {
	if tas.taskStore == nil || taskID == "" {
		return nil
	}
	t, err := tas.taskStore.GetTask(ctx, taskID)
	if err != nil || t == nil {
		return nil
	}
	return t
}
