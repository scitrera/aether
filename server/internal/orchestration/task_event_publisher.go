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
	"encoding/json"
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

// DomainEventPublisher publishes a raw EventPayload (JSON bytes) onto the
// event plane (event::*). A nil publisher disables feed-B emission.
type DomainEventPublisher interface {
	PublishDomainEvent(ctx context.Context, workspace string, payload []byte) error
}

// SetEventPublisher injects a TaskEventPublisher. Pass nil to disable (the
// default). Idempotent — last writer wins.
func (tas *TaskAssignmentService) SetEventPublisher(p TaskEventPublisher) {
	tas.eventPub = p
}

// SetDomainEventPublisher injects a DomainEventPublisher used for "feed B"
// domain event emission onto the event plane (event::*). Pass nil to disable
// (the default). Idempotent — last writer wins.
func (tas *TaskAssignmentService) SetDomainEventPublisher(p DomainEventPublisher) {
	tas.domainEventPub = p
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
	if pre == nil {
		return
	}
	if tas.eventPub != nil {
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
	// Feed B: domain event emission onto the event plane (event::*) when the
	// task opted into completion_event and reaches a selected terminal status.
	tas.publishCompletionEvent(ctx, pre, taskID, to)
}

// terminalStatusString returns the lowercase wire string for a terminal status
// (completed / failed / cancelled), used both for the default event name and
// the event data's "status" field. Non-terminal statuses return the raw
// TaskStatus string.
func terminalStatusString(s tasks.TaskStatus) string {
	return string(s)
}

// publishCompletionEvent emits a "feed B" domain event onto the event plane
// (event::*) when the just-completed task opted into completion_event and the
// terminal status passes the on_statuses filter. The payload is the raw JSON
// bytes of an EventPayload (source_agent/event_names/data/workspace), matching
// what a client's SendEvent publishes so the workflow engine's Router can
// json.Unmarshal it. Best-effort: a publish failure never blocks the
// transition (which has already committed to the store), matching
// publishStatusChange's non-fatal error style.
func (tas *TaskAssignmentService) publishCompletionEvent(ctx context.Context, pre *tasks.ExtendedTask, taskID string, to tasks.TaskStatus) {
	if pre == nil || tas.domainEventPub == nil || !tasks.IsTerminal(to) {
		return
	}
	cfg := pre.CompletionEvent
	if cfg == nil || !cfg.Enabled {
		return
	}
	// Status filter: if OnStatuses is set, emit only when `to` is listed;
	// empty ⇒ all terminal statuses emit.
	if len(cfg.OnStatuses) > 0 {
		match := false
		for _, s := range cfg.OnStatuses {
			if s == to {
				match = true
				break
			}
		}
		if !match {
			return
		}
	}

	statusStr := terminalStatusString(to)
	eventName := cfg.EventName
	if eventName == "" {
		eventName = "task." + statusStr
	}

	// Build the EventPayload as a plain map so the JSON keys are exact and
	// match the workflow engine's EventPayload struct tags.
	payload := map[string]any{
		"source_agent": "orchestrator",
		"workspace":    pre.Workspace,
		"event_names":  []string{eventName},
		"data": map[string]any{
			"task_id":        taskID,
			"status":         statusStr,
			"task_type":      pre.TaskType,
			"correlation_id": pre.CorrelationID,
			"root_task_id":   pre.RootTaskID,
			"metadata":       pre.Metadata,
		},
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		logging.Logger.Debug().Err(err).Str("task_id", taskID).Str("workspace", pre.Workspace).
			Msg("publishCompletionEvent: marshal failed (non-fatal)")
		return
	}
	if err := tas.domainEventPub.PublishDomainEvent(ctx, pre.Workspace, bytes); err != nil {
		logging.Logger.Debug().Err(err).Str("task_id", taskID).Str("workspace", pre.Workspace).
			Str("event_name", eventName).Msg("publishCompletionEvent: domain event publish failed (non-fatal)")
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
