package orchestration

import (
	"context"
	"time"

	"github.com/scitrera/aether/internal/logging"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/tasks"
)

// TaskWaker periodically scans WAITING_*/HIBERNATED tasks and fires wake
// transitions when their wake conditions are satisfied:
//
//   - WAITING_DEPENDENCY: all (or any, when WakeOnAny is set) dependencies
//     have reached a terminal state -> resume.
//   - HIBERNATED: ScheduledWakeUnixMs has elapsed -> resume.
//   - Any waiting state: TimeoutMs has elapsed since paused_at -> transition
//     to FAILED with reason "wait timeout".
//
// Authority and INPUT wake triggers are event-driven (handled elsewhere) and
// are not the waker's concern; the waker is the safety net for timer-driven
// wakes and the dependency reconciler. Multi-gateway safe — all transitions
// go through TaskAssignmentService state-machine ops which are idempotent
// on terminal tasks.
//
// Sibling of disconnect_reaper.go; lifecycle is identical (ticker loop,
// cancelled via Run's ctx).
type TaskWaker struct {
	taskStore   taskstore.Store
	taskService *TaskAssignmentService
	interval    time.Duration
	batchLimit  int
}

// NewTaskWaker builds a waker with sensible defaults. Call Run in its own
// goroutine; pass a cancellable ctx so server shutdown stops the loop.
func NewTaskWaker(taskStore taskstore.Store, taskService *TaskAssignmentService) *TaskWaker {
	return &TaskWaker{
		taskStore:   taskStore,
		taskService: taskService,
		interval:    10 * time.Second,
		batchLimit:  500,
	}
}

// Run blocks until ctx is done, ticking every w.interval.
func (w *TaskWaker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

// scan performs one pass over the waiting tasks and applies wake/timeout
// transitions where conditions are satisfied. Errors per-task are logged
// and skipped — a single bad row must never abort the whole scan.
func (w *TaskWaker) scan(ctx context.Context) {
	if w.taskStore == nil || w.taskService == nil {
		return
	}
	rows, err := w.taskStore.ListWaitingTasks(ctx, w.batchLimit)
	if err != nil {
		logging.Logger.Warn().Err(err).Msg("task waker: list failed")
		return
	}
	now := time.Now()
	for _, t := range rows {
		if t == nil || t.TaskID == "" {
			continue
		}
		// Defensive: ListWaitingTasks should only return waiting rows, but
		// double-check in case of a race against a concurrent transition.
		if !tasks.IsWaiting(t.Status) {
			continue
		}

		// 1) Timeout wake-to-fail. TimeoutMs is the maximum a task may sit
		// in any waiting state before the waker forcibly fails it. We need
		// a non-nil paused_at to measure against.
		if t.WaitSpec != nil && t.WaitSpec.TimeoutMs > 0 && t.PausedAt != nil {
			elapsed := now.Sub(*t.PausedAt)
			if elapsed >= time.Duration(t.WaitSpec.TimeoutMs)*time.Millisecond {
				reason := "wait timeout"
				if err := w.taskService.FailTask(ctx, t.TaskID, reason); err != nil {
					logging.Logger.Warn().Err(err).Str("task_id", t.TaskID).Msg("task waker: fail-task on timeout failed (non-fatal)")
					continue
				}
				logging.Logger.Info().
					Str("task_id", t.TaskID).
					Dur("waited_for", elapsed).
					Int64("timeout_ms", t.WaitSpec.TimeoutMs).
					Msg("task waker: failed task past wait timeout")
				continue
			}
		}

		// 2) Scheduled wake (hibernation timer). Independent of TimeoutMs;
		// fires whenever the wall-clock has passed the scheduled instant.
		if t.WaitSpec != nil && t.WaitSpec.ScheduledWakeUnixMs > 0 {
			wakeAt := time.UnixMilli(t.WaitSpec.ScheduledWakeUnixMs)
			if !now.Before(wakeAt) {
				if err := w.taskService.ResumeTask(ctx, t.TaskID, tasks.TaskStatusRunning); err != nil {
					logging.Logger.Warn().Err(err).Str("task_id", t.TaskID).Msg("task waker: scheduled-wake resume failed (non-fatal)")
					continue
				}
				logging.Logger.Info().
					Str("task_id", t.TaskID).
					Time("wake_at", wakeAt).
					Msg("task waker: resumed task on scheduled wake")
				continue
			}
		}

		// 3) Dependency reconciliation. Re-evaluate WaitSpec.DependsOn —
		// event-driven wakes from CompleteTask/FailTask/CancelTask/RejectTask
		// should normally catch this first, but the waker covers cases
		// where the terminal transition happened before the parent reached
		// WAITING_DEPENDENCY (race) or the event-driven path crashed.
		if t.Status == tasks.TaskStatusWaitingDependency {
			wake, err := w.taskService.shouldWakeParent(ctx, t)
			if err != nil {
				logging.Logger.Warn().Err(err).Str("task_id", t.TaskID).Msg("task waker: error evaluating dependency wake (non-fatal)")
				continue
			}
			if wake {
				if err := w.taskService.ResumeTask(ctx, t.TaskID, tasks.TaskStatusRunning); err != nil {
					logging.Logger.Warn().Err(err).Str("task_id", t.TaskID).Msg("task waker: dependency resume failed (non-fatal)")
					continue
				}
				logging.Logger.Info().
					Str("task_id", t.TaskID).
					Msg("task waker: resumed task on satisfied dependencies")
				continue
			}
		}
	}
}
