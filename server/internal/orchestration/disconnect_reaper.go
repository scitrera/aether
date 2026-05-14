package orchestration

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/aether/internal/logging"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
)

// DisconnectReaper periodically scans tasks whose worker has been disconnected
// longer than the task's grace window and fails them. Decoupled from session
// cleanup so workers can reconnect (here or to another gateway in the fleet)
// without losing their task. Multi-gateway safe — TaskService.FailTask is a
// state-machine transition; concurrent calls on already-terminal tasks are
// no-ops.
type DisconnectReaper struct {
	// taskStore is the tasks domain Store (internal/storage/tasks).
	taskStore   taskstore.Store
	taskService *TaskAssignmentService
	sessions    SessionLivenessProbe
	interval    time.Duration
	batchLimit  int
}

// SessionLivenessProbe lets the reaper double-check whether a task's assigned
// worker has reconnected since the last scan. Implementations should be cheap
// and lock-free.
type SessionLivenessProbe interface {
	HasActiveSessionForTask(ctx context.Context, taskID string) bool
}

// NewDisconnectReaper builds a reaper. Call Run in its own goroutine.
func NewDisconnectReaper(
	taskStore taskstore.Store,
	taskService *TaskAssignmentService,
	sessions SessionLivenessProbe,
) *DisconnectReaper {
	return &DisconnectReaper{
		taskStore:   taskStore,
		taskService: taskService,
		sessions:    sessions,
		interval:    10 * time.Second,
		batchLimit:  500,
	}
}

// Run blocks until ctx is done.
func (r *DisconnectReaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.scan(ctx)
		}
	}
}

func (r *DisconnectReaper) scan(ctx context.Context) {
	if r.taskStore == nil || r.taskService == nil {
		return
	}
	rows, err := r.taskStore.ListDisconnectedTasks(ctx, r.batchLimit)
	if err != nil {
		logging.Logger.Warn().Err(err).Msg("disconnect reaper: list failed")
		return
	}
	now := time.Now()
	for _, t := range rows {
		if t.DisconnectedAt == nil || t.GraceWindowMs <= 0 {
			continue
		}
		elapsed := now.Sub(*t.DisconnectedAt)
		if elapsed < time.Duration(t.GraceWindowMs)*time.Millisecond {
			continue
		}
		// Race protection: maybe the worker reconnected between SELECT and now.
		if r.sessions != nil && r.sessions.HasActiveSessionForTask(ctx, t.TaskID) {
			// Reconnect happened — clear the marker (defensive; the connect
			// path also clears it).
			if clearErr := r.taskStore.ClearTaskDisconnected(ctx, t.TaskID); clearErr != nil {
				logging.Logger.Debug().Err(clearErr).Str("task_id", t.TaskID).Msg("disconnect reaper: failed to clear marker on observed reconnect (non-fatal)")
			}
			continue
		}
		reason := fmt.Sprintf("worker disconnected and did not reconnect within grace window (%dms)", t.GraceWindowMs)
		if err := r.taskService.FailTask(ctx, t.TaskID, reason); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", t.TaskID).Msg("disconnect reaper: fail-task error")
			continue
		}
		logging.Logger.Info().
			Str("task_id", t.TaskID).
			Dur("disconnected_for", elapsed).
			Int64("grace_ms", t.GraceWindowMs).
			Msg("disconnect reaper: task failed past grace window")
	}
}
