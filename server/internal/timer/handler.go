package timer

import (
	"context"
	"time"

	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/tasks"
)

// TimeoutHandler handles task timeout events and updates task state
type TimeoutHandler struct {
	taskStore  *tasks.TaskStore
	timerSeq   *TimerSequence
	reschedule func(taskID string, delay time.Duration) // For retry scheduling
}

// NewTimeoutHandler creates a new TimeoutHandler
func NewTimeoutHandler(taskStore *tasks.TaskStore, timerSeq *TimerSequence, rescheduleFn func(taskID string, delay time.Duration)) *TimeoutHandler {
	h := &TimeoutHandler{
		taskStore:  taskStore,
		timerSeq:   timerSeq,
		reschedule: rescheduleFn,
	}

	// Set up callbacks
	timerSeq.SetScheduleToStartCallback(h.handleScheduleToStartTimeout)
	timerSeq.SetStartToCloseCallback(h.handleStartToCloseTimeout)
	timerSeq.SetHeartbeatCallback(h.handleHeartbeatTimeout)
	timerSeq.SetScheduleToCloseCallback(h.handleScheduleToCloseTimeout)

	return h
}

// handleScheduleToStartTimeout handles when a task is not claimed within the allowed time
func (h *TimeoutHandler) handleScheduleToStartTimeout(taskID string) {
	ctx := context.Background()

	task, err := h.taskStore.GetTask(ctx, taskID)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to get task")
		return
	}

	// Task should still be PENDING or ASSIGNED
	if task.Status != tasks.TaskStatusPending && task.Status != tasks.TaskStatusAssigned {
		logging.Logger.Warn().Str("task_id", taskID).Str("status", string(task.Status)).Msg("ScheduleToStart timeout but task not in expected state")
		return
	}

	logging.Logger.Info().Str("task_id", taskID).Str("status", string(task.Status)).Int("retry_count", task.RetryCount).Msg("ScheduleToStart timeout")

	// Check if we should retry or send to DLQ
	if task.RetryCount >= task.MaxRetries-1 {
		// Max retries reached, send to DLQ
		err := h.sendToDLQ(ctx, task, "timeout", "ScheduleToStart timeout - max retries exceeded")
		if err != nil {
			logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to send task to DLQ")
		}
		return
	}

	// Calculate backoff for retry (simple exponential backoff)
	backoff := time.Duration(float64(time.Second) * (1 + float64(task.RetryCount+1)*0.5))

	// Update task state to FAILED with retry info
	nextRetry := time.Now().Add(backoff)
	err = h.taskStore.FailTaskWithRetry(ctx, taskID, "TIMEOUT", "ScheduleToStart timeout", &nextRetry)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to update task for retry")
		return
	}

	// Remove timers for this task
	h.timerSeq.Remove(taskID)

	// Reschedule for later
	if h.reschedule != nil {
		h.reschedule(taskID, backoff)
	}
}

// handleStartToCloseTimeout handles when a worker takes too long to complete a task
func (h *TimeoutHandler) handleStartToCloseTimeout(taskID string) {
	ctx := context.Background()

	task, err := h.taskStore.GetTask(ctx, taskID)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to get task")
		return
	}

	// Task should be RUNNING
	if task.Status != tasks.TaskStatusRunning {
		logging.Logger.Warn().Str("task_id", taskID).Str("status", string(task.Status)).Msg("StartToClose timeout but task not running")
		return
	}

	logging.Logger.Info().Str("task_id", taskID).Int("retry_count", task.RetryCount).Msg("StartToClose timeout")

	// Check retry limits (retry_count will be incremented by FailTaskWithRetry)
	if task.RetryCount+1 >= task.MaxRetries {
		// Max retries reached, send to DLQ
		err = h.sendToDLQ(ctx, task, "timeout", "StartToClose timeout - max retries exceeded")
		if err != nil {
			logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to send task to DLQ")
		}
		return
	}

	// Calculate backoff
	backoff := time.Duration(float64(time.Second) * (1 + float64(task.RetryCount+1)))

	// Update task state for retry
	nextRetry := time.Now().Add(backoff)
	err = h.taskStore.FailTaskWithRetry(ctx, taskID, "TIMEOUT", "StartToClose timeout - worker did not complete in time", &nextRetry)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to update task for retry")
		return
	}

	// Remove timers
	h.timerSeq.Remove(taskID)

	// Reschedule
	if h.reschedule != nil {
		h.reschedule(taskID, backoff)
	}
}

// handleHeartbeatTimeout handles when a worker stops sending heartbeat updates
func (h *TimeoutHandler) handleHeartbeatTimeout(taskID string) {
	ctx := context.Background()

	task, err := h.taskStore.GetTask(ctx, taskID)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to get task")
		return
	}

	// Task should be RUNNING
	if task.Status != tasks.TaskStatusRunning {
		logging.Logger.Warn().Str("task_id", taskID).Str("status", string(task.Status)).Msg("heartbeat timeout but task not running")
		return
	}

	logging.Logger.Info().Str("task_id", taskID).Interface("last_heartbeat", task.LastHeartbeat).Msg("heartbeat timeout")

	// For heartbeat timeout, we can be more aggressive with retries since the worker
	// might still be processing and can resume from checkpoint

	if task.RetryCount+1 >= task.MaxRetries {
		// Max retries reached, send to DLQ
		err = h.sendToDLQ(ctx, task, "timeout", "Heartbeat timeout - max retries exceeded")
		if err != nil {
			logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to send task to DLQ")
		}
		return
	}

	// Shorter backoff for heartbeat timeouts (worker might still be working)
	nextRetryCount := task.RetryCount + 1
	backoff := 30 * time.Second
	if nextRetryCount > 3 {
		backoff = time.Duration(float64(time.Second) * float64(nextRetryCount*2))
	}

	// Update task state for retry
	nextRetry := time.Now().Add(backoff)
	err = h.taskStore.FailTaskWithRetry(ctx, taskID, "TIMEOUT", "Heartbeat timeout - worker stopped sending progress updates", &nextRetry)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to update task for retry")
		return
	}

	// Remove timers
	h.timerSeq.Remove(taskID)

	// Reschedule
	if h.reschedule != nil {
		h.reschedule(taskID, backoff)
	}
}

// handleScheduleToCloseTimeout handles the overall deadline for a task
func (h *TimeoutHandler) handleScheduleToCloseTimeout(taskID string) {
	ctx := context.Background()

	task, err := h.taskStore.GetTask(ctx, taskID)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to get task")
		return
	}

	logging.Logger.Info().Str("task_id", taskID).Str("status", string(task.Status)).Msg("ScheduleToClose timeout")

	// ScheduleToClose is the absolute deadline - no retries, send directly to DLQ
	err = h.sendToDLQ(ctx, task, "timeout", "ScheduleToClose timeout - overall deadline exceeded")
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to send task to DLQ")
		return
	}

	// Remove timers
	h.timerSeq.Remove(taskID)
}

// sendToDLQ sends a task to the dead letter queue
func (h *TimeoutHandler) sendToDLQ(ctx context.Context, task *tasks.Task, category string, reason string) error {
	logging.Logger.Info().Str("task_id", task.TaskID).Str("category", category).Str("reason", reason).Msg("sending task to DLQ")

	// Note: In a full implementation, you'd have a DLQWriter service
	// For now, we update the task status to DLQ
	return h.taskStore.UpdateTaskStatus(ctx, task.TaskID, tasks.TaskStatusDLQ)
}

// StartTaskTimers initializes timers for a newly created task
func (h *TimeoutHandler) StartTaskTimers(task *tasks.Task) {
	config := TimeoutConfig{
		ScheduleToStart: time.Duration(task.ScheduleToStartMs) * time.Millisecond,
		StartToClose:    time.Duration(task.StartToCloseMs) * time.Millisecond,
		Heartbeat:       time.Duration(task.HeartbeatTimeoutMs) * time.Millisecond,
		ScheduleToClose: time.Duration(task.ScheduleToCloseMs) * time.Millisecond,
	}

	h.timerSeq.CreateTimers(task.TaskID, task.CreatedAt, config, string(task.Status))
}

// UpdateTaskExecution updates the timer state when a task starts running
func (h *TimeoutHandler) UpdateTaskExecution(taskID string, startedAt time.Time, config TimeoutConfig) {
	h.timerSeq.UpdateTaskState(taskID, "running", &startedAt, config)
}

// RecordHeartbeat records a heartbeat and resets the heartbeat timer
func (h *TimeoutHandler) RecordHeartbeat(taskID string, config TimeoutConfig) {
	now := time.Now()
	h.timerSeq.ScheduleHeartbeatTimer(taskID, config, now)
}

// CompleteTask removes all timers for a completed task
func (h *TimeoutHandler) CompleteTask(taskID string) {
	h.timerSeq.Remove(taskID)
}

// FailTask removes all timers for a failed task (but retries may reschedule)
func (h *TimeoutHandler) FailTask(taskID string) {
	h.timerSeq.Remove(taskID)
}
