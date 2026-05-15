package orchestration

import (
	"context"
	"time"
)

// TaskDispatcher abstracts the orchestration task dispatch lifecycle.
// The PostgreSQL-backed NotifyTaskDispatcher satisfies this interface;
// in-process implementations can provide lightweight alternatives for lite mode.
type TaskDispatcher interface {
	// SetCallback sets the callback function for handling received task notifications.
	SetCallback(callback func(task *OrchestrationTaskNotification))

	// Start begins listening for orchestration tasks (via NOTIFY, polling, or in-process channel).
	Start(ctx context.Context) error

	// Stop gracefully shuts down the dispatcher.
	Stop()

	// ClaimTask attempts to claim a task for an orchestrator.
	// Returns ErrTaskAlreadyClaimed if another gateway already claimed it.
	ClaimTask(ctx context.Context, queueID, orchestratorID string) error

	// UnclaimTask releases a claimed task back to pending status for retry.
	UnclaimTask(ctx context.Context, queueID string) error

	// CompleteTask marks a task as completed.
	CompleteTask(ctx context.Context, queueID string) error

	// FailTask marks a task as failed with an error message.
	FailTask(ctx context.Context, queueID, errorMsg string) error

	// GetTaskDetails retrieves full task details including launch params.
	GetTaskDetails(ctx context.Context, queueID string) (*OrchestratedTaskPayload, error)

	// RecoverStaleClaims finds tasks stuck in 'claimed' status and unclaims them.
	// Returns the number of tasks recovered.
	RecoverStaleClaims(ctx context.Context, threshold time.Duration) (int, error)
}

// Compile-time checks: both dispatcher impls satisfy TaskDispatcher.
var _ TaskDispatcher = (*NotifyTaskDispatcher)(nil)
var _ TaskDispatcher = (*PollingTaskDispatcher)(nil)
