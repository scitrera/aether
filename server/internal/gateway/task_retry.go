// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"

	"github.com/scitrera/aether/server/internal/logging"
	"github.com/scitrera/aether/server/pkg/tasks"
)

// retryTask restores queue eligibility and wakes an online pool consumer.
// Offline workers discover the persisted queue marker when they connect.
// Callers must perform their existing task-operation authorization first.
func (s *GatewayServer) retryTask(ctx context.Context, taskID string) error {
	if err := s.taskStore.RetryTask(ctx, taskID); err != nil {
		return err
	}
	s.notifyTaskStatusChangeFromTaskID(ctx, taskID, "pending", "")
	task, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("retried task persisted; immediate delivery lookup failed")
		return nil
	}
	if task.AssignmentMode == tasks.AssignmentModePool {
		s.deliverPoolTaskToWorker(ctx, taskID, task.TargetImplementation, task.Workspace,
			task.TaskType, convertMetadataToString(task.Metadata), task.Payload)
	}
	return nil
}
