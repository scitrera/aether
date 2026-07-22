package gateway

import (
	"context"
	"strconv"

	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// Phase 2 "Design M": task-driven chat streaming on a per-task message lane.
//
// Chat tokens / appended messages for a running task are published to
// tk::{workspace}::{task_id}::msg as ordinary MessageEnvelopes (no new proto
// type). The gateway auto-subscribes the task's SUBJECT sessions to that lane
// for the task's lifetime:
//   - subscribe when the task's subject-notice reports status "running"
//     (createProgressFilterHandler, live path),
//   - subscribe-from-timestamp at connect time for already-RUNNING subject
//     tasks (backfillRunningSubjectTaskMessages, replay path),
//   - unsubscribe when the subject-notice reports a terminal status.
//
// Because the session principal IS the task's subject (the subject-notice only
// reaches the subject's own windows via the bare-user recipient filter), no new
// authorization is required to attach the subscription.

// taskMsgSubKey is the per-task subscription key under which a client's
// task-message-lane subscription is registered. Keying on the task id (rather
// than the topic string) keeps unsubscribe lookups independent of workspace.
func taskMsgSubKey(taskID string) string {
	return "taskmsg::" + taskID
}

// subscribeClientToTaskMessages subscribes a client to a task's per-task chat
// message lane (tk::{ws}::{task}::msg). Live path: invoked when a running
// subject-notice arrives, so no historical replay is wanted — new tokens flow
// from the point of subscription. Uses a per-(window, task) named consumer with
// resume-or-tail semantics (matching the timestamp/backfill variant's consumer
// name): a brand-new lane starts at the tail (not a full replay from seq 0,
// which the previous anonymous Subscribe did), and a reconnect resumes from the
// committed offset. Idempotent.
func (s *GatewayServer) subscribeClientToTaskMessages(client *ClientSession, workspace, taskID string) {
	if workspace == "" || taskID == "" {
		return
	}
	key := taskMsgSubKey(taskID)
	if client.HasSubscription(key) {
		return
	}
	topic, err := models.TaskMessageTopic(workspace, taskID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Str("task_id", taskID).Msg("invalid task-message topic; skipping subscribe")
		return
	}
	client.identityMu.RLock()
	consumerName := client.Identity.String() + models.IdentitySep + taskID
	client.identityMu.RUnlock()

	cancel, err := s.router.SubscribeExclusiveResumeOrTail(topic, consumerName, s.createMessageHandler(client))
	if err != nil {
		logging.Logger.Warn().Err(err).Str("topic", topic).Msg("failed to subscribe to task-message topic")
		return
	}
	client.AddSubscription(key, func() {
		cancel()
		topicSubscriptions.Dec()
	})
	topicSubscriptions.Inc()
}

// subscribeClientToTaskMessagesFromTimestamp subscribes a client to a task's
// per-task chat message lane with a per-window exclusive consumer that replays
// from startTimestampMs. Backfill/replay path: a reconnecting tab re-attaches
// to an already-RUNNING subject task and needs the chat tokens emitted while it
// was away. The consumer name is per-(window, task) so each tab replays
// independently without stealing another tab's offset. Idempotent.
func (s *GatewayServer) subscribeClientToTaskMessagesFromTimestamp(client *ClientSession, workspace, taskID string, startTimestampMs int64) {
	if workspace == "" || taskID == "" {
		return
	}
	key := taskMsgSubKey(taskID)
	if client.HasSubscription(key) {
		return
	}
	topic, err := models.TaskMessageTopic(workspace, taskID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Str("task_id", taskID).Msg("invalid task-message topic; skipping backfill subscribe")
		return
	}
	client.identityMu.RLock()
	consumerName := client.Identity.String() + models.IdentitySep + taskID
	client.identityMu.RUnlock()

	cancel, err := s.router.SubscribeExclusiveFromTimestamp(topic, consumerName, startTimestampMs, s.createMessageHandler(client))
	if err != nil {
		logging.Logger.Warn().Err(err).Str("topic", topic).Msg("failed to subscribe-from-timestamp to task-message topic")
		return
	}
	client.AddSubscription(key, func() {
		cancel()
		topicSubscriptions.Dec()
	})
	topicSubscriptions.Inc()
}

// unsubscribeClientFromTaskMessages removes a client's task-message-lane
// subscription for the given task id (no-op when absent).
func (s *GatewayServer) unsubscribeClientFromTaskMessages(client *ClientSession, taskID string) {
	client.RemoveSubscription(taskMsgSubKey(taskID))
}

// backfillRunningSubjectTaskMessages, at user-session setup time, finds the
// user's currently-RUNNING tasks for which the user is the participating OBO
// subject and subscribes the session to each task's message lane from the
// task's start timestamp. This re-attaches a freshly-connected (or
// just-reconnected) browser tab to in-flight chat streams it would otherwise
// miss, because the running notice that drives the live subscribe path already
// fired before the tab connected.
//
// The task-store query filters server-side by workspace + RUNNING status +
// subject (SubjectType=User, SubjectID=user). The subject_participant opt-in
// flag is not a storage column, so it is checked client-side. Best-effort: any
// query failure is logged and the connect path proceeds.
func (s *GatewayServer) backfillRunningSubjectTaskMessages(ctx context.Context, client *ClientSession) {
	if s.taskStore == nil {
		return
	}

	client.identityMu.RLock()
	identity := client.Identity
	client.identityMu.RUnlock()

	if identity.Type != models.PrincipalUser || identity.ID == "" || identity.Workspace == "" {
		return
	}

	filter := &tasks.TaskFilter{
		Workspace:   identity.Workspace,
		Statuses:    []tasks.TaskStatus{tasks.TaskStatusRunning},
		SubjectType: string(models.PrincipalUser),
		SubjectID:   identity.ID,
		Limit:       maxRecursiveSubscriptionFanout,
	}
	rows, err := s.taskStore.ListTasks(ctx, filter)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("user_id", identity.ID).Str("workspace", identity.Workspace).Msg("backfillRunningSubjectTaskMessages: ListTasks failed")
		return
	}

	for _, row := range rows {
		if row == nil || row.TaskID == "" {
			continue
		}
		// Defense-in-depth: re-check subject + opt-in client-side. The filter
		// already gated subject_type/subject_id; subject_participant is not a
		// storage column.
		if !taskMetadataTruthy(row.Metadata["subject_participant"]) {
			continue
		}
		if row.Authority.SubjectType != string(models.PrincipalUser) || row.Authority.SubjectID != identity.ID {
			continue
		}

		workspace := row.Workspace
		if workspace == "" {
			workspace = identity.Workspace
		}
		startMs := backfillStartTimestampMs(row)
		s.subscribeClientToTaskMessagesFromTimestamp(client, workspace, row.TaskID, startMs)
	}
}

// backfillStartTimestampMs derives the replay start point for a running subject
// task, preferring the metadata["started_at_ms"] hint and falling back to the
// task's StartedAt timestamp. Returns 0 ("no hint") when neither is available.
func backfillStartTimestampMs(row *tasks.Task) int64 {
	if v := metadataString(row.Metadata, "started_at_ms"); v != "" {
		if parsed, perr := strconv.ParseInt(v, 10, 64); perr == nil && parsed > 0 {
			return parsed
		}
	}
	if row.StartedAt != nil {
		return row.StartedAt.UnixMilli()
	}
	return 0
}
