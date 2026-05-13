package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
)

// bareUserRecipientID extracts the user ID from a bare `us::{user}` recipient
// string. Returns empty + false when the input has a window specifier (full
// form) or doesn't start with the user prefix.
func bareUserRecipientID(recipient string) (string, bool) {
	const userPrefix = "us" + models.IdentitySep
	if !strings.HasPrefix(recipient, userPrefix) {
		return "", false
	}
	rest := recipient[len(userPrefix):]
	if rest == "" || strings.Contains(rest, models.IdentitySep) {
		return "", false
	}
	return rest, true
}

// isBareUserRecipientMatch reports whether `recipient` is a bare user-level
// identity (us::{user}, no window specifier) that should match every one of
// that user's window-specific topics (us::{user}::{window}). Returns false
// when the recipient already has a window specifier or is not a User-typed
// identity at all.
//
// Example:
//
//	isBareUserRecipientMatch("us::alice", "us::alice::tab1") => true
//	isBareUserRecipientMatch("us::alice::tab1", "us::alice::tab1") => false  (caller already exact-matched)
//	isBareUserRecipientMatch("us::alice::tab1", "us::alice::tab2") => false
//	isBareUserRecipientMatch("ag::ws::impl::spec", "us::alice::tab1") => false
func isBareUserRecipientMatch(recipient, myTopic string) bool {
	const userPrefix = "us" + models.IdentitySep
	if !strings.HasPrefix(recipient, userPrefix) {
		return false
	}
	// Bare user identity has exactly two segments after the "us" prefix:
	// "us::{user}". Anything with more segments is window-specific.
	if strings.Contains(recipient[len(userPrefix):], models.IdentitySep) {
		return false
	}
	// myTopic must be a window-specific identity for the same user:
	// "us::{user}::{window}".
	return strings.HasPrefix(myTopic, recipient+models.IdentitySep)
}

// handleProgressReport processes an upstream progress report from an agent or task.
// It validates the sender, builds a ProgressUpdate, publishes it to the pg.{workspace}
// RabbitMQ stream, and updates the task heartbeat in the database as a side-effect.
func (s *GatewayServer) handleProgressReport(ctx context.Context, client *ClientSession, report *pb.ProgressReport) {
	ctx, span := tracing.Tracer.Start(ctx, "gateway.HandleProgressReport")
	defer span.End()

	// Snapshot identity under RLock to avoid data race with handleSwitchWorkspace
	client.identityMu.RLock()
	sender := client.Identity
	client.identityMu.RUnlock()

	span.SetAttributes(
		attribute.String("sender", sender.String()),
		attribute.String("task_id", report.TaskId),
	)

	// Only agents and tasks can report progress
	if sender.Type != models.PrincipalAgent && sender.Type != models.PrincipalTask {
		if sendErr := client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Error{
				Error: &pb.ErrorResponse{
					Code:    "ERR_INVALID_PRINCIPAL",
					Message: "only agents and tasks can report progress",
				},
			},
		}); sendErr != nil {
			logging.Logger.Error().Err(sendErr).Msg("failed to send progress error response")
		}
		return
	}

	// Per-client rate limiting (reuse the same limiter as messages)
	if client.rateLimiter != nil && !client.rateLimiter.Allow() {
		logging.Logger.Warn().Str("identity", sender.String()).Msg("progress report rate limit exceeded")
		if sendErr := client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Error{
				Error: &pb.ErrorResponse{
					Code:      "ERR_RATE_LIMITED",
					Message:   "progress report rate limit exceeded",
					Retryable: true,
				},
			},
		}); sendErr != nil {
			logging.Logger.Error().Err(sendErr).Msg("failed to send progress rate limit error")
		}
		return
	}

	now := time.Now()

	// Recipient-aware topic routing. When report.Recipient is a user identity
	// topic, publish to the per-user progress topic pg.us.<user> so the update
	// can cross workspaces and reach the user even when the sender is hosted
	// in a different workspace (e.g. an `_apps` agent reporting chat progress
	// to a user chatting in `default`).
	//
	// Recipient forms for users:
	//   - us::{user}::{window}  → window-specific, filter delivers to that window only
	//   - us::{user}            → bare user, filter delivers to ALL of the user's windows
	//
	// Both forms publish to the same pg.us.{user} stream; per-window
	// fan-out and matching happens in the gateway-side filter handler.
	// For empty or non-user recipients, fall back to pg.{sender.Workspace}
	// broadcast — preserving orchestrator/parent-agent consumption patterns
	// for task-kind progress.
	progressTopic, err := models.ProgressTopic(sender.Workspace)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", sender.Workspace).Msg("invalid workspace for progress topic; dropping report")
		return
	}
	if report.Recipient != "" {
		// Try the full us::{user}::{window} form first (3-part identity).
		if recipientIdentity, parseErr := models.ParseIdentity(report.Recipient); parseErr == nil &&
			recipientIdentity.Type == models.PrincipalUser &&
			recipientIdentity.ID != "" {
			t, perr := models.UserProgressTopic(recipientIdentity.ID)
			if perr != nil {
				logging.Logger.Warn().Err(perr).Str("user_id", recipientIdentity.ID).Msg("invalid user_id in progress recipient")
				return
			}
			progressTopic = t
		} else if userID, ok := bareUserRecipientID(report.Recipient); ok {
			// Fall back to the bare us::{user} form (2-part identity).
			// ParseIdentity intentionally rejects this — it's only valid
			// as a progress recipient marker, not as a connectable identity.
			t, perr := models.UserProgressTopic(userID)
			if perr != nil {
				logging.Logger.Warn().Err(perr).Str("user_id", userID).Msg("invalid user_id in bare progress recipient")
				return
			}
			progressTopic = t
		}
	}

	// Build the ProgressUpdate (this is what gets published to the stream)
	update := &pb.ProgressUpdate{
		Source:      sender.ToTopic(),
		TaskId:      report.TaskId,
		State:       report.State,
		Completion:  report.Completion,
		Summary:     report.Summary,
		Step:        report.Step,
		TimestampMs: now.UnixMilli(),
		Workspace:   sender.Workspace,
		RequestId:   report.RequestId,
		Metadata:    report.Metadata,
		Recipient:   report.Recipient,
		Kind:        report.Kind,
	}

	updateBytes, err := proto.Marshal(update)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("failed to marshal progress update")
		return
	}

	// Publish to pg.{workspace} stream via circuit breaker
	err = s.publishBreaker.Execute(func() error {
		return s.router.Publish(ctx, progressTopic, updateBytes)
	})
	if err != nil {
		var errCode, errMsg string
		if err == circuitbreaker.ErrCircuitOpen {
			logging.Logger.Warn().Str("identity", sender.String()).Str("topic", progressTopic).Msg("progress publish circuit breaker open")
			errCode = "ERR_CIRCUIT_OPEN"
			errMsg = "message broker temporarily unavailable, please retry"
		} else {
			logging.Logger.Error().Err(err).Str("topic", progressTopic).Msg("failed to publish progress update")
			errCode = "ERR_PUBLISH_FAILED"
			errMsg = "progress delivery failed"
		}

		if sendErr := client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Error{
				Error: &pb.ErrorResponse{
					Code:      errCode,
					Message:   errMsg,
					Retryable: true,
				},
			},
		}); sendErr != nil {
			logging.Logger.Error().Err(sendErr).Msg("failed to send progress publish error response")
		}
		return
	}

	// Side-effect: update task heartbeat if this is an orchestrated task
	if report.TaskId != "" && s.taskStore != nil {
		details := map[string]interface{}{
			"state":      report.State,
			"completion": report.Completion,
			"summary":    report.Summary,
			"source":     sender.ToTopic(),
		}
		if report.Step != nil {
			details["step_name"] = report.Step.Name
			details["step_sequence"] = report.Step.Sequence
			details["step_total"] = report.Step.TotalSteps
		}
		if err := s.taskStore.UpdateHeartbeat(ctx, report.TaskId, details); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", report.TaskId).Msg("failed to update task heartbeat from progress report")
		}
		if err := s.maybeRenewTaskAuthorityGrants(ctx, report.TaskId); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", report.TaskId).Msg("failed to renew task authority grants from progress report")
		}
	}
}

// createProgressFilterHandler creates a per-client handler for progress updates
// received from the pg.{workspace} RabbitMQ stream. It deserializes the
// ProgressUpdate, applies server-side recipient filtering, suppresses self-echo,
// and delivers matching updates to the client.
func (s *GatewayServer) createProgressFilterHandler(client *ClientSession) func([]byte) {
	// Snapshot identity string at closure creation time for logging (avoids data race)
	client.identityMu.RLock()
	identityStr := client.Identity.String()
	client.identityMu.RUnlock()

	return func(updateBytes []byte) {
		var update pb.ProgressUpdate
		if err := proto.Unmarshal(updateBytes, &update); err != nil {
			logging.Logger.Error().Err(err).Str("identity", identityStr).Msg("failed to unmarshal progress update from stream")
			return
		}

		client.identityMu.RLock()
		myTopic := client.Identity.ToTopic()
		client.identityMu.RUnlock()

		// Don't echo progress back to the sender
		if update.Source == myTopic {
			return
		}

		// Server-side recipient filtering. Three accepted forms:
		//   - empty Recipient: workspace broadcast, deliver to all subscribers
		//   - exact match: window-specific user/agent/task targeting
		//   - prefix match on `us::{user}::`: bare user-level recipient
		//     (us::{user} with no specifier) targets every window of that
		//     user. Cheap to evaluate; only checked when exact match misses.
		if update.Recipient != "" && myTopic != update.Recipient {
			if !isBareUserRecipientMatch(update.Recipient, myTopic) {
				return
			}
		}

		client.Deliver(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_ProgressUpdate{
				ProgressUpdate: &update,
			},
		})
	}
}

// notifyTaskStatusChange publishes a synthetic ProgressUpdate to pg.{workspace}
// when a task transitions state (running, completed, failed, cancelled). The
// update's recipient field is set to the parent agent's topic so that only the
// spawning agent receives the notification via server-side filtering.
//
// This method is best-effort: failures are logged but do not block the caller.
func (s *GatewayServer) notifyTaskStatusChange(ctx context.Context, taskID, newStatus, workspace, parentAgentID, errorMsg string) {
	if parentAgentID == "" || workspace == "" {
		return // No parent to notify or no workspace for the progress topic
	}

	progressTopic, err := models.ProgressTopic(workspace)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("workspace", workspace).Str("task_id", taskID).Msg("invalid workspace for task status notification; skipping")
		return
	}
	now := time.Now()

	summary := "task " + newStatus
	if errorMsg != "" {
		summary = fmt.Sprintf("task %s: %s", newStatus, errorMsg)
	}

	update := &pb.ProgressUpdate{
		Source:      s.gatewayID,
		TaskId:      taskID,
		State:       newStatus,
		Summary:     summary,
		TimestampMs: now.UnixMilli(),
		Workspace:   workspace,
		Recipient:   parentAgentID, // Server-side filter: only parent sees this
		Kind:        pb.ProgressKind_PROGRESS_KIND_TASK,
	}

	updateBytes, err := proto.Marshal(update)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to marshal task status notification")
		return
	}

	publishErr := s.publishBreaker.Execute(func() error {
		return s.router.Publish(ctx, progressTopic, updateBytes)
	})
	if publishErr != nil {
		logging.Logger.Warn().Err(publishErr).Str("task_id", taskID).Str("status", newStatus).Str("parent", parentAgentID).Msg("failed to publish task status notification")
	} else {
		logging.Logger.Debug().Str("task_id", taskID).Str("status", newStatus).Str("parent", parentAgentID).Msg("task status notification sent to parent")
	}
}

// notifyTaskStatusChangeFromTaskID looks up a task by ID, extracts the parent
// agent and workspace, and publishes a status notification. Best-effort.
func (s *GatewayServer) notifyTaskStatusChangeFromTaskID(ctx context.Context, taskID, newStatus, errorMsg string) {
	if s.taskStore == nil || taskID == "" {
		return
	}
	task, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil || task == nil {
		return
	}
	s.notifyTaskStatusChange(ctx, taskID, newStatus, task.Workspace, task.ParentAgentID, errorMsg)
}

// subscribeClientToProgress subscribes a client to the pg.{workspace} progress
// stream using a shared consumer with a per-client filtering handler.
func (s *GatewayServer) subscribeClientToProgress(client *ClientSession, workspace string) error {
	pgTopic, err := models.ProgressTopic(workspace)
	if err != nil {
		return fmt.Errorf("invalid progress topic: %w", err)
	}
	if client.HasSubscription(pgTopic) {
		return nil
	}

	cancel, err := s.router.Subscribe(pgTopic, s.createProgressFilterHandler(client))
	if err != nil {
		return fmt.Errorf("failed to subscribe to progress topic %s: %w", pgTopic, err)
	}

	client.AddSubscription(pgTopic, func() {
		cancel()
		topicSubscriptions.Dec()
	})
	topicSubscriptions.Inc()
	return nil
}

// subscribeClientToUserProgress subscribes a user client to a per-user
// progress topic (pg.us.{user}.{window}). The topic is self-scoped to a
// single user-window, so no additional server-side recipient filtering is
// required — the filter handler still applies self-echo suppression.
func (s *GatewayServer) subscribeClientToUserProgress(client *ClientSession, topic string) error {
	if client.HasSubscription(topic) {
		return nil
	}

	cancel, err := s.router.Subscribe(topic, s.createProgressFilterHandler(client))
	if err != nil {
		return fmt.Errorf("failed to subscribe to user-progress topic %s: %w", topic, err)
	}

	client.AddSubscription(topic, func() {
		cancel()
		topicSubscriptions.Dec()
	})
	topicSubscriptions.Inc()
	return nil
}
