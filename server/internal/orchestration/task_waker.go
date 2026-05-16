package orchestration

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/aether/internal/logging"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/tasks"
)

// AuthorityRequestSource is the narrow interface the task waker uses to
// reconcile WAITING_AUTHORITY tasks against authority-request lifecycle
// state. Both internal/acl.Service (postgres) and
// internal/storage/acl/sqlite.Store (aetherlite) satisfy this shape, so the
// waker is backend-agnostic.
//
// The interface is intentionally limited to the two methods the waker needs:
//   - GetAuthorityRequest: per-task lookup keyed by the request id captured
//     in WaitSpec.AuthorityRequestID.
//   - SweepExpiredAuthorityRequests: bulk-flip pending rows whose
//     expires_at has elapsed. Called once per scan tick so the per-task
//     loop later in the same scan picks up the EXPIRED status without an
//     extra wait.
type AuthorityRequestSource interface {
	GetAuthorityRequest(ctx context.Context, requestID string) (*aclstore.AuthorityRequest, error)
	SweepExpiredAuthorityRequests(ctx context.Context, now time.Time, limit int) ([]*aclstore.AuthorityRequest, error)
}

// TaskWaker periodically scans WAITING_*/HIBERNATED tasks and fires wake
// transitions when their wake conditions are satisfied:
//
//   - WAITING_DEPENDENCY: all (or any, when WakeOnAny is set) dependencies
//     have reached a terminal state -> resume.
//   - HIBERNATED: ScheduledWakeUnixMs has elapsed -> resume.
//   - WAITING_AUTHORITY: the bound authority-request row has resolved -> resume
//     on APPROVED, or fail with the resolution reason on DENIED/EXPIRED/
//     CANCELLED.
//   - Any waiting state: TimeoutMs has elapsed since paused_at -> transition
//     to FAILED with reason "wait timeout".
//
// INPUT wake triggers remain event-driven (handled elsewhere); the waker is
// the safety net for timer-driven wakes, the dependency reconciler, and the
// authority-resolution reconciler. Multi-gateway safe — all transitions go
// through TaskAssignmentService state-machine ops which are idempotent on
// terminal tasks.
//
// Sibling of disconnect_reaper.go; lifecycle is identical (ticker loop,
// cancelled via Run's ctx).
type TaskWaker struct {
	taskStore         taskstore.Store
	taskService       *TaskAssignmentService
	authorityRequests AuthorityRequestSource
	interval          time.Duration
	batchLimit        int
	// sweepLimit bounds the per-scan SweepExpiredAuthorityRequests call.
	// 256 matches the conservative bound used in Stage B.
	sweepLimit int
}

// NewTaskWaker builds a waker with sensible defaults. Call Run in its own
// goroutine; pass a cancellable ctx so server shutdown stops the loop.
//
// `authorityRequests` may be nil — when omitted the waker silently skips the
// authority-resolution reconcile step but still runs the dependency / timeout
// / scheduled-wake loop. The server.go construction site passes the real
// ACL service handle so production code always has both axes active.
func NewTaskWaker(taskStore taskstore.Store, taskService *TaskAssignmentService, authorityRequests AuthorityRequestSource) *TaskWaker {
	return &TaskWaker{
		taskStore:         taskStore,
		taskService:       taskService,
		authorityRequests: authorityRequests,
		interval:          10 * time.Second,
		batchLimit:        500,
		sweepLimit:        256,
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

	// 0) Drive authority-request expiry first so the per-task loop below
	//    observes the latest EXPIRED rows on the same tick. The sweep is
	//    best-effort: a failure is logged but does not abort the rest of
	//    the scan (timeouts + dependency wakes must still run).
	if w.authorityRequests != nil {
		expired, err := w.authorityRequests.SweepExpiredAuthorityRequests(ctx, time.Now(), w.sweepLimit)
		if err != nil {
			logging.Logger.Warn().Err(err).Msg("task waker: authority-request sweep failed (non-fatal)")
		} else if len(expired) > 0 {
			logging.Logger.Info().
				Int("expired_count", len(expired)).
				Msg("task waker: swept expired authority requests")
		}
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
		//
		// Stage B: HIBERNATED timeouts honor HibernationDescriptor.EscalationPolicy:
		//   "" / "fail"  -> FailTask (default)
		//   "retry"      -> route through WakeHibernatedTask so the task is
		//                   re-queued and a fresh worker can retry.
		//   "alert"      -> log a warning; the task remains hibernated.
		//                   (No external alerting integration in Stage B —
		//                   the entry-point is the log line.)
		if t.WaitSpec != nil && t.WaitSpec.TimeoutMs > 0 && t.PausedAt != nil {
			elapsed := now.Sub(*t.PausedAt)
			if elapsed >= time.Duration(t.WaitSpec.TimeoutMs)*time.Millisecond {
				if t.Status == tasks.TaskStatusHibernated && t.WaitSpec.Hibernation != nil {
					policy := t.WaitSpec.Hibernation.EscalationPolicy
					switch policy {
					case "retry":
						if err := w.taskService.WakeHibernatedTask(ctx, t.TaskID); err != nil {
							logging.Logger.Warn().Err(err).
								Str("task_id", t.TaskID).
								Msg("task waker: hibernation timeout retry-wake failed (non-fatal)")
							continue
						}
						logging.Logger.Info().
							Str("task_id", t.TaskID).
							Dur("waited_for", elapsed).
							Int64("timeout_ms", t.WaitSpec.TimeoutMs).
							Msg("task waker: hibernation timeout, retry policy -> re-queued for fresh worker")
						continue
					case "alert":
						// Best-effort surface for operators. Stage B does not
						// integrate an alerting backend; the warning log is
						// the entry-point. Leave the task hibernated.
						logging.Logger.Warn().
							Str("task_id", t.TaskID).
							Dur("waited_for", elapsed).
							Int64("timeout_ms", t.WaitSpec.TimeoutMs).
							Msg("task waker: hibernation timeout, alert policy -> task remains hibernated")
						continue
					}
					// Default / "fail": fall through to FailTask below.
				}
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
		//
		// Stage B: HIBERNATED rows route through WakeHibernatedTask which
		// flips status to pending, captures the hibernation handoff into
		// reserved metadata keys, and re-queues the task for a fresh
		// worker spawn. Non-hibernation rows (e.g. WAITING_INPUT with a
		// scheduled wake) still resume directly to running because they
		// retain their worker.
		if t.WaitSpec != nil && t.WaitSpec.ScheduledWakeUnixMs > 0 {
			wakeAt := time.UnixMilli(t.WaitSpec.ScheduledWakeUnixMs)
			if !now.Before(wakeAt) {
				if t.Status == tasks.TaskStatusHibernated {
					if err := w.taskService.WakeHibernatedTask(ctx, t.TaskID); err != nil {
						logging.Logger.Warn().Err(err).Str("task_id", t.TaskID).Msg("task waker: hibernation scheduled-wake failed (non-fatal)")
						continue
					}
					logging.Logger.Info().
						Str("task_id", t.TaskID).
						Time("wake_at", wakeAt).
						Msg("task waker: woke hibernated task on scheduled wake (re-queued for fresh worker)")
					continue
				}
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

		// 4) Authority-request reconciliation. WAITING_AUTHORITY tasks parked
		// on a Stage B request row resume when the row is APPROVED and fail
		// with the resolution reason on DENIED / EXPIRED / CANCELLED. Skip
		// silently when the ACL source is not wired in (lite test harness
		// or misconfigured server).
		if w.authorityRequests != nil &&
			t.Status == tasks.TaskStatusWaitingAuthority &&
			t.WaitSpec != nil &&
			t.WaitSpec.Reason == tasks.WaitReasonAuthority &&
			t.WaitSpec.AuthorityRequestID != "" {
			req, err := w.authorityRequests.GetAuthorityRequest(ctx, t.WaitSpec.AuthorityRequestID)
			if err != nil {
				logging.Logger.Warn().Err(err).
					Str("task_id", t.TaskID).
					Str("authority_request_id", t.WaitSpec.AuthorityRequestID).
					Msg("task waker: authority request lookup failed (non-fatal)")
				continue
			}
			if req == nil {
				continue
			}
			switch req.Status {
			case aclstore.AuthorityRequestStatusPending:
				// Still waiting; nothing to do.
			case aclstore.AuthorityRequestStatusApproved:
				if err := w.taskService.ResumeTask(ctx, t.TaskID, tasks.TaskStatusRunning); err != nil {
					logging.Logger.Warn().Err(err).
						Str("task_id", t.TaskID).
						Str("authority_request_id", req.RequestID).
						Msg("task waker: authority approval resume failed (non-fatal)")
					continue
				}
				logging.Logger.Info().
					Str("task_id", t.TaskID).
					Str("authority_request_id", req.RequestID).
					Str("granted_grant_id", req.GrantedGrantID).
					Msg("task waker: resumed task on authority approval")
			case aclstore.AuthorityRequestStatusDenied,
				aclstore.AuthorityRequestStatusExpired,
				aclstore.AuthorityRequestStatusCancelled:
				reason := authorityRequestFailureReason(req)
				if err := w.taskService.FailTask(ctx, t.TaskID, reason); err != nil {
					logging.Logger.Warn().Err(err).
						Str("task_id", t.TaskID).
						Str("authority_request_id", req.RequestID).
						Msg("task waker: authority denial fail failed (non-fatal)")
					continue
				}
				logging.Logger.Info().
					Str("task_id", t.TaskID).
					Str("authority_request_id", req.RequestID).
					Str("status", string(req.Status)).
					Msg("task waker: failed task on authority resolution")
			}
		}
	}
}

// authorityRequestFailureReason formats the FailTask reason for tasks parked
// on a request that resolved DENIED / EXPIRED / CANCELLED. Includes the
// resolution_reason field when populated so operators have a breadcrumb in
// the task error_message column.
func authorityRequestFailureReason(req *aclstore.AuthorityRequest) string {
	if req == nil {
		return "authority request resolved without approval"
	}
	if req.ResolutionReason != "" {
		return fmt.Sprintf("authority request %s: %s", req.Status, req.ResolutionReason)
	}
	return fmt.Sprintf("authority request %s", req.Status)
}
