package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
)

const (
	maxScheduleCatchUpPerPoll = 100

	scheduleMetadataID               = "aether.schedule.id"
	scheduleMetadataScheduledFor     = "aether.schedule.scheduled_for"
	scheduleMetadataDispatchedAt     = "aether.schedule.dispatched_at"
	scheduleMetadataMissPolicy       = "aether.schedule.miss_policy"
	scheduleMetadataDisposition      = "aether.schedule.disposition"
	scheduleMetadataBacklogCount     = "aether.schedule.backlog_count"
	scheduleMetadataBacklogTruncated = "aether.schedule.backlog_truncated"
	scheduleMetadataBacklogIndex     = "aether.schedule.backlog_index"
)

type scheduleActionDispatcher interface {
	DispatchScheduledAction(ctx context.Context, action *ActionDef, scheduleID string, authorization *pb.AuthorizationContext) error
}

// joinDeadlineHandler fires the timeout path for an open join whose deadline
// has elapsed. *JoinEngine satisfies it.
type joinDeadlineHandler interface {
	HandleDeadline(ctx context.Context, j *Join) error
}

// Scheduler handles recurring and one-time scheduled tasks.
type Scheduler struct {
	store    WorkflowStore
	executor scheduleActionDispatcher
	dagEng   *DAGEngine
	leader   LeaderElector
	joins    joinDeadlineHandler
	parser   cron.Parser
	interval time.Duration
	now      func() time.Time
}

func NewScheduler(store WorkflowStore, executor scheduleActionDispatcher, dagEng *DAGEngine, leader LeaderElector, joins joinDeadlineHandler, pollInterval time.Duration) *Scheduler {
	return &Scheduler{
		store:    store,
		executor: executor,
		dagEng:   dagEng,
		leader:   leader,
		joins:    joins,
		parser:   cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		interval: pollInterval,
		now:      time.Now,
	}
}

// Run starts the scheduler polling loop. It blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	log.Info().Dur("interval", s.interval).Msg("scheduler started")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("scheduler stopped")
			return
		case <-ticker.C:
			if !s.leader.IsLeader() {
				continue
			}
			if err := s.poll(ctx); err != nil {
				log.Error().Err(err).Msg("scheduler poll error")
			}
		}
	}
}

func (s *Scheduler) poll(ctx context.Context) error {
	now := s.now()
	schedules, err := s.store.GetDueSchedules(ctx, now)
	if err != nil {
		return err
	}

	for _, sc := range schedules {
		dueAt := now
		if sc.NextFireAt != nil {
			dueAt = *sc.NextFireAt
		}
		backlogCount, backlogTruncated := s.measureBacklog(sc, dueAt, now)

		// Concurrency control: if max_concurrent=1 and a task is active, check staleness
		if sc.MaxConcurrent == 1 && sc.ActiveTaskID != "" {
			if markerTime, err := time.Parse(time.RFC3339, sc.ActiveTaskID); err == nil {
				d, _ := time.ParseDuration(sc.ScheduleExpr)
				if d > 0 && now.Sub(markerTime) >= d {
					log.Warn().Str("schedule_id", sc.ID).Msg("clearing stale active task marker")
					_ = s.store.SetScheduleActiveTask(ctx, sc.ID, "")
					// Fall through to fire
				} else {
					log.Debug().Str("schedule_id", sc.ID).Msg("skipping schedule: previous task still active")
					nextFire := s.advanceToFuture(sc, now)
					if err := s.recordSkipped(ctx, sc, dueAt, nextFire, ScheduleSkipReasonMaxConcurrent, backlogCount, backlogTruncated); err != nil {
						log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to advance skipped schedule")
					}
					continue
				}
			} else {
				// Non-timestamp marker; skip
				log.Debug().Str("schedule_id", sc.ID).Msg("skipping schedule: active task marker set")
				nextFire := s.advanceToFuture(sc, now)
				if err := s.recordSkipped(ctx, sc, dueAt, nextFire, ScheduleSkipReasonMaxConcurrent, backlogCount, backlogTruncated); err != nil {
					log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to advance skipped schedule")
				}
				continue
			}
		}

		// Apply miss_policy. A single due occurrence is an ordinary on-time fire;
		// "skip" only discards a backlog containing more than one occurrence.
		// This distinction matters because every scheduler poll necessarily sees
		// an occurrence after its exact due timestamp.
		switch sc.MissPolicy {
		case ScheduleMissPolicySkip:
			if backlogCount > 1 {
				nextFire := s.advanceToFuture(sc, now)
				if err := s.recordSkipped(ctx, sc, dueAt, nextFire, ScheduleSkipReasonMissPolicy, backlogCount, backlogTruncated); err != nil {
					log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to advance skipped schedule backlog")
				}
				continue
			}

		case ScheduleMissPolicyFireAll:
			// Advance the durable cursor after every emitted occurrence. The batch
			// cap bounds one poll without discarding older backlog; a still-due
			// cursor is picked up by the next poll. Per-occurrence idempotency makes
			// a retry safe when dispatch succeeds but cursor persistence does not.
			occurrence := dueAt
			disposition := ScheduleDispositionOrdinary
			if backlogCount > 1 {
				disposition = ScheduleDispositionCatchUp
			}
			for i := 0; i < maxScheduleCatchUpPerPoll && !occurrence.After(now); i++ {
				decision := ScheduleOccurrence{
					ScheduledFor: occurrence, DispatchedAt: timePointer(now), Disposition: disposition,
					BacklogCount: backlogCount, BacklogTruncated: backlogTruncated, BacklogIndex: i + 1,
				}
				if err := s.fire(ctx, sc, decision); err != nil {
					if s.blockOnPermanentAuthorityError(ctx, sc, decision, err, now) {
						break
					}
					log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to fire schedule (fire_all)")
					break
				}
				nextFire := s.calculateNextFire(sc, occurrence)
				if err := s.store.RecordScheduleOccurrence(ctx, sc.ID, decision, nextFire); err != nil {
					log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to update schedule during fire_all")
					break
				}
				if nextFire == nil {
					break
				}
				occurrence = *nextFire
			}
			continue

		default: // "fire_once" and any unrecognized policy
			// Fire exactly once, then advance to next future time
		}

		disposition := ScheduleDispositionOrdinary
		if backlogCount > 1 {
			disposition = ScheduleDispositionCoalesced
		}
		decision := ScheduleOccurrence{
			ScheduledFor: dueAt, DispatchedAt: timePointer(now), Disposition: disposition,
			BacklogCount: backlogCount, BacklogTruncated: backlogTruncated, BacklogIndex: 1,
		}
		if err := s.fire(ctx, sc, decision); err != nil {
			if s.blockOnPermanentAuthorityError(ctx, sc, decision, err, now) {
				continue
			}
			log.Error().Err(err).
				Str("schedule_id", sc.ID).
				Str("name", sc.Name).
				Msg("failed to fire schedule")
			continue
		}

		nextFire := s.advanceToFuture(sc, now)
		if err := s.store.RecordScheduleOccurrence(ctx, sc.ID, decision, nextFire); err != nil {
			log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to update schedule after fire")
		}
	}

	// Join deadline sweep: fire on_timeout (or abort) for open joins past their
	// deadline. Leader-gated like the schedule sweep above.
	due, err := s.store.GetDueJoinDeadlines(ctx, now)
	if err != nil {
		log.Error().Err(err).Msg("failed to get due join deadlines")
	} else if s.joins != nil {
		for i := range due {
			if err := s.joins.HandleDeadline(ctx, &due[i]); err != nil {
				log.Warn().Err(err).
					Str("join", due[i].JoinName).
					Str("correlation_key", due[i].CorrelationKey).
					Msg("failed to handle join deadline")
			}
		}
	}

	return nil
}

func (s *Scheduler) blockOnPermanentAuthorityError(ctx context.Context, sc Schedule, occurrence ScheduleOccurrence, dispatchErr error, now time.Time) bool {
	var authorityErr *ScheduleAuthorityInvalidError
	if !errors.As(dispatchErr, &authorityErr) {
		return false
	}
	reason := authorityErr.Error()
	if err := s.store.SetScheduleAuthorityBlocked(ctx, sc.ID, reason); err != nil {
		log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to block schedule with invalid authority")
		return true
	}
	nextFire := s.advanceToFuture(sc, now)
	if err := s.recordSkipped(ctx, sc, occurrence.ScheduledFor, nextFire,
		ScheduleSkipReasonAuthorityInvalid, occurrence.BacklogCount, occurrence.BacklogTruncated); err != nil {
		log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to record schedule authority skip")
	}
	log.Warn().Err(dispatchErr).Str("schedule_id", sc.ID).Msg("blocked schedule after permanent authority failure")
	return true
}

// measureBacklog returns the bounded number of occurrences due at this poll.
// The cap is one beyond the per-poll fire_all dispatch limit, which is enough to
// say whether a completed batch still leaves work without walking an unbounded
// outage gap.
func (s *Scheduler) measureBacklog(sc Schedule, firstDue, now time.Time) (int, bool) {
	const detailLimit = maxScheduleCatchUpPerPoll + 1
	count := 1
	current := firstDue
	for count < detailLimit {
		next := s.calculateNextFire(sc, current)
		if next == nil || next.After(now) {
			return count, false
		}
		current = *next
		count++
	}
	next := s.calculateNextFire(sc, current)
	return count, next != nil && !next.After(now)
}

func (s *Scheduler) recordSkipped(
	ctx context.Context,
	sc Schedule,
	scheduledFor time.Time,
	nextFire *time.Time,
	reason string,
	backlogCount int,
	backlogTruncated bool,
) error {
	return s.store.RecordScheduleOccurrence(ctx, sc.ID, ScheduleOccurrence{
		ScheduledFor: scheduledFor, Disposition: ScheduleDispositionSkipped, Reason: reason,
		BacklogCount: backlogCount, BacklogTruncated: backlogTruncated,
	}, nextFire)
}

func timePointer(value time.Time) *time.Time {
	copy := value
	return &copy
}

func (s *Scheduler) fire(ctx context.Context, sc Schedule, occurrence ScheduleOccurrence) error {
	log.Info().
		Str("schedule_id", sc.ID).
		Str("name", sc.Name).
		Str("type", sc.ScheduleType).
		Msg("firing schedule")

	// If schedule triggers a DAG, start the DAG execution
	if sc.WorkflowID != "" {
		triggerData, _ := json.Marshal(map[string]any{
			"schedule_id":       sc.ID,
			"schedule_name":     sc.Name,
			"scheduled_for":     occurrence.ScheduledFor.UTC().Format(time.RFC3339Nano),
			"fired_at":          occurrence.DispatchedAt.UTC().Format(time.RFC3339Nano),
			"disposition":       occurrence.Disposition,
			"backlog_count":     occurrence.BacklogCount,
			"backlog_truncated": occurrence.BacklogTruncated,
			"backlog_index":     occurrence.BacklogIndex,
		})
		_, err := s.dagEng.StartExecution(ctx, sc.WorkflowID, sc.Workspace, triggerData)
		return err
	}

	// Otherwise, dispatch the action directly
	var action ActionDef
	if err := json.Unmarshal(sc.Action, &action); err != nil {
		return err
	}
	if action.Workspace == "" {
		action.Workspace = sc.Workspace
	}
	action.Metadata = cloneStringMap(action.Metadata)
	action.Metadata[scheduleMetadataID] = sc.ID
	action.Metadata[scheduleMetadataScheduledFor] = occurrence.ScheduledFor.UTC().Format(time.RFC3339Nano)
	action.Metadata[scheduleMetadataDispatchedAt] = occurrence.DispatchedAt.UTC().Format(time.RFC3339Nano)
	action.Metadata[scheduleMetadataMissPolicy] = normalizedMissPolicy(sc.MissPolicy)
	action.Metadata[scheduleMetadataDisposition] = occurrence.Disposition
	action.Metadata[scheduleMetadataBacklogCount] = strconv.Itoa(occurrence.BacklogCount)
	action.Metadata[scheduleMetadataBacklogTruncated] = strconv.FormatBool(occurrence.BacklogTruncated)
	action.Metadata[scheduleMetadataBacklogIndex] = strconv.Itoa(occurrence.BacklogIndex)
	if action.Type == "create_task" && action.IdempotencyKey == "" {
		action.IdempotencyKey = scheduleOccurrenceIdempotencyKey(sc, occurrence.ScheduledFor)
	}

	var authorization *pb.AuthorizationContext
	if sc.Authority != nil {
		if !sc.Authority.ExpiresAt.After(s.now()) {
			return &ScheduleAuthorityInvalidError{Code: "ERR_AUTHORITY_INVALID", Message: "workflow schedule authority expired"}
		}
		authorization = sc.Authority.Authorization
	}
	if err := s.executor.DispatchScheduledAction(ctx, &action, sc.ID, authorization); err != nil {
		return err
	}

	// Track active task for concurrency control
	if sc.MaxConcurrent == 1 {
		marker := occurrence.DispatchedAt.Format(time.RFC3339)
		if err := s.store.SetScheduleActiveTask(ctx, sc.ID, marker); err != nil {
			log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to set active task marker")
		}
	}

	return nil
}

func normalizedMissPolicy(policy string) string {
	switch policy {
	case ScheduleMissPolicySkip, ScheduleMissPolicyFireAll:
		return policy
	default:
		return ScheduleMissPolicyFireOnce
	}
}

func scheduleOccurrenceIdempotencyKey(sc Schedule, scheduledFor time.Time) string {
	sum := sha256.Sum256([]byte(sc.Workspace + "\x00" + sc.ID + "\x00" + scheduledFor.UTC().Format(time.RFC3339Nano)))
	return "schedule:" + hex.EncodeToString(sum[:])
}

func cloneStringMap(source map[string]string) map[string]string {
	cloned := make(map[string]string, len(source)+8)
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func (s *Scheduler) calculateNextFire(sc Schedule, now time.Time) *time.Time {
	switch sc.ScheduleType {
	case ScheduleTypeCron:
		schedule, err := s.parser.Parse(sc.ScheduleExpr)
		if err != nil {
			log.Warn().Err(err).Str("expr", sc.ScheduleExpr).Msg("invalid cron expression")
			return nil
		}
		next := schedule.Next(now)
		return &next

	case ScheduleTypeInterval:
		d, err := time.ParseDuration(sc.ScheduleExpr)
		if err != nil {
			log.Warn().Err(err).Str("expr", sc.ScheduleExpr).Msg("invalid interval expression")
			return nil
		}
		next := now.Add(d)
		return &next

	case ScheduleTypeOnce:
		// One-shot schedule: no next fire
		return nil

	case ScheduleTypeEventDelayed:
		// Event-delayed schedules are triggered by events, not the poller
		return nil

	default:
		log.Warn().Str("type", sc.ScheduleType).Msg("unknown schedule type")
		return nil
	}
}

// advanceToFuture calculates the next fire time that is strictly in the future.
// For intervals, this jumps past any gap. For cron, it uses the parser.
func (s *Scheduler) advanceToFuture(sc Schedule, now time.Time) *time.Time {
	switch sc.ScheduleType {
	case ScheduleTypeCron:
		schedule, err := s.parser.Parse(sc.ScheduleExpr)
		if err != nil {
			log.Warn().Err(err).Str("expr", sc.ScheduleExpr).Msg("invalid cron expression")
			return nil
		}
		next := schedule.Next(now)
		return &next

	case ScheduleTypeInterval:
		d, err := time.ParseDuration(sc.ScheduleExpr)
		if err != nil {
			log.Warn().Err(err).Str("expr", sc.ScheduleExpr).Msg("invalid interval expression")
			return nil
		}
		next := now.Add(d)
		return &next

	case ScheduleTypeOnce:
		return nil

	case ScheduleTypeEventDelayed:
		return nil

	default:
		return nil
	}
}

// ComputeInitialNextFire calculates the first fire time for a new schedule.
func (s *Scheduler) ComputeInitialNextFire(scheduleType, scheduleExpr string) (*time.Time, error) {
	now := time.Now()
	switch scheduleType {
	case ScheduleTypeCron:
		schedule, err := s.parser.Parse(scheduleExpr)
		if err != nil {
			return nil, err
		}
		next := schedule.Next(now)
		return &next, nil

	case ScheduleTypeInterval:
		d, err := time.ParseDuration(scheduleExpr)
		if err != nil {
			return nil, err
		}
		next := now.Add(d)
		return &next, nil

	case ScheduleTypeOnce:
		t, err := time.Parse(time.RFC3339, scheduleExpr)
		if err != nil {
			return nil, err
		}
		return &t, nil

	default:
		return nil, nil
	}
}
