package workflow

import (
	"context"
	"encoding/json"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
)

// Scheduler handles recurring and one-time scheduled tasks.
type Scheduler struct {
	store    *Store
	executor *Executor
	dagEng   *DAGEngine
	leader   LeaderElector
	parser   cron.Parser
	interval time.Duration
}

func NewScheduler(store *Store, executor *Executor, dagEng *DAGEngine, leader LeaderElector, pollInterval time.Duration) *Scheduler {
	return &Scheduler{
		store:    store,
		executor: executor,
		dagEng:   dagEng,
		leader:   leader,
		parser:   cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		interval: pollInterval,
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
	now := time.Now()
	schedules, err := s.store.GetDueSchedules(ctx, now)
	if err != nil {
		return err
	}

	for _, sc := range schedules {
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
					if err := s.store.UpdateScheduleAfterFire(ctx, sc.ID, now, nextFire); err != nil {
						log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to advance skipped schedule")
					}
					continue
				}
			} else {
				// Non-timestamp marker; skip
				log.Debug().Str("schedule_id", sc.ID).Msg("skipping schedule: active task marker set")
				nextFire := s.advanceToFuture(sc, now)
				if err := s.store.UpdateScheduleAfterFire(ctx, sc.ID, now, nextFire); err != nil {
					log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to advance skipped schedule")
				}
				continue
			}
		}

		// Apply miss_policy
		switch sc.MissPolicy {
		case "skip":
			// If multiple fires were missed, advance to next future time without firing
			nextFire := s.advanceToFuture(sc, now)
			if err := s.store.UpdateScheduleAfterFire(ctx, sc.ID, now, nextFire); err != nil {
				log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to advance schedule")
			}
			continue

		case "fire_all":
			// Fire once per missed interval, capped at 100
			count := s.countMissedFires(sc, now)
			if count > 100 {
				count = 100
			}
			for i := 0; i < count; i++ {
				if err := s.fire(ctx, sc, now); err != nil {
					log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to fire schedule (fire_all)")
					break
				}
			}
			nextFire := s.advanceToFuture(sc, now)
			if err := s.store.UpdateScheduleAfterFire(ctx, sc.ID, now, nextFire); err != nil {
				log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to update schedule after fire_all")
			}
			continue

		default: // "fire_once" and any unrecognized policy
			// Fire exactly once, then advance to next future time
		}

		if err := s.fire(ctx, sc, now); err != nil {
			log.Error().Err(err).
				Str("schedule_id", sc.ID).
				Str("name", sc.Name).
				Msg("failed to fire schedule")
			continue
		}

		nextFire := s.advanceToFuture(sc, now)
		if err := s.store.UpdateScheduleAfterFire(ctx, sc.ID, now, nextFire); err != nil {
			log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to update schedule after fire")
		}
	}

	return nil
}

func (s *Scheduler) fire(ctx context.Context, sc Schedule, now time.Time) error {
	log.Info().
		Str("schedule_id", sc.ID).
		Str("name", sc.Name).
		Str("type", sc.ScheduleType).
		Msg("firing schedule")

	// If schedule triggers a DAG, start the DAG execution
	if sc.WorkflowID != "" {
		triggerData, _ := json.Marshal(map[string]any{
			"schedule_id":   sc.ID,
			"schedule_name": sc.Name,
			"fired_at":      now.Format(time.RFC3339),
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

	if err := s.executor.DispatchAction(&action); err != nil {
		return err
	}

	// Track active task for concurrency control
	if sc.MaxConcurrent == 1 {
		marker := now.Format(time.RFC3339)
		if err := s.store.SetScheduleActiveTask(ctx, sc.ID, marker); err != nil {
			log.Error().Err(err).Str("schedule_id", sc.ID).Msg("failed to set active task marker")
		}
	}

	return nil
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

// countMissedFires returns how many interval fires were missed between
// the scheduled next_fire_at and now.
func (s *Scheduler) countMissedFires(sc Schedule, now time.Time) int {
	if sc.NextFireAt == nil {
		return 1
	}
	switch sc.ScheduleType {
	case ScheduleTypeInterval:
		d, err := time.ParseDuration(sc.ScheduleExpr)
		if err != nil || d <= 0 {
			return 1
		}
		missed := int(now.Sub(*sc.NextFireAt)/d) + 1
		if missed < 1 {
			return 1
		}
		return missed
	case ScheduleTypeCron:
		schedule, err := s.parser.Parse(sc.ScheduleExpr)
		if err != nil {
			return 1
		}
		count := 0
		t := *sc.NextFireAt
		for t.Before(now) && count < 101 {
			count++
			t = schedule.Next(t)
		}
		if count < 1 {
			return 1
		}
		return count
	default:
		return 1
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
