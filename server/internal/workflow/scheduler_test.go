package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

type scheduleCursorUpdate struct {
	occurrence ScheduleOccurrence
	nextFire   *time.Time
}

type schedulePollStore struct {
	WorkflowStore
	due     []Schedule
	updates []scheduleCursorUpdate
}

func (s *schedulePollStore) GetDueSchedules(context.Context, time.Time) ([]Schedule, error) {
	return append([]Schedule(nil), s.due...), nil
}

func (s *schedulePollStore) RecordScheduleOccurrence(_ context.Context, _ string, occurrence ScheduleOccurrence, nextFire *time.Time) error {
	var nextCopy *time.Time
	if nextFire != nil {
		value := *nextFire
		nextCopy = &value
	}
	s.updates = append(s.updates, scheduleCursorUpdate{occurrence: occurrence, nextFire: nextCopy})
	return nil
}

func (s *schedulePollStore) GetDueJoinDeadlines(context.Context, time.Time) ([]Join, error) {
	return nil, nil
}

type recordingScheduleDispatcher struct {
	actions []*ActionDef
	failAt  int
}

func (d *recordingScheduleDispatcher) DispatchScheduledAction(_ context.Context, action *ActionDef) error {
	if d.failAt > 0 && len(d.actions)+1 == d.failAt {
		return errors.New("injected dispatch failure")
	}
	copy := *action
	copy.Metadata = cloneStringMap(action.Metadata)
	d.actions = append(d.actions, &copy)
	return nil
}

func newSchedulePollHarness(now time.Time, schedule Schedule) (*Scheduler, *schedulePollStore, *recordingScheduleDispatcher) {
	store := &schedulePollStore{due: []Schedule{schedule}}
	dispatcher := &recordingScheduleDispatcher{}
	scheduler := NewScheduler(store, dispatcher, nil, nil, nil, time.Second)
	scheduler.now = func() time.Time { return now }
	return scheduler, store, dispatcher
}

func dueCreateTaskSchedule(now time.Time, overdue time.Duration, policy string) Schedule {
	dueAt := now.Add(-overdue)
	action, _ := json.Marshal(ActionDef{
		Type: "create_task", TaskType: "test.schedule", Workspace: "workspace-a",
		Metadata: map[string]string{"caller": "preserved"},
	})
	return Schedule{
		ID: "schedule-a", Name: "Schedule A", Workspace: "workspace-a",
		ScheduleType: ScheduleTypeInterval, ScheduleExpr: "1m", Action: action,
		Enabled: true, NextFireAt: &dueAt, MissPolicy: policy,
	}
}

// newTestScheduler builds a Scheduler with nil store/executor/dagEng/leader — safe
// for unit tests that only exercise pure computation methods.
func newTestScheduler() *Scheduler {
	return &Scheduler{
		parser:   cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		interval: time.Second,
	}
}

func TestScheduleMissPolicyValidation(t *testing.T) {
	for _, policy := range []string{ScheduleMissPolicySkip, ScheduleMissPolicyFireOnce, ScheduleMissPolicyFireAll} {
		if !validScheduleMissPolicy(policy) {
			t.Fatalf("supported policy %q was rejected", policy)
		}
	}
	if validScheduleMissPolicy("") || validScheduleMissPolicy("eventually") {
		t.Fatal("empty or unknown missed-fire policy was accepted")
	}
}

func TestSchedulerPollSkipFiresSingleDueOccurrenceButDropsBacklog(t *testing.T) {
	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	for name, overdue := range map[string]time.Duration{
		"single due occurrence":    30 * time.Second,
		"multiple due occurrences": 90 * time.Second,
	} {
		t.Run(name, func(t *testing.T) {
			scheduler, store, dispatcher := newSchedulePollHarness(now, dueCreateTaskSchedule(now, overdue, "skip"))
			if err := scheduler.poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			wantFires := 1
			if overdue > time.Minute {
				wantFires = 0
			}
			if len(dispatcher.actions) != wantFires {
				t.Fatalf("dispatched actions = %d, want %d", len(dispatcher.actions), wantFires)
			}
			if len(store.updates) != 1 || store.updates[0].nextFire == nil || !store.updates[0].nextFire.After(now) {
				t.Fatalf("cursor updates = %+v, want one future cursor", store.updates)
			}
			wantDisposition := ScheduleDispositionOrdinary
			wantReason := ""
			wantBacklog := 1
			if wantFires == 0 {
				wantDisposition = ScheduleDispositionSkipped
				wantReason = ScheduleSkipReasonMissPolicy
				wantBacklog = 2
			}
			got := store.updates[0].occurrence
			if got.Disposition != wantDisposition || got.Reason != wantReason || got.BacklogCount != wantBacklog || got.BacklogTruncated {
				t.Fatalf("occurrence = %+v", got)
			}
		})
	}
}

func TestSchedulerPollFireOnceCoalescesWithDeterministicOccurrenceIdentity(t *testing.T) {
	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	schedule := dueCreateTaskSchedule(now, 150*time.Second, "fire_once")
	scheduler, store, dispatcher := newSchedulePollHarness(now, schedule)
	if err := scheduler.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.actions) != 1 {
		t.Fatalf("dispatched actions = %d, want 1", len(dispatcher.actions))
	}
	action := dispatcher.actions[0]
	wantScheduledFor := schedule.NextFireAt.UTC().Format(time.RFC3339Nano)
	if action.Metadata["caller"] != "preserved" ||
		action.Metadata[scheduleMetadataID] != schedule.ID ||
		action.Metadata[scheduleMetadataScheduledFor] != wantScheduledFor ||
		action.Metadata[scheduleMetadataDispatchedAt] != now.Format(time.RFC3339Nano) ||
		action.Metadata[scheduleMetadataMissPolicy] != "fire_once" ||
		action.Metadata[scheduleMetadataDisposition] != ScheduleDispositionCoalesced ||
		action.Metadata[scheduleMetadataBacklogCount] != "3" ||
		action.Metadata[scheduleMetadataBacklogTruncated] != "false" ||
		action.Metadata[scheduleMetadataBacklogIndex] != "1" {
		t.Fatalf("scheduled action metadata = %#v", action.Metadata)
	}
	wantKey := scheduleOccurrenceIdempotencyKey(schedule, *schedule.NextFireAt)
	if action.IdempotencyKey != wantKey || action.IdempotencyKey == "" {
		t.Fatalf("idempotency key = %q, want %q", action.IdempotencyKey, wantKey)
	}
	if len(store.updates) != 1 || store.updates[0].nextFire == nil || !store.updates[0].nextFire.After(now) {
		t.Fatalf("cursor updates = %+v, want one future cursor", store.updates)
	}
	if got := store.updates[0].occurrence; got.Disposition != ScheduleDispositionCoalesced || got.BacklogCount != 3 || got.BacklogIndex != 1 {
		t.Fatalf("coalesced occurrence = %+v", got)
	}

	// A response-loss retry of the same durable cursor produces the same key,
	// allowing the gateway idempotency ledger to suppress a duplicate task.
	if err := scheduler.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.actions) != 2 || dispatcher.actions[1].IdempotencyKey != wantKey {
		t.Fatalf("retry idempotency keys = %q, %q", dispatcher.actions[0].IdempotencyKey, dispatcher.actions[1].IdempotencyKey)
	}
}

func TestSchedulerPollFireAllAdvancesEveryOccurrenceWithoutDiscardingCappedBacklog(t *testing.T) {
	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	schedule := dueCreateTaskSchedule(now, 150*time.Second, "fire_all")
	scheduler, store, dispatcher := newSchedulePollHarness(now, schedule)
	if err := scheduler.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.actions) != 3 || len(store.updates) != 3 {
		t.Fatalf("actions/updates = %d/%d, want 3/3", len(dispatcher.actions), len(store.updates))
	}
	seenKeys := map[string]struct{}{}
	for i, action := range dispatcher.actions {
		if _, duplicate := seenKeys[action.IdempotencyKey]; duplicate || action.IdempotencyKey == "" {
			t.Fatalf("occurrence %d idempotency key = %q", i, action.IdempotencyKey)
		}
		seenKeys[action.IdempotencyKey] = struct{}{}
		if action.Metadata[scheduleMetadataDisposition] != ScheduleDispositionCatchUp ||
			action.Metadata[scheduleMetadataBacklogCount] != "3" ||
			action.Metadata[scheduleMetadataBacklogIndex] != strconv.Itoa(i+1) {
			t.Fatalf("catch-up occurrence %d metadata = %#v", i, action.Metadata)
		}
	}
	if last := store.updates[len(store.updates)-1].nextFire; last == nil || !last.After(now) {
		t.Fatalf("final cursor = %v, want future", last)
	}

	large := dueCreateTaskSchedule(now, (maxScheduleCatchUpPerPoll+2)*time.Minute+30*time.Second, "fire_all")
	largeScheduler, largeStore, largeDispatcher := newSchedulePollHarness(now, large)
	if err := largeScheduler.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(largeDispatcher.actions) != maxScheduleCatchUpPerPoll || len(largeStore.updates) != maxScheduleCatchUpPerPoll {
		t.Fatalf("capped actions/updates = %d/%d", len(largeDispatcher.actions), len(largeStore.updates))
	}
	if last := largeStore.updates[len(largeStore.updates)-1].nextFire; last == nil || last.After(now) {
		t.Fatalf("capped cursor = %v, want retained due backlog", last)
	}
	if got := largeStore.updates[len(largeStore.updates)-1].occurrence; got.Disposition != ScheduleDispositionCatchUp ||
		got.BacklogCount != maxScheduleCatchUpPerPoll+1 || !got.BacklogTruncated || got.BacklogIndex != maxScheduleCatchUpPerPoll {
		t.Fatalf("bounded catch-up occurrence = %+v", got)
	}
}

func TestSchedulerPollRecordsMaxConcurrentSkipWithoutOverwritingARealFire(t *testing.T) {
	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	schedule := dueCreateTaskSchedule(now, 90*time.Second, ScheduleMissPolicyFireAll)
	schedule.MaxConcurrent = 1
	schedule.ActiveTaskID = now.Add(-10 * time.Second).Format(time.RFC3339)
	scheduler, store, dispatcher := newSchedulePollHarness(now, schedule)
	if err := scheduler.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.actions) != 0 || len(store.updates) != 1 {
		t.Fatalf("actions/updates = %d/%d", len(dispatcher.actions), len(store.updates))
	}
	got := store.updates[0].occurrence
	if got.DispatchedAt != nil || got.Disposition != ScheduleDispositionSkipped ||
		got.Reason != ScheduleSkipReasonMaxConcurrent || got.BacklogCount != 2 {
		t.Fatalf("max-concurrent occurrence = %+v", got)
	}
}

func TestSchedulerPollFireAllLeavesFailedOccurrenceAtDurableCursor(t *testing.T) {
	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	schedule := dueCreateTaskSchedule(now, 150*time.Second, "fire_all")
	scheduler, store, dispatcher := newSchedulePollHarness(now, schedule)
	dispatcher.failAt = 2
	if err := scheduler.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.actions) != 1 || len(store.updates) != 1 {
		t.Fatalf("actions/updates = %d/%d, want first occurrence only", len(dispatcher.actions), len(store.updates))
	}
	wantRetryCursor := schedule.NextFireAt.Add(time.Minute)
	if store.updates[0].nextFire == nil || !store.updates[0].nextFire.Equal(wantRetryCursor) {
		t.Fatalf("retry cursor = %v, want %v", store.updates[0].nextFire, wantRetryCursor)
	}
}

// ---- calculateNextFire ----

func TestScheduler_calculateNextFire_cronScheduleReturnsNextTime(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{
		ScheduleType: ScheduleTypeCron,
		ScheduleExpr: "0 * * * *", // top of every hour
	}
	now := time.Date(2025, 1, 15, 14, 30, 0, 0, time.UTC)

	next := s.calculateNextFire(sc, now)
	if next == nil {
		t.Fatal("calculateNextFire() = nil, want non-nil for cron schedule")
	}
	want := time.Date(2025, 1, 15, 15, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("calculateNextFire() = %v, want %v", next, want)
	}
}

func TestScheduler_calculateNextFire_intervalScheduleAddsIntervalToNow(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{
		ScheduleType: ScheduleTypeInterval,
		ScheduleExpr: "5m",
	}
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	next := s.calculateNextFire(sc, now)
	if next == nil {
		t.Fatal("calculateNextFire() = nil for interval schedule")
	}
	want := now.Add(5 * time.Minute)
	if !next.Equal(want) {
		t.Errorf("calculateNextFire() = %v, want %v", next, want)
	}
}

func TestScheduler_calculateNextFire_onceScheduleReturnsNil(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{ScheduleType: ScheduleTypeOnce}
	now := time.Now()

	next := s.calculateNextFire(sc, now)
	if next != nil {
		t.Errorf("calculateNextFire() = %v, want nil for once schedule", next)
	}
}

func TestScheduler_calculateNextFire_eventDelayedReturnsNil(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{ScheduleType: ScheduleTypeEventDelayed}
	now := time.Now()

	next := s.calculateNextFire(sc, now)
	if next != nil {
		t.Errorf("calculateNextFire() = %v, want nil for event_delayed schedule", next)
	}
}

func TestScheduler_calculateNextFire_invalidCronExpressionReturnsNil(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{
		ScheduleType: ScheduleTypeCron,
		ScheduleExpr: "not-a-cron-expr",
	}

	next := s.calculateNextFire(sc, time.Now())
	if next != nil {
		t.Errorf("calculateNextFire() = %v, want nil for invalid cron", next)
	}
}

func TestScheduler_calculateNextFire_invalidIntervalExpressionReturnsNil(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{
		ScheduleType: ScheduleTypeInterval,
		ScheduleExpr: "not-a-duration",
	}

	next := s.calculateNextFire(sc, time.Now())
	if next != nil {
		t.Errorf("calculateNextFire() = %v, want nil for invalid interval", next)
	}
}

func TestScheduler_calculateNextFire_unknownTypeReturnsNil(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{ScheduleType: "unknown_type", ScheduleExpr: "5m"}

	next := s.calculateNextFire(sc, time.Now())
	if next != nil {
		t.Errorf("calculateNextFire() = %v, want nil for unknown type", next)
	}
}

// ---- advanceToFuture ----

func TestScheduler_advanceToFuture_cronAlwaysReturnsFutureTime(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{
		ScheduleType: ScheduleTypeCron,
		ScheduleExpr: "* * * * *", // every minute
	}
	now := time.Now()

	next := s.advanceToFuture(sc, now)
	if next == nil {
		t.Fatal("advanceToFuture() = nil for cron schedule")
	}
	if !next.After(now) {
		t.Errorf("advanceToFuture() = %v is not after now=%v", next, now)
	}
}

func TestScheduler_advanceToFuture_intervalReturnsFutureTime(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{
		ScheduleType: ScheduleTypeInterval,
		ScheduleExpr: "10s",
	}
	now := time.Now()

	next := s.advanceToFuture(sc, now)
	if next == nil {
		t.Fatal("advanceToFuture() = nil for interval schedule")
	}
	if !next.After(now) {
		t.Errorf("advanceToFuture() = %v is not after now=%v", next, now)
	}
}

// ---- ComputeInitialNextFire ----

func TestScheduler_ComputeInitialNextFire_cronReturnsFutureTime(t *testing.T) {
	s := newTestScheduler()

	next, err := s.ComputeInitialNextFire(ScheduleTypeCron, "0 * * * *")
	if err != nil {
		t.Fatalf("ComputeInitialNextFire() error = %v", err)
	}
	if next == nil {
		t.Fatal("ComputeInitialNextFire() = nil, want non-nil")
	}
	if !next.After(time.Now()) {
		t.Errorf("ComputeInitialNextFire() = %v is not in the future", next)
	}
}

func TestScheduler_ComputeInitialNextFire_intervalReturnsFutureTime(t *testing.T) {
	s := newTestScheduler()

	next, err := s.ComputeInitialNextFire(ScheduleTypeInterval, "30s")
	if err != nil {
		t.Fatalf("ComputeInitialNextFire() error = %v", err)
	}
	if next == nil {
		t.Fatal("ComputeInitialNextFire() = nil, want non-nil")
	}
	if !next.After(time.Now()) {
		t.Errorf("ComputeInitialNextFire() = %v is not in the future", next)
	}
}

func TestScheduler_ComputeInitialNextFire_onceReturnsSpecifiedTime(t *testing.T) {
	s := newTestScheduler()
	want := time.Date(2099, 6, 1, 12, 0, 0, 0, time.UTC)

	next, err := s.ComputeInitialNextFire(ScheduleTypeOnce, want.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("ComputeInitialNextFire() error = %v", err)
	}
	if next == nil {
		t.Fatal("ComputeInitialNextFire() = nil, want non-nil")
	}
	if !next.Equal(want) {
		t.Errorf("ComputeInitialNextFire() = %v, want %v", next, want)
	}
}

func TestScheduler_ComputeInitialNextFire_onceWithInvalidTimeReturnsError(t *testing.T) {
	s := newTestScheduler()

	_, err := s.ComputeInitialNextFire(ScheduleTypeOnce, "not-a-time")
	if err == nil {
		t.Error("ComputeInitialNextFire() should return error for invalid once time")
	}
}

func TestScheduler_ComputeInitialNextFire_invalidCronReturnsError(t *testing.T) {
	s := newTestScheduler()

	_, err := s.ComputeInitialNextFire(ScheduleTypeCron, "bad expr")
	if err == nil {
		t.Error("ComputeInitialNextFire() should return error for invalid cron expression")
	}
}

func TestScheduler_ComputeInitialNextFire_invalidIntervalReturnsError(t *testing.T) {
	s := newTestScheduler()

	_, err := s.ComputeInitialNextFire(ScheduleTypeInterval, "not-a-duration")
	if err == nil {
		t.Error("ComputeInitialNextFire() should return error for invalid interval")
	}
}

func TestScheduler_ComputeInitialNextFire_unknownTypeReturnsNilAndNoError(t *testing.T) {
	s := newTestScheduler()

	next, err := s.ComputeInitialNextFire("unknown_type", "5m")
	if err != nil {
		t.Errorf("ComputeInitialNextFire() error = %v, want nil for unknown type", err)
	}
	if next != nil {
		t.Errorf("ComputeInitialNextFire() = %v, want nil for unknown type", next)
	}
}
