package workflow

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

// newTestScheduler builds a Scheduler with nil store/executor/dagEng/leader — safe
// for unit tests that only exercise pure computation methods.
func newTestScheduler() *Scheduler {
	return &Scheduler{
		parser:   cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		interval: time.Second,
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

// ---- countMissedFires ----

func TestScheduler_countMissedFires_returnsOneWhenNoNextFireAt(t *testing.T) {
	s := newTestScheduler()
	sc := Schedule{
		ScheduleType: ScheduleTypeInterval,
		ScheduleExpr: "1m",
		NextFireAt:   nil,
	}

	count := s.countMissedFires(sc, time.Now())
	if count != 1 {
		t.Errorf("countMissedFires() = %d, want 1 when NextFireAt is nil", count)
	}
}

func TestScheduler_countMissedFires_intervalCountsMissedPeriods(t *testing.T) {
	s := newTestScheduler()
	base := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	// NextFireAt is 3 minutes ago, interval is 1m → should count 3 misses
	nextFire := base.Add(-3 * time.Minute)
	sc := Schedule{
		ScheduleType: ScheduleTypeInterval,
		ScheduleExpr: "1m",
		NextFireAt:   &nextFire,
	}

	count := s.countMissedFires(sc, base)
	if count < 3 {
		t.Errorf("countMissedFires() = %d, want ≥3 for 3-minute gap with 1m interval", count)
	}
}

func TestScheduler_countMissedFires_cronCountsMissedSlots(t *testing.T) {
	s := newTestScheduler()
	// nextFire was 3 hours ago, cron fires every hour → 3 missed
	base := time.Date(2025, 1, 15, 15, 0, 0, 0, time.UTC)
	nextFire := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	sc := Schedule{
		ScheduleType: ScheduleTypeCron,
		ScheduleExpr: "0 * * * *", // top of hour
		NextFireAt:   &nextFire,
	}

	count := s.countMissedFires(sc, base)
	if count < 3 {
		t.Errorf("countMissedFires() = %d, want ≥3 for 3-hour gap with hourly cron", count)
	}
}

func TestScheduler_countMissedFires_invalidIntervalReturnsOne(t *testing.T) {
	s := newTestScheduler()
	now := time.Now()
	nextFire := now.Add(-5 * time.Minute)
	sc := Schedule{
		ScheduleType: ScheduleTypeInterval,
		ScheduleExpr: "bad-duration",
		NextFireAt:   &nextFire,
	}

	count := s.countMissedFires(sc, now)
	if count != 1 {
		t.Errorf("countMissedFires() = %d with invalid interval, want 1", count)
	}
}

func TestScheduler_countMissedFires_nonIntervalNonCronReturnsOne(t *testing.T) {
	s := newTestScheduler()
	now := time.Now()
	nextFire := now.Add(-1 * time.Minute)
	sc := Schedule{
		ScheduleType: ScheduleTypeOnce,
		NextFireAt:   &nextFire,
	}

	count := s.countMissedFires(sc, now)
	if count != 1 {
		t.Errorf("countMissedFires() = %d for once schedule, want 1", count)
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
