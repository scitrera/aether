package tasks

import (
	"math"
	"testing"
	"time"
)

func TestComputeNextRetryAt_NilPolicy_ReturnsNow(t *testing.T) {
	before := time.Now()
	got := ComputeNextRetryAt(nil, 1, nil)
	if got.Before(before) || got.After(time.Now().Add(50*time.Millisecond)) {
		t.Errorf("nil policy should return ~now; got %v (before=%v)", got, before)
	}
}

func TestComputeNextRetryAt_HonorsRetryAfter(t *testing.T) {
	target := time.Now().Add(7 * time.Minute)
	p := &RetryPolicy{
		Backoff:         BackoffStrategyExponential,
		InitialDelayMs:  100,
		HonorRetryAfter: true,
	}
	got := ComputeNextRetryAt(p, 1, &target)
	if !got.Equal(target) {
		t.Errorf("HonorRetryAfter=true should pass through; got %v, want %v", got, target)
	}
}

func TestComputeNextRetryAt_IgnoresRetryAfterWhenDisabled(t *testing.T) {
	target := time.Now().Add(1 * time.Hour)
	p := &RetryPolicy{
		Backoff:        BackoffStrategyFixed,
		InitialDelayMs: 1000,
	}
	got := ComputeNextRetryAt(p, 1, &target)
	if got.After(time.Now().Add(2 * time.Second)) {
		t.Errorf("HonorRetryAfter unset should ignore retryAfter; got %v", got)
	}
}

func TestComputeNextRetryAt_Fixed(t *testing.T) {
	p := &RetryPolicy{
		Backoff:        BackoffStrategyFixed,
		InitialDelayMs: 5000,
	}
	// Same delay regardless of attempt.
	for _, attempt := range []int{1, 2, 5, 10} {
		before := time.Now()
		got := ComputeNextRetryAt(p, attempt, nil)
		delta := got.Sub(before)
		if delta < 4900*time.Millisecond || delta > 5200*time.Millisecond {
			t.Errorf("attempt=%d: fixed 5s, got delta=%v", attempt, delta)
		}
	}
}

func TestComputeNextRetryAt_Exponential(t *testing.T) {
	p := &RetryPolicy{
		Backoff:        BackoffStrategyExponential,
		InitialDelayMs: 100,
		MaxDelayMs:     0, // no cap
	}
	cases := map[int]int64{
		1: 100,
		2: 200,
		3: 400,
		4: 800,
		5: 1600,
	}
	for attempt, wantMs := range cases {
		before := time.Now()
		got := ComputeNextRetryAt(p, attempt, nil)
		delta := got.Sub(before).Milliseconds()
		// allow 50ms slack for clock skew + jitter-free computation.
		if delta < wantMs-50 || delta > wantMs+50 {
			t.Errorf("attempt=%d: want ~%dms, got %dms", attempt, wantMs, delta)
		}
	}
}

func TestComputeNextRetryAt_ExponentialCappedAtMax(t *testing.T) {
	p := &RetryPolicy{
		Backoff:        BackoffStrategyExponential,
		InitialDelayMs: 100,
		MaxDelayMs:     500,
	}
	// At attempt 5, raw would be 100 * 2^4 = 1600; should clamp to 500.
	before := time.Now()
	got := ComputeNextRetryAt(p, 5, nil)
	delta := got.Sub(before).Milliseconds()
	if delta > 600 {
		t.Errorf("attempt=5 with max=500: got %dms; should be capped", delta)
	}
}

func TestComputeNextRetryAt_ExponentialDefaultsBase(t *testing.T) {
	// When InitialDelayMs is zero, base fallback (1s) kicks in.
	p := &RetryPolicy{
		Backoff: BackoffStrategyExponential,
	}
	before := time.Now()
	got := ComputeNextRetryAt(p, 1, nil)
	delta := got.Sub(before).Milliseconds()
	if delta < 950 || delta > 1100 {
		t.Errorf("exponential fallback base 1s on attempt=1 expected ~1000ms, got %dms", delta)
	}
}

func TestComputeNextRetryAt_ExplicitSchedule(t *testing.T) {
	p := &RetryPolicy{
		Backoff:    BackoffStrategyExplicitSchedule,
		ScheduleMs: []int64{5000, 300000, 1800000},
	}
	cases := map[int]int64{
		1: 5000,
		2: 300000,
		3: 1800000,
		4: 1800000, // past-end clamps to last entry
		5: 1800000,
	}
	for attempt, wantMs := range cases {
		before := time.Now()
		got := ComputeNextRetryAt(p, attempt, nil)
		delta := got.Sub(before).Milliseconds()
		if delta < wantMs-50 || delta > wantMs+50 {
			t.Errorf("attempt=%d: want ~%dms, got %dms", attempt, wantMs, delta)
		}
	}
}

func TestComputeNextRetryAt_ExplicitScheduleEmptyFallsBackToInitial(t *testing.T) {
	p := &RetryPolicy{
		Backoff:        BackoffStrategyExplicitSchedule,
		InitialDelayMs: 250,
	}
	before := time.Now()
	got := ComputeNextRetryAt(p, 1, nil)
	delta := got.Sub(before).Milliseconds()
	if delta < 200 || delta > 350 {
		t.Errorf("empty schedule should fall back to InitialDelayMs; got %dms", delta)
	}
}

func TestComputeNextRetryAt_JitterStaysInRange(t *testing.T) {
	p := &RetryPolicy{
		Backoff:        BackoffStrategyFixed,
		InitialDelayMs: 10000,
		JitterFactor:   0.5, // ±50%
	}
	// Sample several times; every sample must fall within [5000ms, 15000ms].
	for i := 0; i < 20; i++ {
		before := time.Now()
		got := ComputeNextRetryAt(p, 1, nil)
		delta := got.Sub(before).Milliseconds()
		if delta < 4900 || delta > 15100 {
			t.Errorf("sample %d: jittered fixed 10s with +-50%% out of bounds: %dms", i, delta)
		}
	}
}

func TestEffectiveMaxAttempts(t *testing.T) {
	cases := []struct {
		in   *RetryPolicy
		want int32
	}{
		{nil, 3},
		{&RetryPolicy{}, 3},
		{&RetryPolicy{MaxAttempts: 0}, 3},
		{&RetryPolicy{MaxAttempts: 1}, 1},
		{&RetryPolicy{MaxAttempts: 7}, 7},
		{&RetryPolicy{MaxAttempts: -5}, 3},
	}
	for _, c := range cases {
		if got := c.in.EffectiveMaxAttempts(); got != c.want {
			t.Errorf("%v: EffectiveMaxAttempts() = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestComputeNextRetryAt_ExponentialOverflowSafe(t *testing.T) {
	p := &RetryPolicy{
		Backoff:        BackoffStrategyExponential,
		InitialDelayMs: math.MaxInt32,
	}
	// Don't crash on absurd attempt counts.
	_ = ComputeNextRetryAt(p, 100, nil)
}
