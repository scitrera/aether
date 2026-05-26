package tasks

import (
	"math"
	"math/rand/v2"
	"time"
)

// BackoffStrategy mirrors the proto pb.BackoffStrategy enum. Stored as an
// integer code so JSON round-trips cleanly without proto coupling in
// non-server packages.
type BackoffStrategy int32

const (
	BackoffStrategyUnspecified      BackoffStrategy = 0
	BackoffStrategyFixed            BackoffStrategy = 1
	BackoffStrategyExponential      BackoffStrategy = 2
	BackoffStrategyExplicitSchedule BackoffStrategy = 3
)

// RetryPolicy mirrors the proto pb.RetryPolicy message. Persisted as JSON
// on the task row in retry_policy_json. Kept independent of pb so that
// non-server packages (e.g. webhookservice) can construct policies without
// depending on the proto package.
//
// Workers that need different semantics — e.g., honoring an HTTP
// Retry-After response header — call RescheduleTaskAt explicitly to
// override the policy-computed next_retry_at.
type RetryPolicy struct {
	// MaxAttempts is the total attempts allowed (1 = no retries).
	// 0 means use the server default (3).
	MaxAttempts int32 `json:"max_attempts,omitempty"`

	// Backoff is the backoff strategy. Defaults to EXPONENTIAL when
	// unspecified to preserve the legacy gateway behavior.
	Backoff BackoffStrategy `json:"backoff,omitempty"`

	// InitialDelayMs is the base delay for EXPONENTIAL and the constant
	// for FIXED, in milliseconds.
	InitialDelayMs int64 `json:"initial_delay_ms,omitempty"`

	// MaxDelayMs caps the computed delay for EXPONENTIAL. 0 means no cap.
	MaxDelayMs int64 `json:"max_delay_ms,omitempty"`

	// JitterFactor scales a uniform random multiplier applied to the
	// computed delay: final = computed * (1 + uniform(-jitter, +jitter)).
	// Range [0,1]; values outside that range are clamped.
	JitterFactor float64 `json:"jitter_factor,omitempty"`

	// ScheduleMs is the explicit retry-delay schedule (milliseconds) used
	// when Backoff == EXPLICIT_SCHEDULE. Indexed by 0-based attempt
	// number; the final entry is used for any subsequent attempt.
	ScheduleMs []int64 `json:"schedule_ms,omitempty"`

	// RetryableStatusCodes is a worker convention: HTTP-style status
	// codes that should trigger a retry. The task store does not inspect
	// failure context; workers use this to decide whether to FailTask or
	// CompleteTask with permanent-failure semantics.
	RetryableStatusCodes []int32 `json:"retryable_status_codes,omitempty"`

	// HonorRetryAfter is a worker hint: prefer Retry-After (or analogous)
	// over computed delay when set on the failure response.
	HonorRetryAfter bool `json:"honor_retry_after,omitempty"`
}

// EffectiveMaxAttempts returns the policy's MaxAttempts, substituting the
// server default (3) when the caller left it zero.
func (p *RetryPolicy) EffectiveMaxAttempts() int32 {
	if p == nil || p.MaxAttempts <= 0 {
		return 3
	}
	return p.MaxAttempts
}

// ComputeNextRetryAt returns the wall-clock time at which the next retry
// attempt should fire. attempt is 1-based (1 = first retry, after the
// initial attempt failed).
//
// When retryAfter is non-nil AND policy.HonorRetryAfter is true, the
// returned time is exactly retryAfter — the caller has authoritative
// guidance from the failed receiver and the policy yields to it.
// Otherwise the delay is computed from the policy's backoff strategy
// (with optional jitter) and added to time.Now().
//
// Safe to call with a nil receiver: returns time.Now() (the same as
// today's "immediate re-pend" behavior).
func ComputeNextRetryAt(policy *RetryPolicy, attempt int, retryAfter *time.Time) time.Time {
	now := time.Now()
	if policy == nil {
		return now
	}
	if policy.HonorRetryAfter && retryAfter != nil && !retryAfter.IsZero() {
		return *retryAfter
	}

	var delayMs int64
	switch policy.Backoff {
	case BackoffStrategyFixed:
		delayMs = policy.InitialDelayMs
	case BackoffStrategyExplicitSchedule:
		if len(policy.ScheduleMs) == 0 {
			delayMs = policy.InitialDelayMs
		} else {
			idx := attempt - 1
			if idx < 0 {
				idx = 0
			}
			if idx >= len(policy.ScheduleMs) {
				idx = len(policy.ScheduleMs) - 1
			}
			delayMs = policy.ScheduleMs[idx]
		}
	default:
		// Unspecified and EXPONENTIAL both fall through to exponential.
		base := policy.InitialDelayMs
		if base <= 0 {
			base = 1000 // 1s fallback; preserves prior gateway timer behavior.
		}
		// delay = base * 2^(attempt-1).
		exp := attempt - 1
		if exp < 0 {
			exp = 0
		}
		if exp > 62 {
			// Prevent int64 overflow on absurd attempt counts.
			exp = 62
		}
		scaled := float64(base) * math.Pow(2, float64(exp))
		if scaled > float64(math.MaxInt64) {
			delayMs = math.MaxInt64
		} else {
			delayMs = int64(scaled)
		}
		if policy.MaxDelayMs > 0 && delayMs > policy.MaxDelayMs {
			delayMs = policy.MaxDelayMs
		}
	}

	if delayMs < 0 {
		delayMs = 0
	}

	// Apply multiplicative jitter, clamped to [0, 1].
	jitter := policy.JitterFactor
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	if jitter > 0 && delayMs > 0 {
		// uniform in [-jitter, +jitter]
		offset := (rand.Float64()*2 - 1) * jitter
		scaled := float64(delayMs) * (1 + offset)
		if scaled < 0 {
			scaled = 0
		}
		delayMs = int64(scaled)
	}

	return now.Add(time.Duration(delayMs) * time.Millisecond)
}
