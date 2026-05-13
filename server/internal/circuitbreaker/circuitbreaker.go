// Package circuitbreaker provides a generic circuit breaker for protecting calls
// to external services (Redis, RabbitMQ, etc.) against transient failures.
// It prevents thundering herd reconnects by fast-failing during outages and
// allowing gradual recovery via the half-open state.
package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when the circuit breaker is open and calls are being rejected.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// State represents the current state of the circuit breaker.
type State int

const (
	// StateClosed is the normal operating state — calls pass through.
	StateClosed State = iota
	// StateOpen means the circuit has tripped due to too many failures; calls are rejected fast.
	StateOpen
	// StateHalfOpen means the circuit is testing recovery; a limited number of calls are allowed.
	StateHalfOpen
)

// String returns a human-readable name for the state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker tracks failures and transitions between closed/open/half-open states.
type CircuitBreaker struct {
	name         string
	maxFailures  int
	resetTimeout time.Duration
	halfOpenMax  int

	mu            sync.Mutex
	state         State
	failures      int
	lastFailure   time.Time
	successesInHO int
}

// Option is a functional option for configuring a CircuitBreaker.
type Option func(*CircuitBreaker)

// WithMaxFailures sets the number of consecutive failures before the circuit opens.
// Default: 5.
func WithMaxFailures(n int) Option {
	return func(cb *CircuitBreaker) {
		cb.maxFailures = n
	}
}

// WithResetTimeout sets how long to wait in the open state before transitioning to half-open.
// Default: 30s.
func WithResetTimeout(d time.Duration) Option {
	return func(cb *CircuitBreaker) {
		cb.resetTimeout = d
	}
}

// WithHalfOpenMax sets the maximum number of calls allowed in the half-open state before
// deciding to close or re-open the circuit. Default: 1.
func WithHalfOpenMax(n int) Option {
	return func(cb *CircuitBreaker) {
		cb.halfOpenMax = n
	}
}

// New creates a new CircuitBreaker with the given name and options.
func New(name string, opts ...Option) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:         name,
		maxFailures:  5,
		resetTimeout: 30 * time.Second,
		halfOpenMax:  1,
		state:        StateClosed,
	}
	for _, opt := range opts {
		opt(cb)
	}
	return cb
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.currentState()
}

// Name returns the circuit breaker name.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// currentState returns the current state, transitioning from open to half-open if the
// reset timeout has elapsed. Must be called with cb.mu held.
func (cb *CircuitBreaker) currentState() State {
	if cb.state == StateOpen && time.Since(cb.lastFailure) >= cb.resetTimeout {
		cb.state = StateHalfOpen
		cb.successesInHO = 0
	}
	return cb.state
}

// Execute calls fn if the circuit allows it, updating state based on the outcome.
// Returns ErrCircuitOpen immediately when the circuit is open. Any error returned
// by fn counts as a failure.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	state := cb.currentState()

	switch state {
	case StateOpen:
		cb.mu.Unlock()
		return ErrCircuitOpen

	case StateHalfOpen:
		// Only allow up to halfOpenMax concurrent probes.
		if cb.successesInHO >= cb.halfOpenMax {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		cb.mu.Unlock()

		err := fn()
		cb.mu.Lock()
		if err != nil {
			// Probe failed — re-open the circuit.
			cb.failures++
			cb.lastFailure = time.Now()
			cb.state = StateOpen
			cb.mu.Unlock()
			return err
		}
		// Probe succeeded.
		cb.successesInHO++
		if cb.successesInHO >= cb.halfOpenMax {
			// Enough successes — close the circuit.
			cb.state = StateClosed
			cb.failures = 0
		}
		cb.mu.Unlock()
		return nil

	default: // StateClosed
		cb.mu.Unlock()
		err := fn()
		if err != nil {
			cb.mu.Lock()
			cb.failures++
			cb.lastFailure = time.Now()
			if cb.failures >= cb.maxFailures {
				cb.state = StateOpen
			}
			cb.mu.Unlock()
			return err
		}
		// Success resets the consecutive failure counter.
		cb.mu.Lock()
		cb.failures = 0
		cb.mu.Unlock()
		return nil
	}
}
