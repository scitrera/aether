package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errSentinel = errors.New("test error")

func succeed() error { return nil }
func fail() error    { return errSentinel }

// TestNew verifies defaults.
func TestNew(t *testing.T) {
	cb := New("test")
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %s", cb.State())
	}
	if cb.Name() != "test" {
		t.Fatalf("expected name 'test', got %q", cb.Name())
	}
}

// TestNormalOperation verifies that successful calls pass through and reset failure count.
func TestNormalOperation(t *testing.T) {
	cb := New("normal", WithMaxFailures(3))

	for i := 0; i < 10; i++ {
		if err := cb.Execute(succeed); err != nil {
			t.Fatalf("unexpected error on successful call %d: %v", i, err)
		}
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after 10 successes, got %s", cb.State())
	}
}

// TestTransitionToOpen verifies the circuit opens after maxFailures consecutive failures.
func TestTransitionToOpen(t *testing.T) {
	cb := New("open-test", WithMaxFailures(3))

	for i := 0; i < 3; i++ {
		err := cb.Execute(fail)
		if err == nil {
			t.Fatalf("expected error on failure call %d", i)
		}
		if err == ErrCircuitOpen {
			t.Fatalf("got ErrCircuitOpen before max failures reached (call %d)", i)
		}
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after 3 failures, got %s", cb.State())
	}
}

// TestRejectionInOpenState verifies that calls are rejected immediately when open.
func TestRejectionInOpenState(t *testing.T) {
	cb := New("reject-test", WithMaxFailures(1), WithResetTimeout(10*time.Minute))

	// Trip the circuit.
	_ = cb.Execute(fail)
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %s", cb.State())
	}

	called := false
	err := cb.Execute(func() error {
		called = true
		return nil
	})
	if err != ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if called {
		t.Fatal("fn should not be called when circuit is open")
	}
}

// TestTransitionToHalfOpen verifies that the circuit moves to half-open after resetTimeout.
func TestTransitionToHalfOpen(t *testing.T) {
	cb := New("half-open-test",
		WithMaxFailures(1),
		WithResetTimeout(50*time.Millisecond),
	)

	_ = cb.Execute(fail)
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %s", cb.State())
	}

	time.Sleep(80 * time.Millisecond)

	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen after reset timeout, got %s", cb.State())
	}
}

// TestRecoveryToClosedOnSuccess verifies that a success in half-open closes the circuit.
func TestRecoveryToClosedOnSuccess(t *testing.T) {
	cb := New("recovery-test",
		WithMaxFailures(1),
		WithResetTimeout(50*time.Millisecond),
		WithHalfOpenMax(1),
	)

	_ = cb.Execute(fail)
	time.Sleep(80 * time.Millisecond)

	// Should be half-open now; one successful probe closes it.
	if err := cb.Execute(succeed); err != nil {
		t.Fatalf("unexpected error in half-open: %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after successful probe, got %s", cb.State())
	}
}

// TestReopenOnFailureInHalfOpen verifies that a failure in half-open re-opens the circuit.
func TestReopenOnFailureInHalfOpen(t *testing.T) {
	cb := New("reopen-test",
		WithMaxFailures(1),
		WithResetTimeout(50*time.Millisecond),
		WithHalfOpenMax(1),
	)

	_ = cb.Execute(fail)
	time.Sleep(80 * time.Millisecond)

	err := cb.Execute(fail)
	if err == nil {
		t.Fatal("expected error from probe failure")
	}
	if err == ErrCircuitOpen {
		t.Fatal("expected the underlying error, not ErrCircuitOpen, during the probe call itself")
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after probe failure, got %s", cb.State())
	}
}

// TestSuccessResetsFailureCount verifies that a success resets the failure counter.
func TestSuccessResetsFailureCount(t *testing.T) {
	cb := New("reset-count", WithMaxFailures(3))

	// Two failures.
	_ = cb.Execute(fail)
	_ = cb.Execute(fail)

	// One success — should reset count.
	if err := cb.Execute(succeed); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %s", cb.State())
	}

	// Two more failures shouldn't open it (counter was reset to 0).
	_ = cb.Execute(fail)
	_ = cb.Execute(fail)
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after reset+2 failures, got %s", cb.State())
	}

	// One more failure (total 3 from reset) should open it.
	_ = cb.Execute(fail)
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %s", cb.State())
	}
}

// TestConcurrentSafety verifies the circuit breaker is safe under concurrent access.
func TestConcurrentSafety(t *testing.T) {
	cb := New("concurrent",
		WithMaxFailures(10),
		WithResetTimeout(50*time.Millisecond),
	)

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var fn func() error
			if n%3 == 0 {
				fn = fail
			} else {
				fn = succeed
			}
			if err := cb.Execute(fn); err != nil && !errors.Is(err, ErrCircuitOpen) && !errors.Is(err, errSentinel) {
				errCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	// No unexpected errors (only sentinel or ErrCircuitOpen).
	if errCount.Load() != 0 {
		t.Fatalf("unexpected non-sentinel errors: %d", errCount.Load())
	}

	// State must be one of the valid states.
	s := cb.State()
	if s != StateClosed && s != StateOpen && s != StateHalfOpen {
		t.Fatalf("invalid state: %v", s)
	}
}

// TestStateString verifies the String() method on State.
func TestStateString(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(99), "unknown"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("State(%d).String() = %q, want %q", c.s, got, c.want)
		}
	}
}
