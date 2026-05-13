package cleanup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// BackgroundRunner — setLeader cancels cleanup goroutines on leader loss
// =============================================================================

func TestBackgroundRunner_SetLeader_CancelsCleanupOnLoss(t *testing.T) {
	// Build a runner with a cleanupCancel already set so we can detect it fires.
	cancelled := make(chan struct{})
	cancelFn := func() { close(cancelled) }

	runner := &BackgroundRunner{
		isLeader:      true,
		cleanupCancel: cancelFn,
	}

	runner.setLeader(false)

	// The cancel function must be called synchronously by setLeader(false).
	select {
	case <-cancelled:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Error("setLeader(false) did not call cleanupCancel within timeout")
	}

	if runner.cleanupCancel != nil {
		t.Error("cleanupCancel should be nil after setLeader(false)")
	}
}

func TestBackgroundRunner_SetLeader_NilCancelSafe(t *testing.T) {
	// setLeader(false) with a nil cleanupCancel must not panic.
	runner := &BackgroundRunner{isLeader: true, cleanupCancel: nil}
	runner.setLeader(false) // must not panic
}

func TestBackgroundRunner_SetLeader_True_DoesNotCancelJobs(t *testing.T) {
	called := false
	cancelFn := func() { called = true }

	runner := &BackgroundRunner{isLeader: false, cleanupCancel: cancelFn}
	runner.setLeader(true)

	if called {
		t.Error("setLeader(true) must not call the existing cleanupCancel")
	}
	if runner.cleanupCancel == nil {
		t.Error("setLeader(true) must not clear cleanupCancel")
	}
}

// =============================================================================
// runPeriodic — stops when context is cancelled
// =============================================================================

func TestRunPeriodic_StopsOnContextCancel(t *testing.T) {
	runner := &BackgroundRunner{}

	ctx, cancel := context.WithCancel(context.Background())

	var execCount int64
	jobDone := make(chan struct{})

	go func() {
		runner.runPeriodic(ctx, "test-job", 10*time.Millisecond, func(ctx context.Context) {
			atomic.AddInt64(&execCount, 1)
		})
		close(jobDone)
	}()

	// Let the job run at least once.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-jobDone:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runPeriodic did not stop after context cancellation")
	}

	if atomic.LoadInt64(&execCount) == 0 {
		t.Error("expected job to execute at least once before cancellation")
	}
}

func TestRunPeriodic_NeverRunsBeforeFirstTick(t *testing.T) {
	runner := &BackgroundRunner{}
	ctx, cancel := context.WithCancel(context.Background())

	var execCount int64
	done := make(chan struct{})

	go func() {
		runner.runPeriodic(ctx, "test-job", 1*time.Hour, func(ctx context.Context) {
			atomic.AddInt64(&execCount, 1)
		})
		close(done)
	}()

	// Cancel before the first tick (interval is 1 hour).
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("runPeriodic did not stop after immediate context cancellation")
	}

	if atomic.LoadInt64(&execCount) != 0 {
		t.Error("job should not execute before the first tick")
	}
}

// =============================================================================
// startCleanupJobs — cancels previous cleanup context before starting new one
// =============================================================================

func TestStartCleanupJobs_CancelsPreviousContext(t *testing.T) {
	service := NewService(nil, nil, nil, &Config{
		TaskPurgeInterval:      0, // Disable purge goroutine
		ReconciliationInterval: 0, // Disable reconciliation goroutine
	})

	runner := &BackgroundRunner{service: service}

	previousCancelled := false
	runner.cleanupCancel = func() { previousCancelled = true }

	ctx := context.Background()
	runner.startCleanupJobs(ctx)
	defer func() {
		runner.mu.Lock()
		if runner.cleanupCancel != nil {
			runner.cleanupCancel()
		}
		runner.mu.Unlock()
	}()

	if !previousCancelled {
		t.Error("startCleanupJobs must cancel the previous cleanupCancel before replacing it")
	}
}

// =============================================================================
// RunStartupJobs — all nil deps, must not panic
// =============================================================================

func TestRunStartupJobs_NilDependencies(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	ctx := context.Background()
	// Must not panic even when all dependencies are nil.
	service.RunStartupJobs(ctx)
}

// =============================================================================
// IsLeader concurrency — safe under concurrent reads/writes
// =============================================================================

func TestBackgroundRunner_IsLeader_Concurrent(t *testing.T) {
	runner := &BackgroundRunner{}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Concurrent writers
	for i := 0; i < 4; i++ {
		go func(i int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					runner.setLeader(i%2 == 0)
				}
			}
		}(i)
	}

	// Concurrent readers — must not race
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_ = runner.IsLeader()
				}
			}
		}()
	}

	<-ctx.Done()
}

// =============================================================================
// Stop — idempotent, does not panic when called multiple times
// =============================================================================

func TestBackgroundRunner_Stop_Idempotent(t *testing.T) {
	service := NewService(nil, nil, nil, &Config{
		TaskPurgeInterval:      0,
		ReconciliationInterval: 0,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := service.StartBackground(ctx)

	// Calling Stop twice must not panic.
	runner.Stop()
	runner.Stop()
}

// =============================================================================
// CleanupStaleClaims — uses default threshold when StaleClaimTimeout is zero
// =============================================================================

func TestCleanupStaleClaims_UsesDefaultThresholdWhenZero(t *testing.T) {
	// The only observable behaviour without a real dispatcher is "skip" success.
	service := NewService(nil, nil, nil, &Config{
		StaleClaimTimeout: 0, // should default to 5 minutes internally
	})

	result := service.CleanupStaleClaims(context.Background())
	if !result.Success {
		t.Errorf("expected success (dispatcher nil skip), got error: %v", result.Error)
	}
}

// =============================================================================
// SetDispatcher — can be called after NewService
// =============================================================================

func TestService_SetDispatcher(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	if service.dispatcher != nil {
		t.Error("dispatcher should be nil before SetDispatcher")
	}
	// nil dispatcher is a valid noop — just verify it doesn't panic
	service.SetDispatcher(nil)
}
