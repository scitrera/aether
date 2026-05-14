// Package cleanup provides background cleanup jobs for the Aether gateway.
// Jobs can be run either by the gateway's background goroutines or by a standalone
// cleanup command for external scheduling (e.g., cron).
//
// When multiple gateway instances are running, leader election ensures only one
// instance runs the cleanup jobs. This uses the existing session lock mechanism
// with a system identity (tu._system._cleanup.leader).
package cleanup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// Config holds configuration for cleanup jobs
type Config struct {
	// Task purge settings
	TaskPurgeInterval      time.Duration // How often to run task purge (0 = disabled)
	CompletedTaskRetention time.Duration // How long to keep completed tasks
	FailedTaskRetention    time.Duration // How long to keep failed tasks
	CancelledTaskRetention time.Duration // How long to keep cancelled tasks

	// Reconciliation settings
	ReconciliationInterval time.Duration // How often to run orphaned task reconciliation (0 = disabled)

	// Stale claim settings
	StaleClaimTimeout time.Duration // How long a task can stay 'claimed' before being recovered (0 = use default 5m)

	// Leader election settings
	LeaderElectionRetryInterval time.Duration // How often to retry acquiring leadership if not leader (0 = use default 30s)
}

// DefaultConfig returns the default cleanup configuration
func DefaultConfig() *Config {
	return &Config{
		TaskPurgeInterval:           24 * time.Hour, // Daily
		CompletedTaskRetention:      7 * 24 * time.Hour,
		FailedTaskRetention:         14 * 24 * time.Hour,
		CancelledTaskRetention:      7 * 24 * time.Hour,
		ReconciliationInterval:      1 * time.Minute,
		StaleClaimTimeout:           5 * time.Minute,
		LeaderElectionRetryInterval: 30 * time.Second,
	}
}

// SessionRegistry is the narrow surface the cleanup service needs from a
// session store: leader-election lock acquire/release/refresh plus a
// stale-lock sweeper. Both *state.SessionRegistry (Redis) and
// *state.BadgerSessionRegistry (lite) satisfy this surface.
//
// History: until 2026-05-13 this field was typed as *state.SessionRegistry.
// In lite mode the gateway type-asserted *state.BadgerSessionRegistry to the
// concrete Redis type, which failed silently and left the field nil — the
// `if s.sessionRegistry == nil` guard in StartBackground then bypassed
// leader election entirely. Functional degradation, but not a crash. The
// interface restores symmetry: lite mode now uses real leader-election
// state (trivially: there's only one candidate), and the seam is in place
// for a future multi-node lite story without retyping the struct field.
type SessionRegistry interface {
	AcquireLock(ctx context.Context, identity models.Identity, sessionID string) (bool, error)
	ReleaseLock(ctx context.Context, identity models.Identity, sessionID string) error
	RefreshLock(ctx context.Context, identity models.Identity, sessionID string) (bool, error)
	CleanupStaleLocks(ctx context.Context) (int, error)
}

// compile-time conformance.
var _ SessionRegistry = (*state.SessionRegistry)(nil)
var _ SessionRegistry = (*state.BadgerSessionRegistry)(nil)

// Service provides cleanup operations for the gateway.
// It can be used for both background goroutines and standalone cleanup commands.
type Service struct {
	taskStore       *tasks.TaskStore
	taskService     *orchestration.TaskAssignmentService
	dispatcher      orchestration.TaskDispatcher
	sessionRegistry SessionRegistry
	config          *Config
}

// NewService creates a new cleanup service
func NewService(
	taskStore *tasks.TaskStore,
	taskService *orchestration.TaskAssignmentService,
	sessionRegistry SessionRegistry,
	config *Config,
) *Service {
	if config == nil {
		config = DefaultConfig()
	}
	return &Service{
		taskStore:       taskStore,
		taskService:     taskService,
		sessionRegistry: sessionRegistry,
		config:          config,
	}
}

// SetDispatcher sets the orchestration dispatcher for stale claim recovery.
func (s *Service) SetDispatcher(dispatcher orchestration.TaskDispatcher) {
	s.dispatcher = dispatcher
}

// JobResult contains the result of a cleanup job
type JobResult struct {
	JobName   string
	Success   bool
	Error     error
	Details   string
	Duration  time.Duration
	ItemCount int64
}

// RunAllJobs runs all cleanup jobs once and returns the results.
// This is intended for use by the standalone cleanup command.
func (s *Service) RunAllJobs(ctx context.Context) []JobResult {
	var results []JobResult

	// Run stale lock cleanup
	result := s.CleanupStaleLocks(ctx)
	results = append(results, result)

	// Run stale claim recovery
	result = s.CleanupStaleClaims(ctx)
	results = append(results, result)

	// Run orphaned task reconciliation
	result = s.ReconcileOrphanedTasks(ctx)
	results = append(results, result)

	// Run task purge
	result = s.PurgeTasks(ctx)
	results = append(results, result)

	return results
}

// CleanupStaleLocks removes locks with no TTL (from before TTL was added).
func (s *Service) CleanupStaleLocks(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "stale_lock_cleanup"}

	if s.sessionRegistry == nil {
		result.Success = true
		result.Details = "skipped (no session registry)"
		return result
	}

	count, err := s.sessionRegistry.CleanupStaleLocks(ctx)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = int64(count)
	if count > 0 {
		result.Details = fmt.Sprintf("removed %d stale locks", count)
	} else {
		result.Details = "no stale locks found"
	}

	return result
}

// ReconcileOrphanedTasks finds and fails tasks whose agents/orchestrators have disconnected.
func (s *Service) ReconcileOrphanedTasks(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "orphaned_task_reconciliation"}

	if s.taskService == nil {
		result.Error = fmt.Errorf("task service not configured")
		return result
	}

	count, err := s.taskService.ReconcileOrphanedTasks(ctx)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = int64(count)
	if count > 0 {
		result.Details = fmt.Sprintf("reconciled %d orphaned tasks", count)
	} else {
		result.Details = "no orphaned tasks found"
	}

	return result
}

// CleanupStaleClaims recovers orchestration tasks stuck in 'claimed' status.
// This handles gateway crashes that leave tasks claimed but never delivered.
func (s *Service) CleanupStaleClaims(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "stale_claim_recovery"}

	if s.dispatcher == nil {
		result.Success = true
		result.Details = "dispatcher not configured, skipping"
		return result
	}

	threshold := s.config.StaleClaimTimeout
	if threshold <= 0 {
		threshold = 5 * time.Minute
	}

	count, err := s.dispatcher.RecoverStaleClaims(ctx, threshold)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = int64(count)
	if count > 0 {
		result.Details = fmt.Sprintf("recovered %d stale claims (threshold: %v)", count, threshold)
	} else {
		result.Details = "no stale claims found"
	}

	return result
}

// PurgeTasks deletes old completed/failed/cancelled tasks based on retention settings.
func (s *Service) PurgeTasks(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "task_purge"}

	if s.taskStore == nil {
		result.Error = fmt.Errorf("task store not configured")
		return result
	}

	purgeResult, err := s.taskStore.PurgeOldTasks(
		ctx,
		s.config.CompletedTaskRetention,
		s.config.FailedTaskRetention,
		s.config.CancelledTaskRetention,
	)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = purgeResult.Total()
	if purgeResult.Total() > 0 {
		result.Details = fmt.Sprintf("purged %d completed, %d failed, %d cancelled tasks",
			purgeResult.Completed, purgeResult.Failed, purgeResult.Cancelled)
	} else {
		result.Details = "no tasks to purge"
	}

	return result
}

// BackgroundRunner manages periodic execution of cleanup jobs.
// It can be stopped by canceling the provided context or calling Stop().
// Uses leader election to ensure only one instance runs cleanup jobs across
// multiple gateway instances.
type BackgroundRunner struct {
	service    *Service
	cancelFunc context.CancelFunc
	stopped    chan struct{}

	// Leader election state
	mu            sync.RWMutex
	isLeader      bool
	sessionID     string
	identity      models.Identity
	cleanupCancel context.CancelFunc // cancels the current set of cleanup goroutines
}

// StartBackground starts all configured cleanup jobs as background goroutines.
// Uses leader election to ensure only one gateway instance runs cleanup jobs.
// Returns a BackgroundRunner that can be used to stop the jobs.
func (s *Service) StartBackground(ctx context.Context) *BackgroundRunner {
	ctx, cancel := context.WithCancel(ctx)
	runner := &BackgroundRunner{
		service:    s,
		cancelFunc: cancel,
		stopped:    make(chan struct{}),
		sessionID:  uuid.New().String(),
		identity:   models.CleanupLeaderIdentity(),
	}

	// Check if leader election is possible (requires session registry)
	if s.sessionRegistry == nil {
		logging.Logger.Warn().Msg("no session registry available, running cleanup jobs without leader election")
		runner.startCleanupJobs(ctx)
		return runner
	}

	// Start the leader election loop
	go runner.leaderElectionLoop(ctx)

	return runner
}

// leaderElectionLoop continuously tries to acquire/maintain leadership
func (r *BackgroundRunner) leaderElectionLoop(ctx context.Context) {
	retryInterval := r.service.config.LeaderElectionRetryInterval
	if retryInterval <= 0 {
		retryInterval = 30 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if r.IsLeader() {
			// Already leader, refresh the lock
			refreshed, err := r.service.sessionRegistry.RefreshLock(ctx, r.identity, r.sessionID)
			if err != nil {
				logging.Logger.Error().Err(err).Msg("error refreshing leadership lock")
				r.setLeader(false)
			} else if !refreshed {
				logging.Logger.Warn().Msg("lost cleanup leadership (lock refresh failed)")
				r.setLeader(false)
			}
		} else {
			// Try to acquire leadership
			acquired, err := r.service.sessionRegistry.AcquireLock(ctx, r.identity, r.sessionID)
			if err != nil {
				logging.Logger.Error().Err(err).Msg("error acquiring leadership lock")
			} else if acquired {
				logging.Logger.Info().Msg("acquired cleanup leadership")
				r.setLeader(true)
				// Start cleanup jobs now that we're the leader
				r.startCleanupJobs(ctx)
			}
		}

		// Wait before next check
		// Use LockRefreshInterval when leader (to maintain lock), retryInterval when not
		waitInterval := retryInterval
		if r.IsLeader() {
			waitInterval = state.LockRefreshInterval
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(waitInterval):
		}
	}
}

// startCleanupJobs starts the actual periodic cleanup jobs under a cancellable
// sub-context derived from ctx. The cancel function is stored in r.cleanupCancel
// so that setLeader(false) can stop these goroutines when leadership is lost.
// Must be called with r.mu held or from the single-threaded leaderElectionLoop.
func (r *BackgroundRunner) startCleanupJobs(parentCtx context.Context) {
	cleanupCtx, cancel := context.WithCancel(parentCtx)

	r.mu.Lock()
	// Cancel any previously running cleanup goroutines before replacing.
	if r.cleanupCancel != nil {
		r.cleanupCancel()
	}
	r.cleanupCancel = cancel
	r.mu.Unlock()

	s := r.service

	// Start task purge if enabled
	if s.config.TaskPurgeInterval > 0 {
		go r.runPeriodic(cleanupCtx, "task_purge", s.config.TaskPurgeInterval, func(ctx context.Context) {
			result := s.PurgeTasks(ctx)
			if result.Error != nil {
				logging.Logger.Error().Err(result.Error).Msg("task purge error")
			} else if result.ItemCount > 0 {
				logging.Logger.Info().Str("details", result.Details).Msg("task purge completed")
			}
		})
	}

	// Start reconciliation if enabled
	if s.config.ReconciliationInterval > 0 {
		go r.runPeriodic(cleanupCtx, "reconciliation", s.config.ReconciliationInterval, func(ctx context.Context) {
			// First clean up stale locks
			lockResult := s.CleanupStaleLocks(ctx)
			if lockResult.Error != nil {
				logging.Logger.Error().Err(lockResult.Error).Msg("stale lock cleanup error")
			} else if lockResult.ItemCount > 0 {
				logging.Logger.Info().Str("details", lockResult.Details).Msg("stale lock cleanup completed")
			}

			// Recover stale claims
			claimResult := s.CleanupStaleClaims(ctx)
			if claimResult.Error != nil {
				logging.Logger.Error().Err(claimResult.Error).Msg("stale claim recovery error")
			} else if claimResult.ItemCount > 0 {
				logging.Logger.Info().Str("details", claimResult.Details).Msg("stale claim recovery completed")
			}

			// Then reconcile orphaned tasks
			taskResult := s.ReconcileOrphanedTasks(ctx)
			if taskResult.Error != nil {
				logging.Logger.Error().Err(taskResult.Error).Msg("orphaned task reconciliation error")
			} else if taskResult.ItemCount > 0 {
				logging.Logger.Info().Str("details", taskResult.Details).Msg("orphaned task reconciliation completed")
			}
		})
	}
}

// IsLeader returns whether this runner currently holds the cleanup leader lock
func (r *BackgroundRunner) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeader
}

// setLeader updates the leader status. When losing leadership, cancels any
// running cleanup goroutines so they do not continue as stale workers.
func (r *BackgroundRunner) setLeader(leader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.isLeader = leader
	if !leader && r.cleanupCancel != nil {
		r.cleanupCancel()
		r.cleanupCancel = nil
	}
}

// runPeriodic runs a job periodically until context is cancelled
func (r *BackgroundRunner) runPeriodic(ctx context.Context, name string, interval time.Duration, job func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job(ctx)
		}
	}
}

// Stop stops all background cleanup jobs and releases leadership if held
func (r *BackgroundRunner) Stop() {
	// Release leadership lock if we hold it
	if r.IsLeader() && r.service.sessionRegistry != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.service.sessionRegistry.ReleaseLock(ctx, r.identity, r.sessionID); err != nil {
			logging.Logger.Error().Err(err).Msg("error releasing leadership lock")
		} else {
			logging.Logger.Info().Msg("released cleanup leadership")
		}
		r.setLeader(false)
	}

	// Stop any running cleanup goroutines.
	r.mu.Lock()
	if r.cleanupCancel != nil {
		r.cleanupCancel()
		r.cleanupCancel = nil
	}
	r.mu.Unlock()

	if r.cancelFunc != nil {
		r.cancelFunc()
	}
}

// RunStartupJobs runs cleanup jobs that should run once at startup.
// This includes stale lock cleanup and orphaned task reconciliation.
func (s *Service) RunStartupJobs(ctx context.Context) {
	// Clean up stale locks first
	lockResult := s.CleanupStaleLocks(ctx)
	if lockResult.Error != nil {
		logging.Logger.Error().Err(lockResult.Error).Msg("startup stale lock cleanup error")
	} else if lockResult.ItemCount > 0 {
		logging.Logger.Info().Str("details", lockResult.Details).Msg("startup stale lock cleanup")
	}

	// Recover stale claims (gateway crashes that left tasks claimed)
	claimResult := s.CleanupStaleClaims(ctx)
	if claimResult.Error != nil {
		logging.Logger.Error().Err(claimResult.Error).Msg("startup stale claim recovery error")
	} else if claimResult.ItemCount > 0 {
		logging.Logger.Info().Str("details", claimResult.Details).Msg("startup stale claim recovery")
	}

	// Then reconcile orphaned tasks
	taskResult := s.ReconcileOrphanedTasks(ctx)
	if taskResult.Error != nil {
		logging.Logger.Error().Err(taskResult.Error).Msg("startup reconciliation error")
	} else if taskResult.ItemCount > 0 {
		logging.Logger.Info().Str("details", taskResult.Details).Msg("startup reconciliation")
	} else {
		logging.Logger.Debug().Str("details", taskResult.Details).Msg("startup reconciliation")
	}
}
