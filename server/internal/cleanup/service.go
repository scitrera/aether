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
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/server/internal/logging"
	"github.com/scitrera/aether/server/internal/orchestration"
	"github.com/scitrera/aether/server/internal/state"
	taskstore "github.com/scitrera/aether/server/internal/storage/tasks"
	"github.com/scitrera/aether/server/pkg/models"
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

	// Orphaned queue-entry reconciliation settings
	QueueReconcileInterval time.Duration // How often to retire orphaned orchestrated_task_queue rows whose task is terminal/missing (0 = disabled)

	// Stale interactive task settings
	InteractiveTaskTTL            time.Duration // How old a non-terminal INTERACTIVE (chat turn) task may get before being auto-cancelled
	InteractiveTaskCancelInterval time.Duration // How often to run the stale interactive task sweep (0 = disabled)

	// Stale startup task settings
	StartupTaskTTL            time.Duration // How old an UNCLAIMED (pending) agent_startup task may get before being auto-cancelled (generous, so a briefly-unavailable orchestrator's pending task is never reaped)
	StartupTaskCancelInterval time.Duration // How often to run the stale startup task sweep (0 = disabled)

	// Stale pool task settings
	PoolTaskTTL            time.Duration // How old an UNCLAIMED (pending) regular POOL task may get before being auto-cancelled (generous, so a briefly-absent worker's pending task is never reaped)
	PoolTaskCancelInterval time.Duration // How often to run the stale pool task sweep (0 = disabled)

	// Audit-log retention settings
	AuditRetentionDays   int           // Delete comprehensive_audit_log rows older than this many days (default 90)
	AuditCleanupInterval time.Duration // How often to run the scheduled audit-log retention sweep (0 = disabled)

	// Stale claim settings
	StaleClaimTimeout time.Duration // How long a task can stay 'claimed' before being recovered (0 = use default 5m)

	// Leader election settings
	LeaderElectionRetryInterval time.Duration // How often to retry acquiring leadership if not leader (0 = use default 30s)

	// SingleNode indicates a single-node/standalone deployment (aetherlite
	// standalone / polling-dispatcher path). When true, StartBackground runs the
	// periodic sweeps DIRECTLY without gating on distributed leader election —
	// there is only one node, so election adds no safety and only risk: a
	// stuck/foreign leader lease (any AcquireLock returning false) would silently
	// starve every sweep with no recovery and no log signal. Clustered backends
	// (Redis / JetStream) leave this false and keep lease-based election so
	// exactly one node sweeps.
	SingleNode bool
}

// DefaultConfig returns the default cleanup configuration
func DefaultConfig() *Config {
	return &Config{
		TaskPurgeInterval:           24 * time.Hour, // Daily
		CompletedTaskRetention:      7 * 24 * time.Hour,
		FailedTaskRetention:         14 * 24 * time.Hour,
		CancelledTaskRetention:      7 * 24 * time.Hour,
		ReconciliationInterval:        1 * time.Minute,
		QueueReconcileInterval:        5 * time.Minute,
		InteractiveTaskTTL:            1 * time.Hour,
		InteractiveTaskCancelInterval: 15 * time.Minute,
		StartupTaskTTL:                30 * time.Minute,
		StartupTaskCancelInterval:     15 * time.Minute,
		PoolTaskTTL:                   1 * time.Hour,
		PoolTaskCancelInterval:        15 * time.Minute,
		AuditRetentionDays:            90,
		AuditCleanupInterval:          24 * time.Hour,
		StaleClaimTimeout:             5 * time.Minute,
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

// AuditStore is the narrow surface the cleanup service needs to enforce audit-
// log retention: a single delete-old-rows call. The audit domain Store
// (internal/storage/audit) satisfies this across all three backends (sqlite,
// postgres, jetstream). Declared as a local interface — mirroring the
// SessionRegistry pattern above — so the cleanup package does not take a hard
// dependency on the audit storage package.
type AuditStore interface {
	// CleanupOldLogs deletes audit rows older than retentionDays and returns
	// the number of rows removed.
	CleanupOldLogs(ctx context.Context, retentionDays int) (int64, error)
}

// Service provides cleanup operations for the gateway.
// It can be used for both background goroutines and standalone cleanup commands.
type Service struct {
	// taskStore is the tasks domain Store (internal/storage/tasks).
	taskStore       taskstore.Store
	taskService     *orchestration.TaskAssignmentService
	dispatcher      orchestration.TaskDispatcher
	sessionRegistry SessionRegistry
	auditStore      AuditStore
	config          *Config
}

// NewService creates a new cleanup service
func NewService(
	taskStore taskstore.Store,
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

// SetAuditStore sets the audit store used by the scheduled audit-retention job.
// Passing nil leaves audit cleanup disabled (the job skips with success). A
// setter (rather than a NewService param) keeps the many existing NewService
// call sites unchanged.
func (s *Service) SetAuditStore(auditStore AuditStore) {
	s.auditStore = auditStore
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

	// Run orphaned orchestrated_task_queue reconciliation
	result = s.ReconcileOrphanedQueueEntries(ctx)
	results = append(results, result)

	// Run stale interactive task cancellation
	result = s.CancelStaleInteractiveTasks(ctx)
	results = append(results, result)

	// Run stale startup task cancellation
	result = s.CancelStaleStartupTasks(ctx)
	results = append(results, result)

	// Run stale pool task cancellation
	result = s.CancelStalePoolTasks(ctx)
	results = append(results, result)

	// Run task purge
	result = s.PurgeTasks(ctx)
	results = append(results, result)

	// Run audit-log retention cleanup
	result = s.CleanupOldAuditLogs(ctx)
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

// ReconcileOrphanedQueueEntries retires orchestrated_task_queue rows that were
// orphaned when their task went terminal (or was purged) without the
// best-effort, dispatcher-gated retire firing. Without this sweep such rows sit
// pending forever and the polling dispatcher polls the ghosts every tick. The
// store method is idempotent and a no-op in clustered/JetStream mode (no SQL
// queue rows), so this job is safe on every backend.
func (s *Service) ReconcileOrphanedQueueEntries(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "orphaned_queue_entry_reconciliation"}

	if s.taskStore == nil {
		result.Error = fmt.Errorf("task store not configured")
		return result
	}

	count, err := s.taskStore.ReconcileOrphanedQueueEntries(ctx)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = count
	if count > 0 {
		result.Details = fmt.Sprintf("retired %d orphaned queue entries", count)
	} else {
		result.Details = "no orphaned queue entries found"
	}

	return result
}

// CancelStaleInteractiveTasks cancels non-terminal INTERACTIVE (chat turn)
// tasks older than the configured TTL. These are foreground turns that never
// reached a terminal state (dead/offline sandbox at mint, crashed harness) and
// would otherwise linger forever.
func (s *Service) CancelStaleInteractiveTasks(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "interactive_task_ttl_cancel"}

	if s.taskService == nil {
		result.Error = fmt.Errorf("task service not configured")
		return result
	}

	count, err := s.taskService.CancelStaleInteractiveTasks(ctx, s.config.InteractiveTaskTTL)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = int64(count)
	if count > 0 {
		result.Details = fmt.Sprintf("cancelled %d stale interactive tasks (ttl: %v)", count, s.config.InteractiveTaskTTL)
	} else {
		result.Details = "no stale interactive tasks found"
	}

	return result
}

// CancelStaleStartupTasks cancels UNCLAIMED (pending) agent_startup orchestration
// tasks older than the configured TTL. These are pool tasks minted to spin up an
// offline agent that no orchestrator ever claimed; cancelling them is
// self-healing (the next message re-triggers a fresh startup task) and the TTL
// is generous so a briefly-unavailable orchestrator's pending task is never
// reaped out from under it.
func (s *Service) CancelStaleStartupTasks(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "startup_task_ttl_cancel"}

	if s.taskService == nil {
		result.Error = fmt.Errorf("task service not configured")
		return result
	}

	count, err := s.taskService.CancelStaleStartupTasks(ctx, s.config.StartupTaskTTL)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = int64(count)
	if count > 0 {
		result.Details = fmt.Sprintf("cancelled %d stale startup tasks (ttl: %v)", count, s.config.StartupTaskTTL)
	} else {
		result.Details = "no stale startup tasks found"
	}

	return result
}

// CancelStalePoolTasks cancels UNCLAIMED (pending) regular POOL orchestration
// tasks older than the configured TTL. These are pool tasks whose target-
// implementation worker never connected to claim them; unlike agent_startup pool
// tasks (covered by CancelStaleStartupTasks) and active INTERACTIVE turns
// (covered by CancelStaleInteractiveTasks), nothing else collects them, so they
// linger forever. The TTL is generous so a briefly-absent worker's pending task
// is never reaped out from under it.
func (s *Service) CancelStalePoolTasks(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "pool_task_ttl_cancel"}

	if s.taskService == nil {
		result.Error = fmt.Errorf("task service not configured")
		return result
	}

	count, err := s.taskService.CancelStalePoolTasks(ctx, s.config.PoolTaskTTL)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = int64(count)
	if count > 0 {
		result.Details = fmt.Sprintf("cancelled %d stale pool tasks (ttl: %v)", count, s.config.PoolTaskTTL)
	} else {
		result.Details = "no stale pool tasks found"
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

// CleanupOldAuditLogs deletes comprehensive_audit_log rows older than the
// configured retention window (default 90 days).
//
// Note on space reclamation: on SQLite (aetherlite) a DELETE frees the pages
// for reuse — this caps growth / plateaus audit.db but does NOT shrink the file
// on disk; reclaiming already-allocated space requires a manual VACUUM. We do
// NOT auto-VACUUM here because VACUUM takes an exclusive lock and rewrites the
// whole DB, which would stall the gateway. With a 90-day window nothing is
// deleted until rows age past 90 days, so on a young database this is a no-op.
func (s *Service) CleanupOldAuditLogs(ctx context.Context) JobResult {
	start := time.Now()
	result := JobResult{JobName: "audit_log_cleanup"}

	// Nil-audit-store guard: audit cleanup is optional wiring. Skip cleanly
	// rather than error so RunAllJobs / periodic runs stay quiet when no audit
	// store is configured.
	if s.auditStore == nil {
		result.Success = true
		result.Details = "skipped (no audit store)"
		return result
	}

	retentionDays := s.config.AuditRetentionDays
	if retentionDays <= 0 {
		retentionDays = 90
	}

	count, err := s.auditStore.CleanupOldLogs(ctx, retentionDays)
	result.Duration = time.Since(start)

	if err != nil {
		result.Error = err
		return result
	}

	result.Success = true
	result.ItemCount = count
	if count > 0 {
		result.Details = fmt.Sprintf("deleted %d audit log rows older than %d days", count, retentionDays)
	} else {
		result.Details = fmt.Sprintf("no audit log rows older than %d days", retentionDays)
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

	// Single-node/standalone deployments (aetherlite standalone / polling-
	// dispatcher path) must ALWAYS run the sweeps. There is exactly one node, so
	// distributed leader election adds no safety and only risk: a stuck/foreign
	// leader lease (any AcquireLock returning false) would silently starve every
	// sweep with no recovery and no log signal (the exact failure reproduced in
	// TestStartBackground_Badger_ForeignLeaderLock_StarvesSweep). Run the
	// periodic jobs directly. Clustered backends fall through to lease-based
	// election below so exactly one node sweeps.
	if s.config.SingleNode {
		logging.Logger.Info().Msg("cleanup: single-node mode — running jobs directly without leader election")
		runner.startCleanupJobs(ctx)
		return runner
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

	// Start stale interactive task cancellation if enabled
	if s.config.InteractiveTaskCancelInterval > 0 {
		go r.runPeriodic(cleanupCtx, "interactive_task_ttl_cancel", s.config.InteractiveTaskCancelInterval, func(ctx context.Context) {
			result := s.CancelStaleInteractiveTasks(ctx)
			if result.Error != nil {
				logging.Logger.Error().Err(result.Error).Msg("stale interactive task cancel error")
			} else if result.ItemCount > 0 {
				logging.Logger.Info().Str("details", result.Details).Msg("stale interactive task cancel completed")
			}
		})
	}

	// Start stale startup task cancellation if enabled
	if s.config.StartupTaskCancelInterval > 0 {
		go r.runPeriodic(cleanupCtx, "startup_task_ttl_cancel", s.config.StartupTaskCancelInterval, func(ctx context.Context) {
			result := s.CancelStaleStartupTasks(ctx)
			if result.Error != nil {
				logging.Logger.Error().Err(result.Error).Msg("stale startup task cancel error")
			} else if result.ItemCount > 0 {
				logging.Logger.Info().Str("details", result.Details).Msg("stale startup task cancel completed")
			}
		})
	}

	// Start stale pool task cancellation if enabled
	if s.config.PoolTaskCancelInterval > 0 {
		go r.runPeriodic(cleanupCtx, "pool_task_ttl_cancel", s.config.PoolTaskCancelInterval, func(ctx context.Context) {
			result := s.CancelStalePoolTasks(ctx)
			if result.Error != nil {
				logging.Logger.Error().Err(result.Error).Msg("stale pool task cancel error")
			} else if result.ItemCount > 0 {
				logging.Logger.Info().Str("details", result.Details).Msg("stale pool task cancel completed")
			}
		})
	}

	// Start scheduled audit-log retention if enabled and an audit store is
	// wired. Runs on the same single-node-direct / leader-gated path as the
	// other sweeps. Backend-agnostic: the store's CleanupOldLogs is implemented
	// for sqlite, postgres, and jetstream.
	if s.config.AuditCleanupInterval > 0 && s.auditStore != nil {
		go r.runPeriodic(cleanupCtx, "audit_log_cleanup", s.config.AuditCleanupInterval, func(ctx context.Context) {
			result := s.CleanupOldAuditLogs(ctx)
			if result.Error != nil {
				logging.Logger.Error().Err(result.Error).Msg("audit log cleanup error")
			} else if result.ItemCount > 0 {
				logging.Logger.Info().Str("details", result.Details).Msg("audit log cleanup completed")
			}
		})
	}

	// Start orphaned queue-entry reconciliation if enabled. This runs under the
	// same single-node-direct / leader-gated path as the other sweeps and is a
	// no-op on clustered/JetStream backends (no SQL orchestrated_task_queue rows).
	if s.config.QueueReconcileInterval > 0 {
		go r.runPeriodic(cleanupCtx, "orphaned_queue_entry_reconciliation", s.config.QueueReconcileInterval, func(ctx context.Context) {
			result := s.ReconcileOrphanedQueueEntries(ctx)
			if result.Error != nil {
				logging.Logger.Error().Err(result.Error).Msg("orphaned queue entry reconciliation error")
			} else if result.ItemCount > 0 {
				logging.Logger.Info().Str("details", result.Details).Msg("orphaned queue entry reconciliation completed")
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

// runPeriodic runs a job periodically until context is cancelled.
//
// Each iteration is isolated behind panic recovery: a single failing sweep is
// logged (with stack) and skipped rather than killing the loop. This matters
// doubly because an unrecovered panic in this background goroutine would
// otherwise crash the whole gateway process. Mirrors the recover()+debug.Stack()
// guard on the background goroutine in gateway.SetCleanupService.
func (r *BackgroundRunner) runPeriodic(ctx context.Context, name string, interval time.Duration, job func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		defer func() {
			if rec := recover(); rec != nil {
				logging.Logger.Error().
					Interface("panic", rec).
					Str("stack", string(debug.Stack())).
					Str("job", name).
					Msg("recovered from panic in cleanup job iteration")
			}
		}()
		job(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
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
