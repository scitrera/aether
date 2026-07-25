package cleanup

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	taskstore "github.com/scitrera/aether/server/internal/storage/tasks"
	taskssqlite "github.com/scitrera/aether/server/internal/storage/tasks/sqlite"

	// Register the bare "sqlite" driver so the reconcile-job wiring test can run
	// against the native sqlite store (always available, never skipped).
	_ "modernc.org/sqlite"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	// Verify default values
	if config.TaskPurgeInterval != 24*time.Hour {
		t.Errorf("TaskPurgeInterval = %v, want %v", config.TaskPurgeInterval, 24*time.Hour)
	}
	if config.CompletedTaskRetention != 7*24*time.Hour {
		t.Errorf("CompletedTaskRetention = %v, want %v", config.CompletedTaskRetention, 7*24*time.Hour)
	}
	if config.FailedTaskRetention != 14*24*time.Hour {
		t.Errorf("FailedTaskRetention = %v, want %v", config.FailedTaskRetention, 14*24*time.Hour)
	}
	if config.CancelledTaskRetention != 7*24*time.Hour {
		t.Errorf("CancelledTaskRetention = %v, want %v", config.CancelledTaskRetention, 7*24*time.Hour)
	}
	if config.ReconciliationInterval != 1*time.Minute {
		t.Errorf("ReconciliationInterval = %v, want %v", config.ReconciliationInterval, 1*time.Minute)
	}
	if config.StartupTaskTTL != 30*time.Minute {
		t.Errorf("StartupTaskTTL = %v, want %v", config.StartupTaskTTL, 30*time.Minute)
	}
	if config.StartupTaskCancelInterval != 15*time.Minute {
		t.Errorf("StartupTaskCancelInterval = %v, want %v", config.StartupTaskCancelInterval, 15*time.Minute)
	}
	if config.PoolTaskTTL != 1*time.Hour {
		t.Errorf("PoolTaskTTL = %v, want %v", config.PoolTaskTTL, 1*time.Hour)
	}
	if config.PoolTaskCancelInterval != 15*time.Minute {
		t.Errorf("PoolTaskCancelInterval = %v, want %v", config.PoolTaskCancelInterval, 15*time.Minute)
	}
	if config.StaleClaimTimeout != 5*time.Minute {
		t.Errorf("StaleClaimTimeout = %v, want %v", config.StaleClaimTimeout, 5*time.Minute)
	}
	if config.LeaderElectionRetryInterval != 30*time.Second {
		t.Errorf("LeaderElectionRetryInterval = %v, want %v", config.LeaderElectionRetryInterval, 30*time.Second)
	}
}

func TestNewService(t *testing.T) {
	t.Run("WithNilConfig", func(t *testing.T) {
		service := NewService(nil, nil, nil, nil)
		if service == nil {
			t.Fatal("NewService() returned nil")
		}
		// Should use default config
		if service.config == nil {
			t.Error("Service config should not be nil")
		}
		if service.config.TaskPurgeInterval != 24*time.Hour {
			t.Error("Service should use default config when nil is passed")
		}
	})

	t.Run("WithCustomConfig", func(t *testing.T) {
		config := &Config{
			TaskPurgeInterval:      1 * time.Hour,
			CompletedTaskRetention: 1 * time.Hour,
			FailedTaskRetention:    2 * time.Hour,
			CancelledTaskRetention: 3 * time.Hour,
			ReconciliationInterval: 5 * time.Minute,
		}
		service := NewService(nil, nil, nil, config)
		if service == nil {
			t.Fatal("NewService() returned nil")
		}
		if service.config.TaskPurgeInterval != 1*time.Hour {
			t.Error("Service should use provided config")
		}
	})
}

func TestJobResult(t *testing.T) {
	result := JobResult{
		JobName:   "test_job",
		Success:   true,
		Error:     nil,
		Details:   "test details",
		Duration:  100 * time.Millisecond,
		ItemCount: 42,
	}

	if result.JobName != "test_job" {
		t.Errorf("JobName = %q, want %q", result.JobName, "test_job")
	}
	if !result.Success {
		t.Error("Success = false, want true")
	}
	if result.Error != nil {
		t.Errorf("Error = %v, want nil", result.Error)
	}
	if result.Details != "test details" {
		t.Errorf("Details = %q, want %q", result.Details, "test details")
	}
	if result.Duration != 100*time.Millisecond {
		t.Errorf("Duration = %v, want %v", result.Duration, 100*time.Millisecond)
	}
	if result.ItemCount != 42 {
		t.Errorf("ItemCount = %d, want 42", result.ItemCount)
	}
}

func TestCleanupStaleLocks_NilRegistry(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	ctx := context.Background()

	result := service.CleanupStaleLocks(ctx)

	if result.JobName != "stale_lock_cleanup" {
		t.Errorf("JobName = %q, want %q", result.JobName, "stale_lock_cleanup")
	}
	if !result.Success {
		t.Error("Success should be true when registry is nil (graceful skip)")
	}
	if result.Error != nil {
		t.Errorf("Error should be nil when registry is nil, got: %v", result.Error)
	}
}

func TestReconcileOrphanedTasks_NilService(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	ctx := context.Background()

	result := service.ReconcileOrphanedTasks(ctx)

	if result.JobName != "orphaned_task_reconciliation" {
		t.Errorf("JobName = %q, want %q", result.JobName, "orphaned_task_reconciliation")
	}
	if result.Success {
		t.Error("Success should be false when task service is nil")
	}
	if result.Error == nil {
		t.Error("Error should not be nil when task service is nil")
	}
}

func TestPurgeTasks_NilStore(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	ctx := context.Background()

	result := service.PurgeTasks(ctx)

	if result.JobName != "task_purge" {
		t.Errorf("JobName = %q, want %q", result.JobName, "task_purge")
	}
	if result.Success {
		t.Error("Success should be false when task store is nil")
	}
	if result.Error == nil {
		t.Error("Error should not be nil when task store is nil")
	}
}

func TestRunAllJobs_NilDependencies(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	ctx := context.Background()

	results := service.RunAllJobs(ctx)

	// Should return 9 results (stale locks, stale claims, orphaned tasks,
	// orphaned queue entries, interactive-task TTL cancel, startup-task TTL
	// cancel, pool-task TTL cancel, task purge, audit-log cleanup)
	if len(results) != 9 {
		t.Fatalf("RunAllJobs() returned %d results, want 9", len(results))
	}

	// All should fail or skip due to nil dependencies
	for _, result := range results {
		// These jobs gracefully skip when their dependency is nil
		if result.JobName == "stale_claim_recovery" || result.JobName == "stale_lock_cleanup" || result.JobName == "audit_log_cleanup" {
			if !result.Success {
				t.Errorf("Job %q should succeed (skip) when dependencies are nil", result.JobName)
			}
			continue
		}
		if result.Success {
			t.Errorf("Job %q should fail when dependencies are nil", result.JobName)
		}
		if result.Error == nil {
			t.Errorf("Job %q should have an error when dependencies are nil", result.JobName)
		}
	}
}

func TestCleanupStaleClaims_NilDispatcher(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	ctx := context.Background()

	result := service.CleanupStaleClaims(ctx)

	if result.JobName != "stale_claim_recovery" {
		t.Errorf("JobName = %q, want %q", result.JobName, "stale_claim_recovery")
	}
	if !result.Success {
		t.Error("Success should be true (skip) when dispatcher is nil")
	}
	if result.Error != nil {
		t.Errorf("Error should be nil when dispatcher is nil, got %v", result.Error)
	}
}

func TestBackgroundRunner(t *testing.T) {
	t.Run("IsLeader", func(t *testing.T) {
		runner := &BackgroundRunner{
			isLeader: false,
		}

		if runner.IsLeader() {
			t.Error("IsLeader() should return false initially")
		}

		runner.setLeader(true)

		if !runner.IsLeader() {
			t.Error("IsLeader() should return true after setLeader(true)")
		}

		runner.setLeader(false)

		if runner.IsLeader() {
			t.Error("IsLeader() should return false after setLeader(false)")
		}
	})
}

func TestStartBackground_NilRegistry(t *testing.T) {
	// Without a session registry, cleanup runs without leader election
	service := NewService(nil, nil, nil, &Config{
		TaskPurgeInterval:      0, // Disabled
		ReconciliationInterval: 0, // Disabled
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := service.StartBackground(ctx)
	if runner == nil {
		t.Fatal("StartBackground() returned nil")
	}

	// Stop immediately
	runner.Stop()
}

func TestStop_WithoutLeadership(t *testing.T) {
	service := NewService(nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := service.StartBackground(ctx)

	// Should not panic
	runner.Stop()
}

func TestConfig_ZeroValues(t *testing.T) {
	config := &Config{
		TaskPurgeInterval:           0, // Disabled
		CompletedTaskRetention:      0,
		FailedTaskRetention:         0,
		CancelledTaskRetention:      0,
		ReconciliationInterval:      0, // Disabled
		LeaderElectionRetryInterval: 0, // Should use default
	}

	service := NewService(nil, nil, nil, config)

	// Should not panic with zero values
	if service.config.TaskPurgeInterval != 0 {
		t.Error("TaskPurgeInterval should be 0")
	}
	if service.config.ReconciliationInterval != 0 {
		t.Error("ReconciliationInterval should be 0")
	}
}

// TestReconcileOrphanedQueueEntries_Job verifies the cleanup-service wiring:
// the ReconcileOrphanedQueueEntries JobResult retires orphaned queue rows via
// the store and reports the count. Runs against the native sqlite store.
func TestReconcileOrphanedQueueEntries_Job(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cleanup_tasks.db")
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := taskssqlite.New(db)
	if err != nil {
		t.Fatalf("taskssqlite.New: %v", err)
	}
	ctx := context.Background()

	// One cancelled (terminal) task with a pending queue row -> orphaned.
	termID := uuid.New().String()
	if err := store.CreateTask(ctx, &taskstore.Task{TaskID: termID, TaskType: "agent_startup", Workspace: "ws"}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := store.CancelTask(ctx, termID); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	if err := store.InsertQueueEntry(ctx, uuid.New().String(), termID, "impl", "ws", "local", nil, int(taskstore.PriorityNormal)); err != nil {
		t.Fatalf("InsertQueueEntry: %v", err)
	}

	service := NewService(store, nil, nil, nil)
	result := service.ReconcileOrphanedQueueEntries(ctx)

	if result.JobName != "orphaned_queue_entry_reconciliation" {
		t.Errorf("JobName = %q, want %q", result.JobName, "orphaned_queue_entry_reconciliation")
	}
	if !result.Success {
		t.Errorf("Success = false, err = %v", result.Error)
	}
	if result.ItemCount != 1 {
		t.Errorf("ItemCount = %d, want 1", result.ItemCount)
	}
}

// TestReconcileOrphanedQueueEntries_NilTaskStore guards the nil-store branch.
func TestReconcileOrphanedQueueEntries_NilTaskStore(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	result := service.ReconcileOrphanedQueueEntries(context.Background())
	if result.Success {
		t.Error("Success should be false when task store is nil")
	}
	if result.Error == nil {
		t.Error("Error should not be nil when task store is nil")
	}
}
