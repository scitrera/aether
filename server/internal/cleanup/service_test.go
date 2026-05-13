package cleanup

import (
	"context"
	"testing"
	"time"
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

	// Should return 4 results (stale locks, stale claims, orphaned tasks, task purge)
	if len(results) != 4 {
		t.Fatalf("RunAllJobs() returned %d results, want 4", len(results))
	}

	// All should fail or skip due to nil dependencies
	for _, result := range results {
		// These jobs gracefully skip when their dependency is nil
		if result.JobName == "stale_claim_recovery" || result.JobName == "stale_lock_cleanup" {
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
