package cleanup

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeAuditStore records the retention arg it was called with and returns a
// canned row count / error. It satisfies the cleanup.AuditStore surface.
type fakeAuditStore struct {
	calledWith int
	callCount  int
	ret        int64
	err        error
}

func (f *fakeAuditStore) CleanupOldLogs(_ context.Context, retentionDays int) (int64, error) {
	f.callCount++
	f.calledWith = retentionDays
	return f.ret, f.err
}

func TestCleanupOldAuditLogs_NilAuditStore(t *testing.T) {
	// No audit store wired → skip cleanly with success (mirrors other
	// nil-dependency skips).
	service := NewService(nil, nil, nil, nil)

	result := service.CleanupOldAuditLogs(context.Background())

	if result.JobName != "audit_log_cleanup" {
		t.Errorf("JobName = %q, want %q", result.JobName, "audit_log_cleanup")
	}
	if !result.Success {
		t.Error("Success should be true (skip) when audit store is nil")
	}
	if result.Error != nil {
		t.Errorf("Error should be nil when audit store is nil, got %v", result.Error)
	}
}

func TestCleanupOldAuditLogs_CallsStoreWithConfiguredRetention(t *testing.T) {
	fake := &fakeAuditStore{ret: 42}
	service := NewService(nil, nil, nil, &Config{AuditRetentionDays: 30})
	service.SetAuditStore(fake)

	result := service.CleanupOldAuditLogs(context.Background())

	if fake.callCount != 1 {
		t.Fatalf("CleanupOldLogs call count = %d, want 1", fake.callCount)
	}
	if fake.calledWith != 30 {
		t.Errorf("CleanupOldLogs retentionDays = %d, want 30", fake.calledWith)
	}
	if !result.Success {
		t.Errorf("Success should be true, got error %v", result.Error)
	}
	if result.ItemCount != 42 {
		t.Errorf("ItemCount = %d, want 42", result.ItemCount)
	}
}

func TestCleanupOldAuditLogs_DefaultsRetentionWhenUnset(t *testing.T) {
	fake := &fakeAuditStore{}
	// Config with zero AuditRetentionDays must fall back to 90.
	service := NewService(nil, nil, nil, &Config{})
	service.SetAuditStore(fake)

	service.CleanupOldAuditLogs(context.Background())

	if fake.calledWith != 90 {
		t.Errorf("CleanupOldLogs retentionDays = %d, want 90 (default)", fake.calledWith)
	}
}

func TestCleanupOldAuditLogs_PropagatesError(t *testing.T) {
	sentinel := errors.New("delete failed")
	fake := &fakeAuditStore{err: sentinel}
	service := NewService(nil, nil, nil, DefaultConfig())
	service.SetAuditStore(fake)

	result := service.CleanupOldAuditLogs(context.Background())

	if result.Success {
		t.Error("Success should be false when the store returns an error")
	}
	if !errors.Is(result.Error, sentinel) {
		t.Errorf("Error = %v, want %v", result.Error, sentinel)
	}
}

func TestDefaultConfig_AuditRetentionDefaults(t *testing.T) {
	config := DefaultConfig()

	if config.AuditRetentionDays != 90 {
		t.Errorf("AuditRetentionDays = %d, want 90", config.AuditRetentionDays)
	}
	if config.AuditCleanupInterval != 24*time.Hour {
		t.Errorf("AuditCleanupInterval = %v, want %v", config.AuditCleanupInterval, 24*time.Hour)
	}
}
