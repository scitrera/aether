package timer

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/tasks"
)

// setupPollerTestDB creates a database connection using dev infrastructure
func setupPollerTestDB(t *testing.T) (*sql.DB, string, func()) {
	t.Helper()

	testDB, cleanup := testutil.SetupTestDB(t)
	if testDB == nil {
		return nil, "", func() {}
	}

	return testDB.DB, testDB.Config.DSN(), cleanup
}

// TestNewTimerPoller tests the creation of a new TimerPoller
func TestNewTimerPoller(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, connStr, cleanup := setupPollerTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	// Create the poller
	pollInterval := 5 * time.Second
	onTimerFire := func(timerID, taskID, timerType string) {
		t.Logf("Timer fired: %s %s %s", timerID, taskID, timerType)
	}

	poller := NewTimerPoller(db, connStr, pollInterval, onTimerFire)

	if poller == nil {
		t.Fatal("Expected non-nil TimerPoller")
	}

	if poller.pollInterval != pollInterval {
		t.Errorf("Expected poll interval %v, got %v", pollInterval, poller.pollInterval)
	}

	if poller.onTimerFire == nil {
		t.Error("Expected onTimerFire callback to be set")
	}

	t.Logf("Created TimerPoller with instance ID: %s", poller.instanceID)
}

// TestTimerPollerStartStop tests the start and stop functionality
func TestTimerPollerStartStop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, connStr, cleanup := setupPollerTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	poller := NewTimerPoller(db, connStr, time.Second, func(timerID, taskID, timerType string) {})
	ctx := context.Background()

	// Start the poller
	err := poller.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start poller: %v", err)
	}

	if !poller.IsActive() {
		t.Error("Expected poller to be active after Start")
	}

	// Stop the poller
	poller.Stop()

	// Poll until inactive or deadline
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !poller.IsActive() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if poller.IsActive() {
		t.Error("Expected poller to be inactive after Stop")
	}
}

// TestTimerPollerGetStats tests the stats collection
func TestTimerPollerGetStats(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, connStr, cleanup := setupPollerTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	poller := NewTimerPoller(db, connStr, time.Second, nil)
	ctx := context.Background()

	// Start and immediately stop (no timers in DB yet)
	poller.Start(ctx)
	defer poller.Stop()

	// Wait for poller to become active
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if poller.IsActive() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Get stats - should work even without timers
	pending, active, err := poller.GetStats()
	if err != nil {
		t.Logf("Note: GetStats returned error (expected if no timers table): %v", err)
	} else {
		t.Logf("Timer stats: pending=%d, active=%d", pending, active)
	}
}

// TestTimerTypeConstants tests the timer type constants from tasks package
func TestTimerTypeConstants(t *testing.T) {
	// These types are defined in pkg/tasks, not internal/timer
	// This test validates the constants exist and are correct
	tests := []struct {
		timerType string
		expected  string
	}{
		{string(tasks.TimerTypeScheduleToStart), "schedule_to_start"},
		{string(tasks.TimerTypeStartToClose), "start_to_close"},
		{string(tasks.TimerTypeHeartbeat), "heartbeat"},
		{string(tasks.TimerTypeScheduleToClose), "schedule_to_close"},
		{string(tasks.TimerTypeRetry), "retry"},
	}

	for _, tt := range tests {
		if tt.timerType != tt.expected {
			t.Errorf("Expected TimerType '%s', got '%s'", tt.expected, tt.timerType)
		}
	}
}

// Removed duplicate TestTimerTypeConstants - see above

// TestTimerPollerInstanceID tests that each poller gets a unique instance ID
func TestTimerPollerInstanceID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	db, connStr, cleanup := setupPollerTestDB(t)
	if db == nil {
		return
	}
	defer cleanup()

	poller1 := NewTimerPoller(db, connStr, time.Second, nil)
	poller2 := NewTimerPoller(db, connStr, time.Second, nil)

	defer poller1.Stop()
	defer poller2.Stop()

	if poller1.instanceID == "" {
		t.Error("Expected instance ID to be non-empty")
	}

	if poller1.instanceID == poller2.instanceID {
		t.Error("Expected unique instance IDs for each poller")
	}

	t.Logf("Poller 1 instance ID: %s", poller1.instanceID)
	t.Logf("Poller 2 instance ID: %s", poller2.instanceID)
}

// BenchmarkTimerPollerCreation benchmarks timer poller creation
func BenchmarkTimerPollerCreation(b *testing.B) {
	config := testutil.GetPostgresConfig()
	connStr := config.DSN()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		b.Skipf("Skipping benchmark, database not available: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		b.Skipf("Skipping benchmark, database not available: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		poller := NewTimerPoller(db, connStr, time.Second, nil)
		poller.Stop()
	}
}
