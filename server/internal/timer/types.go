package timer

import (
	"time"

	"github.com/scitrera/aether/pkg/tasks"
)

// TimeoutType is an alias for tasks.TimerType so the DB snake_case values are used consistently.
type TimeoutType = tasks.TimerType

const (
	// TimeoutScheduleToStart is the maximum time a task can wait before being claimed by a worker
	TimeoutScheduleToStart TimeoutType = tasks.TimerTypeScheduleToStart
	// TimeoutStartToClose is the maximum time a worker can spend executing a task
	TimeoutStartToClose TimeoutType = tasks.TimerTypeStartToClose
	// TimeoutHeartbeat is the maximum time between heartbeat updates from a worker
	TimeoutHeartbeat TimeoutType = tasks.TimerTypeHeartbeat
	// TimeoutScheduleToClose is the overall maximum time from task creation to completion
	TimeoutScheduleToClose TimeoutType = tasks.TimerTypeScheduleToClose
)

// TimeoutConfig holds the timeout duration configuration for a task
type TimeoutConfig struct {
	ScheduleToStart time.Duration
	StartToClose    time.Duration
	Heartbeat       time.Duration
	ScheduleToClose time.Duration
}

// Timer represents a single timeout timer
type Timer struct {
	Type      TimeoutType
	Expiry    time.Time
	TaskID    string
	Cancel    func()
	CreatedAt time.Time
}

// TimeoutInfo holds timeout state for a specific task
type TimeoutInfo struct {
	TaskID        string
	Timeouts      map[TimeoutType]*Timer
	State         string // Current task state for determining which timers are active
	CreatedAt     time.Time
	StartedAt     *time.Time
	LastHeartbeat *time.Time
}
