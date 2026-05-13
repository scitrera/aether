package timer

import (
	"context"
	"database/sql"
	"strings"

	"github.com/scitrera/aether/internal/logging"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// TimerPoller provides distributed timer polling using PostgreSQL NOTIFY.
// This enables low-latency timer firing across multiple gateway instances.
type TimerPoller struct {
	db           *sql.DB
	pollInterval time.Duration
	listener     *pq.Listener
	onTimerFire  func(timerID, taskID, timerType string)
	stopCh       chan struct{}
	wg           sync.WaitGroup

	// Instance ID for coordination
	instanceID string

	mu      sync.RWMutex
	active  bool
	running bool
}

// NewTimerPoller creates a new distributed timer poller.
// connStr is the PostgreSQL connection string for the pq.Listener (required for NOTIFY).
// If connStr is empty, NOTIFY-based instant notifications are disabled and only polling is used.
func NewTimerPoller(db *sql.DB, connStr string, pollInterval time.Duration, onTimerFire func(timerID, taskID, timerType string)) *TimerPoller {
	var listener *pq.Listener

	if connStr != "" {
		// Create PostgreSQL listener for instant notification
		listener = pq.NewListener(
			connStr,
			10*time.Second, // min reconnect interval
			time.Minute,    // reconnect interval
			func(ev pq.ListenerEventType, err error) {
				if err != nil {
					logging.Logger.Error().Err(err).Msg("timer poller listener error")
				}
			},
		)
	} else {
		logging.Logger.Info().Msg("timer poller: no connection string provided, NOTIFY disabled (polling only)")
	}

	return &TimerPoller{
		db:           db,
		pollInterval: pollInterval,
		listener:     listener,
		onTimerFire:  onTimerFire,
		stopCh:       make(chan struct{}),
		instanceID:   uuid.New().String()[:8],
	}
}

// Start begins the timer polling loop.
func (tp *TimerPoller) Start(ctx context.Context) error {
	tp.mu.Lock()
	if tp.running {
		tp.mu.Unlock()
		return nil
	}
	tp.running = true
	tp.active = true
	tp.mu.Unlock()

	// Listen for timer notifications (if listener is available)
	if tp.listener != nil {
		if err := tp.listener.Listen("task_timer_inserted"); err != nil {
			return err
		}
		if err := tp.listener.Listen("task_created"); err != nil {
			return err
		}
		logging.Logger.Info().Str("instance_id", tp.instanceID).Msg("timer poller started with NOTIFY")
	} else {
		logging.Logger.Info().Str("instance_id", tp.instanceID).Msg("timer poller started (polling only)")
	}

	// Start polling loop
	tp.wg.Add(1)
	go tp.run(ctx)

	return nil
}

// Stop gracefully shuts down the timer poller.
func (tp *TimerPoller) Stop() {
	tp.mu.Lock()
	if !tp.running {
		tp.mu.Unlock()
		return
	}
	tp.running = false
	tp.active = false
	tp.mu.Unlock()

	close(tp.stopCh)
	tp.wg.Wait()

	if tp.listener != nil {
		tp.listener.Close()
	}

	logging.Logger.Info().Str("instance_id", tp.instanceID).Msg("timer poller stopped")
}

// IsActive returns whether the poller is currently active.
func (tp *TimerPoller) IsActive() bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.active
}

// run is the main polling loop.
func (tp *TimerPoller) run(ctx context.Context) {
	defer tp.wg.Done()

	// Use a ticker for periodic polling as backup
	ticker := time.NewTicker(tp.pollInterval)
	defer ticker.Stop()

	// Get notify channel (may be nil if listener is nil)
	var notifyCh <-chan *pq.Notification
	if tp.listener != nil {
		notifyCh = tp.listener.Notify
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tp.stopCh:
			return
		case notification := <-notifyCh:
			if notification != nil {
				tp.handleNotification(notification)
			}
		case <-ticker.C:
			tp.pollTimers(ctx)
		}
	}
}

// handleNotification processes a PostgreSQL NOTIFY event.
func (tp *TimerPoller) handleNotification(notification *pq.Notification) {
	if notification == nil {
		return
	}

	// Parse the notification payload: timer_id:task_id:timer_type
	parts := strings.SplitN(notification.Extra, ":", 3)
	if len(parts) < 3 {
		logging.Logger.Warn().Str("payload", notification.Extra).Msg("invalid timer notification payload")
		return
	}

	timerID := parts[0]
	taskID := parts[1]
	timerType := parts[2]

	logging.Logger.Info().Str("timer_id", timerID).Str("task_id", taskID).Str("type", timerType).Msg("received NOTIFY for timer")

	// Fire the timer callback
	if tp.onTimerFire != nil {
		tp.onTimerFire(timerID, taskID, timerType)
	}
}

// pollTimers polls the database for timers that need to fire.
func (tp *TimerPoller) pollTimers(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	// Find timers that have fired and update them atomically
	query := `
		UPDATE task_timers
		SET fired = true, fired_at = NOW()
		WHERE fires_at <= NOW()
		  AND NOT fired
		  AND timer_type != 'retry'  -- Retry timers are handled separately
		RETURNING timer_id, task_id, timer_type
	`

	rows, err := tp.db.QueryContext(ctx, query)
	if err != nil {
		if ctx.Err() == nil {
			logging.Logger.Error().Err(err).Msg("failed to poll timers")
		}
		return
	}
	defer rows.Close()

	firedCount := 0
	for rows.Next() {
		var timerID, taskID, timerType string
		if err := rows.Scan(&timerID, &taskID, &timerType); err != nil {
			logging.Logger.Error().Err(err).Msg("failed to scan timer row")
			continue
		}

		firedCount++

		logging.Logger.Info().Str("timer_id", timerID).Str("task_id", taskID).Str("type", timerType).Msg("fired timer")

		// Fire the callback
		if tp.onTimerFire != nil {
			tp.onTimerFire(timerID, taskID, timerType)
		}
	}

	if err := rows.Err(); err != nil {
		logging.Logger.Error().Err(err).Msg("error iterating timer rows")
	}

	if firedCount > 0 {
		logging.Logger.Info().Int("count", firedCount).Str("instance_id", tp.instanceID).Msg("polled timers")
	}
}

// GetStats returns polling statistics.
func (tp *TimerPoller) GetStats() (pendingCount, activeCount int, err error) {
	// Count pending (not fired) timers
	query := `SELECT COUNT(*) FROM task_timers WHERE NOT fired`
	err = tp.db.QueryRow(query).Scan(&pendingCount)
	if err != nil {
		return 0, 0, err
	}

	// Count pending retry timers specifically
	query = `SELECT COUNT(*) FROM task_timers WHERE NOT fired AND timer_type = 'retry'`
	err = tp.db.QueryRow(query).Scan(&activeCount)
	if err != nil {
		return 0, 0, err
	}

	return pendingCount, activeCount, nil
}
