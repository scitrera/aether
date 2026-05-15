// Package postgres provides the postgres-backed implementation of
// tasks.Store. It wraps the legacy *pkg/tasks.TaskStore, forwarding all
// non-transactional methods via embedding. The Stage 2 StoreTx
// abstraction is implemented here: BeginTx returns a pgStoreTx wrapping
// *sql.Tx, and the transactional methods type-assert back to recover it.
//
// The legacy package's RecordAuditEventTx still takes a raw *sql.Tx; this
// wrapper translates between the domain-owned StoreTx and the raw *sql.Tx
// so no changes are needed in pkg/tasks/ itself.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/storage/tasks"
	legacy "github.com/scitrera/aether/pkg/tasks"
)

// Store is the postgres-backed task store. It embeds the legacy
// *pkg/tasks.TaskStore for all non-transactional methods and implements
// the StoreTx surface (BeginTx, RecordAuditEventTx, queue-mutation
// methods) in terms of the underlying *sql.DB.
type Store struct {
	*legacy.TaskStore
	db *sql.DB
}

// New constructs a postgres-backed task Store on top of the given *sql.DB.
// Callers retain ownership of db; the store does not own connection-pool
// lifetime.
func New(db *sql.DB) *Store {
	return &Store{
		TaskStore: legacy.NewTaskStore(db),
		db:        db,
	}
}

// Compile-time conformance assert. This is the load-bearing check that
// tasks.Store and *Store agree on the full method set. If a method is added
// to tasks.Store or its signature changes, the build breaks here.
var _ tasks.Store = (*Store)(nil)

// =========================================================================
// pgStoreTx — postgres-specific StoreTx wrapper
// =========================================================================

// pgStoreTx wraps a *sql.Tx and satisfies tasks.StoreTx.
type pgStoreTx struct {
	tx *sql.Tx
}

func (t *pgStoreTx) Commit() error   { return t.tx.Commit() }
func (t *pgStoreTx) Rollback() error { return t.tx.Rollback() }

// unwrapTx recovers the *sql.Tx from a tasks.StoreTx. Panics if the
// concrete type is not *pgStoreTx — this is intentional: passing a sqlite
// StoreTx to a postgres Store is a programming error, not a runtime
// condition.
func unwrapTx(tx tasks.StoreTx) *sql.Tx {
	ptx, ok := tx.(*pgStoreTx)
	if !ok {
		panic(fmt.Sprintf("postgres.Store: expected *pgStoreTx, got %T", tx))
	}
	return ptx.tx
}

// =========================================================================
// StoreTx lifecycle
// =========================================================================

// BeginTx starts a new database transaction and returns a postgres-specific
// StoreTx wrapper.
func (s *Store) BeginTx(ctx context.Context) (tasks.StoreTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &pgStoreTx{tx: tx}, nil
}

// =========================================================================
// Transactional audit event
// =========================================================================

// RecordAuditEventTx inserts a task_audit_events row inside the given
// StoreTx transaction.
func (s *Store) RecordAuditEventTx(ctx context.Context, tx tasks.StoreTx, event *tasks.TaskAuditEvent) error {
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}
	rawTx := unwrapTx(tx)

	// Delegate to the legacy package's TX method which takes *sql.Tx.
	return s.TaskStore.RecordAuditEventTx(ctx, rawTx, event)
}

// =========================================================================
// Transactional queue operations (orchestrated_task_queue)
// =========================================================================

// QueryQueueEntryForUnclaimTx reads a claimed queue entry within a TX.
func (s *Store) QueryQueueEntryForUnclaimTx(ctx context.Context, tx tasks.StoreTx, queueID string) (taskID, workspace string, retryCount, maxRetries int, err error) {
	rawTx := unwrapTx(tx)
	err = rawTx.QueryRowContext(ctx, `
		SELECT task_id, workspace, retry_count, max_retries
		FROM orchestrated_task_queue
		WHERE queue_id = $1 AND status = $2
	`, queueID, "claimed").Scan(&taskID, &workspace, &retryCount, &maxRetries)
	return
}

// UpdateQueueEntryForRetryTx sets the queue entry back to pending with
// incremented retry count and exponential backoff.
func (s *Store) UpdateQueueEntryForRetryTx(ctx context.Context, tx tasks.StoreTx, queueID string, newRetryCount, backoffSeconds int) error {
	rawTx := unwrapTx(tx)
	_, err := rawTx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $4,
		    claimed_by = NULL,
		    claimed_at = NULL,
		    retry_count = $2,
		    next_retry_at = NOW() + ($3 || ' seconds')::interval
		WHERE queue_id = $1
	`, queueID, newRetryCount, fmt.Sprintf("%d", backoffSeconds), "pending")
	return err
}

// MarkQueueEntryFailedTx marks a queue entry as failed inside a TX.
func (s *Store) MarkQueueEntryFailedTx(ctx context.Context, tx tasks.StoreTx, queueID, errorMsg string) error {
	rawTx := unwrapTx(tx)
	_, err := rawTx.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`, queueID, errorMsg, "failed")
	return err
}

// InsertDLQEntryTx inserts a DLQ row inside a TX.
func (s *Store) InsertDLQEntryTx(ctx context.Context, tx tasks.StoreTx, taskID, workspace, reason string, attemptCount int) error {
	rawTx := unwrapTx(tx)
	_, err := rawTx.ExecContext(ctx, `
		INSERT INTO dlq (original_task_id, category, workspace, failure_reason, attempt_count, last_attempt_at)
		VALUES ($1, 'delivery_failure', $2, $3, $4, NOW())
	`, taskID, workspace, reason, attemptCount)
	return err
}

// =========================================================================
// Non-transactional queue operations (orchestrated_task_queue)
// =========================================================================

func (s *Store) InsertQueueEntry(ctx context.Context, queueID, taskID, targetImplementation, workspace, profile string, launchParamsJSON []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO orchestrated_task_queue
		(queue_id, task_id, target_implementation, workspace, profile, launch_params, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending')
	`, queueID, taskID, targetImplementation, workspace, profile, launchParamsJSON)
	return err
}

func (s *Store) PollPendingQueueEntries(ctx context.Context, limit int) ([]*tasks.QueueEntryNotification, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT queue_id, task_id, profile, workspace, target_implementation
		FROM orchestrated_task_queue
		WHERE status = $1 AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at ASC
		LIMIT $2
	`, "pending", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*tasks.QueueEntryNotification
	for rows.Next() {
		var e tasks.QueueEntryNotification
		if err := rows.Scan(&e.QueueID, &e.TaskID, &e.Profile, &e.Workspace, &e.TargetImplementation); err != nil {
			return nil, err
		}
		entries = append(entries, &e)
	}
	return entries, rows.Err()
}

func (s *Store) CountPendingQueueEntries(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM orchestrated_task_queue WHERE status = $1
	`, "pending").Scan(&count)
	return count, err
}

func (s *Store) ClaimQueueEntry(ctx context.Context, queueID, claimedBy string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $3, claimed_by = $2, claimed_at = NOW()
		WHERE queue_id = $1 AND status = $4
	`, queueID, claimedBy, "claimed", "pending")
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) CompleteQueueEntry(ctx context.Context, queueID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $2, completed_at = NOW()
		WHERE queue_id = $1
	`, queueID, "completed")
	return err
}

func (s *Store) FailQueueEntry(ctx context.Context, queueID, errorMsg string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE queue_id = $1
	`, queueID, errorMsg, "failed")
	return err
}

func (s *Store) GetQueueEntryDetails(ctx context.Context, queueID string) (*tasks.QueueEntryDetails, error) {
	var d tasks.QueueEntryDetails
	var launchParamsJSON []byte

	err := s.db.QueryRowContext(ctx, `
		SELECT task_id, target_implementation, workspace, profile, launch_params
		FROM orchestrated_task_queue
		WHERE queue_id = $1
	`, queueID).Scan(&d.TaskID, &d.TargetImplementation, &d.Workspace, &d.Profile, &launchParamsJSON)
	if err != nil {
		return nil, err
	}

	if launchParamsJSON != nil {
		if err := json.Unmarshal(launchParamsJSON, &d.LaunchParams); err != nil {
			return nil, fmt.Errorf("unmarshal launch_params: %w", err)
		}
	}
	return &d, nil
}

func (s *Store) ListStaleClaimedQueueEntries(ctx context.Context, threshold time.Duration, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT queue_id
		FROM orchestrated_task_queue
		WHERE status = $2 AND claimed_at < NOW() - $1::interval
		ORDER BY claimed_at ASC
		LIMIT $3
	`, fmt.Sprintf("%d seconds", int(threshold.Seconds())), "claimed", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) CompleteQueueEntryByTaskID(ctx context.Context, taskID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $2, completed_at = NOW()
		WHERE task_id = $1 AND status IN ($3, $4)
	`, taskID, "completed", "pending", "claimed")
	return err
}

func (s *Store) FailQueueEntryByTaskID(ctx context.Context, taskID, errorMsg string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE orchestrated_task_queue
		SET status = $3, error_message = $2, completed_at = NOW()
		WHERE task_id = $1 AND status IN ($4, $5)
	`, taskID, errorMsg, "failed", "pending", "claimed")
	return err
}

// =========================================================================
// Admin workspace queries
// =========================================================================

func (s *Store) ListDistinctTaskWorkspaces(ctx context.Context) ([]*tasks.WorkspaceTaskSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT workspace, MIN(created_at) AS created_at, COUNT(*) AS task_count
		FROM tasks
		WHERE workspace IS NOT NULL AND workspace != ''
		GROUP BY workspace
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*tasks.WorkspaceTaskSummary
	for rows.Next() {
		var ws tasks.WorkspaceTaskSummary
		// Scan created_at as interface{} to handle both postgres (time.Time)
		// and dbcompat (string) drivers transparently.
		var createdAtRaw interface{}
		if err := rows.Scan(&ws.Workspace, &createdAtRaw, &ws.TaskCount); err != nil {
			return nil, err
		}
		if createdAtRaw != nil {
			switch v := createdAtRaw.(type) {
			case time.Time:
				ws.CreatedAt = v
			case string:
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					ws.CreatedAt = t
				} else if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
					ws.CreatedAt = t
				} else if t, err := time.Parse("2006-01-02T15:04:05.000", v); err == nil {
					ws.CreatedAt = t
				}
			}
		}
		results = append(results, &ws)
	}
	return results, rows.Err()
}

func (s *Store) GetWorkspaceTaskStats(ctx context.Context, workspaceID string) (*tasks.WorkspaceTaskStats, error) {
	var stats tasks.WorkspaceTaskStats
	var count sql.NullInt64
	// Scan created_at as interface{} to handle both postgres (time.Time)
	// and dbcompat (string) drivers transparently.
	var createdAtRaw interface{}

	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(created_at)
		FROM tasks
		WHERE workspace = $1
	`, workspaceID).Scan(&count, &createdAtRaw)
	if err != nil {
		return nil, err
	}

	if count.Valid {
		stats.TaskCount = count.Int64
	}
	if createdAtRaw != nil {
		switch v := createdAtRaw.(type) {
		case time.Time:
			stats.CreatedAt = v
		case string:
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				stats.CreatedAt = t
			} else if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				stats.CreatedAt = t
			} else if t, err := time.Parse("2006-01-02T15:04:05.000", v); err == nil {
				stats.CreatedAt = t
			}
		}
	}
	return &stats, nil
}

// =========================================================================
// Phase 1: Paused-state lifecycle — delegate to legacy TaskStore
// =========================================================================

// PauseTask, ResumeTask, RejectTask, ListWaitingTasks,
// ListTasksWaitingOnDependency, and ListTasksByContext are implemented on
// the embedded *legacy.TaskStore and promoted here automatically via
// Go embedding. No override is needed; the legacy methods use the same
// *sql.DB handle.

// =========================================================================
// Overrides for methods using NOW() — ensure time functions work
// =========================================================================

// Note: All non-transactional methods (CreateTask, GetTask, ListTasks,
// etc.) are inherited from the embedded *legacy.TaskStore and work
// identically to Stage 1. The embedded type's RecordAuditEventTx (which
// takes *sql.Tx) is shadowed by our StoreTx-based version above.
