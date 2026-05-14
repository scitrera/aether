package orchestration

// QueueStatus is the canonical string value stored in orchestrated_task_queue.status.
// The SQL schema's CHECK constraint in migrations/004_orchestration_schema.sql is
// authoritative; this Go type mirrors it.
type QueueStatus string

const (
	QueueStatusPending   QueueStatus = "pending"
	QueueStatusClaimed   QueueStatus = "claimed"
	QueueStatusCompleted QueueStatus = "completed"
	QueueStatusFailed    QueueStatus = "failed"
)
