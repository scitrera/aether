-- Wire up dispatch priority for durable tasks. The tasks.priority column has
-- existed since migration 002 (INT NOT NULL DEFAULT 0) but was never set or
-- used. This migration makes NORMAL (30) the effective default, backfills
-- legacy rows, adds a dispatch-ordering index, and extends the orchestrated
-- task queue with the same priority weight so both delivery paths order by it.
--
-- Priority weights mirror the proto TaskPriority enum (spaced to allow future
-- levels): XLOW=10, LOW=20, NORMAL=30, HIGH=40, PREEMPT=50. Higher = delivered
-- first; the numeric value is used directly as the descending sort key.

-- tasks: default to NORMAL and backfill rows written before this feature.
ALTER TABLE tasks ALTER COLUMN priority SET DEFAULT 30;
UPDATE tasks SET priority = 30 WHERE priority = 0;

-- Dispatch-selection index: highest priority first, FIFO within a level,
-- scoped to pending tasks (the only rows the selection paths scan).
CREATE INDEX IF NOT EXISTS idx_task_priority_dispatch
    ON tasks (priority DESC, created_at ASC)
    WHERE status = 'pending';

-- orchestrated_task_queue: carry the task's priority so the polling/notify
-- dispatcher can order spawns the same way.
ALTER TABLE orchestrated_task_queue ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 30;

CREATE INDEX IF NOT EXISTS idx_orch_queue_priority
    ON orchestrated_task_queue (priority DESC, created_at ASC)
    WHERE status = 'pending';
