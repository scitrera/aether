-- Wire up dispatch priority for durable tasks (sqlite mirror of postgres
-- migration 026). The sqlite tasks table already has priority INTEGER NOT NULL
-- DEFAULT 0 (since 001); SQLite cannot alter a column default in place, so the
-- Go store normalizes UNSPECIFIED (0) to NORMAL (30) on write. Here we backfill
-- legacy rows, add the dispatch index, and extend the orchestrated task queue.
--
-- Priority weights mirror the proto TaskPriority enum: XLOW=10, LOW=20,
-- NORMAL=30, HIGH=40, PREEMPT=50. Higher = delivered first.

-- Backfill rows written before this feature (stored 0) to NORMAL.
UPDATE tasks SET priority = 30 WHERE priority = 0;

-- Dispatch-selection index: highest priority first, FIFO within a level.
CREATE INDEX IF NOT EXISTS idx_task_priority_dispatch
    ON tasks (priority DESC, created_at ASC)
    WHERE status = 'pending';

-- orchestrated_task_queue: carry the task's priority so the polling dispatcher
-- can order spawns the same way. SQLite supports ADD COLUMN with a constant
-- default since 3.35.
ALTER TABLE orchestrated_task_queue ADD COLUMN priority INTEGER NOT NULL DEFAULT 30;

CREATE INDEX IF NOT EXISTS idx_orch_queue_priority
    ON orchestrated_task_queue (priority DESC, created_at ASC)
    WHERE status = 'pending';
