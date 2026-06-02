-- Migration 005: correlation / flow-root lineage and feed-B completion-event
-- config (SQLite).
--
-- SQLite stores:
--   correlation_id   as TEXT
--   root_task_id     as TEXT
--   completion_event as TEXT (JSON object), NULL when the task did not opt into
--                    feed B (consistent with the Go layer marshaling nil ⇒ NULL)

ALTER TABLE tasks ADD COLUMN correlation_id   TEXT;
ALTER TABLE tasks ADD COLUMN root_task_id     TEXT;
ALTER TABLE tasks ADD COLUMN completion_event TEXT;

-- Index: quick lookup of all tasks in a correlation group (join barrier).
CREATE INDEX IF NOT EXISTS idx_tasks_correlation_id
    ON tasks (correlation_id)
    WHERE correlation_id IS NOT NULL;

-- Index: quick lookup of all tasks in a flow.
CREATE INDEX IF NOT EXISTS idx_tasks_root_task_id
    ON tasks (root_task_id)
    WHERE root_task_id IS NOT NULL;
