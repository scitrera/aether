-- Migration 002: Phase 1 A2A paused states and context grouping (SQLite)
--
-- Adds four columns to the tasks table to support the paused-state lifecycle
-- (waiting_input, waiting_authority, waiting_dependency, hibernated, rejected)
-- and the A2A contextId session-grouping field.
--
-- SQLite stores:
--   wait_spec  as TEXT (JSON object)
--   depends_on as TEXT (JSON array of task ID strings)
--   context_id as TEXT
--   paused_at  as INTEGER (Unix milliseconds, consistent with the Go layer)

ALTER TABLE tasks ADD COLUMN wait_spec  TEXT;
ALTER TABLE tasks ADD COLUMN depends_on TEXT;
ALTER TABLE tasks ADD COLUMN context_id TEXT;
ALTER TABLE tasks ADD COLUMN paused_at  INTEGER;

-- Index: quick lookup of all tasks in a context session.
CREATE INDEX IF NOT EXISTS idx_tasks_context_id
    ON tasks (context_id)
    WHERE context_id IS NOT NULL;

-- Index: paused-state reaper / reconciler ordered scan.
CREATE INDEX IF NOT EXISTS idx_tasks_paused_at
    ON tasks (paused_at)
    WHERE paused_at IS NOT NULL;
