-- Migration 022: Phase 1 A2A paused states and context grouping
--
-- Adds four columns to the tasks table to support the paused-state lifecycle
-- (WAITING_INPUT, WAITING_AUTHORITY, WAITING_DEPENDENCY, HIBERNATED, REJECTED)
-- and the A2A contextId session-grouping field.
--
-- All columns are nullable (DEFAULT NULL) so existing rows are unaffected.

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS wait_spec    JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS depends_on   JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS context_id   TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS paused_at    TIMESTAMPTZ;

-- Index: quick lookup of all tasks paused by context (Phase 1 ListTasksByContext).
CREATE INDEX IF NOT EXISTS idx_tasks_context_id
    ON tasks (context_id)
    WHERE context_id IS NOT NULL;

-- Index: dependency-wake sweep — find tasks waiting on a specific upstream task.
-- GIN with jsonb_path_ops supports the @> containment query used by
-- ListTasksWaitingOnDependency. depends_on is stored as a JSONB array of
-- task-id strings, matching the JSON encoding used by CreateTask / PauseTask.
CREATE INDEX IF NOT EXISTS idx_tasks_depends_on_gin
    ON tasks USING GIN (depends_on jsonb_path_ops)
    WHERE depends_on IS NOT NULL;

-- Index: paused-state reaper / reconciler ordered scan.
CREATE INDEX IF NOT EXISTS idx_tasks_paused_at
    ON tasks (paused_at)
    WHERE paused_at IS NOT NULL;
