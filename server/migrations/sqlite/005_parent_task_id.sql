-- Migration 005: Add parent_task_id for task-tree lineage.
-- SQLite counterpart of postgres migration 014_parent_task_id.sql.
--
-- Unlike most of the post-012 migrations, this column is NOT yet in
-- 001_full_schema.sql, so we genuinely need to add it. The disconnect reaper
-- and task-tree queries select on tasks.parent_task_id and error out with
-- "no such column: parent_task_id" without it.
--
-- TEXT (instead of UUID) per the sqlite convention used elsewhere in the
-- aether-lite schema; the column is nullable so no DEFAULT is required.

ALTER TABLE tasks ADD COLUMN parent_task_id TEXT;

CREATE INDEX IF NOT EXISTS idx_tasks_parent_task_id
    ON tasks (parent_task_id)
    WHERE parent_task_id IS NOT NULL;
