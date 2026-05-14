-- Migration 007: task_class UI-surface hint column.
-- SQLite counterpart of postgres migration 016_task_class.sql.
--
-- 001_full_schema.sql already declares task_class INTEGER NOT NULL DEFAULT 0
-- on the tasks table, so this migration is a no-op on fresh databases.

-- Intentionally empty.
