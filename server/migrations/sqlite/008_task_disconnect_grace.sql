-- Migration 008: task disconnect/limbo state columns.
-- SQLite counterpart of postgres migration 017_task_disconnect_grace.sql.
--
-- disconnected_at and grace_window_ms are already declared on the tasks
-- table in 001_full_schema.sql (with TEXT and INTEGER instead of
-- TIMESTAMPTZ/BIGINT, which is the sqlite convention used throughout).
-- The idx_tasks_disconnected_at partial index is also already created.
-- No-op on fresh databases.

-- Intentionally empty.
