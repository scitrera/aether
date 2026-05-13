-- Tasks gain a "limbo" state: disconnected_at marks when the assigned worker's
-- gRPC stream went away. A periodic reaper fails tasks whose grace window has
-- elapsed without reconnect. Worker reconnect (via session resume or a fresh
-- connection that re-establishes the per-task token association) clears
-- disconnected_at. Connection-as-heartbeat — no application-level heartbeat
-- protocol required.
--
-- grace_window_ms is per-task with class-based defaults applied at task creation:
--   INTERACTIVE/UNSPECIFIED = 30000  (30s)
--   BATCH                   = 300000 (5min)
--   BACKGROUND              = 600000 (10min)

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS disconnected_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS grace_window_ms BIGINT NOT NULL DEFAULT 0;

-- Reaper scans this column. Partial index keeps it tiny — most tasks are connected.
CREATE INDEX IF NOT EXISTS idx_tasks_disconnected_at
    ON tasks (disconnected_at)
    WHERE disconnected_at IS NOT NULL AND status = 'running';
