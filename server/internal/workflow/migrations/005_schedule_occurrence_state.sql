-- Bounded observability for the latest scheduler decision. Fired occurrences
-- also carry this descriptor in task metadata; skipped occurrences have no task
-- and therefore remain visible only on the authoritative schedule row.
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS last_occurrence_at TIMESTAMPTZ;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS last_occurrence_disposition TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS last_occurrence_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS last_backlog_count INT NOT NULL DEFAULT 0;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS last_backlog_truncated BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS last_backlog_index INT NOT NULL DEFAULT 0;
