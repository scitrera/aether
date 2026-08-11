-- Bounded observability for the latest scheduler decision. Fired occurrences
-- also carry this descriptor in task metadata; skipped occurrences have no task
-- and therefore remain visible only on the authoritative schedule row.
ALTER TABLE workflow_schedules ADD COLUMN last_occurrence_at TEXT;
ALTER TABLE workflow_schedules ADD COLUMN last_occurrence_disposition TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_schedules ADD COLUMN last_occurrence_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_schedules ADD COLUMN last_backlog_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_schedules ADD COLUMN last_backlog_truncated INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_schedules ADD COLUMN last_backlog_index INTEGER NOT NULL DEFAULT 0;
