-- Add concurrency control and active task tracking to schedules (SQLite)
ALTER TABLE workflow_schedules ADD COLUMN max_concurrent INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_schedules ADD COLUMN active_task_id TEXT;
