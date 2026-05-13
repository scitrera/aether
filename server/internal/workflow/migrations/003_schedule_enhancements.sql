-- Add concurrency control and active task tracking to schedules
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS max_concurrent INT NOT NULL DEFAULT 0;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS active_task_id TEXT;
