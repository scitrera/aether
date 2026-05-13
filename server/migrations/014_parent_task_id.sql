-- Migration: first-class parent_task_id column for task-tree lineage

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS parent_task_id UUID;

CREATE INDEX IF NOT EXISTS idx_tasks_parent_task_id
    ON tasks (parent_task_id)
    WHERE parent_task_id IS NOT NULL;
