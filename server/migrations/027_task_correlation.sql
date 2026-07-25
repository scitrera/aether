-- Migration: correlation / flow-root lineage and feed-B completion-event config
--
-- correlation_id   : fan-out/fan-in correlation identity (the barrier/group id a
--                    workflow join matches against), distinct from task_id.
-- root_task_id     : flow-root task id propagated from a task's spawner. A task
--                    with no provided root is its own flow root.
-- completion_event : "feed B" config (JSON) — emit a domain event onto event::*
--                    when the task reaches a selected terminal status.

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(255);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS root_task_id VARCHAR(255);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS completion_event JSONB;

CREATE INDEX IF NOT EXISTS idx_tasks_correlation_id
    ON tasks (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tasks_root_task_id
    ON tasks (root_task_id)
    WHERE root_task_id IS NOT NULL;
