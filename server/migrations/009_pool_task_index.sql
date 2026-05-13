-- Pool task assignment: partial index for efficient pending pool task lookup
CREATE INDEX IF NOT EXISTS idx_task_pool_pending
    ON tasks(target_implementation, workspace)
    WHERE assignment_mode = 'pool' AND status = 'pending' AND queued_for_startup = true;
