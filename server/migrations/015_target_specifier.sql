-- Add target_specifier to tasks for per-user singleton agent dedup.
--
-- CoworkAgent is moving to per-user scoping by setting
-- (workspace="_apps", implementation="cowork", specifier=<user_id>).
-- The existing HasActiveStartupTask dedup filters on (target_implementation,
-- workspace) alone, so two users on the same _apps workspace would collide.
-- Adding target_specifier to the dedup key (and to the stored row) fixes that.
--
-- Pool-mode queries deliberately aggregate across specifiers and are NOT
-- affected by this column.

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS target_specifier VARCHAR(255);

-- Partial index for the startup-task dedup lookup. Lookups are always filtered
-- by task_type='agent_startup' AND an active-status set, so keep the index
-- narrow to avoid bloat on non-startup rows.
CREATE INDEX IF NOT EXISTS idx_startup_lookup
    ON tasks (target_implementation, workspace, target_specifier)
    WHERE task_type = 'agent_startup'
      AND status IN ('pending', 'assigned', 'starting', 'running');
