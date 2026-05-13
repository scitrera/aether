-- Add task_class column to tasks for UI-surface classification.
--
-- task_class is a UI-surface hint distinct from task_type (the domain
-- identifier). Values mirror the proto TaskClass enum:
--   0 = UNSPECIFIED (treated as INTERACTIVE for back-compat)
--   1 = INTERACTIVE  — short-lived, user actively waiting
--   2 = BACKGROUND   — long-running infra (sandbox lease, idle reaper, ...)
--   3 = BATCH        — long-running user-initiated job
--
-- The gateway plumbs this through proto -> internal Task -> proto so admin
-- dashboards can filter by class (positive task_class match) or invert
-- (exclude_task_classes list). No effect on scheduling, retry, or ACL.

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS task_class INTEGER NOT NULL DEFAULT 0;
