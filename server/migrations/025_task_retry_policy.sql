-- Add nullable retry_policy_json to tasks table. Phase 2 of the webhook
-- integration initiative lifts retry scheduling out of individual workers
-- and into the task store. Tasks without a policy keep legacy behavior.

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS retry_policy_json JSONB;

-- No index needed: retry_policy is read on FailTask via task_id (already
-- the primary key) and not used in any scan path.
