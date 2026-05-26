-- Add nullable retry_policy_json column to the sqlite tasks table to
-- mirror migration 025 on postgres. Phase 2 of the webhook integration
-- initiative lifts retry scheduling out of individual workers and into
-- the task store. Tasks without a policy keep legacy behavior.
--
-- SQLite supports IF NOT EXISTS on ALTER TABLE ADD COLUMN since version
-- 3.35 (March 2021), which is well below the modernc driver's minimum.

ALTER TABLE tasks ADD COLUMN retry_policy_json TEXT;
