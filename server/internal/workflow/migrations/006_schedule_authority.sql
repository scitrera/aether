-- Private authorization for scheduled task creation. These columns are never
-- projected into schedule JSON or task payload/metadata.
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_grant_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_subject_type TEXT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_subject_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_root_grant_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_source_grant_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_expires_at TIMESTAMPTZ;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_policy_digest TEXT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_policy_version INT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_lifetime_mode INT;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_blocked BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE workflow_schedules ADD COLUMN IF NOT EXISTS authority_blocked_reason TEXT NOT NULL DEFAULT '';
