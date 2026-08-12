-- Private authorization for scheduled task creation. These columns are never
-- projected into schedule JSON or task payload/metadata.
ALTER TABLE workflow_schedules ADD COLUMN authority_grant_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN authority_subject_type TEXT;
ALTER TABLE workflow_schedules ADD COLUMN authority_subject_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN authority_root_grant_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN authority_source_grant_id TEXT;
ALTER TABLE workflow_schedules ADD COLUMN authority_expires_at TEXT;
ALTER TABLE workflow_schedules ADD COLUMN authority_policy_digest TEXT;
ALTER TABLE workflow_schedules ADD COLUMN authority_policy_version INTEGER;
ALTER TABLE workflow_schedules ADD COLUMN authority_lifetime_mode INTEGER;
ALTER TABLE workflow_schedules ADD COLUMN authority_blocked INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_schedules ADD COLUMN authority_blocked_reason TEXT NOT NULL DEFAULT '';
