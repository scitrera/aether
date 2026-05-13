-- Migration: first-class task authority lineage fields

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS authority_mode VARCHAR(50);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS subject_type VARCHAR(50);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS subject_id VARCHAR(255);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS root_subject_type VARCHAR(50);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS root_subject_id VARCHAR(255);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS authority_grant_id UUID;
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS root_authority_grant_id UUID;
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS parent_authority_grant_id UUID;
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS authority_audience_type VARCHAR(50);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS authority_audience_id VARCHAR(255);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS authority_delegate_type VARCHAR(50);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS authority_delegate_id VARCHAR(255);

UPDATE tasks
SET authority_mode = COALESCE(authority_mode, metadata->>'authority_mode'),
    subject_type = COALESCE(subject_type, metadata->>'subject_type'),
    subject_id = COALESCE(subject_id, metadata->>'subject_id'),
    root_subject_type = COALESCE(root_subject_type, metadata->>'root_subject_type'),
    root_subject_id = COALESCE(root_subject_id, metadata->>'root_subject_id'),
    authority_grant_id = COALESCE(
        authority_grant_id,
        CASE
            WHEN NULLIF(metadata->>'authority_grant_id', '') IS NOT NULL
                THEN (metadata->>'authority_grant_id')::uuid
            ELSE NULL
        END
    ),
    root_authority_grant_id = COALESCE(
        root_authority_grant_id,
        CASE
            WHEN NULLIF(metadata->>'root_authority_grant_id', '') IS NOT NULL
                THEN (metadata->>'root_authority_grant_id')::uuid
            ELSE NULL
        END
    ),
    parent_authority_grant_id = COALESCE(
        parent_authority_grant_id,
        CASE
            WHEN NULLIF(metadata->>'parent_authority_grant_id', '') IS NOT NULL
                THEN (metadata->>'parent_authority_grant_id')::uuid
            ELSE NULL
        END
    ),
    authority_audience_type = COALESCE(authority_audience_type, metadata->>'authority_audience_type'),
    authority_audience_id = COALESCE(authority_audience_id, metadata->>'authority_audience_id'),
    authority_delegate_type = COALESCE(authority_delegate_type, metadata->>'authority_delegate_type'),
    authority_delegate_id = COALESCE(authority_delegate_id, metadata->>'authority_delegate_id')
WHERE metadata IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tasks_root_authority_grant
    ON tasks (root_authority_grant_id)
    WHERE root_authority_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_authority_grant
    ON tasks (authority_grant_id)
    WHERE authority_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_subject
    ON tasks (subject_type, subject_id)
    WHERE subject_type IS NOT NULL AND subject_id IS NOT NULL;
