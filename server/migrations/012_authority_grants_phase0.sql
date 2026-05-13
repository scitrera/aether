-- Migration: Phase 0 authority-grant and audit-lineage foundation

CREATE TABLE IF NOT EXISTS acl_authority_grants
(
    grant_id           UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    root_grant_id      UUID         NOT NULL,
    subject_type       VARCHAR(50)  NOT NULL,
    subject_id         VARCHAR(255) NOT NULL,
    delegate_type      VARCHAR(50)  NOT NULL,
    delegate_id        VARCHAR(255) NOT NULL,
    issued_by_type     VARCHAR(50)  NOT NULL,
    issued_by_id       VARCHAR(255) NOT NULL,
    root_subject_type  VARCHAR(50)  NOT NULL,
    root_subject_id    VARCHAR(255) NOT NULL,
    parent_grant_id    UUID REFERENCES acl_authority_grants (grant_id) ON DELETE SET NULL,
    may_delegate       BOOLEAN      NOT NULL DEFAULT FALSE,
    remaining_hops     INTEGER      NOT NULL DEFAULT 0,
    workspace_scope    JSONB        NOT NULL DEFAULT '[]'::jsonb,
    resource_scope     JSONB        NOT NULL DEFAULT '{}'::jsonb,
    operation_scope    JSONB        NOT NULL DEFAULT '[]'::jsonb,
    max_access_level   INTEGER      NOT NULL,
    audience_type      VARCHAR(50)  NOT NULL,
    audience_id        VARCHAR(255) NOT NULL,
    valid_while_audience_active BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at         TIMESTAMP    NOT NULL,
    renewable_until    TIMESTAMP    NOT NULL,
    renewed_at         TIMESTAMP,
    revoked            BOOLEAN      NOT NULL DEFAULT FALSE,
    revoked_at         TIMESTAMP,
    reason             TEXT,
    metadata           JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_authority_grant_renewal_window CHECK (renewable_until >= expires_at),
    CONSTRAINT chk_authority_grant_hops CHECK (remaining_hops >= 0),
    CONSTRAINT chk_authority_grant_delegate_depth CHECK (
        (may_delegate = FALSE AND remaining_hops = 0) OR
        (may_delegate = TRUE AND remaining_hops > 0)
        ),
    CONSTRAINT chk_authority_grant_revocation CHECK (
        (revoked = FALSE AND revoked_at IS NULL) OR
        (revoked = TRUE AND revoked_at IS NOT NULL)
        )
);

CREATE INDEX IF NOT EXISTS idx_authority_grants_root ON acl_authority_grants (root_grant_id);
CREATE INDEX IF NOT EXISTS idx_authority_grants_delegate_active
    ON acl_authority_grants (delegate_type, delegate_id, expires_at)
    WHERE revoked = FALSE;
CREATE INDEX IF NOT EXISTS idx_authority_grants_subject_active
    ON acl_authority_grants (subject_type, subject_id, expires_at)
    WHERE revoked = FALSE;
CREATE INDEX IF NOT EXISTS idx_authority_grants_audience_active
    ON acl_authority_grants (audience_type, audience_id, expires_at)
    WHERE revoked = FALSE;
CREATE INDEX IF NOT EXISTS idx_authority_grants_parent ON acl_authority_grants (parent_grant_id)
    WHERE parent_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_authority_grants_expires_at ON acl_authority_grants (expires_at);

ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS subject_type VARCHAR(50);
ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS subject_id VARCHAR(255);
ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS root_subject_type VARCHAR(50);
ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS root_subject_id VARCHAR(255);
ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS authority_mode VARCHAR(50) NOT NULL DEFAULT 'direct';
ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS root_authority_grant_id UUID;
ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS authority_grant_id UUID;
ALTER TABLE comprehensive_audit_log
    ADD COLUMN IF NOT EXISTS parent_authority_grant_id UUID;

UPDATE comprehensive_audit_log
SET subject_type = COALESCE(subject_type, actor_type),
    subject_id = COALESCE(subject_id, actor_id),
    root_subject_type = COALESCE(root_subject_type, actor_type),
    root_subject_id = COALESCE(root_subject_id, actor_id),
    authority_mode = COALESCE(authority_mode, 'direct')
WHERE subject_type IS NULL
   OR subject_id IS NULL
   OR root_subject_type IS NULL
   OR root_subject_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_cal_subject ON comprehensive_audit_log (subject_type, subject_id);
CREATE INDEX IF NOT EXISTS idx_cal_root_authority_grant ON comprehensive_audit_log (root_authority_grant_id)
    WHERE root_authority_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cal_authority_grant ON comprehensive_audit_log (authority_grant_id)
    WHERE authority_grant_id IS NOT NULL;

DROP VIEW IF EXISTS acl_audit_log;

CREATE VIEW acl_audit_log AS
SELECT
    audit_id,
    timestamp,
    COALESCE(metadata->>'decision', CASE WHEN success THEN 'ALLOW' ELSE 'DENY' END) AS decision,
    (metadata->>'access_level')::integer                                            AS access_level,
    actor_type                                                                      AS principal_type,
    actor_id                                                                        AS principal_id,
    subject_type,
    subject_id,
    root_subject_type,
    root_subject_id,
    authority_mode,
    root_authority_grant_id,
    authority_grant_id,
    parent_authority_grant_id,
    resource_type,
    resource_id,
    operation,
    workspace,
    (metadata->>'delegation_chain_id')::uuid                                        AS delegation_chain_id,
    (metadata->>'rule_id')::uuid                                                    AS rule_id,
    COALESCE((metadata->>'fallback_applied')::boolean, false)                       AS fallback_applied,
    gateway_id,
    session_id,
    metadata
FROM comprehensive_audit_log
WHERE event_type = 'authorization';
