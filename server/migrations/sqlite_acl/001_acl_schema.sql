-- AetherLite native SQLite ACL schema
-- Stage 2 of the storage-interfaces refactor: native sqlite implementation
-- that does NOT go through pkg/dbcompat.
--
-- Tables: acl_rules, acl_fallback_policies, acl_authority_grants
-- View: acl_audit_log (over comprehensive_audit_log in the audit DB)
-- Seed data: 11 fallback policies + default _global workspace rule
--
-- Schema is structurally equivalent to postgres migrations 003 + 012 + 018
-- with SQLite-native types (TEXT for timestamps, INTEGER for booleans,
-- TEXT for JSON columns, INTEGER PRIMARY KEY AUTOINCREMENT where needed).

-- =============================================================================
-- ACL Rules
-- =============================================================================

CREATE TABLE IF NOT EXISTS acl_rules (
    rule_id        TEXT PRIMARY KEY,
    principal_type TEXT    NOT NULL,
    principal_id   TEXT    NOT NULL,
    resource_type  TEXT    NOT NULL,
    resource_id    TEXT    NOT NULL,
    access_level   INTEGER NOT NULL,
    granted_by     TEXT,
    granted_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at     TEXT,
    reason         TEXT,
    CONSTRAINT unique_acl_rule UNIQUE (principal_type, principal_id, resource_type, resource_id)
);

CREATE INDEX IF NOT EXISTS idx_acl_principal ON acl_rules (principal_type, principal_id);
CREATE INDEX IF NOT EXISTS idx_acl_resource ON acl_rules (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_acl_expiration ON acl_rules (expires_at) WHERE expires_at IS NOT NULL;

-- =============================================================================
-- ACL Fallback Policies
-- =============================================================================

CREATE TABLE IF NOT EXISTS acl_fallback_policies (
    policy_id             TEXT PRIMARY KEY,
    rule_category         TEXT    NOT NULL UNIQUE,
    fallback_access_level INTEGER NOT NULL,
    updated_by            TEXT,
    updated_at            TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- =============================================================================
-- ACL Authority Grants
-- =============================================================================

CREATE TABLE IF NOT EXISTS acl_authority_grants (
    grant_id                   TEXT    PRIMARY KEY,
    root_grant_id              TEXT    NOT NULL,
    subject_type               TEXT    NOT NULL,
    subject_id                 TEXT    NOT NULL,
    delegate_type              TEXT    NOT NULL,
    delegate_id                TEXT    NOT NULL,
    issued_by_type             TEXT    NOT NULL,
    issued_by_id               TEXT    NOT NULL,
    root_subject_type          TEXT    NOT NULL,
    root_subject_id            TEXT    NOT NULL,
    parent_grant_id            TEXT,
    may_delegate               INTEGER NOT NULL DEFAULT 0,
    remaining_hops             INTEGER NOT NULL DEFAULT 0,
    workspace_scope            TEXT    NOT NULL DEFAULT '[]',
    resource_scope             TEXT    NOT NULL DEFAULT '{}',
    operation_scope            TEXT    NOT NULL DEFAULT '[]',
    max_access_level           INTEGER NOT NULL,
    audience_type              TEXT    NOT NULL,
    audience_id                TEXT    NOT NULL,
    valid_while_audience_active INTEGER NOT NULL DEFAULT 0,
    expires_at                 TEXT    NOT NULL,
    renewable_until            TEXT    NOT NULL,
    renewed_at                 TEXT,
    revoked                    INTEGER NOT NULL DEFAULT 0,
    revoked_at                 TEXT,
    reason                     TEXT,
    metadata                   TEXT    NOT NULL DEFAULT '{}',
    created_at                 TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CONSTRAINT chk_authority_grant_renewal_window CHECK (renewable_until >= expires_at),
    CONSTRAINT chk_authority_grant_hops CHECK (remaining_hops >= 0),
    CONSTRAINT chk_authority_grant_delegate_depth CHECK (
        (may_delegate = 0 AND remaining_hops = 0) OR
        (may_delegate = 1 AND remaining_hops > 0)
    ),
    CONSTRAINT chk_authority_grant_revocation CHECK (
        (revoked = 0 AND revoked_at IS NULL) OR
        (revoked = 1 AND revoked_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_authority_grants_root ON acl_authority_grants (root_grant_id);
CREATE INDEX IF NOT EXISTS idx_authority_grants_delegate_active
    ON acl_authority_grants (delegate_type, delegate_id, expires_at) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_authority_grants_subject_active
    ON acl_authority_grants (subject_type, subject_id, expires_at) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_authority_grants_audience_active
    ON acl_authority_grants (audience_type, audience_id, expires_at) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_authority_grants_parent
    ON acl_authority_grants (parent_grant_id) WHERE parent_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_authority_grants_expires_at ON acl_authority_grants (expires_at);

-- =============================================================================
-- Seed data: ALL 11 fallback policies
-- Parity with postgres migrations/003_acl_schema.sql:88-99.
-- DO NOT drop any of these -- the 4 kv_scope rows are load-bearing for
-- service principals accessing user-bound KV scopes (see section 15.1 of
-- the storage-interfaces plan).
-- =============================================================================

INSERT INTO acl_fallback_policies (policy_id, rule_category, fallback_access_level, updated_by)
VALUES ('pol-user-workspace',      'user_workspace',      20, '_system'),
       ('pol-agent-workspace',     'agent_workspace',     20, '_system'),
       ('pol-user-agent',          'user_agent',          20, '_system'),
       ('pol-agent-agent',         'agent_agent',         20, '_system'),
       ('pol-task-workspace',      'task_workspace',      20, '_system'),
       ('pol-global-read',         'global_read',         20, '_system'),
       ('pol-orchestrator-system', 'orchestrator_system', 20, '_system'),
       ('pol-user-kv-scope',       'user_kv_scope',       20, '_system'),
       ('pol-agent-kv-scope',      'agent_kv_scope',      20, '_system'),
       ('pol-task-kv-scope',       'task_kv_scope',       20, '_system'),
       ('pol-service-kv-scope',    'service_kv_scope',    20, '_system')
ON CONFLICT (rule_category) DO NOTHING;

-- =============================================================================
-- Seed data: Default _global workspace rule
-- Parity with postgres migrations/003_acl_schema.sql:104-107.
-- =============================================================================

INSERT INTO acl_rules (rule_id, principal_type, principal_id, resource_type, resource_id, access_level, granted_by, reason)
VALUES ('rule-default-global', 'wildcard', '_any_authenticated', 'workspace', '_global', 20, '_system',
        'Default READ_WRITE access for all authenticated principals (development mode)')
ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING;
