-- Migration: ACL System Schema for Phase 5
-- This enables identity-based access control with delegation and audit logging

-- ACL Rules Table
-- Stores explicit access grants for principals to resources
CREATE TABLE IF NOT EXISTS acl_rules
(
    rule_id        UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    principal_type VARCHAR(50)  NOT NULL, -- 'agent', 'task', 'user', 'wildcard'
    principal_id   VARCHAR(255) NOT NULL, -- Identity ID or wildcard pattern
    resource_type  VARCHAR(50)  NOT NULL, -- 'workspace', 'agent', 'permission'
    resource_id    VARCHAR(255) NOT NULL, -- Resource identifier
    access_level   INTEGER      NOT NULL, -- 0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN, 50=SUPERADMIN
    granted_by     VARCHAR(255),          -- Who granted this rule
    granted_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    expires_at     TIMESTAMP,             -- Optional expiration
    reason         TEXT,                  -- Explanation for this grant
    CONSTRAINT unique_acl_rule UNIQUE (principal_type, principal_id, resource_type, resource_id)
);

-- Indexes for efficient ACL lookups
CREATE INDEX IF NOT EXISTS idx_acl_principal ON acl_rules (principal_type, principal_id);
CREATE INDEX IF NOT EXISTS idx_acl_resource ON acl_rules (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_acl_expiration ON acl_rules (expires_at) WHERE expires_at IS NOT NULL;

-- Delegation Chain Tracking
-- Records how permissions flow from root principals (users) to derived principals (tasks/agents)
CREATE TABLE IF NOT EXISTS acl_delegation_chains
(
    chain_id               UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    root_principal_type    VARCHAR(50)  NOT NULL, -- Original principal type (usually 'user')
    root_principal_id      VARCHAR(255) NOT NULL, -- Original principal ID
    derived_principal_type VARCHAR(50)  NOT NULL, -- Derived principal type ('task' or 'agent')
    derived_principal_id   VARCHAR(255) NOT NULL, -- Derived principal ID
    workspace              VARCHAR(255),          -- Workspace context
    delegation_path        JSONB        NOT NULL, -- Array of identities in delegation chain
    effective_access_level INTEGER      NOT NULL, -- Minimum access level along chain
    created_at             TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_delegation_root ON acl_delegation_chains (root_principal_type, root_principal_id);
CREATE INDEX IF NOT EXISTS idx_delegation_derived ON acl_delegation_chains (derived_principal_type, derived_principal_id);
CREATE INDEX IF NOT EXISTS idx_delegation_workspace ON acl_delegation_chains (workspace);

-- ACL Audit Log
-- Records all ACL decisions for security audit and debugging
CREATE TABLE IF NOT EXISTS acl_audit_log
(
    audit_id            BIGSERIAL PRIMARY KEY,
    timestamp           TIMESTAMP    NOT NULL DEFAULT NOW(),
    decision            VARCHAR(20)  NOT NULL,               -- 'ALLOW' or 'DENY'
    access_level        INTEGER,                             -- Effective access level granted
    principal_type      VARCHAR(50)  NOT NULL,
    principal_id        VARCHAR(255) NOT NULL,
    resource_type       VARCHAR(50)  NOT NULL,
    resource_id         VARCHAR(255) NOT NULL,
    operation           VARCHAR(100) NOT NULL,               -- 'connect', 'send_message', 'create_task', etc.
    workspace           VARCHAR(255),
    delegation_chain_id UUID,                                -- Link to delegation chain if applicable
    rule_id             UUID,                                -- Link to ACL rule that granted access
    fallback_applied    BOOLEAN               DEFAULT FALSE, -- Whether fallback policy was used
    gateway_id          VARCHAR(100),                        -- Which gateway made the decision
    session_id          UUID,                                -- Session context
    metadata            JSONB                                -- Additional context
);

-- Indexes for audit log queries
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON acl_audit_log (timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_principal ON acl_audit_log (principal_type, principal_id);
CREATE INDEX IF NOT EXISTS idx_audit_resource ON acl_audit_log (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_audit_decision ON acl_audit_log (decision);
CREATE INDEX IF NOT EXISTS idx_audit_workspace ON acl_audit_log (workspace);

-- Fallback Policy Configuration
-- Defines default behavior when no explicit rule exists
CREATE TABLE IF NOT EXISTS acl_fallback_policies
(
    policy_id             UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    rule_category         VARCHAR(100) NOT NULL UNIQUE, -- Category of rule (e.g., 'user_workspace', 'agent_workspace')
    fallback_access_level INTEGER      NOT NULL,        -- Default access level for this category
    updated_by            VARCHAR(255),
    updated_at            TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Initial fallback policies
-- These define system-wide defaults when no explicit ACL rule exists
-- NOTE: Using permissive defaults (READ_WRITE=20) for development; tighten for production
INSERT INTO acl_fallback_policies (rule_category, fallback_access_level, updated_by)
VALUES ('user_workspace', 20, '_system'),     -- READ_WRITE for development
       ('agent_workspace', 20, '_system'),    -- READ_WRITE for development
       ('user_agent', 20, '_system'),         -- READ_WRITE for development
       ('agent_agent', 20, '_system'),        -- READ_WRITE for development
       ('task_workspace', 20, '_system'),     -- READ_WRITE for development
       ('global_read', 20, '_system'),        -- READ_WRITE for development
       ('orchestrator_system', 20, '_system'),-- READ_WRITE for orchestrator operations
       ('user_kv_scope', 20, '_system'),      -- READ_WRITE: OBO-subject user accessing KV scopes
       ('agent_kv_scope', 20, '_system'),     -- READ_WRITE: agent accessing its own KV scopes
       ('task_kv_scope', 20, '_system'),      -- READ_WRITE: task accessing its own KV scopes
       ('service_kv_scope', 20, '_system')    -- READ_WRITE: service accessing its own KV scopes
ON CONFLICT (rule_category) DO NOTHING;

-- Default _global workspace rule
-- All authenticated principals have READ_WRITE access to the _global workspace
INSERT INTO acl_rules (principal_type, principal_id, resource_type, resource_id, access_level, granted_by, reason)
VALUES ('wildcard', '_any_authenticated', 'workspace', '_global', 20, '_system',
        'Default READ_WRITE access for all authenticated principals (development mode)')
ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING;

-- Function to clean up expired ACL rules (call periodically)
CREATE OR REPLACE FUNCTION cleanup_expired_acl_rules()
    RETURNS INTEGER AS
$$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE
    FROM acl_rules
    WHERE expires_at IS NOT NULL
      AND expires_at < NOW();

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old audit logs (call periodically, default 90 days retention)
CREATE OR REPLACE FUNCTION cleanup_old_audit_logs(retention_days INTEGER DEFAULT 90)
    RETURNS INTEGER AS
$$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE
    FROM acl_audit_log
    WHERE timestamp < NOW() - (retention_days || ' days')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- View for easy audit log analysis
CREATE OR REPLACE VIEW acl_audit_summary AS
SELECT DATE(timestamp)              as audit_date,
       decision,
       principal_type,
       resource_type,
       operation,
       COUNT(*)                     as decision_count,
       COUNT(DISTINCT principal_id) as unique_principals,
       COUNT(DISTINCT resource_id)  as unique_resources
FROM acl_audit_log
GROUP BY DATE(timestamp), decision, principal_type, resource_type, operation
ORDER BY audit_date DESC, decision_count DESC;

-- View for delegation chain analysis
CREATE OR REPLACE VIEW acl_delegation_summary AS
SELECT root_principal_type,
       root_principal_id,
       workspace,
       COUNT(*)                               as derived_count,
       COUNT(DISTINCT derived_principal_type) as derived_types,
       MIN(effective_access_level)            as min_access_level,
       MAX(effective_access_level)            as max_access_level
FROM acl_delegation_chains
GROUP BY root_principal_type, root_principal_id, workspace
ORDER BY derived_count DESC;
