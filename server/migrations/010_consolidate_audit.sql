-- Migration: Consolidate ACL audit log into comprehensive audit log
-- The acl_audit_log and comprehensive_audit_log tables overlap significantly.
-- This migration moves ACL audit writes to comprehensive_audit_log (using event_type='authorization')
-- and creates a backward-compatible view over the new table.

-- Step 1: Migrate existing acl_audit_log data into comprehensive_audit_log
INSERT INTO comprehensive_audit_log (
    timestamp, event_type, actor_type, actor_id,
    resource_type, resource_id, operation, workspace,
    session_id, gateway_id, success, error_message, metadata
)
SELECT
    a.timestamp,
    'authorization' AS event_type,
    a.principal_type AS actor_type,
    a.principal_id AS actor_id,
    a.resource_type,
    a.resource_id,
    a.operation,
    a.workspace,
    a.session_id,
    a.gateway_id,
    (a.decision = 'ALLOW') AS success,
    CASE WHEN a.decision = 'DENY' THEN 'Access denied' ELSE NULL END AS error_message,
    jsonb_build_object(
        'decision', a.decision,
        'access_level', a.access_level,
        'fallback_applied', a.fallback_applied,
        'rule_id', a.rule_id,
        'delegation_chain_id', a.delegation_chain_id
    ) || COALESCE(a.metadata, '{}'::jsonb) AS metadata
FROM acl_audit_log a
ON CONFLICT DO NOTHING;

-- Step 2: Drop the old table (CASCADE to drop dependent functions/views)
DROP TABLE IF EXISTS acl_audit_log CASCADE;

-- Step 3: Create a backward-compatible view for code that still queries acl_audit_log
CREATE OR REPLACE VIEW acl_audit_log AS
SELECT
    audit_id,
    timestamp,
    COALESCE(metadata->>'decision', CASE WHEN success THEN 'ALLOW' ELSE 'DENY' END) AS decision,
    (metadata->>'access_level')::integer AS access_level,
    actor_type AS principal_type,
    actor_id AS principal_id,
    resource_type,
    resource_id,
    operation,
    workspace,
    (metadata->>'delegation_chain_id')::uuid AS delegation_chain_id,
    (metadata->>'rule_id')::uuid AS rule_id,
    COALESCE((metadata->>'fallback_applied')::boolean, false) AS fallback_applied,
    gateway_id,
    session_id,
    metadata
FROM comprehensive_audit_log
WHERE event_type = 'authorization';

-- Step 4: Drop old cleanup function (signature may differ) and recreate for consolidated table
DROP FUNCTION IF EXISTS cleanup_old_audit_logs(integer);
CREATE OR REPLACE FUNCTION cleanup_old_audit_logs(retention_days INTEGER DEFAULT 90)
RETURNS bigint AS $$
DECLARE
    deleted_count bigint;
BEGIN
    DELETE FROM comprehensive_audit_log
    WHERE timestamp < NOW() - (retention_days || ' days')::interval;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
