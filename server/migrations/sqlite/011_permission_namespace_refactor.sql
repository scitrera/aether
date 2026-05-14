-- Migration 011: Permission namespace refactor.
-- SQLite counterpart of postgres migration 020_permission_namespace_refactor.sql.
--
-- Migrates legacy `_perm:*` ACL rules from resource_type='permission' to the
-- typed 'admin/*' and 'capability/*' resource families. Identical UPDATE
-- semantics to the postgres version — sqlite's CASE/IN syntax is the same.
-- Idempotent: only affects rows whose resource_id is in the known whitelist.
-- Unknown _perm:* rows are left for the in-process rewriteLegacyPermission
-- alias layer to translate at read time.

UPDATE acl_rules
SET resource_type = CASE
    WHEN resource_id IN ('_perm:admin_operations', '_perm:admin_acl', '_perm:admin_tokens',
                         '_perm:admin_workspaces', '_perm:admin_agents') THEN 'admin'
    ELSE 'capability'
END,
    resource_id = CASE resource_id
        WHEN '_perm:admin_operations' THEN 'admin/*'
        WHEN '_perm:admin_acl' THEN 'admin/acl'
        WHEN '_perm:admin_tokens' THEN 'admin/tokens'
        WHEN '_perm:admin_workspaces' THEN 'admin/workspaces'
        WHEN '_perm:admin_agents' THEN 'admin/agents'
        WHEN '_perm:create_workspace' THEN 'capability/create_workspace'
        WHEN '_perm:exchange_authority_grants' THEN 'capability/exchange_authority_grants'
        WHEN '_perm:authority_intermediary' THEN 'capability/authority_intermediary'
        WHEN '_perm:metric_credit' THEN 'capability/metric_credit'
        WHEN '_perm:resolve_authority' THEN 'capability/resolve_authority'
        WHEN '_perm:query_connections' THEN 'capability/query_connections'
    END
WHERE resource_type = 'permission' AND resource_id IN (
    '_perm:admin_operations', '_perm:admin_acl', '_perm:admin_tokens',
    '_perm:admin_workspaces', '_perm:admin_agents',
    '_perm:create_workspace', '_perm:exchange_authority_grants',
    '_perm:authority_intermediary', '_perm:metric_credit',
    '_perm:resolve_authority', '_perm:query_connections'
);
