-- 019_kv_new_scope_fallbacks.sql
--
-- Default ACL rules for the new KV scopes introduced in the scope revamp:
--   - global-exclusive          (per-agent tenant-wide)
--   - workspace-exclusive       (per-agent per-workspace)
--   - user-shared               (cross-agent per-user)
--   - user-workspace-shared     (cross-agent per-user-per-workspace)
--
-- Strategy: explicit wildcard rules. The acl_fallback_policies table is
-- keyed by (principal_type, resource_type) and cannot distinguish
-- between scope names (global vs user-shared), so per-scope defaults
-- must be encoded as wildcard acl_rules rows.
--
--   * The two SHARED user scopes (user-shared, user-workspace-shared)
--     get DEFAULT-DENY rules. Cross-agent reads of per-user data are
--     privacy-sensitive (billing, API keys, OAuth tokens) and require
--     explicit grants or OBO authority.
--   * The two EXCLUSIVE scopes (global-exclusive, workspace-exclusive)
--     do NOT need explicit rules: the gateway's owner fast-path
--     short-circuits ALLOW when the caller's identity matches the
--     embedded `agent:{impl}|{spec}` segment. Cross-agent access (via
--     OBO) still requires an explicit grant, which the absence of a
--     fallback enforces correctly.

INSERT INTO acl_rules (principal_type, principal_id, resource_type, resource_id, access_level, granted_by, reason)
VALUES ('wildcard', '_any_authenticated', 'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_authenticated', 'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO')
ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING;
