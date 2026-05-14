-- Migration 010: Default-deny ACL rules for the new shared KV scopes.
-- SQLite counterpart of postgres migration 019_kv_new_scope_fallbacks.sql.
--
-- Two wildcard DENY rules cover cross-agent reads of per-user KV scopes:
--   - user-shared              (cross-agent per-user)
--   - user-workspace-shared    (cross-agent per-user-per-workspace)
--
-- The exclusive scopes (global-exclusive, workspace-exclusive) are handled
-- by the gateway's owner fast-path and do not need explicit rules.
--
-- The unique constraint on acl_rules is named "unique_acl_rule"
-- (principal_type, principal_id, resource_type, resource_id) per
-- 001_full_schema.sql; ON CONFLICT against that target makes this safe to
-- re-run.

INSERT INTO acl_rules (rule_id, principal_type, principal_id, resource_type, resource_id, access_level, granted_by, reason)
VALUES ('rule-kv-user-shared-deny', 'wildcard', '_any_authenticated', 'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-kv-user-workspace-shared-deny', 'wildcard', '_any_authenticated', 'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO')
ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING;
