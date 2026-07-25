-- 006_kv_scope_deny_by_default.sql
--
-- Parity with postgres migrations/031_kv_scope_deny_by_default.sql. See that
-- file for the full rationale; the short version:
--
--   * The row-based deny added in 004 encoded "deny" as access_level NONE(0),
--     which is the ABSENCE of an access level rather than an assertion of
--     denial. A real explicit-deny sentinel would be a distinct nonzero level
--     below READ(10) — a future change.
--   * Worse, it could not be carved out. The enforcer returns at the first
--     matching specificity tier, and a wildcard-principal rule on an exact
--     resource (tier 2) always outranks an sv::<impl>::* glob grant (tier 5).
--     A tier-1 exact rule is impossible because service identities embed the
--     pod name. platform-server was denied on kv_scope user-shared even holding
--     an explicit grant.
--
-- Deny-by-default in the FALLBACK expresses the same policy without the
-- tier-ordering pathology: an explicit grant becomes the only way into any
-- kv_scope, and grants at every tier work because nothing short-circuits ahead
-- of them. Net STRICTER than before — the old fallback handed out READ_WRITE(20)
-- on every scope that lacked a rule.
--
-- Owners of the *-exclusive scopes are unaffected: the gateway's owner fast-path
-- short-circuits ALLOW before the ACL is consulted.

-- 1) Drop the NONE rows added by 004 — superseded by the fallback below.
DELETE
FROM acl_rules
WHERE principal_type = 'wildcard'
  AND principal_id IN ('_any_authenticated', '_any_authenticated_user',
                       '_any_agent', '_any_task', '_any_service')
  AND resource_type = 'kv_scope'
  AND resource_id IN ('user-shared', 'user-workspace-shared')
  AND access_level = 0;

-- 2) Deny-by-default for every kv_scope: no explicit grant, no access.
UPDATE acl_fallback_policies
SET fallback_access_level = 0,
    updated_by            = '_system'
WHERE rule_category IN ('user_kv_scope', 'agent_kv_scope',
                        'task_kv_scope', 'service_kv_scope');
