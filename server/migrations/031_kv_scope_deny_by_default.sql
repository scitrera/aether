-- 031_kv_scope_deny_by_default.sql
--
-- Replace the ROW-based deny on the shared per-user KV scopes with a
-- deny-by-default FALLBACK. Net effect is STRICTER, not weaker.
--
-- WHAT WAS WRONG WITH THE ROW-BASED DENY (019, repaired by 029)
-- ------------------------------------------------------------
-- Two problems, one conceptual and one structural.
--
-- Conceptual: it encoded "deny" as access_level NONE(0). NONE is the absence of
-- an access level, not an assertion of denial. A real explicit-deny sentinel
-- would be a distinct nonzero level below READ(10) — that is a future change;
-- this migration simply stops leaning on NONE to mean something it does not.
--
-- Structural: it could not be carved out. The enforcer resolves by specificity
-- TIER and returns at the first match:
--
--   1. subject set + exact resource
--   2. wildcard principal + exact resource   <- the NONE rows matched HERE
--   3. subject set + wildcard resource
--   4. wildcard principal + wildcard resource
--   5. glob rules                            <- sv::<impl>::* grants live HERE
--
-- so a legitimate service holding a tier-5 glob grant could never outrank the
-- tier-2 deny, and a tier-1 exact rule is impossible because service identities
-- embed the pod name. Observed live: platform-server writes per-user session
-- state (ti:<tenant>:session.wnd_<id>) to kv_scope user-shared and was denied
-- "Wildcard rule: NONE" even WITH an explicit sv::platform-server::* grant.
--
-- WHAT THIS DOES INSTEAD
-- ----------------------
-- Drops the NONE rows and sets the four *_kv_scope fallback policies to 0, so
-- an explicit grant becomes the ONLY way into ANY kv_scope. Deny-by-default,
-- expressed where defaults belong, with no tier-ordering pathology: grants at
-- any tier now work, because nothing short-circuits ahead of them.
--
-- This realizes what 019 already ASSUMED. Its own comment on the exclusive
-- scopes reads: "Cross-agent access (via OBO) still requires an explicit grant,
-- which the absence of a fallback enforces correctly" — written as though no
-- kv_scope fallback existed. One did (all four at READ_WRITE 20, seeded in 003),
-- which is why the shared-scope deny was needed as a patch in the first place.
--
-- WHO KEEPS ACCESS (all via explicit acl_seed grants, verified against a live
-- tenant's seeded rule set):
--   wildcard:_any_service  @40  global, workspace, user, user-workspace
--   wildcard:_any_agent    @20  global, workspace, user, user-workspace
--   sv::platform-bridge::* @20  kv_scope:*
--   sv::platform-server::* @20  user-shared, user-workspace-shared
--
-- WHO LOSES IT — deliberately:
--   * anything reaching user-shared / user-workspace-shared without a grant
--     (this was 019's target: cross-agent reads of billing, API keys, OAuth
--     tokens);
--   * cross-principal access to global-exclusive / workspace-exclusive. Owners
--     are unaffected — the gateway's owner fast-path short-circuits ALLOW before
--     the ACL when the caller matches the scope's embedded identity segment.
--
-- Paired with sqlite_acl/006 so lite and full stay in lockstep.

-- 1) Drop the NONE rows added by 019/029 — superseded by the fallback below.
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
