-- 029_fix_wildcard_principal_drift.sql
--
-- Repair seeded wildcard ACL rows that can never match a principal.
--
-- THE DRIFT
-- ---------
-- Migrations 003 and 019 seed rows with principal_id '_any_authenticated'.
-- The enforcer never looks that subject up. wildcardSubjects()
-- (internal/acl/enforcer.go) derives the wildcard subject from the principal
-- TYPE, and the only spellings it produces are:
--
--     user    -> wildcard:_any_authenticated_user   (WildcardAnyAuthenticatedUser)
--     agent   -> wildcard:_any_agent
--     task    -> wildcard:_any_task
--     service -> wildcard:_any_service
--
-- There is no '_any_authenticated' (no `_user` suffix) and no alias. The glob
-- pass cannot rescue these rows either: findGlobMatch skips policies with no
-- '*' or '?' in them. So every row seeded with that principal_id is inert.
--
-- WHY THAT MATTERS (this is not cosmetic)
-- ---------------------------------------
-- 019's rows are DEFAULT-DENY (access_level 0) on the two cross-agent shared
-- per-user KV scopes, which its own comment describes as privacy-sensitive:
-- "billing, API keys, OAuth tokens". 019 exists precisely because
-- acl_fallback_policies is keyed by (principal_type, resource_type) and cannot
-- distinguish scope names, so per-scope denies MUST be explicit rows.
--
-- Because those rows never match, the evaluation falls through instead:
--   Service.evaluateAccessNoAudit -> enforcer returns nil (no rule matched)
--     -> applyFallback(principal, "kv_scope")
--     -> RuleCategory(<type>, "kv_scope") = "<type>_kv_scope"
--     -> 003 seeds user/agent/task/service_kv_scope at 20 (READ_WRITE)
--     -> ALLOWED
--
-- i.e. the intended deny is inverted into a read-write allow. This migration
-- restores it, writing ONE ROW PER WILDCARD SUBJECT because "all authenticated
-- principals" cannot be expressed as a single row under the current enforcer.
--
-- DELIBERATELY NOT FIXED HERE: 003's '_global' workspace row (level 20). That
-- one is a GRANT, so re-animating it would WIDEN access — every authenticated
-- principal would gain READ_WRITE on the _global workspace, which on a
-- deployment that has run without it is a behaviour change, not a repair. It is
-- left inert on purpose; enable it deliberately if that default is wanted.
--
-- ROLLOUT NOTE: this TIGHTENS access. Any caller that has been relying on the
-- accidental allow to read another agent's user-shared KV will begin to be
-- denied — which is the intended policy, but it is a real behaviour change and
-- should be rolled out knowingly rather than discovered.
--
-- Idempotent: ON CONFLICT DO NOTHING on the unique_acl_rule key, and the old
-- inert rows are removed afterwards so the two spellings cannot coexist.

-- 1) Seed the deny for every wildcard subject the enforcer actually consults.
INSERT INTO acl_rules (principal_type, principal_id, resource_type, resource_id, access_level, granted_by, reason)
VALUES ('wildcard', '_any_authenticated_user', 'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_agent', 'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_task', 'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_service', 'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_authenticated_user', 'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_agent', 'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_task', 'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO'),
       ('wildcard', '_any_service', 'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO')
ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING;

-- 2) Drop the inert originals so the misspelling does not linger and get copied
--    into new rules. Scoped to the kv_scope rows this migration replaces; the
--    '_global' workspace row is left alone (see the note above).
DELETE
FROM acl_rules
WHERE principal_type = 'wildcard'
  AND principal_id = '_any_authenticated'
  AND resource_type = 'kv_scope'
  AND resource_id IN ('user-shared', 'user-workspace-shared');
