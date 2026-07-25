-- 004_kv_scope_deny_parity.sql
--
-- Parity with postgres migrations/019_kv_new_scope_fallbacks.sql (as repaired
-- by 029_fix_wildcard_principal_drift.sql).
--
-- THE GAP
-- -------
-- The postgres tree seeds DEFAULT-DENY rules on the two cross-agent shared
-- per-user KV scopes; this SQLite tree never did. lite and full are meant to
-- differ in deployment logistics, not behaviour, so that was a real functional
-- divergence: every lite tenant permitted what full tenants were meant to deny.
--
-- Why an explicit rule is required rather than a fallback: acl_fallback_policies
-- is keyed by (principal_type, resource_type) and cannot distinguish scope
-- NAMES, so it cannot express "kv_scope is open EXCEPT these two". Both trees
-- seed user/agent/task/service_kv_scope at READ_WRITE(20) — identical in
-- postgres 003 and sqlite 001 — so with no explicit deny, a cross-agent read of
-- another user's shared KV resolves:
--
--   enforcer finds no rule -> applyFallback(<type>, "kv_scope") -> 20 -> ALLOWED
--
-- These scopes hold privacy-sensitive per-user data (billing, API keys, OAuth
-- tokens), which is exactly what 019 exists to protect.
--
-- ONE ROW PER WILDCARD SUBJECT: "all authenticated principals" cannot be
-- expressed as a single row. wildcardSubjects() (internal/acl/enforcer.go)
-- derives the wildcard from the principal TYPE and only ever produces
-- _any_authenticated_user / _any_agent / _any_task / _any_service. Note the
-- `_user` suffix — 001 seeds the _global rule as `_any_authenticated` WITHOUT
-- it, which matches nothing; do not copy that spelling.
--
-- DELIBERATELY NOT FIXED HERE: 001's `_global` workspace row carries the same
-- inert spelling, but it is a GRANT — re-animating it would WIDEN access for
-- every authenticated principal. Postgres 029 leaves its counterpart inert for
-- the same reason, so leaving it alone here KEEPS the two trees in parity.
-- Enabling it is a separate, deliberate decision on both sides at once.
--
-- ROLLOUT NOTE: this TIGHTENS access on lite tenants. Anything relying on the
-- current permissive behaviour to read another agent's user-shared KV will
-- begin to be denied — the intended policy, but a real behaviour change.
--
-- Idempotent: ON CONFLICT on the unique_acl_rule key. The runner wraps each
-- migration in a transaction, so no BEGIN/COMMIT here (same as the pg tree).

INSERT INTO acl_rules (rule_id, principal_type, principal_id, resource_type, resource_id,
                       access_level, granted_by, reason)
VALUES ('rule-deny-user-shared-user',    'wildcard', '_any_authenticated_user', 'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-deny-user-shared-agent',   'wildcard', '_any_agent',              'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-deny-user-shared-task',    'wildcard', '_any_task',               'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-deny-user-shared-service', 'wildcard', '_any_service',            'kv_scope', 'user-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-deny-uws-user',            'wildcard', '_any_authenticated_user', 'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-deny-uws-agent',           'wildcard', '_any_agent',              'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-deny-uws-task',            'wildcard', '_any_task',               'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO'),
       ('rule-deny-uws-service',         'wildcard', '_any_service',            'kv_scope', 'user-workspace-shared', 0, '_system',
        'Default DENY on cross-agent shared per-user-per-workspace KV (privacy-sensitive); requires explicit grant or OBO')
ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING;

-- Defensive: this tree never seeded the misspelled rows, but a hand-added one
-- would silently shadow nothing while looking authoritative. Mirrors the
-- cleanup in postgres 029 so the two trees converge on identical state.
DELETE
FROM acl_rules
WHERE principal_type = 'wildcard'
  AND principal_id = '_any_authenticated'
  AND resource_type = 'kv_scope'
  AND resource_id IN ('user-shared', 'user-workspace-shared');
