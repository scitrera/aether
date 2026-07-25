-- 030_drop_inert_global_wildcard_rule.sql
--
-- Remove the `_global` workspace rule seeded by 003. It has never had any
-- effect, and keeping a dead row that reads like a grant is worse than not
-- having one: it invites the reader (and the superadmin ACL console) to believe
-- authenticated principals hold READ_WRITE on _global by rule, when they do not.
--
-- WHY IT IS INERT: the row is
--   ('wildcard', '_any_authenticated', 'workspace', '_global', 20)
-- but wildcardSubjects() (internal/acl/enforcer.go) derives the wildcard subject
-- from the principal TYPE and only ever produces _any_authenticated_user /
-- _any_agent / _any_task / _any_service. There is no '_any_authenticated'
-- (missing the `_user` suffix) and no alias; findGlobMatch skips it too, having
-- no '*' or '?'. It therefore matches no principal of any type.
--
-- WHY REMOVE RATHER THAN REPAIR: repairing it would GRANT every authenticated
-- principal READ_WRITE on _global — access nobody currently has. That is a
-- widening, and a widening should be a deliberate decision with its own
-- rationale, not a side effect of fixing a spelling.
--
-- THIS IS A NO-OP FOR ACCESS. _global is already reachable via the seeded
-- fallback policies (user_workspace / agent_workspace / task_workspace at
-- READ_WRITE 20, seeded in 003 alongside the rule), so evaluation for a real
-- principal already resolves:
--   enforcer finds no rule (this one never matched) -> applyFallback -> 20
-- Deleting the row changes no decision; it only stops the schema asserting
-- something untrue. Nothing reads it by literal coordinates (verified across
-- the server and the platform backend).
--
-- FOLLOW-ON NOTE: if those *_workspace fallbacks are ever tightened for
-- production, _global will need a REAL grant — to wildcard subjects that
-- actually resolve. That was already true before this migration, since the row
-- being deleted never contributed anything.
--
-- Paired with sqlite_acl/005 so the two trees stay in lockstep (lite and full
-- are meant to differ in deployment logistics, not behaviour).

DELETE
FROM acl_rules
WHERE principal_type = 'wildcard'
  AND principal_id = '_any_authenticated'
  AND resource_type = 'workspace'
  AND resource_id = '_global';
