-- 005_drop_inert_global_wildcard_rule.sql
--
-- Parity with postgres migrations/030_drop_inert_global_wildcard_rule.sql.
--
-- Removes the `rule-default-global` row seeded by 001. It is inert for the same
-- reason as its postgres counterpart: principal_id is '_any_authenticated',
-- while wildcardSubjects() only ever builds _any_authenticated_user /
-- _any_agent / _any_task / _any_service. It matches no principal of any type,
-- and the glob pass skips it (no '*' or '?').
--
-- Removed rather than repaired because repairing would GRANT every
-- authenticated principal READ_WRITE on _global — a widening nobody has today,
-- which deserves its own decision rather than arriving as a spelling fix.
--
-- NO-OP FOR ACCESS: _global already resolves through the seeded fallback
-- policies (user/agent/task_workspace at READ_WRITE 20, seeded in 001), so a
-- real principal's decision is unchanged. This only stops the schema asserting
-- a grant that never existed.
--
-- Deleted by its coordinates rather than by rule_id so a hand-inserted variant
-- is cleaned up too.

DELETE
FROM acl_rules
WHERE principal_type = 'wildcard'
  AND principal_id = '_any_authenticated'
  AND resource_type = 'workspace'
  AND resource_id = '_global';
