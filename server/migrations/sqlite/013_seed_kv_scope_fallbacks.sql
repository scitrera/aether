-- Migration 013: Backfill the four kv_scope ACL fallback policies that the
-- monolithic 001_full_schema.sql omitted.
--
-- Parity gap: postgres `003_acl_schema.sql:88-99` seeds 11 fallback policy
-- rows, including the four kv_scope categories. The forked sqlite monolithic
-- migration only seeded 7 — missing user_kv_scope / agent_kv_scope /
-- task_kv_scope / service_kv_scope.
--
-- Observable consequence in lite mode: the ws-server's Service principal
-- (sv::platform-server::*) attempting KV writes to user-bound scopes
-- (e.g. chat_active_task at user-workspace-shared) hits ACL evaluation
-- with no specific rule, no service_kv_scope fallback, and the wildcard
-- "_any_authenticated:NONE" deny from migration 010 → denied. Full mode
-- has the fallback at READ_WRITE so the same op succeeds.
--
-- Values match postgres 003: all four categories at READ_WRITE (20). This
-- is the dev-mode posture; production tightens via SetFallbackPolicy.
--
-- ON CONFLICT against rule_category (UNIQUE constraint per 001_full_schema)
-- makes this idempotent — running against an existing DB is a no-op.

INSERT INTO acl_fallback_policies (policy_id, rule_category, fallback_access_level, updated_by)
VALUES
    ('pol-user-kv-scope',    'user_kv_scope',    20, '_system'),
    ('pol-agent-kv-scope',   'agent_kv_scope',   20, '_system'),
    ('pol-task-kv-scope',    'task_kv_scope',    20, '_system'),
    ('pol-service-kv-scope', 'service_kv_scope', 20, '_system')
ON CONFLICT (rule_category) DO NOTHING;
