-- Migration 009: Drop legacy delegation chains.
-- SQLite counterpart of postgres migration 018_drop_delegation_chains.sql.
--
-- The legacy acl_delegation_chains table was never created in
-- 001_full_schema.sql (the consolidated schema deliberately omitted it),
-- and the acl_audit_log / acl_delegation_summary postgres VIEWs are not
-- materialised in sqlite either.
--
-- DROP TABLE IF EXISTS is safe in sqlite and self-guards against missing
-- tables, so we issue it for any aether-lite database created via a
-- hand-rolled schema that may have included the legacy table. Same for
-- the views.

DROP VIEW IF EXISTS acl_delegation_summary;
DROP VIEW IF EXISTS acl_audit_log;
DROP TABLE IF EXISTS acl_delegation_chains;
