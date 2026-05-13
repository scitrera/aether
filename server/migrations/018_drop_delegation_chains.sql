-- Migration 018: Drop legacy delegation chains
-- Legacy DelegationChain feature (gateway/internal/acl) was never wired up at the call site
-- (EstablishDelegationChain was unused). OBO authority grants (acl_authority_grants) replaced
-- it. This migration drops the orphaned table and recreates the acl_audit_log compatibility
-- view without the JSON-derived delegation_chain_id column so the schema honestly reflects
-- the code.

-- 1. Drop the summary view that depends on the table
DROP VIEW IF EXISTS acl_delegation_summary;

-- 2. Drop the table
DROP TABLE IF EXISTS acl_delegation_chains;

-- 3. Recreate the acl_audit_log compatibility view (latest definition is from
--    012_authority_grants_phase0.sql:95-122) WITHOUT the delegation_chain_id field.
--    Must DROP first because CREATE OR REPLACE cannot remove columns.
--    All other columns and the JSON metadata extraction are preserved.
DROP VIEW IF EXISTS acl_audit_log;
CREATE VIEW acl_audit_log AS
SELECT audit_id,
       timestamp,
       COALESCE(metadata ->> 'decision', CASE WHEN success THEN 'ALLOW' ELSE 'DENY' END) AS decision,
       (metadata ->> 'access_level')::integer                                            AS access_level,
       actor_type                                                                        AS principal_type,
       actor_id                                                                          AS principal_id,
       subject_type,
       subject_id,
       root_subject_type,
       root_subject_id,
       authority_mode,
       root_authority_grant_id,
       authority_grant_id,
       parent_authority_grant_id,
       resource_type,
       resource_id,
       operation,
       workspace,
       (metadata ->> 'rule_id')::uuid                                                    AS rule_id,
       COALESCE((metadata ->> 'fallback_applied')::boolean, false)                       AS fallback_applied,
       gateway_id,
       session_id,
       metadata
FROM comprehensive_audit_log
WHERE event_type = 'authorization';
