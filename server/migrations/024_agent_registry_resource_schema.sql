-- Migration 024: Phase 5 agent-registry resource/capability/extension surface.
--
-- Extends agent_registry with the data model that lets a registration declare
-- what it exposes (owned resource type prefixes + permission verbs +
-- capability flags + A2A-style extension URIs). Stage A is data-model only:
-- ACL CheckAccess routing through these columns and uniqueness enforcement on
-- resource_type_prefix collisions land in Stage B.
--
-- All columns are nullable so legacy registrations (rows written before this
-- migration) continue to read back cleanly with empty/zero values on the
-- Phase 5 fields.

ALTER TABLE agent_registry
    ADD COLUMN IF NOT EXISTS resource_schema JSONB,
    ADD COLUMN IF NOT EXISTS capabilities    JSONB,
    ADD COLUMN IF NOT EXISTS extensions      JSONB;

-- Lookup helper: search registrations by resource_type_prefix. The GIN index
-- supports the @> containment query used by the Phase 5 uniqueness check
-- (Stage B). jsonb_path_ops is the smallest, fastest GIN variant for pure
-- containment queries.
CREATE INDEX IF NOT EXISTS idx_agent_registry_resource_schema_gin
    ON agent_registry USING GIN (resource_schema jsonb_path_ops)
    WHERE resource_schema IS NOT NULL;
