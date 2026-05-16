-- Migration: Phase 5 agent-registry resource/capability/extension surface.
--
-- Mirrors postgres migration 024 for AetherLite's per-domain registry.db.
-- All columns are nullable so legacy rows continue to read back cleanly.

ALTER TABLE agent_registry ADD COLUMN resource_schema TEXT;  -- JSON-encoded array
ALTER TABLE agent_registry ADD COLUMN capabilities    TEXT;  -- JSON-encoded map
ALTER TABLE agent_registry ADD COLUMN extensions      TEXT;  -- JSON-encoded array

-- SQLite has no JSONB GIN index; per-prefix uniqueness queries (Stage B) will
-- do a full scan over agent_registry rows. The registry is small N (handful
-- of registrations per deployment) and queries run on the admin/registration
-- path, not on the hot CheckAccess path, so the scan cost is acceptable.
