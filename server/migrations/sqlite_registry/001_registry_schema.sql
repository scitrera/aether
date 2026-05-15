-- AetherLite registry.db -- agent_registry + orchestrator_profiles schema
--
-- Native SQLite types only. Timestamps stored as ISO-8601 TEXT and parsed
-- inline by the Go implementation (no driver-level coercion). JSON columns
-- stored as TEXT and queried with json_extract where needed.
--
-- This is the per-domain migration tree for the Stage 2 native sqlite
-- registry.Store implementation. It mirrors the postgres schema defined in
-- migrations/004_orchestration_schema.sql but uses sqlite-native idioms.

-- =============================================================================
-- Agent Registry
-- =============================================================================

CREATE TABLE IF NOT EXISTS agent_registry (
    implementation TEXT PRIMARY KEY,
    launch_params  TEXT NOT NULL,       -- JSON object; must include "profile" key
    description    TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_agent_registry_updated ON agent_registry (updated_at);

CREATE TRIGGER IF NOT EXISTS agent_registry_updated_at
    AFTER UPDATE ON agent_registry
    FOR EACH ROW
BEGIN
    UPDATE agent_registry SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE implementation = NEW.implementation;
END;

-- =============================================================================
-- Orchestrator Profiles
-- =============================================================================

CREATE TABLE IF NOT EXISTS orchestrator_profiles (
    id              TEXT PRIMARY KEY,
    orchestrator_id TEXT NOT NULL,
    profile_name    TEXT NOT NULL,
    workspace       TEXT NOT NULL DEFAULT '_system',
    last_heartbeat  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (orchestrator_id, profile_name)
);

CREATE INDEX IF NOT EXISTS idx_orch_profile_name ON orchestrator_profiles (profile_name);
CREATE INDEX IF NOT EXISTS idx_orch_profile_workspace ON orchestrator_profiles (workspace);
CREATE INDEX IF NOT EXISTS idx_orch_profile_heartbeat ON orchestrator_profiles (last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_orch_profile_lookup ON orchestrator_profiles (profile_name, workspace, last_heartbeat);
