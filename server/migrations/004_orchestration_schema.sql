-- Aether Orchestration Schema
-- Agent registry and orchestrator profile management

-- =============================================================================
-- Agent Registry
-- =============================================================================
-- Stores registered agent implementations and their launch parameters
-- Used by orchestrators to know how to start agents

CREATE TABLE agent_registry (
    implementation      VARCHAR(255) PRIMARY KEY,       -- Agent type identifier (e.g., 'code-assistant', 'data-processor')
    launch_params       JSONB NOT NULL,                 -- Orchestrator parameters (must include 'profile')
    description         TEXT,                           -- Human-readable description
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_registry_profile ON agent_registry((launch_params->>'profile'));

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_agent_registry_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER agent_registry_updated_at
    BEFORE UPDATE ON agent_registry
    FOR EACH ROW
    EXECUTE FUNCTION update_agent_registry_timestamp();

-- =============================================================================
-- Orchestrator Profiles
-- =============================================================================
-- Tracks which orchestrators support which profiles
-- Orchestrators register their supported profiles on connection
-- Used for load-balancing startup requests across available orchestrators

CREATE TABLE orchestrator_profiles (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    orchestrator_id     VARCHAR(255) NOT NULL,          -- Orchestrator identity string
    profile_name        VARCHAR(100) NOT NULL,          -- Profile this orchestrator supports
    workspace           VARCHAR(255) NOT NULL DEFAULT '_system',  -- Workspace scope
    last_heartbeat      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (orchestrator_id, profile_name)
);

CREATE INDEX idx_orch_profile_name ON orchestrator_profiles(profile_name);
CREATE INDEX idx_orch_profile_workspace ON orchestrator_profiles(workspace);
CREATE INDEX idx_orch_profile_heartbeat ON orchestrator_profiles(last_heartbeat);
CREATE INDEX idx_orch_profile_lookup ON orchestrator_profiles(profile_name, workspace, last_heartbeat);

-- =============================================================================
-- Orchestrated Task Queue
-- =============================================================================
-- Pub/sub queue for orchestration tasks (agent startup requests)
-- This is consumed by orchestrators matching the profile
--
-- TODO: Future schema additions for production robustness:
--   1. retry_count INT NOT NULL DEFAULT 0    -- Track delivery attempts
--   2. max_retries INT NOT NULL DEFAULT 3    -- Limit before DLQ
--   3. Add index: CREATE INDEX idx_orch_queue_stale_claims
--      ON orchestrated_task_queue(claimed_at) WHERE status = 'claimed';
--      (For detecting stuck/abandoned claims from crashed gateways)

CREATE TABLE orchestrated_task_queue (
    queue_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID NOT NULL,                  -- Reference to tasks table
    target_implementation VARCHAR(255) NOT NULL,        -- Agent type to start
    workspace           VARCHAR(255) NOT NULL,
    profile             VARCHAR(100) NOT NULL,          -- Required orchestrator profile
    launch_params       JSONB,                          -- Full launch parameters
    status              VARCHAR(50) NOT NULL DEFAULT 'pending',  -- pending, claimed, completed, failed
    claimed_by          VARCHAR(255),                   -- Orchestrator that claimed this
    claimed_at          TIMESTAMP,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMP,
    error_message       TEXT
);

CREATE INDEX idx_orch_queue_status ON orchestrated_task_queue(status);
CREATE INDEX idx_orch_queue_profile ON orchestrated_task_queue(profile) WHERE status = 'pending';
CREATE INDEX idx_orch_queue_workspace ON orchestrated_task_queue(workspace, profile) WHERE status = 'pending';

-- Notification for new orchestration tasks
CREATE OR REPLACE FUNCTION notify_orchestration_task()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('orchestration_task', json_build_object(
        'queue_id', NEW.queue_id::text,
        'task_id', NEW.task_id::text,
        'profile', NEW.profile,
        'workspace', NEW.workspace,
        'target_implementation', NEW.target_implementation
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orchestration_task_notify
    AFTER INSERT ON orchestrated_task_queue
    FOR EACH ROW
    EXECUTE FUNCTION notify_orchestration_task();
