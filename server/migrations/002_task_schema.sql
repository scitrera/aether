-- Aether Unified Task Schema
-- Supports both messaging delivery and orchestration patterns
-- Phase 6 identity model: workspace, implementation, specifier

-- =============================================================================
-- Unified Tasks Table
-- =============================================================================

CREATE TABLE tasks (
    -- Primary identification
    task_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type           VARCHAR(100) NOT NULL,          -- 'agent_startup', 'data_processing', 'message_delivery', etc.

    -- Identity-based addressing (Aether model)
    workspace           VARCHAR(255) NOT NULL,          -- Tenant/namespace
    implementation      VARCHAR(255),                   -- Agent type (optional for some task types)
    specifier           VARCHAR(255),                   -- Agent instance (optional)

    -- Lifecycle status
    status              VARCHAR(50) NOT NULL DEFAULT 'pending',  -- pending, assigned, running, completed, failed, cancelled, dlq
    priority            INT NOT NULL DEFAULT 0,

    -- Timestamps
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    scheduled_for       TIMESTAMP,                      -- For delayed tasks
    started_at          TIMESTAMP,                      -- When worker started processing
    completed_at        TIMESTAMP,
    failed_at           TIMESTAMP,

    -- Assignment tracking
    assigned_to         VARCHAR(255),                   -- Agent identity string (ag.workspace.impl.spec)
    assigned_at         TIMESTAMP,

    -- Orchestration fields
    assignment_mode     VARCHAR(50) DEFAULT 'self_assign',    -- 'self_assign', 'targeted', 'pool', 'broadcast'
    task_category       VARCHAR(50) DEFAULT 'regular',        -- 'regular', 'orchestrated', 'system'
    target_agent_id     VARCHAR(255),                         -- For targeted mode
    target_implementation VARCHAR(255),                       -- For pool/orchestrated modes
    launch_params       JSONB,                                -- Orchestrator parameters
    queued_for_startup  BOOLEAN DEFAULT FALSE,                -- Task waiting for agent to start
    parent_agent_id     VARCHAR(255),                         -- Parent agent that created this task

    -- Retry handling
    retry_count         INT NOT NULL DEFAULT 0,
    max_retries         INT NOT NULL DEFAULT 3,
    next_retry_at       TIMESTAMP,

    -- Error tracking
    error_message       TEXT,
    error_type          VARCHAR(100),

    -- Payload and metadata
    payload             BYTEA,                          -- Raw payload for delivery tasks
    metadata            JSONB,                          -- Extensible task metadata
    checkpoint_data     JSONB,                          -- Latest checkpoint state

    -- Timeout configuration (milliseconds)
    schedule_to_start_ms   BIGINT,                      -- Max time from creation to worker pickup
    start_to_close_ms      BIGINT,                      -- Max time from start to completion
    heartbeat_timeout_ms   BIGINT,                      -- Max time between heartbeats
    schedule_to_close_ms   BIGINT,                      -- Max total time from creation to completion

    -- Heartbeat tracking
    last_heartbeat      TIMESTAMP,
    heartbeat_details   JSONB,                          -- Progress data from worker

    -- Messaging support (for delivery tasks)
    target_topic        VARCHAR(255),                   -- RabbitMQ topic for delivery
    source_topic        VARCHAR(255),                   -- Origin topic
    message_type        VARCHAR(50)                     -- CHAT, CONTROL, EVENT, METRIC, etc.
);

-- =============================================================================
-- Task Indexes
-- =============================================================================

-- Status-based queries (most common)
CREATE INDEX idx_task_status ON tasks(status);
CREATE INDEX idx_task_status_workspace ON tasks(status, workspace);
CREATE INDEX idx_task_status_created ON tasks(status, created_at);

-- Workspace queries
CREATE INDEX idx_task_workspace ON tasks(workspace);
CREATE INDEX idx_task_workspace_status ON tasks(workspace, status);

-- Assignment queries
CREATE INDEX idx_task_assigned_to ON tasks(assigned_to);
CREATE INDEX idx_task_target_agent ON tasks(target_agent_id);
CREATE INDEX idx_task_target_impl ON tasks(target_implementation);

-- Orchestration queries
CREATE INDEX idx_task_queued_startup ON tasks(target_agent_id) WHERE queued_for_startup = true;
CREATE INDEX idx_task_category ON tasks(task_category);
CREATE INDEX idx_task_orchestrated ON tasks(task_category) WHERE task_category = 'orchestrated';

-- Retry scheduling
CREATE INDEX idx_task_next_retry ON tasks(next_retry_at) WHERE status = 'failed' AND next_retry_at IS NOT NULL;

-- Timeout detection
CREATE INDEX idx_task_scheduled ON tasks(scheduled_for) WHERE status = 'pending' AND scheduled_for IS NOT NULL;

-- =============================================================================
-- Task Timers (for deadline and retry scheduling)
-- =============================================================================

CREATE TABLE task_timers (
    timer_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    timer_type          VARCHAR(50) NOT NULL,           -- 'schedule_to_start', 'start_to_close', 'heartbeat', 'retry'
    fires_at            TIMESTAMP NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    fired               BOOLEAN DEFAULT FALSE,
    fired_at            TIMESTAMP,
    metadata            JSONB
);

CREATE INDEX idx_timer_pending ON task_timers(fires_at) WHERE NOT fired;
CREATE INDEX idx_timer_task ON task_timers(task_id);
CREATE INDEX idx_timer_type_pending ON task_timers(timer_type, fires_at) WHERE NOT fired;

-- =============================================================================
-- Task Assignments (history of who worked on a task)
-- =============================================================================

CREATE TABLE task_assignments (
    assignment_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    worker_identity     VARCHAR(255) NOT NULL,          -- Full identity string
    assigned_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    failed              BOOLEAN DEFAULT FALSE,
    failure_reason      TEXT
);

CREATE INDEX idx_assignment_task ON task_assignments(task_id);
CREATE INDEX idx_assignment_worker ON task_assignments(worker_identity);
CREATE INDEX idx_assignment_time ON task_assignments(assigned_at);

-- =============================================================================
-- Task Checkpoints (for resumable long-running tasks)
-- =============================================================================

CREATE TABLE task_checkpoints (
    checkpoint_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    sequence_number     INT NOT NULL,
    checkpoint_data     JSONB NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by          VARCHAR(255),                   -- Worker identity
    UNIQUE (task_id, sequence_number)
);

CREATE INDEX idx_checkpoint_task_seq ON task_checkpoints(task_id, sequence_number DESC);

-- =============================================================================
-- Task Audit Events (detailed event log for traceability)
-- =============================================================================

CREATE TABLE task_audit_events (
    event_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    event_type          VARCHAR(50) NOT NULL,           -- 'created', 'assigned', 'started', 'completed', 'failed', etc.
    event_data          JSONB,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by          VARCHAR(255)                    -- Identity that triggered event
);

CREATE INDEX idx_audit_task ON task_audit_events(task_id);
CREATE INDEX idx_audit_task_time ON task_audit_events(task_id, created_at);
CREATE INDEX idx_audit_type ON task_audit_events(event_type);
CREATE INDEX idx_audit_time ON task_audit_events(created_at);

-- =============================================================================
-- Dead Letter Queue (for tasks that exhausted retries)
-- =============================================================================

CREATE TABLE dlq (
    dlq_message_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_task_id    UUID NOT NULL,                  -- May reference deleted task
    category            VARCHAR(50) NOT NULL,           -- 'exhausted_retries', 'timeout', 'poison_message'
    workspace           VARCHAR(255) NOT NULL,

    original_payload    BYTEA,
    original_metadata   JSONB,
    failure_reason      TEXT NOT NULL,
    failure_details     JSONB,

    enqueued_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    attempt_count       INT NOT NULL,
    last_attempt_at     TIMESTAMP NOT NULL,

    reprocessed_at      TIMESTAMP,
    resolved            BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_by         VARCHAR(255),
    resolution_notes    TEXT
);

CREATE INDEX idx_dlq_workspace ON dlq(workspace);
CREATE INDEX idx_dlq_category ON dlq(category);
CREATE INDEX idx_dlq_unresolved ON dlq(resolved, enqueued_at) WHERE NOT resolved;

-- =============================================================================
-- Triggers for automatic timestamp updates
-- =============================================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tasks_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- Notification triggers for real-time events
-- =============================================================================

CREATE OR REPLACE FUNCTION notify_task_event()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM pg_notify('task_event', json_build_object(
            'op', 'INSERT',
            'task_id', NEW.task_id::text,
            'status', NEW.status,
            'workspace', NEW.workspace,
            'task_type', NEW.task_type
        )::text);
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        PERFORM pg_notify('task_event', json_build_object(
            'op', 'UPDATE',
            'task_id', NEW.task_id::text,
            'old_status', OLD.status,
            'new_status', NEW.status,
            'workspace', NEW.workspace
        )::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER task_event_notify
    AFTER INSERT OR UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION notify_task_event();

CREATE OR REPLACE FUNCTION notify_timer_event()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('timer_event', json_build_object(
        'op', 'INSERT',
        'timer_id', NEW.timer_id::text,
        'task_id', NEW.task_id::text,
        'timer_type', NEW.timer_type,
        'fires_at', NEW.fires_at::text
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER timer_insert_notify
    AFTER INSERT ON task_timers
    FOR EACH ROW
    EXECUTE FUNCTION notify_timer_event();
