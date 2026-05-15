-- Native SQLite schema for the tasks domain (Stage 2).
--
-- Tables: tasks, task_timers, task_assignments, task_checkpoints,
--         task_audit_events, dlq, orchestrated_task_queue.
--
-- Conventions:
--   - Timestamps as ISO-8601 TEXT via strftime('%Y-%m-%dT%H:%M:%f', 'now')
--   - JSON columns as TEXT
--   - INTEGER PRIMARY KEY AUTOINCREMENT where applicable
--   - No PL/pgSQL, no pg_notify triggers, no partitioning
--   - Parameterized SQL only (no stored functions)

-- =============================================================================
-- Tasks
-- =============================================================================

CREATE TABLE IF NOT EXISTS tasks (
    task_id                 TEXT PRIMARY KEY,
    task_type               TEXT NOT NULL,
    task_class              INTEGER NOT NULL DEFAULT 0,

    workspace               TEXT NOT NULL,
    implementation          TEXT,
    specifier               TEXT,

    status                  TEXT NOT NULL DEFAULT 'pending',
    priority                INTEGER NOT NULL DEFAULT 0,

    created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    updated_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    scheduled_for           TEXT,
    started_at              TEXT,
    completed_at            TEXT,
    failed_at               TEXT,

    assigned_to             TEXT,
    assigned_at             TEXT,

    assignment_mode         TEXT DEFAULT 'self_assign',
    task_category           TEXT DEFAULT 'regular',
    target_agent_id         TEXT,
    target_implementation   TEXT,
    target_specifier        TEXT,
    launch_params           TEXT,
    queued_for_startup      INTEGER DEFAULT 0,
    parent_agent_id         TEXT,
    parent_task_id          TEXT,

    retry_count             INTEGER NOT NULL DEFAULT 0,
    max_retries             INTEGER NOT NULL DEFAULT 3,
    next_retry_at           TEXT,

    error_message           TEXT,
    error_type              TEXT,

    payload                 BLOB,
    metadata                TEXT,
    checkpoint_data         TEXT,
    authority_mode          TEXT,
    subject_type            TEXT,
    subject_id              TEXT,
    root_subject_type       TEXT,
    root_subject_id         TEXT,
    authority_grant_id      TEXT,
    root_authority_grant_id TEXT,
    parent_authority_grant_id TEXT,
    authority_audience_type TEXT,
    authority_audience_id   TEXT,
    authority_delegate_type TEXT,
    authority_delegate_id   TEXT,

    schedule_to_start_ms    INTEGER,
    start_to_close_ms       INTEGER,
    heartbeat_timeout_ms    INTEGER,
    schedule_to_close_ms    INTEGER,

    last_heartbeat          TEXT,
    heartbeat_details       TEXT,

    target_topic            TEXT,
    source_topic            TEXT,
    message_type            TEXT,

    disconnected_at         TEXT,
    grace_window_ms         INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_task_status ON tasks (status);
CREATE INDEX IF NOT EXISTS idx_task_status_workspace ON tasks (status, workspace);
CREATE INDEX IF NOT EXISTS idx_task_status_created ON tasks (status, created_at);
CREATE INDEX IF NOT EXISTS idx_task_workspace ON tasks (workspace);
CREATE INDEX IF NOT EXISTS idx_task_workspace_status ON tasks (workspace, status);
CREATE INDEX IF NOT EXISTS idx_task_assigned_to ON tasks (assigned_to);
CREATE INDEX IF NOT EXISTS idx_task_target_agent ON tasks (target_agent_id);
CREATE INDEX IF NOT EXISTS idx_task_target_impl ON tasks (target_implementation);
CREATE INDEX IF NOT EXISTS idx_task_queued_startup ON tasks (target_agent_id) WHERE queued_for_startup = 1;
CREATE INDEX IF NOT EXISTS idx_task_category ON tasks (task_category);
CREATE INDEX IF NOT EXISTS idx_task_orchestrated ON tasks (task_category) WHERE task_category = 'orchestrated';
CREATE INDEX IF NOT EXISTS idx_task_next_retry ON tasks (next_retry_at) WHERE status = 'failed' AND next_retry_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_scheduled ON tasks (scheduled_for) WHERE status = 'pending' AND scheduled_for IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_pool_pending ON tasks (target_implementation, workspace) WHERE assignment_mode = 'pool' AND status = 'pending' AND queued_for_startup = 1;
CREATE INDEX IF NOT EXISTS idx_startup_lookup ON tasks (target_implementation, workspace, target_specifier) WHERE task_type = 'agent_startup' AND status IN ('pending', 'assigned', 'starting', 'running');
CREATE INDEX IF NOT EXISTS idx_task_root_authority_grant ON tasks (root_authority_grant_id) WHERE root_authority_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_authority_grant ON tasks (authority_grant_id) WHERE authority_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_subject ON tasks (subject_type, subject_id) WHERE subject_type IS NOT NULL AND subject_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_disconnected_at ON tasks (disconnected_at) WHERE disconnected_at IS NOT NULL AND status = 'running';
CREATE INDEX IF NOT EXISTS idx_tasks_parent_task_id ON tasks (parent_task_id) WHERE parent_task_id IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS tasks_updated_at
    AFTER UPDATE ON tasks
    FOR EACH ROW
BEGIN
    UPDATE tasks SET updated_at = strftime('%Y-%m-%dT%H:%M:%f', 'now') WHERE task_id = NEW.task_id;
END;

-- =============================================================================
-- Task Timers
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_timers (
    timer_id    TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    timer_type  TEXT NOT NULL,
    fires_at    TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    fired       INTEGER DEFAULT 0,
    fired_at    TEXT,
    metadata    TEXT
);

CREATE INDEX IF NOT EXISTS idx_timer_pending ON task_timers (fires_at) WHERE fired = 0;
CREATE INDEX IF NOT EXISTS idx_timer_task ON task_timers (task_id);
CREATE INDEX IF NOT EXISTS idx_timer_type_pending ON task_timers (timer_type, fires_at) WHERE fired = 0;

-- =============================================================================
-- Task Assignments
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_assignments (
    assignment_id   TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    worker_identity TEXT NOT NULL,
    assigned_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    started_at      TEXT,
    completed_at    TEXT,
    failed          INTEGER DEFAULT 0,
    failure_reason  TEXT
);

CREATE INDEX IF NOT EXISTS idx_assignment_task ON task_assignments (task_id);
CREATE INDEX IF NOT EXISTS idx_assignment_worker ON task_assignments (worker_identity);
CREATE INDEX IF NOT EXISTS idx_assignment_time ON task_assignments (assigned_at);

-- =============================================================================
-- Task Checkpoints
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_checkpoints (
    checkpoint_id   TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL,
    checkpoint_data TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    created_by      TEXT,
    UNIQUE (task_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_checkpoint_task_seq ON task_checkpoints (task_id, sequence_number DESC);

-- =============================================================================
-- Task Audit Events
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_audit_events (
    event_id    TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,
    event_data  TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    created_by  TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_task ON task_audit_events (task_id);
CREATE INDEX IF NOT EXISTS idx_audit_task_time ON task_audit_events (task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_type ON task_audit_events (event_type);
CREATE INDEX IF NOT EXISTS idx_audit_time ON task_audit_events (created_at);

-- =============================================================================
-- Dead Letter Queue (DLQ)
-- =============================================================================

CREATE TABLE IF NOT EXISTS dlq (
    dlq_message_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    original_task_id  TEXT NOT NULL,
    category          TEXT NOT NULL,
    workspace         TEXT NOT NULL,

    original_payload  BLOB,
    original_metadata TEXT,
    failure_reason    TEXT NOT NULL,
    failure_details   TEXT,

    enqueued_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    attempt_count     INTEGER NOT NULL,
    last_attempt_at   TEXT NOT NULL,

    reprocessed_at    TEXT,
    resolved          INTEGER NOT NULL DEFAULT 0,
    resolved_by       TEXT,
    resolution_notes  TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_workspace ON dlq (workspace);
CREATE INDEX IF NOT EXISTS idx_dlq_category ON dlq (category);
CREATE INDEX IF NOT EXISTS idx_dlq_unresolved ON dlq (resolved, enqueued_at) WHERE resolved = 0;

-- =============================================================================
-- Orchestrated Task Queue
-- =============================================================================

CREATE TABLE IF NOT EXISTS orchestrated_task_queue (
    queue_id               TEXT PRIMARY KEY,
    task_id                TEXT NOT NULL,
    target_implementation  TEXT NOT NULL,
    workspace              TEXT NOT NULL,
    profile                TEXT NOT NULL,
    launch_params          TEXT,
    status                 TEXT NOT NULL DEFAULT 'pending',
    claimed_by             TEXT,
    claimed_at             TEXT,
    created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    completed_at           TEXT,
    error_message          TEXT,
    retry_count            INTEGER NOT NULL DEFAULT 0,
    max_retries            INTEGER NOT NULL DEFAULT 3,
    next_retry_at          TEXT
);

CREATE INDEX IF NOT EXISTS idx_orch_queue_status ON orchestrated_task_queue (status);
CREATE INDEX IF NOT EXISTS idx_orch_queue_profile ON orchestrated_task_queue (profile) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_orch_queue_workspace ON orchestrated_task_queue (workspace, profile) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_orch_queue_stale_claims ON orchestrated_task_queue (claimed_at) WHERE status = 'claimed';
CREATE INDEX IF NOT EXISTS idx_orch_queue_next_retry ON orchestrated_task_queue (next_retry_at) WHERE status = 'pending' AND next_retry_at IS NOT NULL;
