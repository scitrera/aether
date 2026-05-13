-- Aether SQLite Full Schema
-- Consolidated from PostgreSQL migrations 001-012
-- SQLite-compatible: no PL/pgSQL functions, no pg_notify triggers, no partitioning
-- Note: schema_migrations table is created by the migration runner before this file executes.

-- =============================================================================
-- Sessions (001_core_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS sessions (
    session_id     TEXT PRIMARY KEY,
    identity_type  TEXT NOT NULL,
    identity_id    TEXT NOT NULL,
    workspace      TEXT,
    gateway_id     TEXT NOT NULL,
    connected_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata       TEXT,
    UNIQUE (identity_type, identity_id)
);

CREATE INDEX IF NOT EXISTS idx_session_identity ON sessions (identity_type, identity_id);
CREATE INDEX IF NOT EXISTS idx_session_gateway ON sessions (gateway_id);
CREATE INDEX IF NOT EXISTS idx_session_last_heartbeat ON sessions (last_heartbeat);

-- =============================================================================
-- Workflow Traces (001_core_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS workflow_traces (
    trace_id           TEXT PRIMARY KEY,
    span_id            TEXT NOT NULL,
    parent_span_id     TEXT,
    workflow_id        TEXT NOT NULL,
    parent_workflow_id TEXT,
    root_workflow_id   TEXT,
    workflow_depth     INTEGER NOT NULL DEFAULT 0,
    initiator_identity TEXT NOT NULL,
    current_identity   TEXT NOT NULL,
    operation          TEXT NOT NULL,
    workspace          TEXT,
    created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at       TEXT,
    status             TEXT,
    metadata           TEXT
);

CREATE INDEX IF NOT EXISTS idx_trace_workflow_id ON workflow_traces (workflow_id);
CREATE INDEX IF NOT EXISTS idx_trace_parent ON workflow_traces (parent_workflow_id);
CREATE INDEX IF NOT EXISTS idx_trace_root ON workflow_traces (root_workflow_id);
CREATE INDEX IF NOT EXISTS idx_trace_span ON workflow_traces (span_id);
CREATE INDEX IF NOT EXISTS idx_trace_initiator ON workflow_traces (initiator_identity);
CREATE INDEX IF NOT EXISTS idx_trace_created_at ON workflow_traces (created_at);

-- =============================================================================
-- Tasks (002_task_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS tasks (
    task_id                 TEXT PRIMARY KEY,
    task_type               TEXT NOT NULL,
    task_class              INTEGER NOT NULL DEFAULT 0,  -- UI-surface hint; 0=UNSPECIFIED, 1=INTERACTIVE, 2=BACKGROUND, 3=BATCH

    workspace               TEXT NOT NULL,
    implementation          TEXT,
    specifier               TEXT,

    status                  TEXT NOT NULL DEFAULT 'pending',
    priority                INTEGER NOT NULL DEFAULT 0,

    created_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

CREATE TRIGGER IF NOT EXISTS tasks_updated_at
    AFTER UPDATE ON tasks
    FOR EACH ROW
BEGIN
    UPDATE tasks SET updated_at = CURRENT_TIMESTAMP WHERE task_id = NEW.task_id;
END;

-- =============================================================================
-- Task Timers (002_task_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_timers (
    timer_id    TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    timer_type  TEXT NOT NULL,
    fires_at    TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fired       INTEGER DEFAULT 0,
    fired_at    TEXT,
    metadata    TEXT
);

CREATE INDEX IF NOT EXISTS idx_timer_pending ON task_timers (fires_at) WHERE fired = 0;
CREATE INDEX IF NOT EXISTS idx_timer_task ON task_timers (task_id);
CREATE INDEX IF NOT EXISTS idx_timer_type_pending ON task_timers (timer_type, fires_at) WHERE fired = 0;

-- =============================================================================
-- Task Assignments (002_task_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_assignments (
    assignment_id   TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    worker_identity TEXT NOT NULL,
    assigned_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at      TEXT,
    completed_at    TEXT,
    failed          INTEGER DEFAULT 0,
    failure_reason  TEXT
);

CREATE INDEX IF NOT EXISTS idx_assignment_task ON task_assignments (task_id);
CREATE INDEX IF NOT EXISTS idx_assignment_worker ON task_assignments (worker_identity);
CREATE INDEX IF NOT EXISTS idx_assignment_time ON task_assignments (assigned_at);

-- =============================================================================
-- Task Checkpoints (002_task_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_checkpoints (
    checkpoint_id   TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL,
    checkpoint_data TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by      TEXT,
    UNIQUE (task_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_checkpoint_task_seq ON task_checkpoints (task_id, sequence_number DESC);

-- =============================================================================
-- Task Audit Events (002_task_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_audit_events (
    event_id    TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES tasks (task_id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,
    event_data  TEXT,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by  TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_task ON task_audit_events (task_id);
CREATE INDEX IF NOT EXISTS idx_audit_task_time ON task_audit_events (task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_type ON task_audit_events (event_type);
CREATE INDEX IF NOT EXISTS idx_audit_time ON task_audit_events (created_at);

-- =============================================================================
-- Dead Letter Queue (002_task_schema)
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

    enqueued_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
-- ACL Rules (003_acl_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS acl_rules (
    rule_id        TEXT PRIMARY KEY,
    principal_type TEXT NOT NULL,
    principal_id   TEXT NOT NULL,
    resource_type  TEXT NOT NULL,
    resource_id    TEXT NOT NULL,
    access_level   INTEGER NOT NULL,
    granted_by     TEXT,
    granted_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at     TEXT,
    reason         TEXT,
    CONSTRAINT unique_acl_rule UNIQUE (principal_type, principal_id, resource_type, resource_id)
);

CREATE INDEX IF NOT EXISTS idx_acl_principal ON acl_rules (principal_type, principal_id);
CREATE INDEX IF NOT EXISTS idx_acl_resource ON acl_rules (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_acl_expiration ON acl_rules (expires_at) WHERE expires_at IS NOT NULL;

-- =============================================================================
-- ACL Authority Grants (012_authority_grants_phase0)
-- =============================================================================

CREATE TABLE IF NOT EXISTS acl_authority_grants (
    grant_id           TEXT PRIMARY KEY,
    root_grant_id      TEXT NOT NULL,
    subject_type       TEXT NOT NULL,
    subject_id         TEXT NOT NULL,
    delegate_type      TEXT NOT NULL,
    delegate_id        TEXT NOT NULL,
    issued_by_type     TEXT NOT NULL,
    issued_by_id       TEXT NOT NULL,
    root_subject_type  TEXT NOT NULL,
    root_subject_id    TEXT NOT NULL,
    parent_grant_id    TEXT,
    may_delegate       INTEGER NOT NULL DEFAULT 0,
    remaining_hops     INTEGER NOT NULL DEFAULT 0,
    workspace_scope    TEXT NOT NULL DEFAULT '[]',
    resource_scope     TEXT NOT NULL DEFAULT '{}',
    operation_scope    TEXT NOT NULL DEFAULT '[]',
    max_access_level   INTEGER NOT NULL,
    audience_type      TEXT NOT NULL,
    audience_id        TEXT NOT NULL,
    valid_while_audience_active INTEGER NOT NULL DEFAULT 0,
    expires_at         TEXT NOT NULL,
    renewable_until    TEXT NOT NULL,
    renewed_at         TEXT,
    revoked            INTEGER NOT NULL DEFAULT 0,
    revoked_at         TEXT,
    reason             TEXT,
    metadata           TEXT NOT NULL DEFAULT '{}',
    created_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_authority_grant_renewal_window CHECK (renewable_until >= expires_at),
    CONSTRAINT chk_authority_grant_hops CHECK (remaining_hops >= 0),
    CONSTRAINT chk_authority_grant_delegate_depth CHECK (
        (may_delegate = 0 AND remaining_hops = 0) OR
        (may_delegate = 1 AND remaining_hops > 0)
        ),
    CONSTRAINT chk_authority_grant_revocation CHECK (
        (revoked = 0 AND revoked_at IS NULL) OR
        (revoked = 1 AND revoked_at IS NOT NULL)
        )
);

CREATE INDEX IF NOT EXISTS idx_authority_grants_root ON acl_authority_grants (root_grant_id);
CREATE INDEX IF NOT EXISTS idx_authority_grants_delegate_active
    ON acl_authority_grants (delegate_type, delegate_id, expires_at) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_authority_grants_subject_active
    ON acl_authority_grants (subject_type, subject_id, expires_at) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_authority_grants_audience_active
    ON acl_authority_grants (audience_type, audience_id, expires_at) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_authority_grants_parent
    ON acl_authority_grants (parent_grant_id) WHERE parent_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_authority_grants_expires_at ON acl_authority_grants (expires_at);

-- =============================================================================
-- ACL Fallback Policies (003_acl_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS acl_fallback_policies (
    policy_id             TEXT PRIMARY KEY,
    rule_category         TEXT NOT NULL UNIQUE,
    fallback_access_level INTEGER NOT NULL,
    updated_by            TEXT,
    updated_at            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Initial fallback policies
INSERT INTO acl_fallback_policies (policy_id, rule_category, fallback_access_level, updated_by)
VALUES ('pol-user-workspace',      'user_workspace',      20, '_system'),
       ('pol-agent-workspace',     'agent_workspace',     20, '_system'),
       ('pol-user-agent',          'user_agent',          20, '_system'),
       ('pol-agent-agent',         'agent_agent',         20, '_system'),
       ('pol-task-workspace',      'task_workspace',      20, '_system'),
       ('pol-global-read',         'global_read',         20, '_system'),
       ('pol-orchestrator-system', 'orchestrator_system', 20, '_system')
ON CONFLICT (rule_category) DO NOTHING;

-- Default _global workspace rule
INSERT INTO acl_rules (rule_id, principal_type, principal_id, resource_type, resource_id, access_level, granted_by, reason)
VALUES ('rule-default-global', 'wildcard', '_any_authenticated', 'workspace', '_global', 20, '_system',
        'Default READ_WRITE access for all authenticated principals (development mode)')
ON CONFLICT (principal_type, principal_id, resource_type, resource_id) DO NOTHING;

-- =============================================================================
-- Agent Registry (004_orchestration_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS agent_registry (
    implementation TEXT PRIMARY KEY,
    launch_params  TEXT NOT NULL,
    description    TEXT,
    created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- SQLite doesn't support expression indexes (launch_params->>'profile'),
-- so we index the full column and filter in application code.
CREATE INDEX IF NOT EXISTS idx_agent_registry_updated ON agent_registry (updated_at);

CREATE TRIGGER IF NOT EXISTS agent_registry_updated_at
    AFTER UPDATE ON agent_registry
    FOR EACH ROW
BEGIN
    UPDATE agent_registry SET updated_at = CURRENT_TIMESTAMP WHERE implementation = NEW.implementation;
END;

-- =============================================================================
-- Orchestrator Profiles (004_orchestration_schema)
-- =============================================================================

CREATE TABLE IF NOT EXISTS orchestrator_profiles (
    id              TEXT PRIMARY KEY,
    orchestrator_id TEXT NOT NULL,
    profile_name    TEXT NOT NULL,
    workspace       TEXT NOT NULL DEFAULT '_system',
    last_heartbeat  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (orchestrator_id, profile_name)
);

CREATE INDEX IF NOT EXISTS idx_orch_profile_name ON orchestrator_profiles (profile_name);
CREATE INDEX IF NOT EXISTS idx_orch_profile_workspace ON orchestrator_profiles (workspace);
CREATE INDEX IF NOT EXISTS idx_orch_profile_heartbeat ON orchestrator_profiles (last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_orch_profile_lookup ON orchestrator_profiles (profile_name, workspace, last_heartbeat);

-- =============================================================================
-- Orchestrated Task Queue (004-006_orchestration_schema + retry columns)
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
    created_at             TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

-- =============================================================================
-- API Tokens (007_api_tokens)
-- =============================================================================

CREATE TABLE IF NOT EXISTS api_tokens (
    id                  TEXT PRIMARY KEY,
    token_hash          TEXT NOT NULL UNIQUE,

    name                TEXT NOT NULL,
    principal_type      TEXT NOT NULL,

    workspace_patterns  TEXT NOT NULL DEFAULT '[]',
    scopes              TEXT NOT NULL DEFAULT '["connect"]',

    created_by          TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    expires_at          TEXT,
    revoked             INTEGER NOT NULL DEFAULT 0,
    revoked_at          TEXT,

    last_used_at        TEXT,

    metadata            TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_api_tokens_principal_type ON api_tokens (principal_type);
CREATE INDEX IF NOT EXISTS idx_api_tokens_revoked ON api_tokens (revoked) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_api_tokens_expires_at ON api_tokens (expires_at) WHERE expires_at IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS api_tokens_updated_at
    AFTER UPDATE ON api_tokens
    FOR EACH ROW
BEGIN
    UPDATE api_tokens SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- =============================================================================
-- Comprehensive Audit Log (008_comprehensive_audit_schema + 011_audit_partitioning)
-- Regular table — SQLite has no partitioning; retention managed by application
-- =============================================================================

CREATE TABLE IF NOT EXISTS comprehensive_audit_log (
    audit_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp                 TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type                TEXT NOT NULL,
    actor_type                TEXT NOT NULL,
    actor_id                  TEXT NOT NULL,
    subject_type              TEXT,
    subject_id                TEXT,
    root_subject_type         TEXT,
    root_subject_id           TEXT,
    authority_mode            TEXT NOT NULL DEFAULT 'direct',
    root_authority_grant_id   TEXT,
    authority_grant_id        TEXT,
    parent_authority_grant_id TEXT,
    resource_type             TEXT,
    resource_id               TEXT,
    operation                 TEXT NOT NULL,
    workspace                 TEXT,
    session_id                TEXT,
    gateway_id                TEXT,
    success                   INTEGER NOT NULL DEFAULT 1,
    error_message             TEXT,
    metadata                  TEXT,
    source                    TEXT NOT NULL DEFAULT 'gateway'
);

CREATE INDEX IF NOT EXISTS idx_cal_timestamp ON comprehensive_audit_log (timestamp);
CREATE INDEX IF NOT EXISTS idx_cal_actor ON comprehensive_audit_log (actor_type, actor_id);
CREATE INDEX IF NOT EXISTS idx_cal_subject ON comprehensive_audit_log (subject_type, subject_id);
CREATE INDEX IF NOT EXISTS idx_cal_event_type ON comprehensive_audit_log (event_type);
CREATE INDEX IF NOT EXISTS idx_cal_resource ON comprehensive_audit_log (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_cal_operation ON comprehensive_audit_log (operation);
CREATE INDEX IF NOT EXISTS idx_cal_workspace ON comprehensive_audit_log (workspace);
CREATE INDEX IF NOT EXISTS idx_cal_session ON comprehensive_audit_log (session_id) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cal_gateway ON comprehensive_audit_log (gateway_id);
CREATE INDEX IF NOT EXISTS idx_cal_root_authority_grant ON comprehensive_audit_log (root_authority_grant_id) WHERE root_authority_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cal_authority_grant ON comprehensive_audit_log (authority_grant_id) WHERE authority_grant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cal_success ON comprehensive_audit_log (success) WHERE success = 0;
CREATE INDEX IF NOT EXISTS idx_cal_failed_operations ON comprehensive_audit_log (success, actor_type, actor_id, timestamp) WHERE success = 0;
CREATE INDEX IF NOT EXISTS idx_cal_workspace_events ON comprehensive_audit_log (workspace, event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_cal_source ON comprehensive_audit_log (source);
