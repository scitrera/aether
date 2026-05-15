-- Native SQLite schema for the workflow domain.
--
-- This is the Stage 2 native migration set — NOT the dbcompat-translated
-- copy that lives in internal/workflow/migrations/sqlite/. The two share
-- the same logical table shapes but this file uses native SQLite idioms
-- exclusively (TEXT timestamps, INTEGER booleans, no PG-isms).
--
-- Tables: workflow_rules, workflow_definitions, workflow_executions,
--         workflow_step_states, workflow_schedules,
--         workflow_state_machines, workflow_state_machine_instances.

-- =========================================================================
-- Event routing rules
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_rules (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_name             TEXT NOT NULL,
    source_agent          TEXT NOT NULL,
    source_event          TEXT NOT NULL,
    trigger_condition     TEXT,
    transformation_style  TEXT NOT NULL DEFAULT 'template-yaml',
    destination_template  TEXT NOT NULL,
    workspace             TEXT NOT NULL DEFAULT '*',
    priority              INTEGER NOT NULL DEFAULT 0,
    active                INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_workflow_rules_source ON workflow_rules(source_agent, source_event, active);
CREATE INDEX IF NOT EXISTS idx_workflow_rules_workspace ON workflow_rules(workspace, active);

-- Trigger to maintain updated_at on rule modifications.
CREATE TRIGGER IF NOT EXISTS trg_workflow_rules_updated_at
    AFTER UPDATE ON workflow_rules
    FOR EACH ROW
BEGIN
    UPDATE workflow_rules SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = NEW.id;
END;

-- =========================================================================
-- DAG definitions (versioned)
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_definitions (
    id          TEXT NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    workspace   TEXT NOT NULL DEFAULT '*',
    definition  TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (id, version)
);

CREATE TRIGGER IF NOT EXISTS trg_workflow_definitions_updated_at
    AFTER UPDATE ON workflow_definitions
    FOR EACH ROW
BEGIN
    UPDATE workflow_definitions SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = NEW.id AND version = NEW.version;
END;

-- =========================================================================
-- DAG executions (runtime state)
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_executions (
    execution_id     TEXT PRIMARY KEY,
    workflow_id      TEXT NOT NULL,
    workflow_version INTEGER NOT NULL,
    workspace        TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'running',
    trigger_data     TEXT,
    started_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    completed_at     TEXT,
    error_message    TEXT,
    metadata         TEXT
);
CREATE INDEX IF NOT EXISTS idx_wf_exec_status ON workflow_executions(status, workspace);
CREATE INDEX IF NOT EXISTS idx_wf_exec_workflow ON workflow_executions(workflow_id);

-- =========================================================================
-- Step execution state within a DAG
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_step_states (
    execution_id  TEXT NOT NULL REFERENCES workflow_executions(execution_id) ON DELETE CASCADE,
    step_id       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    started_at    TEXT,
    completed_at  TEXT,
    input_data    TEXT,
    output_data   TEXT,
    error_message TEXT,
    attempt       INTEGER NOT NULL DEFAULT 1,
    task_id       TEXT,
    PRIMARY KEY (execution_id, step_id)
);

-- =========================================================================
-- Scheduled tasks (cron, interval, once, event-delayed)
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_schedules (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    workspace      TEXT NOT NULL DEFAULT '*',
    schedule_type  TEXT NOT NULL,
    schedule_expr  TEXT NOT NULL,
    action         TEXT NOT NULL,
    workflow_id    TEXT,
    enabled        INTEGER NOT NULL DEFAULT 1,
    next_fire_at   TEXT,
    last_fired_at  TEXT,
    miss_policy    TEXT NOT NULL DEFAULT 'skip',
    max_concurrent INTEGER NOT NULL DEFAULT 0,
    active_task_id TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_wf_sched_next ON workflow_schedules(next_fire_at, enabled);

CREATE TRIGGER IF NOT EXISTS trg_workflow_schedules_updated_at
    AFTER UPDATE ON workflow_schedules
    FOR EACH ROW
BEGIN
    UPDATE workflow_schedules SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = NEW.id;
END;

-- =========================================================================
-- State machine definitions
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_state_machines (
    id          TEXT PRIMARY KEY,
    workspace   TEXT NOT NULL DEFAULT '*',
    definition  TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TRIGGER IF NOT EXISTS trg_workflow_state_machines_updated_at
    AFTER UPDATE ON workflow_state_machines
    FOR EACH ROW
BEGIN
    UPDATE workflow_state_machines SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = NEW.id;
END;

-- =========================================================================
-- State machine instances (runtime)
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_state_machine_instances (
    instance_id    TEXT PRIMARY KEY,
    machine_id     TEXT NOT NULL REFERENCES workflow_state_machines(id),
    workspace      TEXT NOT NULL,
    current_state  TEXT NOT NULL,
    data           TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    completed_at   TEXT,
    timeout_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_sm_instances_machine ON workflow_state_machine_instances(machine_id);
CREATE INDEX IF NOT EXISTS idx_sm_instances_state ON workflow_state_machine_instances(current_state);
CREATE INDEX IF NOT EXISTS idx_sm_instances_timeout ON workflow_state_machine_instances(timeout_at)
    WHERE timeout_at IS NOT NULL AND completed_at IS NULL;

CREATE TRIGGER IF NOT EXISTS trg_workflow_state_machine_instances_updated_at
    AFTER UPDATE ON workflow_state_machine_instances
    FOR EACH ROW
BEGIN
    UPDATE workflow_state_machine_instances SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE instance_id = NEW.instance_id;
END;
