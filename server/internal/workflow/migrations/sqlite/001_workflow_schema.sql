-- Workflow Server Schema (SQLite)
-- Event routing rules, DAG definitions, executions, schedules

-- Event routing rules
CREATE TABLE IF NOT EXISTS workflow_rules (
    id                    INTEGER PRIMARY KEY,
    rule_name             TEXT NOT NULL,
    source_agent          TEXT NOT NULL,
    source_event          TEXT NOT NULL,
    trigger_condition     TEXT,
    transformation_style  TEXT NOT NULL DEFAULT 'template-yaml',
    destination_template  TEXT NOT NULL,
    workspace             TEXT NOT NULL DEFAULT '*',
    priority              INTEGER NOT NULL DEFAULT 0,
    active                INTEGER NOT NULL DEFAULT 1,
    created_at            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_workflow_rules_source ON workflow_rules(source_agent, source_event, active);
CREATE INDEX IF NOT EXISTS idx_workflow_rules_workspace ON workflow_rules(workspace, active);

-- DAG definitions
CREATE TABLE IF NOT EXISTS workflow_definitions (
    id          TEXT NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    workspace   TEXT NOT NULL DEFAULT '*',
    definition  TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, version)
);

-- DAG executions (runtime state)
CREATE TABLE IF NOT EXISTS workflow_executions (
    execution_id     TEXT PRIMARY KEY,
    workflow_id      TEXT NOT NULL,
    workflow_version INTEGER NOT NULL,
    workspace        TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'running',
    trigger_data     TEXT,
    started_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at     TEXT,
    error_message    TEXT,
    metadata         TEXT
);
CREATE INDEX IF NOT EXISTS idx_wf_exec_status ON workflow_executions(status, workspace);
CREATE INDEX IF NOT EXISTS idx_wf_exec_workflow ON workflow_executions(workflow_id);

-- Step execution state within a DAG
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

-- Scheduled tasks
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
    created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    max_concurrent INTEGER NOT NULL DEFAULT 0,
    active_task_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_wf_sched_next ON workflow_schedules(next_fire_at, enabled);
