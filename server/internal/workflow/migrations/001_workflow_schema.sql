-- Workflow Server Schema
-- Event routing rules, DAG definitions, executions, schedules

-- Event routing rules (replaces Python event bridge)
CREATE TABLE IF NOT EXISTS workflow_rules (
    id                    SERIAL PRIMARY KEY,
    rule_name             TEXT NOT NULL,
    source_agent          TEXT NOT NULL,
    source_event          TEXT NOT NULL,
    trigger_condition     TEXT,
    transformation_style  TEXT NOT NULL DEFAULT 'template-yaml',
    destination_template  TEXT NOT NULL,
    workspace             TEXT NOT NULL DEFAULT '*',
    priority              INT NOT NULL DEFAULT 0,
    active                BOOLEAN NOT NULL DEFAULT true,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_workflow_rules_source ON workflow_rules(source_agent, source_event, active);
CREATE INDEX IF NOT EXISTS idx_workflow_rules_workspace ON workflow_rules(workspace, active);

-- DAG definitions
CREATE TABLE IF NOT EXISTS workflow_definitions (
    id          TEXT NOT NULL,
    version     INT NOT NULL DEFAULT 1,
    workspace   TEXT NOT NULL DEFAULT '*',
    definition  JSONB NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, version)
);

-- DAG executions (runtime state)
CREATE TABLE IF NOT EXISTS workflow_executions (
    execution_id     TEXT PRIMARY KEY,
    workflow_id      TEXT NOT NULL,
    workflow_version INT NOT NULL,
    workspace        TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'running',
    trigger_data     JSONB,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ,
    error_message    TEXT,
    metadata         JSONB
);
CREATE INDEX IF NOT EXISTS idx_wf_exec_status ON workflow_executions(status, workspace);
CREATE INDEX IF NOT EXISTS idx_wf_exec_workflow ON workflow_executions(workflow_id);

-- Step execution state within a DAG
CREATE TABLE IF NOT EXISTS workflow_step_states (
    execution_id  TEXT NOT NULL REFERENCES workflow_executions(execution_id) ON DELETE CASCADE,
    step_id       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    input_data    JSONB,
    output_data   JSONB,
    error_message TEXT,
    attempt       INT NOT NULL DEFAULT 1,
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
    action         JSONB NOT NULL,
    workflow_id    TEXT,
    enabled        BOOLEAN NOT NULL DEFAULT true,
    next_fire_at   TIMESTAMPTZ,
    last_fired_at  TIMESTAMPTZ,
    miss_policy    TEXT NOT NULL DEFAULT 'skip',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wf_sched_next ON workflow_schedules(next_fire_at, enabled);

-- Auto-update timestamps
CREATE OR REPLACE FUNCTION workflow_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'workflow_rules_updated_at') THEN
        CREATE TRIGGER workflow_rules_updated_at BEFORE UPDATE ON workflow_rules
            FOR EACH ROW EXECUTE FUNCTION workflow_update_updated_at();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'workflow_definitions_updated_at') THEN
        CREATE TRIGGER workflow_definitions_updated_at BEFORE UPDATE ON workflow_definitions
            FOR EACH ROW EXECUTE FUNCTION workflow_update_updated_at();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'workflow_schedules_updated_at') THEN
        CREATE TRIGGER workflow_schedules_updated_at BEFORE UPDATE ON workflow_schedules
            FOR EACH ROW EXECUTE FUNCTION workflow_update_updated_at();
    END IF;
END $$;
