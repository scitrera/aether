-- Workflow State Machine Schema (SQLite)

CREATE TABLE IF NOT EXISTS workflow_state_machines (
    id          TEXT PRIMARY KEY,
    workspace   TEXT NOT NULL DEFAULT '*',
    definition  TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_state_machine_instances (
    instance_id    TEXT PRIMARY KEY,
    machine_id     TEXT NOT NULL REFERENCES workflow_state_machines(id),
    workspace      TEXT NOT NULL,
    current_state  TEXT NOT NULL,
    data           TEXT,
    created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at   TEXT,
    timeout_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_sm_instances_machine ON workflow_state_machine_instances(machine_id);
CREATE INDEX IF NOT EXISTS idx_sm_instances_state ON workflow_state_machine_instances(current_state);
CREATE INDEX IF NOT EXISTS idx_sm_instances_timeout ON workflow_state_machine_instances(timeout_at)
    WHERE timeout_at IS NOT NULL AND completed_at IS NULL;
