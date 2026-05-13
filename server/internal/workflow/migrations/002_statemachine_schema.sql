-- Workflow State Machine Schema

CREATE TABLE IF NOT EXISTS workflow_state_machines (
    id          TEXT PRIMARY KEY,
    workspace   TEXT NOT NULL DEFAULT '*',
    definition  JSONB NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_state_machine_instances (
    instance_id    TEXT PRIMARY KEY,
    machine_id     TEXT NOT NULL REFERENCES workflow_state_machines(id),
    workspace      TEXT NOT NULL,
    current_state  TEXT NOT NULL,
    data           JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,
    timeout_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sm_instances_machine ON workflow_state_machine_instances(machine_id);
CREATE INDEX IF NOT EXISTS idx_sm_instances_state ON workflow_state_machine_instances(current_state);
CREATE INDEX IF NOT EXISTS idx_sm_instances_timeout ON workflow_state_machine_instances(timeout_at)
    WHERE timeout_at IS NOT NULL AND completed_at IS NULL;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'workflow_state_machines_updated_at') THEN
        CREATE TRIGGER workflow_state_machines_updated_at BEFORE UPDATE ON workflow_state_machines
            FOR EACH ROW EXECUTE FUNCTION workflow_update_updated_at();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'workflow_sm_instances_updated_at') THEN
        CREATE TRIGGER workflow_sm_instances_updated_at BEFORE UPDATE ON workflow_state_machine_instances
            FOR EACH ROW EXECUTE FUNCTION workflow_update_updated_at();
    END IF;
END $$;
