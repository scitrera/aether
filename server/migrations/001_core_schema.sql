-- Aether Core Schema
-- Sessions and workflow tracing (non-task, non-ACL foundation)

-- Session Registry (PostgreSQL backup for Redis-based registry)
CREATE TABLE IF NOT EXISTS sessions
(
    session_id     UUID PRIMARY KEY,
    identity_type  VARCHAR(50)  NOT NULL,
    identity_id    VARCHAR(255) NOT NULL,
    workspace      VARCHAR(255),
    gateway_id     VARCHAR(100) NOT NULL,
    connected_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMP    NOT NULL DEFAULT NOW(),
    metadata       JSONB,
    UNIQUE (identity_type, identity_id)
);

CREATE INDEX IF NOT EXISTS idx_session_identity ON sessions (identity_type, identity_id);
CREATE INDEX IF NOT EXISTS idx_session_gateway ON sessions (gateway_id);
CREATE INDEX IF NOT EXISTS idx_session_last_heartbeat ON sessions (last_heartbeat);

-- Workflow Traces (for DAG visualization and traceability)
CREATE TABLE IF NOT EXISTS workflow_traces
(
    trace_id           UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    span_id            VARCHAR(64)  NOT NULL,
    parent_span_id     VARCHAR(64),
    workflow_id        VARCHAR(255) NOT NULL,
    parent_workflow_id VARCHAR(255),
    root_workflow_id   VARCHAR(255),
    workflow_depth     INTEGER      NOT NULL DEFAULT 0,
    initiator_identity VARCHAR(255) NOT NULL,
    current_identity   VARCHAR(255) NOT NULL,
    operation          VARCHAR(100) NOT NULL,
    workspace          VARCHAR(255),
    created_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
    completed_at       TIMESTAMP,
    status             VARCHAR(50),
    metadata           JSONB
);

CREATE INDEX IF NOT EXISTS idx_trace_workflow_id ON workflow_traces (workflow_id);
CREATE INDEX IF NOT EXISTS idx_trace_parent ON workflow_traces (parent_workflow_id);
CREATE INDEX IF NOT EXISTS idx_trace_root ON workflow_traces (root_workflow_id);
CREATE INDEX IF NOT EXISTS idx_trace_span ON workflow_traces (span_id);
CREATE INDEX IF NOT EXISTS idx_trace_initiator ON workflow_traces (initiator_identity);
CREATE INDEX IF NOT EXISTS idx_trace_created_at ON workflow_traces (created_at);
