-- Migration: Comprehensive Audit Logging Schema
-- This enables detailed audit trails for security-relevant events including connections,
-- authentication, authorization, message routing, KV operations, and administrative actions

-- Comprehensive Audit Log Table
-- Records all security-relevant events for compliance and incident investigation
CREATE TABLE IF NOT EXISTS comprehensive_audit_log
(
    audit_id       BIGSERIAL PRIMARY KEY,
    timestamp      TIMESTAMP    NOT NULL DEFAULT NOW(),
    event_type     VARCHAR(50)  NOT NULL, -- 'connection', 'authentication', 'authorization', 'message', 'kv_operation', 'admin_action'
    actor_type     VARCHAR(50)  NOT NULL, -- Principal type: 'agent', 'task', 'user', 'orchestrator', 'workflow_engine', 'metrics_bridge'
    actor_id       VARCHAR(255) NOT NULL, -- Principal identifier
    resource_type  VARCHAR(50),           -- Type of resource accessed: 'workspace', 'agent', 'task', 'topic', 'kv_key'
    resource_id    VARCHAR(255),          -- Resource identifier
    operation      VARCHAR(100) NOT NULL, -- Specific operation: 'connect', 'disconnect', 'send_message', 'kv_read', 'kv_write', etc.
    workspace      VARCHAR(255),          -- Workspace context
    session_id     UUID,                  -- Session context (if applicable)
    gateway_id     VARCHAR(100),          -- Which gateway instance recorded this event
    success        BOOLEAN      NOT NULL DEFAULT TRUE, -- Whether the operation succeeded
    error_message  TEXT,                  -- Error details if success=false
    metadata       JSONB                  -- Additional context (message details, KV before/after values, etc.)
);

-- Indexes for efficient audit log queries
-- Time-based queries (most common for audit review)
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON comprehensive_audit_log (timestamp);

-- Actor-based queries (who did what)
CREATE INDEX IF NOT EXISTS idx_audit_actor ON comprehensive_audit_log (actor_type, actor_id);

-- Event type filtering (specific event categories)
CREATE INDEX IF NOT EXISTS idx_audit_event_type ON comprehensive_audit_log (event_type);

-- Resource-based queries (what was accessed)
CREATE INDEX IF NOT EXISTS idx_audit_resource ON comprehensive_audit_log (resource_type, resource_id);

-- Operation-based queries (specific actions)
CREATE INDEX IF NOT EXISTS idx_audit_operation ON comprehensive_audit_log (operation);

-- Workspace-scoped queries
CREATE INDEX IF NOT EXISTS idx_audit_workspace ON comprehensive_audit_log (workspace);

-- Session-based queries (track all events in a session)
CREATE INDEX IF NOT EXISTS idx_audit_session ON comprehensive_audit_log (session_id) WHERE session_id IS NOT NULL;

-- Gateway-based queries (per-instance audit review)
CREATE INDEX IF NOT EXISTS idx_audit_gateway ON comprehensive_audit_log (gateway_id);

-- Success/failure filtering (security incident investigation)
CREATE INDEX IF NOT EXISTS idx_audit_success ON comprehensive_audit_log (success);

-- Composite index for common compliance queries (failed operations by actor)
CREATE INDEX IF NOT EXISTS idx_audit_failed_operations ON comprehensive_audit_log (success, actor_type, actor_id, timestamp) WHERE success = FALSE;

-- Composite index for workspace-scoped event queries
CREATE INDEX IF NOT EXISTS idx_audit_workspace_events ON comprehensive_audit_log (workspace, event_type, timestamp);

-- Function to clean up old audit logs based on retention policy
-- Default: 365 days (1 year) for general audit logs
-- Can be called with different retention periods for specific compliance requirements
CREATE OR REPLACE FUNCTION cleanup_old_comprehensive_audit_logs(retention_days INTEGER DEFAULT 365)
    RETURNS INTEGER AS
$$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE
    FROM comprehensive_audit_log
    WHERE timestamp < NOW() - (retention_days || ' days')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get audit statistics for a time period
CREATE OR REPLACE FUNCTION get_audit_statistics(
    start_time TIMESTAMP,
    end_time TIMESTAMP DEFAULT NOW()
)
    RETURNS TABLE
            (
                event_type     VARCHAR(50),
                operation      VARCHAR(100),
                total_events   BIGINT,
                success_count  BIGINT,
                failure_count  BIGINT,
                unique_actors  BIGINT,
                unique_resources BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT cal.event_type,
               cal.operation,
               COUNT(*)                           AS total_events,
               SUM(CASE WHEN cal.success THEN 1 ELSE 0 END) AS success_count,
               SUM(CASE WHEN NOT cal.success THEN 1 ELSE 0 END) AS failure_count,
               COUNT(DISTINCT cal.actor_id)       AS unique_actors,
               COUNT(DISTINCT cal.resource_id)    AS unique_resources
        FROM comprehensive_audit_log cal
        WHERE cal.timestamp BETWEEN start_time AND end_time
        GROUP BY cal.event_type, cal.operation
        ORDER BY total_events DESC;
END;
$$ LANGUAGE plpgsql;

-- View for daily audit summary (useful for compliance reporting)
CREATE OR REPLACE VIEW audit_daily_summary AS
SELECT DATE(timestamp)              AS audit_date,
       event_type,
       operation,
       COUNT(*)                     AS event_count,
       SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
       SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failure_count,
       COUNT(DISTINCT actor_id)     AS unique_actors,
       COUNT(DISTINCT workspace)    AS unique_workspaces
FROM comprehensive_audit_log
GROUP BY DATE(timestamp), event_type, operation
ORDER BY audit_date DESC, event_count DESC;

-- View for failed operations (security incident investigation)
CREATE OR REPLACE VIEW audit_failed_operations AS
SELECT timestamp,
       event_type,
       actor_type,
       actor_id,
       operation,
       resource_type,
       resource_id,
       workspace,
       session_id,
       gateway_id,
       error_message,
       metadata
FROM comprehensive_audit_log
WHERE success = FALSE
ORDER BY timestamp DESC;

-- View for actor activity summary (who is doing what)
CREATE OR REPLACE VIEW audit_actor_activity AS
SELECT actor_type,
       actor_id,
       workspace,
       COUNT(*)                                  AS total_operations,
       COUNT(DISTINCT event_type)                AS event_types_used,
       SUM(CASE WHEN success THEN 1 ELSE 0 END)  AS successful_ops,
       SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failed_ops,
       MIN(timestamp)                            AS first_seen,
       MAX(timestamp)                            AS last_seen
FROM comprehensive_audit_log
GROUP BY actor_type, actor_id, workspace
ORDER BY total_operations DESC;

-- View for workspace audit summary (per-workspace activity)
CREATE OR REPLACE VIEW audit_workspace_summary AS
SELECT workspace,
       DATE(timestamp)              AS audit_date,
       COUNT(*)                     AS total_operations,
       COUNT(DISTINCT actor_id)     AS unique_actors,
       COUNT(DISTINCT event_type)   AS event_types,
       SUM(CASE WHEN success THEN 1 ELSE 0 END) AS successful_ops,
       SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS failed_ops
FROM comprehensive_audit_log
WHERE workspace IS NOT NULL
GROUP BY workspace, DATE(timestamp)
ORDER BY audit_date DESC, total_operations DESC;
