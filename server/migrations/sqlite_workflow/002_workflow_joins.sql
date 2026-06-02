-- =========================================================================
-- Fan-in/barrier/coalesce join instances. The authoritative arrival counter
-- lives in KV; this table is the durable observability + deadline-sweep
-- surface. One row per (join_name, workspace, correlation_key).
-- =========================================================================
CREATE TABLE IF NOT EXISTS workflow_joins (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    join_name       TEXT NOT NULL,
    workspace       TEXT NOT NULL,
    correlation_key TEXT NOT NULL,
    mode            TEXT NOT NULL,
    expected_count  INTEGER,
    arrived_count   INTEGER NOT NULL DEFAULT 0,
    dirty           INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'open',
    deadline_at     TEXT,
    linger_until    TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (join_name, workspace, correlation_key)
);
CREATE INDEX IF NOT EXISTS idx_workflow_joins_due ON workflow_joins(status, deadline_at);

CREATE TRIGGER IF NOT EXISTS trg_workflow_joins_updated_at
    AFTER UPDATE ON workflow_joins
    FOR EACH ROW
BEGIN
    UPDATE workflow_joins SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = NEW.id;
END;
