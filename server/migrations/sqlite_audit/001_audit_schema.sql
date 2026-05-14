-- AetherLite audit.db — comprehensive_audit_log schema
--
-- This table is the destination for both audit.AuditLogger and
-- acl.AuditLogger batched writes. Splitting it into its own SQLite file
-- (audit.db) isolates the heavy async writer from the synchronous task
-- read path in aether.db, which is the SQLITE_BUSY contention point.
--
-- NOTE: The same table is also declared in migrations/sqlite/001_full_schema.sql
-- for backward compat with pre-split data directories. Going forward the
-- aetherlite binary directs all writes to THIS file via a second *sql.DB
-- handle; the copy in aether.db is a vestigial no-op. New deployments do
-- not write to it.

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
