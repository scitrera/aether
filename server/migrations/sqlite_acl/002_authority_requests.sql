-- AetherLite native SQLite migration: Phase 2 authority-request lifecycle.
--
-- Structural parity with postgres migration 023_acl_authority_requests.sql.
-- SQLite-native types: TEXT for timestamps (parsed inline by the storage
-- layer), INTEGER for booleans, TEXT for JSON columns, no JSONB / TIMESTAMPTZ.

CREATE TABLE IF NOT EXISTS acl_authority_requests
(
    request_id              TEXT PRIMARY KEY,
    status                  TEXT    NOT NULL,
    requesting_actor_type   TEXT    NOT NULL,
    requesting_actor_id     TEXT    NOT NULL,
    target_subject_type     TEXT,
    target_subject_id       TEXT,
    workspace_scope         TEXT    NOT NULL DEFAULT '[]',
    resource_scope          TEXT    NOT NULL DEFAULT '{}',
    operation_scope         TEXT    NOT NULL DEFAULT '[]',
    requested_access        INTEGER NOT NULL DEFAULT 0,
    duration_seconds        INTEGER NOT NULL DEFAULT 0,
    audience_type           TEXT,
    audience_id             TEXT,
    routing_principal_type  TEXT,
    routing_principal_id    TEXT,
    routing_capability      TEXT,
    reason                  TEXT,
    task_id                 TEXT,
    metadata                TEXT,
    created_at              TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at              TEXT    NOT NULL,
    resolved_at             TEXT,
    granted_grant_id        TEXT,
    resolved_by_type        TEXT,
    resolved_by_id          TEXT,
    resolution_reason       TEXT,
    CONSTRAINT chk_authority_request_status CHECK (
        status IN ('pending', 'approved', 'denied', 'expired', 'cancelled')
    ),
    CONSTRAINT chk_authority_request_routing CHECK (
        routing_principal_id IS NOT NULL OR routing_capability IS NOT NULL
    ),
    CONSTRAINT chk_authority_request_resolution CHECK (
        (status = 'pending' AND resolved_at IS NULL AND granted_grant_id IS NULL) OR
        (status = 'approved' AND resolved_at IS NOT NULL AND granted_grant_id IS NOT NULL) OR
        (status IN ('denied', 'expired', 'cancelled') AND resolved_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_authority_requests_pending
    ON acl_authority_requests (status, expires_at) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_authority_requests_routing_principal
    ON acl_authority_requests (routing_principal_type, routing_principal_id, status)
    WHERE routing_principal_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_authority_requests_routing_capability
    ON acl_authority_requests (routing_capability, status)
    WHERE routing_capability IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_authority_requests_task_id
    ON acl_authority_requests (task_id) WHERE task_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_authority_requests_expires_at
    ON acl_authority_requests (expires_at) WHERE status = 'pending';
