-- Migration 023: Phase 2 authority-request lifecycle storage.
--
-- The acl_authority_requests table holds the typed "sudo" handshake:
-- a running task asks for elevated authority via a row in PENDING state;
-- an approver flips it to APPROVED (and the lifecycle service mints a
-- standard acl_authority_grants row, recording the new grant_id in
-- granted_grant_id) or DENIED. Unresolved rows time out via the
-- ExpireAuthorityRequests sweep.
--
-- Parallels the existing acl_authority_grants table (migration 012). The
-- approval path delegates grant minting to CreateAuthorityGrant; this
-- table only owns the request lifecycle.

CREATE TABLE IF NOT EXISTS acl_authority_requests
(
    request_id              UUID PRIMARY KEY,
    status                  VARCHAR(32)  NOT NULL,
    requesting_actor_type   VARCHAR(50)  NOT NULL,
    requesting_actor_id     VARCHAR(255) NOT NULL,
    target_subject_type     VARCHAR(50),
    target_subject_id       VARCHAR(255),
    workspace_scope         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    resource_scope          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    operation_scope         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    requested_access        INTEGER      NOT NULL DEFAULT 0,
    duration_seconds        BIGINT       NOT NULL DEFAULT 0,
    audience_type           VARCHAR(50),
    audience_id             VARCHAR(255),
    routing_principal_type  VARCHAR(50),
    routing_principal_id    VARCHAR(255),
    routing_capability      TEXT,
    reason                  TEXT,
    task_id                 UUID,
    metadata                JSONB,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at              TIMESTAMPTZ  NOT NULL,
    resolved_at             TIMESTAMPTZ,
    granted_grant_id        UUID,
    resolved_by_type        VARCHAR(50),
    resolved_by_id          VARCHAR(255),
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

-- Pending sweep: ExpireAuthorityRequests scans this expression on every
-- pass, and LIST_PENDING with ResolverCapabilities filters down to the
-- PENDING rows whose expires_at hasn't elapsed yet.
CREATE INDEX IF NOT EXISTS idx_authority_requests_pending
    ON acl_authority_requests (status, expires_at)
    WHERE status = 'pending';

-- Approver lookup by specific principal target.
CREATE INDEX IF NOT EXISTS idx_authority_requests_routing_principal
    ON acl_authority_requests (routing_principal_type, routing_principal_id, status)
    WHERE routing_principal_id IS NOT NULL;

-- Approver lookup by capability gate.
CREATE INDEX IF NOT EXISTS idx_authority_requests_routing_capability
    ON acl_authority_requests (routing_capability, status)
    WHERE routing_capability IS NOT NULL;

-- Task-id reverse lookup: the waker needs to find the request a paused
-- task is blocked on without parsing the wait_spec JSON.
CREATE INDEX IF NOT EXISTS idx_authority_requests_task_id
    ON acl_authority_requests (task_id)
    WHERE task_id IS NOT NULL;

-- Expiry sweep scan; redundant with idx_authority_requests_pending today
-- but kept as a separate partial index because ExpireAuthorityRequests
-- may grow additional ORDER BY / LIMIT predicates in later stages.
CREATE INDEX IF NOT EXISTS idx_authority_requests_expires_at
    ON acl_authority_requests (expires_at)
    WHERE status = 'pending';
