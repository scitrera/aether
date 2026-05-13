-- Aether API Token Authentication Schema
-- Long-lived API tokens for agent, task, orchestrator, and other principal types

-- =============================================================================
-- API Tokens Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS api_tokens (
    -- Primary identification
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash              VARCHAR(64) NOT NULL UNIQUE,     -- SHA256 hex hash of the token (plaintext never stored)

    -- Metadata
    name                    VARCHAR(255) NOT NULL,           -- Human-readable name for the token
    principal_type          VARCHAR(50) NOT NULL,            -- 'agent', 'task', 'user', 'orchestrator', 'workflow_engine', 'metrics_bridge'

    -- Authorization
    workspace_patterns      TEXT[] NOT NULL DEFAULT '{}',    -- Array of glob patterns ('prod-*', '*', etc.)
    scopes                  TEXT[] NOT NULL DEFAULT '{"connect"}',  -- Permissions: 'connect', 'admin', 'read', 'write'

    -- Lifecycle tracking
    created_by              VARCHAR(255) NOT NULL,           -- Identity that created this token
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Expiration and revocation
    expires_at              TIMESTAMPTZ,                     -- NULL means no expiration
    revoked                 BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at              TIMESTAMPTZ,

    -- Usage tracking
    last_used_at            TIMESTAMPTZ,

    -- Extensibility
    metadata                JSONB NOT NULL DEFAULT '{}'
);

-- =============================================================================
-- API Token Indexes
-- =============================================================================

-- Query by principal type (common filtering)
CREATE INDEX IF NOT EXISTS idx_api_tokens_principal_type ON api_tokens(principal_type);

-- Active tokens (not revoked)
CREATE INDEX IF NOT EXISTS idx_api_tokens_revoked ON api_tokens(revoked) WHERE revoked = FALSE;

-- Expiration tracking (for cleanup and validation)
CREATE INDEX IF NOT EXISTS idx_api_tokens_expires_at ON api_tokens(expires_at) WHERE expires_at IS NOT NULL;

-- =============================================================================
-- Triggers for automatic timestamp updates
-- =============================================================================

CREATE TRIGGER api_tokens_updated_at
    BEFORE UPDATE ON api_tokens
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();
