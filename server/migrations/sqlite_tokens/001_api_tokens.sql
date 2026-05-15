-- Native SQLite schema for API tokens (mirrors migrations/007_api_tokens.sql).
--
-- Differences from the PostgreSQL schema (all §0-parity safe):
--   - UUID → TEXT PRIMARY KEY (SQLite has no native UUID; IDs generated in Go)
--   - VARCHAR(N) → TEXT (SQLite ignores length constraints; TEXT is idiomatic)
--   - TEXT[] → TEXT (JSON-encoded arrays; parsed in Go)
--   - BOOLEAN → INTEGER (0/1; SQLite has no native boolean)
--   - TIMESTAMPTZ → TEXT (ISO-8601 UTC strings; parsed in Go)
--   - gen_random_uuid() → generated in Go via google/uuid
--   - NOW() → strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
--   - No triggers (updated_at managed in Go)
--   - No partial indexes (SQLite supports them but they're less useful here)
--   - No JSONB metadata column (not used by the Go store implementation)

CREATE TABLE IF NOT EXISTS api_tokens (
    -- Primary identification
    id                  TEXT PRIMARY KEY,
    token_hash          TEXT NOT NULL UNIQUE,

    -- Metadata
    name                TEXT NOT NULL,
    principal_type      TEXT NOT NULL,

    -- Authorization (JSON-encoded arrays)
    workspace_patterns  TEXT NOT NULL DEFAULT '[]',
    scopes              TEXT NOT NULL DEFAULT '["connect"]',

    -- Lifecycle tracking
    created_by          TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),

    -- Expiration and revocation
    expires_at          TEXT,
    revoked             INTEGER NOT NULL DEFAULT 0,
    revoked_at          TEXT,

    -- Usage tracking
    last_used_at        TEXT
);

-- Indexes matching the PostgreSQL schema's intent.
CREATE INDEX IF NOT EXISTS idx_api_tokens_principal_type ON api_tokens(principal_type);
CREATE INDEX IF NOT EXISTS idx_api_tokens_revoked ON api_tokens(revoked) WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS idx_api_tokens_expires_at ON api_tokens(expires_at) WHERE expires_at IS NOT NULL;
