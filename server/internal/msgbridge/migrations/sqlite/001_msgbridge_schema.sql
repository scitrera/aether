-- Channel-to-topic mappings
CREATE TABLE IF NOT EXISTS msgbridge_channel_mappings (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    platform    TEXT NOT NULL,
    channel_id  TEXT NOT NULL,
    direction   TEXT NOT NULL DEFAULT 'both',
    target_type TEXT NOT NULL,
    target_workspace TEXT NOT NULL DEFAULT '',
    target_id   TEXT NOT NULL DEFAULT '',
    enabled     INTEGER NOT NULL DEFAULT 1,
    metadata    TEXT DEFAULT '{}',
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, channel_id)
);

-- External user → Aether user identity mapping
CREATE TABLE IF NOT EXISTS msgbridge_user_mappings (
    id              INTEGER PRIMARY KEY,
    platform        TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    aether_user_id  TEXT NOT NULL,
    display_name    TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, platform_user_id)
);

-- Message delivery log
CREATE TABLE IF NOT EXISTS msgbridge_message_log (
    id           INTEGER PRIMARY KEY,
    direction    TEXT NOT NULL,
    platform     TEXT NOT NULL,
    channel_id   TEXT NOT NULL,
    message_id   TEXT NOT NULL DEFAULT '',
    aether_topic TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'delivered',
    error_msg    TEXT DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_msgbridge_log_created ON msgbridge_message_log(created_at);
CREATE INDEX IF NOT EXISTS idx_msgbridge_log_channel ON msgbridge_message_log(platform, channel_id);
