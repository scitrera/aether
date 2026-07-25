-- ACL Groups & Roles (SQLite dialect of migrations/028_acl_groups_roles.sql)
--
-- A group is a named collection of principals; a role is a named permission
-- bundle. Role/group PERMISSIONS reuse acl_rules (principal_type 'group'/'role').
-- The only new state is membership + assignment, loaded into the Casbin enforcer
-- as `g` edges. UUIDs are generated in Go (github.com/google/uuid); timestamps
-- are ISO-8601 TEXT to match the rest of the sqlite_acl schema.

CREATE TABLE IF NOT EXISTS acl_groups (
    group_id    TEXT PRIMARY KEY,
    group_name  TEXT NOT NULL UNIQUE,
    description TEXT,
    created_by  TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    metadata    TEXT
);

CREATE TABLE IF NOT EXISTS acl_roles (
    role_id     TEXT PRIMARY KEY,
    role_name   TEXT NOT NULL UNIQUE,
    description TEXT,
    created_by  TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    metadata    TEXT
);

CREATE TABLE IF NOT EXISTS acl_group_members (
    id          TEXT PRIMARY KEY,
    group_id    TEXT NOT NULL REFERENCES acl_groups (group_id) ON DELETE CASCADE,
    member_type TEXT NOT NULL,
    member_id   TEXT NOT NULL,
    granted_by  TEXT,
    granted_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at  TEXT,
    CONSTRAINT unique_group_member UNIQUE (group_id, member_type, member_id)
);

CREATE TABLE IF NOT EXISTS acl_role_assignments (
    id            TEXT PRIMARY KEY,
    role_id       TEXT NOT NULL REFERENCES acl_roles (role_id) ON DELETE CASCADE,
    assignee_type TEXT NOT NULL,
    assignee_id   TEXT NOT NULL,
    granted_by    TEXT,
    granted_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at    TEXT,
    CONSTRAINT unique_role_assignment UNIQUE (role_id, assignee_type, assignee_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_member ON acl_group_members (member_type, member_id);
CREATE INDEX IF NOT EXISTS idx_group_members_expiration ON acl_group_members (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_role_assignments_assignee ON acl_role_assignments (assignee_type, assignee_id);
CREATE INDEX IF NOT EXISTS idx_role_assignments_expiration ON acl_role_assignments (expires_at) WHERE expires_at IS NOT NULL;
