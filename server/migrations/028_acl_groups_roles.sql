-- ACL Groups & Roles
--
-- Adds role/group authorization on top of the per-principal acl_rules model.
-- A *group* is a named collection of principals; a *role* is a named bundle of
-- permissions assignable to principals or groups. Both are modeled as synthetic
-- subjects in acl_rules (principal_type 'group' / 'role', which the existing
-- VARCHAR(50) column already accepts), so role/group PERMISSIONS need no new
-- table — they are ordinary acl_rules rows. The only new state is membership
-- (who is in a group) and assignment (who has a role), loaded into the Casbin
-- enforcer as `g` (grouping) edges for transitive resolution.

-- Group definitions
CREATE TABLE IF NOT EXISTS acl_groups
(
    group_id    UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    group_name  VARCHAR(255) NOT NULL UNIQUE, -- canonical id used in subjects: "group:<group_name>"
    description TEXT,
    created_by  VARCHAR(255),
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    metadata    JSONB
);

-- Role definitions
CREATE TABLE IF NOT EXISTS acl_roles
(
    role_id     UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    role_name   VARCHAR(255) NOT NULL UNIQUE, -- canonical id used in subjects: "role:<role_name>"
    description TEXT,
    created_by  VARCHAR(255),
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    metadata    JSONB
);

-- Group membership: which principals (or nested groups) belong to a group.
-- member_type is a principal type ('user','agent','task',...) OR 'group' (nesting).
-- Edge loaded as Casbin g-rule: g("<member_type>:<member_id>", "group:<group_name>").
CREATE TABLE IF NOT EXISTS acl_group_members
(
    id          UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    group_id    UUID         NOT NULL REFERENCES acl_groups (group_id) ON DELETE CASCADE,
    member_type VARCHAR(50)  NOT NULL,
    member_id   VARCHAR(255) NOT NULL,
    granted_by  VARCHAR(255),
    granted_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMP,
    CONSTRAINT unique_group_member UNIQUE (group_id, member_type, member_id)
);

-- Role assignment: which principals (or groups) are granted a role.
-- assignee_type is a principal type OR 'group'.
-- Edge loaded as Casbin g-rule: g("<assignee_type>:<assignee_id>", "role:<role_name>").
CREATE TABLE IF NOT EXISTS acl_role_assignments
(
    id            UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    role_id       UUID         NOT NULL REFERENCES acl_roles (role_id) ON DELETE CASCADE,
    assignee_type VARCHAR(50)  NOT NULL,
    assignee_id   VARCHAR(255) NOT NULL,
    granted_by    VARCHAR(255),
    granted_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMP,
    CONSTRAINT unique_role_assignment UNIQUE (role_id, assignee_type, assignee_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_member ON acl_group_members (member_type, member_id);
CREATE INDEX IF NOT EXISTS idx_group_members_expiration ON acl_group_members (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_role_assignments_assignee ON acl_role_assignments (assignee_type, assignee_id);
CREATE INDEX IF NOT EXISTS idx_role_assignments_expiration ON acl_role_assignments (expires_at) WHERE expires_at IS NOT NULL;

-- Cleanup function for expired memberships/assignments (parallels
-- cleanup_expired_acl_rules()). Returns the total number of rows removed.
CREATE OR REPLACE FUNCTION cleanup_expired_acl_memberships()
    RETURNS INTEGER AS
$$
DECLARE
    deleted_members     INTEGER;
    deleted_assignments INTEGER;
BEGIN
    DELETE FROM acl_group_members WHERE expires_at IS NOT NULL AND expires_at <= NOW();
    GET DIAGNOSTICS deleted_members = ROW_COUNT;

    DELETE FROM acl_role_assignments WHERE expires_at IS NOT NULL AND expires_at <= NOW();
    GET DIAGNOSTICS deleted_assignments = ROW_COUNT;

    RETURN deleted_members + deleted_assignments;
END;
$$ LANGUAGE plpgsql;
