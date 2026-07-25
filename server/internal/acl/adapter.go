package acl

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/casbin/casbin/v3/model"
	"github.com/casbin/casbin/v3/persist"
	"github.com/scitrera/aether/server/internal/logging"
)

// aclRulesAdapter implements the Casbin persist.Adapter interface by reading
// directly from the existing acl_rules table. Write methods are no-ops because
// the Service handles database writes and then updates the in-memory model.
type aclRulesAdapter struct {
	db *sql.DB
}

var _ persist.Adapter = (*aclRulesAdapter)(nil)

func newACLRulesAdapter(db *sql.DB) *aclRulesAdapter {
	return &aclRulesAdapter{db: db}
}

// LoadPolicy reads all rules from acl_rules and populates the Casbin model.
// Expired rules are excluded at load time to keep the in-memory model clean.
func (a *aclRulesAdapter) LoadPolicy(m model.Model) error {
	query := `
		SELECT principal_type, principal_id, resource_type, resource_id,
		       access_level, expires_at, rule_id
		FROM acl_rules
		WHERE expires_at IS NULL OR expires_at > NOW()
	`

	rows, err := a.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to load ACL policies: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var principalType, principalID, resourceType, resourceID string
		var accessLevel int
		var expiresAt sql.NullTime
		var ruleID string

		if err := rows.Scan(&principalType, &principalID, &resourceType, &resourceID,
			&accessLevel, &expiresAt, &ruleID); err != nil {
			return fmt.Errorf("failed to scan ACL rule: %w", err)
		}

		// Defensive: rewrite legacy ("permission", "_perm:*") rows to the
		// typed admin/* and capability/* families so any rule that survived
		// the data migration (or was inserted directly via SQL after the
		// migration) is still visible under the new key shape that
		// CheckAccess looks up.
		resourceType, resourceID, _ = rewriteLegacyPermission(resourceType, resourceID)

		sub := principalType + ":" + principalID
		obj := resourceType + ":" + resourceID
		act := strconv.Itoa(accessLevel)

		exp := ""
		if expiresAt.Valid {
			exp = expiresAt.Time.Format(time.RFC3339)
		}

		// LoadPolicyArray returns an error only when the supplied tokens fail
		// shape validation against the Casbin model. The arity is fixed by
		// construction here, so any failure indicates a model-config bug at
		// startup rather than a per-row condition we could meaningfully
		// recover from.
		_ = persist.LoadPolicyArray([]string{"p", sub, obj, act, exp, ruleID}, m)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Load role/group membership as Casbin grouping (g) edges. This is
	// additive on top of the per-principal rules above: absence of the
	// groups/roles tables (e.g. a DB that predates migration 028) must not
	// break core rule evaluation, so failures here are logged and skipped
	// rather than aborting the whole policy load.
	a.loadGroupingPolicies(m)

	return nil
}

// loadGroupingPolicies loads acl_group_members and acl_role_assignments as
// Casbin g-rules: g("<member_type>:<member_id>", "group:<group_name>") and
// g("<assignee_type>:<assignee_id>", "role:<role_name>"). Expired edges are
// excluded. Best-effort: a query error (e.g. missing table) is logged and
// skipped so the per-principal rules remain authoritative.
func (a *aclRulesAdapter) loadGroupingPolicies(m model.Model) {
	memberQuery := `
		SELECT m.member_type, m.member_id, g.group_name
		FROM acl_group_members m
		JOIN acl_groups g ON g.group_id = m.group_id
		WHERE m.expires_at IS NULL OR m.expires_at > NOW()
	`
	if rows, err := a.db.Query(memberQuery); err != nil {
		logging.Logger.Warn().Err(err).Msg("acl: failed to load group memberships; group-derived access disabled until next reload")
	} else {
		for rows.Next() {
			var memberType, memberID, groupName string
			if err := rows.Scan(&memberType, &memberID, &groupName); err != nil {
				logging.Logger.Warn().Err(err).Msg("acl: failed to scan group membership row")
				continue
			}
			_ = persist.LoadPolicyArray([]string{"g", memberType + ":" + memberID, GroupSubjectPrefix + groupName}, m)
		}
		rows.Close()
	}

	assignQuery := `
		SELECT a.assignee_type, a.assignee_id, r.role_name
		FROM acl_role_assignments a
		JOIN acl_roles r ON r.role_id = a.role_id
		WHERE a.expires_at IS NULL OR a.expires_at > NOW()
	`
	if rows, err := a.db.Query(assignQuery); err != nil {
		logging.Logger.Warn().Err(err).Msg("acl: failed to load role assignments; role-derived access disabled until next reload")
	} else {
		for rows.Next() {
			var assigneeType, assigneeID, roleName string
			if err := rows.Scan(&assigneeType, &assigneeID, &roleName); err != nil {
				logging.Logger.Warn().Err(err).Msg("acl: failed to scan role assignment row")
				continue
			}
			_ = persist.LoadPolicyArray([]string{"g", assigneeType + ":" + assigneeID, RoleSubjectPrefix + roleName}, m)
		}
		rows.Close()
	}
}

// SavePolicy is a no-op. The Service manages database writes directly.
func (a *aclRulesAdapter) SavePolicy(m model.Model) error {
	return nil
}

// AddPolicy is a no-op. The Service writes to acl_rules, then updates the
// in-memory model via the enforcer's AddPolicy method.
func (a *aclRulesAdapter) AddPolicy(sec string, ptype string, rule []string) error {
	return nil
}

// RemovePolicy is a no-op. The Service deletes from acl_rules, then updates
// the in-memory model via the enforcer's RemoveFilteredPolicy method.
func (a *aclRulesAdapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return nil
}

// RemoveFilteredPolicy is a no-op. See RemovePolicy.
func (a *aclRulesAdapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return nil
}
