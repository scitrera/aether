package acl

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/casbin/casbin/v3/model"
	"github.com/casbin/casbin/v3/persist"
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

		persist.LoadPolicyArray([]string{"p", sub, obj, act, exp, ruleID}, m)
	}

	return rows.Err()
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
