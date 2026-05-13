package acl

import (
	"database/sql"
)

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...interface{}) error
}

// scanACLRule scans a single ACL rule from a database row.
func scanACLRule(s scanner) (*ACLRule, error) {
	var rule ACLRule
	var expiresAt sql.NullTime

	err := s.Scan(
		&rule.RuleID,
		&rule.PrincipalType,
		&rule.PrincipalID,
		&rule.ResourceType,
		&rule.ResourceID,
		&rule.AccessLevel,
		&rule.GrantedBy,
		&rule.GrantedAt,
		&expiresAt,
		&rule.Reason,
	)
	if err != nil {
		return nil, err
	}

	if expiresAt.Valid {
		rule.ExpiresAt = &expiresAt.Time
	}

	return &rule, nil
}
