package acl

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/casbin/casbin/v3"
	"github.com/casbin/casbin/v3/model"
	"github.com/scitrera/aether/pkg/models"
)

// Casbin model definition embedded as a string constant so there is no
// external file dependency at runtime. See configs/acl_model.conf for the
// documented version with comments.
const casbinModelText = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act, expires, rule_id

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj
`

// Policy field indices within a Casbin policy slice returned by GetFilteredPolicy.
const (
	pIdxSub     = 0 // "{principal_type}:{principal_id}"
	pIdxObj     = 1 // "{resource_type}:{resource_id}"
	pIdxAct     = 2 // access level as string
	pIdxExpires = 3 // RFC3339 timestamp or ""
	pIdxRuleID  = 4 // UUID
)

// CasbinEnforcer wraps a Casbin SyncedEnforcer to provide in-memory policy
// evaluation using Aether's specificity-priority semantics. Policies are loaded
// from the acl_rules table via the custom adapter; evaluation uses
// GetFilteredPolicy for O(1) in-memory lookups instead of SQL queries.
type CasbinEnforcer struct {
	enforcer *casbin.SyncedEnforcer
	db       *sql.DB
}

// NewCasbinEnforcer creates a new enforcer backed by the acl_rules table.
func NewCasbinEnforcer(db *sql.DB) (*CasbinEnforcer, error) {
	m, err := model.NewModelFromString(casbinModelText)
	if err != nil {
		return nil, fmt.Errorf("failed to create Casbin model: %w", err)
	}

	adapter := newACLRulesAdapter(db)

	e, err := casbin.NewSyncedEnforcer(m, adapter)
	if err != nil {
		return nil, fmt.Errorf("failed to create Casbin enforcer: %w", err)
	}

	// Disable auto-save: the Service handles DB writes, then calls AddPolicy/
	// RemoveFilteredPolicy on the enforcer to update the in-memory model.
	e.EnableAutoSave(false)

	return &CasbinEnforcer{
		enforcer: e,
		db:       db,
	}, nil
}

// EvaluateAccess evaluates whether a principal has the required access level
// to a resource. It performs the 5-step specificity-priority lookup:
//
//  1. Exact principal + exact resource
//  2. Wildcard principal + exact resource
//  3. Exact principal + wildcard resource ("*")
//  4. Wildcard principal + wildcard resource ("*")
//  5. (No match — caller applies fallback policy)
//
// Returns nil if no matching rule is found (caller should apply fallback).
func (ce *CasbinEnforcer) EvaluateAccess(ctx context.Context, principal models.Identity, resourceType, resourceID string, requiredLevel int) (*ACLDecision, error) {
	principalType := PrincipalTypeForModel(principal.Type)
	principalID := principal.CanonicalPrincipalID()

	sub := principalType + ":" + principalID
	obj := resourceType + ":" + resourceID

	// Step 1: Exact principal + exact resource
	if decision := ce.findAndEvaluate(sub, obj, requiredLevel, "Explicit rule"); decision != nil {
		return decision, nil
	}

	// Step 2: Wildcard principal + exact resource
	for _, wSub := range wildcardSubjects(principalType) {
		if decision := ce.findAndEvaluate(wSub, obj, requiredLevel, "Wildcard rule"); decision != nil {
			return decision, nil
		}
	}

	// Step 3: Exact principal + wildcard resource
	if resourceID != WildcardAnyResource {
		wObj := resourceType + ":" + WildcardAnyResource

		if decision := ce.findAndEvaluate(sub, wObj, requiredLevel, "Any-resource rule"); decision != nil {
			return decision, nil
		}

		// Step 4: Wildcard principal + wildcard resource
		for _, wSub := range wildcardSubjects(principalType) {
			if decision := ce.findAndEvaluate(wSub, wObj, requiredLevel, "Wildcard any-resource rule"); decision != nil {
				return decision, nil
			}
		}
	}

	// Step 5: Glob-pattern rules — scan policies with * or ? for pattern matches
	if decision := ce.findGlobMatch(sub, obj, requiredLevel); decision != nil {
		return decision, nil
	}

	// Step 6: No match — caller applies fallback
	return nil, nil
}

// findAndEvaluate looks up policies matching (sub, obj) and returns a decision
// based on the best (highest-level) non-expired rule. Returns nil if no valid
// policy matches.
func (ce *CasbinEnforcer) findAndEvaluate(sub, obj string, requiredLevel int, label string) *ACLDecision {
	policies, _ := ce.enforcer.GetFilteredPolicy(pIdxSub, sub, obj)
	if len(policies) == 0 {
		return nil
	}

	// Find the best (highest level) non-expired rule
	bestLevel := -1
	bestRuleID := ""
	for _, p := range policies {
		// Check expiration (field index 3)
		if len(p) > pIdxExpires && p[pIdxExpires] != "" {
			expiresAt, err := time.Parse(time.RFC3339, p[pIdxExpires])
			if err == nil && time.Now().After(expiresAt) {
				continue // expired
			}
		}

		level, err := strconv.Atoi(p[pIdxAct])
		if err != nil {
			continue
		}

		if level > bestLevel {
			bestLevel = level
			if len(p) > pIdxRuleID {
				bestRuleID = p[pIdxRuleID]
			}
		}
	}

	if bestLevel < 0 {
		return nil // all expired or unparseable
	}

	decision := &ACLDecision{
		Allowed:              bestLevel >= requiredLevel,
		EffectiveAccessLevel: bestLevel,
		Decision:             DecisionDeny,
		Reason:               fmt.Sprintf("%s: %s", label, AccessLevelName(bestLevel)),
	}

	if decision.Allowed {
		decision.Decision = DecisionAllow
	}

	// Populate RuleApplied with the rule ID for audit logging
	if bestRuleID != "" {
		decision.RuleApplied = &ACLRule{
			RuleID:      bestRuleID,
			AccessLevel: bestLevel,
		}
	}

	return decision
}

// findGlobMatch scans all policies for glob-pattern rules that match the given
// subject and object. This handles rules like "agent:ag._system.platform-server.*"
// matching "agent:ag._system.platform-server.ws-spark-2918".
// Only policies whose stored sub or obj contain glob characters (* or ?) are
// evaluated — exact-match policies are handled by findAndEvaluate.
func (ce *CasbinEnforcer) findGlobMatch(sub, obj string, requiredLevel int) *ACLDecision {
	policies, _ := ce.enforcer.GetPolicy()
	if len(policies) == 0 {
		return nil
	}

	bestLevel := -1
	bestRuleID := ""

	for _, p := range policies {
		if len(p) < 3 {
			continue
		}
		pSub, pObj := p[pIdxSub], p[pIdxObj]

		// Skip policies without glob characters — already handled by exact match
		hasGlob := strings.ContainsAny(pSub, "*?") || strings.ContainsAny(pObj, "*?")
		if !hasGlob {
			continue
		}

		// Check glob match on both subject and object
		if !globMatch(sub, pSub) || !globMatch(obj, pObj) {
			continue
		}

		// Check expiration
		if len(p) > pIdxExpires && p[pIdxExpires] != "" {
			expiresAt, err := time.Parse(time.RFC3339, p[pIdxExpires])
			if err == nil && time.Now().After(expiresAt) {
				continue
			}
		}

		level, err := strconv.Atoi(p[pIdxAct])
		if err != nil {
			continue
		}

		if level > bestLevel {
			bestLevel = level
			if len(p) > pIdxRuleID {
				bestRuleID = p[pIdxRuleID]
			}
		}
	}

	if bestLevel < 0 {
		return nil
	}

	decision := &ACLDecision{
		Allowed:              bestLevel >= requiredLevel,
		EffectiveAccessLevel: bestLevel,
		Decision:             DecisionDeny,
		Reason:               fmt.Sprintf("Glob pattern rule: %s", AccessLevelName(bestLevel)),
	}
	if decision.Allowed {
		decision.Decision = DecisionAllow
	}
	if bestRuleID != "" {
		decision.RuleApplied = &ACLRule{
			RuleID:      bestRuleID,
			AccessLevel: bestLevel,
		}
	}
	return decision
}

// globMatch wraps path.Match for glob-style pattern matching.
// Returns true if name matches the pattern. Patterns use * (match any
// sequence of characters) and ? (match single character).
func globMatch(name, pattern string) bool {
	matched, _ := path.Match(pattern, name)
	return matched
}

// AddPolicy adds a rule to the in-memory model. Called by Service after writing
// to acl_rules. The adapter's AddPolicy is a no-op, so this only touches memory.
func (ce *CasbinEnforcer) AddPolicy(sub, obj, act, expires, ruleID string) (bool, error) {
	return ce.enforcer.AddPolicy(sub, obj, act, expires, ruleID)
}

// RemovePolicy removes all policies matching (sub, obj) from the in-memory model.
// Called by Service after deleting from acl_rules.
func (ce *CasbinEnforcer) RemovePolicy(sub, obj string) (bool, error) {
	return ce.enforcer.RemoveFilteredPolicy(pIdxSub, sub, obj)
}

// ReloadPolicies reloads all policies from the database. Used after bulk
// changes (e.g., fallback policy updates) that may affect cached decisions.
func (ce *CasbinEnforcer) ReloadPolicies() error {
	return ce.enforcer.LoadPolicy()
}

// wildcardSubjects returns the wildcard subject strings that match a principal type.
func wildcardSubjects(principalType string) []string {
	switch principalType {
	case PrincipalTypeUser:
		return []string{PrincipalTypeWildcard + ":" + WildcardAnyAuthenticatedUser}
	case PrincipalTypeAgent:
		return []string{PrincipalTypeWildcard + ":" + WildcardAnyAgent}
	case PrincipalTypeTask:
		return []string{PrincipalTypeWildcard + ":" + WildcardAnyTask}
	case PrincipalTypeService:
		return []string{PrincipalTypeWildcard + ":" + WildcardAnyService}
	default:
		return nil
	}
}
