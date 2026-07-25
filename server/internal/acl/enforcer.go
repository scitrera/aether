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
	"github.com/scitrera/aether/server/pkg/models"
)

// Casbin model definition embedded as a string constant so there is no
// external file dependency at runtime. See configs/acl_model.conf for the
// documented version with comments.
const casbinModelText = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act, expires, rule_id

[role_definition]
g = _, _

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
// to a resource. The principal's own subject plus every group/role it
// transitively belongs to (resolved via the Casbin role manager) form the
// "subject set"; rules matching ANY subject in the set are combined additively
// (highest access level wins). The lookup proceeds through specificity tiers:
//
//  1. Subject set + exact resource          (self + groups/roles, additive max)
//  2. Wildcard principal + exact resource
//  3. Subject set + wildcard resource ("*")  (self + groups/roles, additive max)
//  4. Wildcard principal + wildcard resource ("*")
//  5. Glob-pattern rules (any subject in the set)
//  6. (No match — caller applies fallback policy)
//
// With no groups/roles defined the subject set is just the principal, so the
// behavior is identical to the original per-principal evaluation.
//
// Returns nil if no matching rule is found (caller should apply fallback).
func (ce *CasbinEnforcer) EvaluateAccess(ctx context.Context, principal models.Identity, resourceType, resourceID string, requiredLevel int) (*ACLDecision, error) {
	return ce.EvaluateBySubject(PrincipalTypeForModel(principal.Type), principal.CanonicalPrincipalID(), resourceType, resourceID, requiredLevel), nil
}

// EvaluateBySubject is the string-keyed core of EvaluateAccess: it runs the
// specificity ladder for a principal identified by its DB principal_type and
// canonical principal_id (the same values used to build the Casbin subject),
// without requiring a models.Identity. CheckAccess uses EvaluateAccess;
// introspection paths (ExplainAccess) call this directly.
func (ce *CasbinEnforcer) EvaluateBySubject(principalType, principalID, resourceType, resourceID string, requiredLevel int) *ACLDecision {
	sub := principalType + ":" + principalID
	obj := resourceType + ":" + resourceID
	subjects := ce.subjectSet(sub)

	// Step 1: Subject set (self + groups/roles) + exact resource
	if decision := ce.findAndEvaluateMulti(subjects, obj, requiredLevel, "Explicit rule"); decision != nil {
		return decision
	}

	// Step 2: Wildcard principal + exact resource
	for _, wSub := range wildcardSubjects(principalType) {
		if decision := ce.findAndEvaluate(wSub, obj, requiredLevel, "Wildcard rule"); decision != nil {
			return decision
		}
	}

	// Step 3: Subject set + wildcard resource
	if resourceID != WildcardAnyResource {
		wObj := resourceType + ":" + WildcardAnyResource

		if decision := ce.findAndEvaluateMulti(subjects, wObj, requiredLevel, "Any-resource rule"); decision != nil {
			return decision
		}

		// Step 4: Wildcard principal + wildcard resource
		for _, wSub := range wildcardSubjects(principalType) {
			if decision := ce.findAndEvaluate(wSub, wObj, requiredLevel, "Wildcard any-resource rule"); decision != nil {
				return decision
			}
		}
	}

	// Step 5: Glob-pattern rules — scan policies with * or ? for pattern matches
	if decision := ce.findGlobMatch(subjects, obj, requiredLevel); decision != nil {
		return decision
	}

	// Step 6: No match — caller applies fallback
	return nil
}

// subjectSet returns the principal's own subject plus every group/role it
// transitively belongs to. The first element is always the principal's own
// subject. On role-manager error it degrades to just the principal subject.
func (ce *CasbinEnforcer) subjectSet(sub string) []string {
	roles, err := ce.enforcer.GetImplicitRolesForUser(sub)
	if err != nil || len(roles) == 0 {
		return []string{sub}
	}
	return append([]string{sub}, roles...)
}

// findAndEvaluateMulti evaluates (subject, obj) for every subject in the set
// and combines the results additively: the highest non-expired access level
// across all subjects wins. When the winning rule was granted to a group/role
// rather than the principal itself, the reason is annotated for audit clarity.
// subjects[0] is assumed to be the principal's own subject.
func (ce *CasbinEnforcer) findAndEvaluateMulti(subjects []string, obj string, requiredLevel int, label string) *ACLDecision {
	var best *ACLDecision
	var bestSubject string
	for _, s := range subjects {
		d := ce.findAndEvaluate(s, obj, requiredLevel, label)
		if d == nil {
			continue
		}
		if best == nil || d.EffectiveAccessLevel > best.EffectiveAccessLevel {
			best = d
			bestSubject = s
		}
	}
	if best != nil && len(subjects) > 0 && bestSubject != subjects[0] {
		best.Reason = fmt.Sprintf("%s (via %s)", best.Reason, bestSubject)
	}
	return best
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

// findGlobMatch scans all policies once for glob-pattern rules that match any
// subject in the set and the given object. This handles rules like
// "agent:ag._system.platform-server.*" matching
// "agent:ag._system.platform-server.ws-spark-2918". Only policies whose stored
// sub or obj contain glob characters (* or ?) are evaluated — exact-match
// policies are handled by findAndEvaluate. The highest matching level across
// all subjects wins (additive semantics consistent with findAndEvaluateMulti).
func (ce *CasbinEnforcer) findGlobMatch(subjects []string, obj string, requiredLevel int) *ACLDecision {
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

		// Check glob match on the object and on ANY subject in the set
		if !globMatch(obj, pObj) || !anyGlobMatch(subjects, pSub) {
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

// anyGlobMatch reports whether any of the names matches the pattern.
func anyGlobMatch(names []string, pattern string) bool {
	for _, n := range names {
		if globMatch(n, pattern) {
			return true
		}
	}
	return false
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

// AddGrouping adds a grouping (g) edge to the in-memory model: member belongs
// to group/role. Called by the Service after persisting an acl_group_members /
// acl_role_assignments row. member and target are full subjects, e.g.
// ("user:alice", "group:engineering") or ("group:eng", "role:admin").
func (ce *CasbinEnforcer) AddGrouping(member, target string) (bool, error) {
	return ce.enforcer.AddGroupingPolicy(member, target)
}

// RemoveGrouping removes a grouping (g) edge from the in-memory model. Called
// by the Service after deleting the corresponding membership/assignment row.
func (ce *CasbinEnforcer) RemoveGrouping(member, target string) (bool, error) {
	return ce.enforcer.RemoveGroupingPolicy(member, target)
}

// ImplicitRoles returns the transitive set of groups/roles a subject belongs
// to. Used for cycle detection and access-explain introspection.
func (ce *CasbinEnforcer) ImplicitRoles(sub string) ([]string, error) {
	return ce.enforcer.GetImplicitRolesForUser(sub)
}

// SubjectSet returns the principal subject plus its transitive groups/roles.
func (ce *CasbinEnforcer) SubjectSet(sub string) []string {
	return ce.subjectSet(sub)
}

// Contributions returns every rule that matches any subject in the set for the
// given resource — the "why" behind an access decision. See GatherContributions.
func (ce *CasbinEnforcer) Contributions(subjects []string, resourceType, resourceID string) []AccessContribution {
	return GatherContributions(ce.enforcer, subjects, resourceType, resourceID)
}

// GatherContributions scans the in-memory policy set once and returns every
// rule whose subject matches any subject in the set (exact or glob) AND whose
// object matches the resource (exact, the type-wildcard "*", or glob). Each
// contribution records the granting subject, rule id, level, the matched
// resource pattern, and whether the rule is expired — so callers can explain
// exactly which identity (self / group / role) confers which access, and why a
// rule did or did not apply. Shared by both the Postgres and SQLite stores.
func GatherContributions(e *casbin.SyncedEnforcer, subjects []string, resourceType, resourceID string) []AccessContribution {
	subjSet := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		subjSet[s] = true
	}
	obj := resourceType + ":" + resourceID
	wObj := resourceType + ":" + WildcardAnyResource

	policies, _ := e.GetPolicy()
	out := make([]AccessContribution, 0, len(subjects))
	for _, p := range policies {
		if len(p) < 3 {
			continue
		}
		pSub, pObj := p[pIdxSub], p[pIdxObj]

		// Subject match: exact membership, or a glob pattern covering a subject.
		subjMatch := subjSet[pSub]
		if !subjMatch && strings.ContainsAny(pSub, "*?") {
			subjMatch = anyGlobMatch(subjects, pSub)
		}
		if !subjMatch {
			continue
		}

		// Object match: exact, the type-wildcard, or a glob pattern.
		objMatch := pObj == obj || pObj == wObj
		if !objMatch && strings.ContainsAny(pObj, "*?") {
			objMatch = globMatch(obj, pObj)
		}
		if !objMatch {
			continue
		}

		level, err := strconv.Atoi(p[pIdxAct])
		if err != nil {
			continue
		}
		expired := false
		if len(p) > pIdxExpires && p[pIdxExpires] != "" {
			if t, err := time.Parse(time.RFC3339, p[pIdxExpires]); err == nil && time.Now().After(t) {
				expired = true
			}
		}
		ruleID := ""
		if len(p) > pIdxRuleID {
			ruleID = p[pIdxRuleID]
		}
		out = append(out, AccessContribution{
			Subject:     pSub,
			RuleID:      ruleID,
			AccessLevel: level,
			Resource:    pObj,
			Expired:     expired,
		})
	}
	return out
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
