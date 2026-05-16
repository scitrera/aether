package acl

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

// RequestAuthorityContext is the request-time on-behalf-of context supplied by
// the caller after transport authentication.
type RequestAuthorityContext struct {
	Mode    string
	Subject models.Identity
	GrantID string
}

// GrantAudienceContext carries live request/session data used to validate that
// a persisted authority grant is being used by the bound audience.
type GrantAudienceContext struct {
	SessionID        uuid.UUID
	AssociatedTaskID string
	Actor            models.Identity

	// SessionActive optionally reports whether a session is still live.
	// Used when ValidWhileAudienceActive=true and AudienceType=session.
	// If nil, only the SessionID match is checked.
	SessionActive func(sessionID uuid.UUID) bool

	// TaskActive optionally reports whether a task is still in an active state
	// (pending/running/assigned). Used when ValidWhileAudienceActive=true and
	// AudienceType=task. If nil, only the AssociatedTaskID match is checked.
	TaskActive func(taskID string) bool
}

// ResolvedAuthority is the validated authority envelope for a single request.
type ResolvedAuthority struct {
	Actor   models.Identity
	Subject models.Identity
	Grant   *AuthorityGrant
}

// ResolveAuthority validates a request-time authority context against the
// authenticated actor and live audience.
func (s *Service) ResolveAuthority(ctx context.Context, actor models.Identity, req RequestAuthorityContext, audience GrantAudienceContext) (*ResolvedAuthority, error) {
	if req.GrantID == "" {
		return nil, fmt.Errorf("%w: grant_id is required", ErrInvalidAuthorityContext)
	}

	subjectRef := req.Subject.PrincipalRef()
	if subjectRef.IsZero() {
		return nil, fmt.Errorf("%w: subject is required", ErrInvalidAuthorityContext)
	}

	grant, err := s.GetAuthorityGrant(ctx, req.GrantID)
	if err != nil {
		return nil, err
	}
	if err := grant.ValidateActiveAt(time.Now()); err != nil {
		return nil, err
	}

	actorRef := actor.PrincipalRef()
	if actorRef.IsZero() {
		return nil, fmt.Errorf("%w: actor principal is incomplete", ErrInvalidAuthorityContext)
	}
	if PrincipalTypeForModel(actorRef.Type) != grant.DelegateType || actorRef.ID != grant.DelegateID {
		return nil, ErrAuthorityGrantDelegateMismatch
	}
	if PrincipalTypeForModel(subjectRef.Type) != grant.SubjectType || subjectRef.ID != grant.SubjectID {
		return nil, ErrAuthorityGrantSubjectMismatch
	}
	if err := validateGrantAudience(grant, actor, audience); err != nil {
		return nil, err
	}

	return &ResolvedAuthority{
		Actor:   actor,
		Subject: req.Subject,
		Grant:   grant,
	}, nil
}

// CheckAccessWithAuthority evaluates access under a validated on-behalf-of
// grant. The subject's ACL is intersected with the grant constraints.
func (s *Service) CheckAccessWithAuthority(ctx context.Context, actor models.Identity, authority *ResolvedAuthority, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*ACLDecision, error) {
	if authority == nil {
		return s.CheckAccess(ctx, actor, resourceType, resourceID, operation, workspace, sessionID, requiredLevel)
	}

	// Phase 5 Stage B: resolve owning-agent attribution once and reuse for
	// both audit paths below (constraint-violation early-out + happy path).
	owningImpl, owningPrefix := "", ""
	if s.prefixIndex != nil {
		if impl, prefix, ok := s.prefixIndex.Lookup(resourceType); ok {
			owningImpl = impl
			owningPrefix = prefix
		}
	}

	if decision := validateGrantConstraints(authority.Grant, resourceType, resourceID, operation, workspace, requiredLevel); decision != nil {
		decision.AuthorityGrant = authority.Grant
		decision.AuthorityMode = "on_behalf_of"
		s.audit.LogDecisionWithAttribution(ctx, decision, actor, resourceType, resourceID, operation, workspace, sessionID, owningImpl, owningPrefix)
		return decision, nil
	}

	decision, err := s.evaluateAccessNoAudit(ctx, authority.Subject, resourceType, resourceID, requiredLevel)
	if err != nil {
		return nil, err
	}
	decision.AuthorityGrant = authority.Grant
	decision.AuthorityMode = "on_behalf_of"

	s.audit.LogDecisionWithAttribution(ctx, decision, actor, resourceType, resourceID, operation, workspace, sessionID, owningImpl, owningPrefix)
	return decision, nil
}

func validateGrantAudience(grant *AuthorityGrant, actor models.Identity, audience GrantAudienceContext) error {
	switch grant.AudienceType {
	case AuthorityAudienceSession:
		// Strict match: the grant's audience session IS the current session
		// (e.g. user self-exchange grants used on the same connection).
		currentSessionID := ""
		if audience.SessionID != uuid.Nil {
			currentSessionID = audience.SessionID.String()
		}
		strictMatch := currentSessionID != "" && grant.AudienceID == currentSessionID
		if strictMatch {
			if grant.ValidWhileAudienceActive && audience.SessionActive != nil {
				if !audience.SessionActive(audience.SessionID) {
					return ErrAuthorityGrantAudienceMismatch
				}
			}
			break
		}
		// Non-strict: the grant's audience is a different session (e.g. a
		// trusted-service exchange where the grant lifetime tracks the source
		// user session, not the service's own session). Require that the
		// grant was explicitly issued with audience-active lifecycle binding
		// and that the bound session is still live.
		if !grant.ValidWhileAudienceActive || audience.SessionActive == nil {
			return ErrAuthorityGrantAudienceMismatch
		}
		boundUUID, err := uuid.Parse(grant.AudienceID)
		if err != nil || !audience.SessionActive(boundUUID) {
			return ErrAuthorityGrantAudienceMismatch
		}
	case AuthorityAudienceTask:
		if audience.AssociatedTaskID == "" || grant.AudienceID != audience.AssociatedTaskID {
			return ErrAuthorityGrantAudienceMismatch
		}
		if grant.ValidWhileAudienceActive && audience.TaskActive != nil {
			if !audience.TaskActive(grant.AudienceID) {
				return ErrAuthorityGrantAudienceMismatch
			}
		}
	case AuthorityAudienceAgent:
		if actor.Type != models.PrincipalAgent || actor.CanonicalPrincipalID() != grant.AudienceID {
			return ErrAuthorityGrantAudienceMismatch
		}
	case AuthorityAudienceService:
		if actor.Type != models.PrincipalService || actor.CanonicalPrincipalID() != grant.AudienceID {
			return ErrAuthorityGrantAudienceMismatch
		}
	default:
		return ErrAuthorityGrantAudienceMismatch
	}

	return nil
}

func validateGrantConstraints(grant *AuthorityGrant, resourceType, resourceID, operation, workspace string, requiredLevel int) *ACLDecision {
	switch {
	case requiredLevel > grant.MaxAccessLevel:
		return authorityDenyDecision(grant, ErrAuthorityGrantScopeEscalation.Error())
	case !matchesConstraintValue(operation, grant.OperationScope):
		return authorityDenyDecision(grant, ErrAuthorityGrantOperationDenied.Error())
	case workspace != "" && !matchesWorkspaceConstraint(workspace, grant.WorkspaceScope):
		return authorityDenyDecision(grant, ErrAuthorityGrantWorkspaceDenied.Error())
	}

	if len(grant.ResourceScope) > 0 {
		patterns, ok := grant.ResourceScope[resourceType]
		if !ok || !matchesConstraintValue(resourceID, patterns) {
			return authorityDenyDecision(grant, ErrAuthorityGrantResourceDenied.Error())
		}
	}

	return nil
}

func authorityDenyDecision(grant *AuthorityGrant, reason string) *ACLDecision {
	return &ACLDecision{
		Allowed:              false,
		EffectiveAccessLevel: grant.MaxAccessLevel,
		Decision:             DecisionDeny,
		AuthorityGrant:       grant,
		AuthorityMode:        "on_behalf_of",
		Reason:               reason,
	}
}

func matchesConstraintValue(value string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if pattern == value {
			return true
		}
		matched, err := path.Match(pattern, value)
		if err == nil && matched {
			return true
		}
	}
	return false
}

// matchesWorkspaceConstraint applies workspace-scope matching with explicit
// recognition of the WorkspaceScopeSubjectInherited magic value, which signals
// "delegate to subject ACL" and matches any workspace. This is semantically
// equivalent to "*" but documents intent for audit and tooling.
func matchesWorkspaceConstraint(workspace string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if pattern == WorkspaceScopeSubjectInherited {
			return true
		}
	}
	return matchesConstraintValue(workspace, patterns)
}
