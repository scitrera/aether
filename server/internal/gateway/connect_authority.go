package gateway

import (
	"context"
	"strings"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// handleResolveAuthority resolves an authority grant against the caller's
// identity and the live audience, then projects the grant to a public-safe
// subset (AuthorityGrantInfo) and applies visibility ACL.
//
// The caller is the actor for ACL purposes. By default the request's `actor`
// field is used; if it is empty, the gateway substitutes the calling
// session's authenticated identity.
//
// Visibility rules (in order):
//  1. caller == grant.DelegateID  → allow (implicit self-grant)
//  2. caller == grant.AudienceID  → allow (implicit audience self-resolve)
//  3. caller has READ on `capability/resolve_authority` → allow
//  4. otherwise → deny with `error="not authorized"` and NO grant fields.
//
// On any underlying resolution error (grant not found, expired, audience
// invalid, etc.) the response is `ok=false, error=<err>` with no grant
// fields, regardless of visibility.
func (s *GatewayServer) handleResolveAuthority(ctx context.Context, client *ClientSession, identity models.Identity, req *pb.ResolveAuthorityRequest) {
	requestID := req.GetRequestId()

	if s.acl == nil {
		sendResolveAuthorityError(client, requestID, "ACL service not configured")
		return
	}

	if strings.TrimSpace(req.GetGrantId()) == "" {
		sendResolveAuthorityError(client, requestID, "grant_id is required")
		return
	}

	// Determine the actor identity. The request can supply one, but on the
	// happy path callers leave it empty and we use the session identity.
	actor := identity
	if req.GetActor() != nil &&
		(strings.TrimSpace(req.GetActor().GetPrincipalType()) != "" ||
			strings.TrimSpace(req.GetActor().GetPrincipalId()) != "") {
		parsed, err := protoPrincipalRefToIdentity(req.GetActor())
		if err != nil {
			sendResolveAuthorityError(client, requestID, "invalid actor: "+err.Error())
			return
		}
		actor = parsed
	}

	// Subject must be supplied.
	if req.GetSubject() == nil {
		sendResolveAuthorityError(client, requestID, "subject is required")
		return
	}
	subject, err := protoPrincipalRefToIdentity(req.GetSubject())
	if err != nil {
		sendResolveAuthorityError(client, requestID, "invalid subject: "+err.Error())
		return
	}

	// Resolve. ResolveAuthority validates the actor against grant.delegate,
	// the subject against grant.subject, and the audience freshness via
	// validateGrantAudience. Any failure yields a non-nil error and we
	// return ok=false with no grant fields.
	resolved, err := s.acl.ResolveAuthority(ctx, actor, acl.RequestAuthorityContext{
		Mode:    "on_behalf_of",
		Subject: subject,
		GrantID: req.GetGrantId(),
	}, acl.GrantAudienceContext{
		SessionID:        client.SessionUUID,
		AssociatedTaskID: client.AssociatedTaskID,
		Actor:            actor,
		SessionActive: func(sessionID uuid.UUID) bool {
			ident, err := s.sessions.GetSessionIdentity(ctx, sessionID.String())
			if err != nil {
				return false
			}
			active, err := s.sessions.IsActive(ctx, ident.String())
			return err == nil && active
		},
		TaskActive: nil, // task liveness check not required for resolution; reuse grant audience strict-match path
	})
	if err != nil {
		// Grant not found / expired / revoked / audience mismatch / etc.
		// Per spec: no fields leaked, just error string.
		sendResolveAuthorityError(client, requestID, err.Error())
		return
	}
	grant := resolved.Grant

	// Visibility ACL: allow if caller is the grant's delegate or the grant's
	// audience principal; else require READ on capability/resolve_authority.
	if !s.callerCanSeeResolvedAuthority(ctx, client, identity, grant) {
		sendResolveAuthorityError(client, requestID, "not authorized")
		return
	}

	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ResolveAuthorityResponse{
			ResolveAuthorityResponse: &pb.ResolveAuthorityResponse{
				RequestId: requestID,
				Ok:        true,
				Authority: &pb.ResolvedAuthority{
					Actor:   actorPrincipalRef(resolved.Actor),
					Subject: actorPrincipalRef(resolved.Subject),
					Grant:   authorityGrantToInfoProto(grant),
				},
			},
		},
	})
}

// handleConnectionStatus reports whether a principal currently has a live
// session lock. Self-queries are trivially allowed; cross-principal queries
// require READ on capability/query_connections.
func (s *GatewayServer) handleConnectionStatus(ctx context.Context, client *ClientSession, identity models.Identity, req *pb.ConnectionStatusRequest) {
	requestID := req.GetRequestId()

	if req.GetPrincipal() == nil {
		sendConnectionStatusError(client, requestID, "principal is required")
		return
	}
	target, err := protoPrincipalRefToIdentity(req.GetPrincipal())
	if err != nil {
		sendConnectionStatusError(client, requestID, "invalid principal: "+err.Error())
		return
	}

	// Self-check short-circuit. Comparing canonical identity strings sidesteps
	// nuances around Workspace defaults across principal types.
	if identity.String() == target.String() {
		s.sendConnectionStatusForIdentity(ctx, client, requestID, target)
		return
	}

	// Cross-principal: require capability/query_connections at READ.
	if !s.isAllowedConnectionQuery(ctx, client, identity) {
		sendConnectionStatusError(client, requestID, "not authorized")
		return
	}

	s.sendConnectionStatusForIdentity(ctx, client, requestID, target)
}

func (s *GatewayServer) sendConnectionStatusForIdentity(ctx context.Context, client *ClientSession, requestID string, target models.Identity) {
	connected := false
	if s.sessions != nil {
		active, err := s.sessions.IsActive(ctx, target.String())
		if err != nil {
			logging.Logger.Warn().Err(err).Str("target", target.String()).Msg("handleConnectionStatus: IsActive failed")
			sendConnectionStatusError(client, requestID, err.Error())
			return
		}
		connected = active
	}

	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionStatusResponse{
			ConnectionStatusResponse: &pb.ConnectionStatusResponse{
				RequestId: requestID,
				Ok:        true,
				Connected: connected,
				// last_seen_at intentionally omitted — the SessionRegistry
				// IsActive path doesn't carry a cheap timestamp; leaving
				// zero per the documented "0 = unknown" contract.
			},
		},
	})
}

// callerCanSeeResolvedAuthority returns true if the caller's identity matches
// the grant's delegate or audience principal, OR if the caller has READ on
// capability/resolve_authority. Otherwise false.
func (s *GatewayServer) callerCanSeeResolvedAuthority(ctx context.Context, client *ClientSession, identity models.Identity, grant *acl.AuthorityGrant) bool {
	if grant == nil {
		return false
	}

	// Implicit allow: caller is the grant's delegate (actor side).
	if authorityGrantPrincipalMatches(identity, grant.DelegateType, grant.DelegateID) {
		return true
	}
	// Implicit allow: caller is the grant's audience (audience side). This
	// only applies for principal-typed audiences (agent, service); session-
	// and task-typed audiences are scoped to a session ID / task ID, not a
	// principal identity, so the implicit-audience match doesn't apply.
	switch grant.AudienceType {
	case acl.AuthorityAudienceAgent:
		if identity.Type == models.PrincipalAgent && identity.CanonicalPrincipalID() == grant.AudienceID {
			return true
		}
	case acl.AuthorityAudienceService:
		if identity.Type == models.PrincipalService && identity.CanonicalPrincipalID() == grant.AudienceID {
			return true
		}
	}

	return s.isAllowedAuthorityResolve(ctx, client, identity)
}

// isAllowedAuthorityResolve checks whether the caller has READ on
// capability/resolve_authority. System principals (WorkflowEngine, Orchestrator)
// are implicitly allowed — they're already trusted to drive admin/system
// flows that need to introspect grants.
func (s *GatewayServer) isAllowedAuthorityResolve(ctx context.Context, client *ClientSession, identity models.Identity) bool {
	switch identity.Type {
	case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator:
		return true
	}

	if s.acl == nil {
		return false
	}

	decision, err := s.acl.CheckAccess(
		ctx, identity,
		acl.ResourceTypeCapability, acl.PermissionResolveAuthority,
		"resolve_authority", identity.Workspace, client.SessionUUID, acl.AccessRead,
	)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("identity", identity.String()).Msg("isAllowedAuthorityResolve: ACL check failed")
		return false
	}
	return decision != nil && decision.Allowed
}

// isAllowedConnectionQuery checks whether the caller has READ on
// capability/query_connections. System principals (WorkflowEngine, Orchestrator)
// are implicitly allowed.
func (s *GatewayServer) isAllowedConnectionQuery(ctx context.Context, client *ClientSession, identity models.Identity) bool {
	switch identity.Type {
	case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator:
		return true
	}

	if s.acl == nil {
		return false
	}

	decision, err := s.acl.CheckAccess(
		ctx, identity,
		acl.ResourceTypeCapability, acl.PermissionQueryConnections,
		"query_connections", identity.Workspace, client.SessionUUID, acl.AccessRead,
	)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("identity", identity.String()).Msg("isAllowedConnectionQuery: ACL check failed")
		return false
	}
	return decision != nil && decision.Allowed
}

// grantToResolvedAuthorityInfo projects an internal AuthorityGrant to the
// minimal subset the SDK terminator needs for header minting:
// root-subject, audience binding, max access level, workspace scope, and
// expiry. Stamped onto AuthorizationContext.resolved by the gateway after
// proxyACLCheck so audience-side terminators don't need to re-resolve.
//
// Compared to authorityGrantToInfoProto this is leaner: grant_id /
// subject / revoked are already on AuthorizationContext, and the terminator
// has no use for delegate fields (the gateway already validated against
// the delegate at routing time).
func grantToResolvedAuthorityInfo(grant *acl.AuthorityGrant) *pb.ResolvedAuthorityInfo {
	if grant == nil {
		return nil
	}
	expiresAtMs := int64(0)
	if !grant.ExpiresAt.IsZero() {
		expiresAtMs = grant.ExpiresAt.UnixMilli()
	}
	var rootSubject *pb.PrincipalRef
	if grant.RootSubjectType != "" || grant.RootSubjectID != "" {
		rootSubject = &pb.PrincipalRef{
			PrincipalType: grant.RootSubjectType,
			PrincipalId:   grant.RootSubjectID,
		}
	}
	return &pb.ResolvedAuthorityInfo{
		RootSubject:    rootSubject,
		AudienceType:   grant.AudienceType,
		AudienceId:     grant.AudienceID,
		MaxAccessLevel: int32(grant.MaxAccessLevel),
		WorkspaceScope: expandWorkspaceScopeForWire(grant.WorkspaceScope),
		ExpiresAtMs:    expiresAtMs,
	}
}

// expandWorkspaceScopeForWire normalizes the grant's workspace_scope for
// downstream consumption. The intent-bearing magic value
// acl.WorkspaceScopeSubjectInherited ("_subject_workspaces") is preserved
// in the grant DB row and Aether's internal matcher, but terminators
// (memorylayer, etc.) implement their own "* in scope OR workspace_id in
// scope" check and don't know about the magic value. Expand it to "*"
// at the wire boundary so terminators do the right thing without needing
// to know the symbol.
func expandWorkspaceScopeForWire(scope []string) []string {
	if len(scope) == 0 {
		return nil
	}
	out := make([]string, 0, len(scope))
	expanded := false
	for _, s := range scope {
		if s == acl.WorkspaceScopeSubjectInherited {
			if !expanded {
				out = append(out, "*")
				expanded = true
			}
			continue
		}
		out = append(out, s)
	}
	return out
}

// authorityGrantToInfoProto projects an internal AuthorityGrant to the
// public-safe AuthorityGrantInfo used in ResolveAuthorityResponse. ResourceScope,
// OperationScope, Metadata, RemainingHops, ParentGrantID, etc. are
// intentionally omitted — terminators don't need them and exposing them
// would broaden the leakage surface.
func authorityGrantToInfoProto(grant *acl.AuthorityGrant) *pb.AuthorityGrantInfo {
	if grant == nil {
		return nil
	}
	expiresAt := int64(0)
	if !grant.ExpiresAt.IsZero() {
		expiresAt = grant.ExpiresAt.Unix()
	}
	return &pb.AuthorityGrantInfo{
		GrantId:         grant.GrantID,
		SubjectType:     grant.SubjectType,
		SubjectId:       grant.SubjectID,
		RootSubjectType: grant.RootSubjectType,
		RootSubjectId:   grant.RootSubjectID,
		AudienceType:    grant.AudienceType,
		AudienceId:      grant.AudienceID,
		MaxAccessLevel:  int32(grant.MaxAccessLevel),
		WorkspaceScope:  expandWorkspaceScopeForWire(grant.WorkspaceScope),
		ExpiresAt:       expiresAt,
		Revoked:         grant.Revoked,
	}
}

// actorPrincipalRef converts a models.Identity to a PrincipalRef using the
// canonical principal id form so downstream consumers see consistent strings.
func actorPrincipalRef(id models.Identity) *pb.PrincipalRef {
	if id.Type == "" {
		return nil
	}
	return &pb.PrincipalRef{
		PrincipalType: acl.PrincipalTypeForModel(id.Type),
		PrincipalId:   id.CanonicalPrincipalID(),
	}
}

func sendResolveAuthorityError(client *ClientSession, requestID, errMsg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ResolveAuthorityResponse{
			ResolveAuthorityResponse: &pb.ResolveAuthorityResponse{
				RequestId: requestID,
				Ok:        false,
				Error:     errMsg,
			},
		},
	})
}

func sendConnectionStatusError(client *ClientSession, requestID, errMsg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionStatusResponse{
			ConnectionStatusResponse: &pb.ConnectionStatusResponse{
				RequestId: requestID,
				Ok:        false,
				Error:     errMsg,
			},
		},
	})
}
