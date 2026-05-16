// Phase 2 Stage C: gateway handler for AuthorityRequestOperation.
//
// This file routes UpstreamMessage.authority_request_op (wire tag 30) into the
// Stage B lifecycle methods on *acl.Service. Like its sibling
// authority_grant_handler.go, the dispatch keys on the embedded OpType enum
// and emits responses + lifecycle events via DownstreamMessage.
//
// Authorization:
//   - CREATE:  always allowed (the caller is asking on their own behalf).
//   - GET:     caller must be the requester, the target subject, the routing
//              principal, OR hold the routing capability. Info-hiding: on
//              authorization failure we return a "not found"-shaped error.
//   - LIST_PENDING: caller-declared filter (matching_capabilities) is
//              propagated to the storage layer's ResolverCapabilities. The
//              ResolverPrincipal is fixed to the caller's own identity. The
//              server does NOT auto-discover the caller's full capability set
//              in this phase.
//   - RESOLVE: if routing.principal is set, caller's identity must match
//              exactly. If routing.capability is set, caller must pass an
//              ACL CheckAccess against ResourceTypeCapability with the
//              routing capability string. On deny, surface a "not found"-style
//              error (info hiding -- consistent with GET).
//   - CANCEL:  caller's identity must equal the request's requesting_actor.
//
// Event emission:
//   The gateway does not yet have a task-scoped event topic taxonomy (that
//   lands with subscribe-to-task in Stage 4 of the broader plan), so for now
//   every state transition pushes the AuthorityRequestEvent directly to the
//   originating client session via SafeSend. The waker (task_waker.go)
//   handles WAITING_AUTHORITY transitions independently of these events.

package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// handleAuthorityRequestOp dispatches an inbound AuthorityRequestOperation to
// the appropriate Stage B lifecycle method. Mirrors handleAuthorityGrantOp
// in shape: switch on op type, log-and-respond on success/failure.
func (s *GatewayServer) handleAuthorityRequestOp(ctx context.Context, client *ClientSession, op *pb.AuthorityRequestOperation) {
	if op == nil {
		sendAuthorityRequestError(client, "", "authority_request_op is required")
		return
	}
	if s.acl == nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), "ACL service not configured")
		return
	}

	client.identityMu.RLock()
	caller := client.Identity
	client.identityMu.RUnlock()

	switch op.GetOp() {
	case pb.AuthorityRequestOperation_CREATE:
		s.handleAuthorityRequestCreate(ctx, client, caller, op)
	case pb.AuthorityRequestOperation_GET:
		s.handleAuthorityRequestGet(ctx, client, caller, op)
	case pb.AuthorityRequestOperation_LIST_PENDING:
		s.handleAuthorityRequestList(ctx, client, caller, op)
	case pb.AuthorityRequestOperation_RESOLVE:
		s.handleAuthorityRequestResolve(ctx, client, caller, op)
	case pb.AuthorityRequestOperation_CANCEL:
		s.handleAuthorityRequestCancel(ctx, client, caller, op)
	default:
		sendAuthorityRequestError(client, op.GetClientRequestId(), "unknown authority request operation")
	}
}

// =============================================================================
// CREATE
// =============================================================================

func (s *GatewayServer) handleAuthorityRequestCreate(ctx context.Context, client *ClientSession, caller models.Identity, op *pb.AuthorityRequestOperation) {
	payload := op.GetCreate()
	if payload == nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), "create payload is required")
		return
	}

	// RequestingActor: caller-supplied or default to the connected identity.
	requesting, err := optionalPrincipalRefToIdentity(payload.GetRequestingActor(), caller)
	if err != nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), fmt.Sprintf("invalid requesting_actor: %v", err))
		return
	}

	// TargetSubject: optional. Zero-valued when omitted.
	var targetSubject models.Identity
	if ref := payload.GetTargetSubject(); ref != nil {
		ts, err := protoPrincipalRefToIdentity(ref)
		if err != nil {
			sendAuthorityRequestError(client, op.GetClientRequestId(), fmt.Sprintf("invalid target_subject: %v", err))
			return
		}
		targetSubject = ts
	}

	// Routing target: exactly one of (principal, capability).
	routing, err := protoAuthorityRequestRoutingToACL(payload.GetRoutingTarget())
	if err != nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
		return
	}

	req := &acl.AuthorityRequest{
		RequestingActor: requesting,
		TargetSubject:   targetSubject,
		WorkspaceScope:  append([]string(nil), payload.GetDesiredWorkspaceScope()...),
		ResourceScope:   protoAuthorityRequestResourceScopeToACL(payload.GetDesiredResourceScope()),
		OperationScope:  append([]string(nil), payload.GetDesiredOperationScope()...),
		RequestedAccess: accessLevelProtoToInt(payload.GetRequestedAccessLevel()),
		DurationSeconds: payload.GetRequestedDurationSeconds(),
		AudienceType:    payload.GetAudienceType(),
		AudienceID:      payload.GetAudienceId(),
		RoutingTarget:   routing,
		Reason:          payload.GetReason(),
		TaskID:          payload.GetTaskId(),
		Metadata:        protoStringMapToMetadata(payload.GetMetadata()),
	}

	persisted, err := s.acl.SubmitAuthorityRequest(ctx, req)
	if err != nil {
		logging.Logger.Error().Err(err).Str("actor", caller.String()).Msg("handleAuthorityRequestOp: create failed")
		sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
		return
	}

	// Push the response and a CREATED event on the originating session.
	sendAuthorityRequestResponse(client, op.GetClientRequestId(), "authority request submitted", persisted, nil)
	s.emitAuthorityRequestEvent(client, persisted, pb.AuthorityRequestEvent_AUTHORITY_REQUEST_EVENT_CREATED)
}

// =============================================================================
// GET
// =============================================================================

func (s *GatewayServer) handleAuthorityRequestGet(ctx context.Context, client *ClientSession, caller models.Identity, op *pb.AuthorityRequestOperation) {
	requestID := strings.TrimSpace(op.GetRequestId())
	if requestID == "" {
		sendAuthorityRequestError(client, op.GetClientRequestId(), "request_id is required")
		return
	}

	req, err := s.acl.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
		return
	}

	if !s.callerMayViewAuthorityRequest(ctx, client, caller, req) {
		// Info hiding: leak no signal that the row exists. Match Stage 4's
		// preferred shape: act as if GetAuthorityRequest returned not-found.
		sendAuthorityRequestError(client, op.GetClientRequestId(), acl.ErrAuthorityRequestNotFound.Error())
		return
	}

	sendAuthorityRequestResponse(client, op.GetClientRequestId(), "", req, nil)
}

// =============================================================================
// LIST_PENDING
// =============================================================================

func (s *GatewayServer) handleAuthorityRequestList(ctx context.Context, client *ClientSession, caller models.Identity, op *pb.AuthorityRequestOperation) {
	filter := acl.AuthorityRequestFilter{
		Status:            acl.AuthorityRequestStatusPending,
		ResolverPrincipal: &caller,
	}
	if lf := op.GetListFilter(); lf != nil {
		filter.Workspace = lf.GetWorkspace()
		filter.Limit = int(lf.GetLimit())
		filter.Offset = int(lf.GetOffset())
		filter.ResolverCapabilities = append([]string(nil), lf.GetMatchingCapabilities()...)
		// Allow callers to widen status filter on LIST_PENDING only when
		// they explicitly ask for a different one; the default remains
		// pending.
		if proto := lf.GetStatus(); proto != pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_UNSPECIFIED {
			filter.Status = authorityRequestStatusFromProto(proto)
		}
	}

	rows, err := s.acl.ListAuthorityRequests(ctx, filter)
	if err != nil {
		logging.Logger.Error().Err(err).Str("actor", caller.String()).Msg("handleAuthorityRequestOp: list failed")
		sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
		return
	}

	sendAuthorityRequestResponse(client, op.GetClientRequestId(), "", nil, rows)
}

// =============================================================================
// RESOLVE
// =============================================================================

func (s *GatewayServer) handleAuthorityRequestResolve(ctx context.Context, client *ClientSession, caller models.Identity, op *pb.AuthorityRequestOperation) {
	requestID := strings.TrimSpace(op.GetRequestId())
	if requestID == "" {
		sendAuthorityRequestError(client, op.GetClientRequestId(), "request_id is required")
		return
	}
	payload := op.GetResolve()
	if payload == nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), "resolve payload is required")
		return
	}

	existing, err := s.acl.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
		return
	}

	// Authorization: caller must be the routing principal or hold the routing
	// capability. Info-hiding on deny.
	if !s.callerMayResolveAuthorityRequest(ctx, client, caller, existing) {
		sendAuthorityRequestError(client, op.GetClientRequestId(), acl.ErrAuthorityRequestNotFound.Error())
		return
	}

	switch payload.GetDecision() {
	case pb.ResolveAuthorityRequestPayload_APPROVE:
		decision := &acl.ApproveDecision{
			Reason:                 payload.GetReason(),
			GrantedWorkspaceScope:  append([]string(nil), payload.GetGrantedWorkspaceScope()...),
			GrantedResourceScope:   protoAuthorityRequestResourceScopeToACL(payload.GetGrantedResourceScope()),
			GrantedOperationScope:  append([]string(nil), payload.GetGrantedOperationScope()...),
			GrantedAccessLevel:     accessLevelProtoToInt(payload.GetGrantedAccessLevel()),
			GrantedDurationSeconds: payload.GetGrantedDurationSeconds(),
			MayDelegate:            payload.GetMayDelegate(),
			RemainingHops:          int(payload.GetRemainingHops()),
		}
		resolved, err := s.acl.ApproveAuthorityRequest(ctx, requestID, caller, decision)
		if err != nil {
			logging.Logger.Error().Err(err).Str("actor", caller.String()).Str("request_id", requestID).Msg("handleAuthorityRequestOp: approve failed")
			sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
			return
		}
		sendAuthorityRequestResponse(client, op.GetClientRequestId(), "authority request approved", resolved, nil)
		s.emitAuthorityRequestEvent(client, resolved, pb.AuthorityRequestEvent_AUTHORITY_REQUEST_EVENT_APPROVED)

	case pb.ResolveAuthorityRequestPayload_DENY:
		resolved, err := s.acl.DenyAuthorityRequest(ctx, requestID, caller, payload.GetReason())
		if err != nil {
			logging.Logger.Error().Err(err).Str("actor", caller.String()).Str("request_id", requestID).Msg("handleAuthorityRequestOp: deny failed")
			sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
			return
		}
		sendAuthorityRequestResponse(client, op.GetClientRequestId(), "authority request denied", resolved, nil)
		s.emitAuthorityRequestEvent(client, resolved, pb.AuthorityRequestEvent_AUTHORITY_REQUEST_EVENT_DENIED)

	default:
		sendAuthorityRequestError(client, op.GetClientRequestId(), "resolve decision must be APPROVE or DENY")
	}
}

// =============================================================================
// CANCEL
// =============================================================================

func (s *GatewayServer) handleAuthorityRequestCancel(ctx context.Context, client *ClientSession, caller models.Identity, op *pb.AuthorityRequestOperation) {
	requestID := strings.TrimSpace(op.GetRequestId())
	if requestID == "" {
		sendAuthorityRequestError(client, op.GetClientRequestId(), "request_id is required")
		return
	}

	existing, err := s.acl.GetAuthorityRequest(ctx, requestID)
	if err != nil {
		sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
		return
	}

	// Only the original requester may cancel. Match by canonical principal id.
	if !authorityRequestPrincipalMatchesIdentity(caller, existing.RequestingActor) {
		// Info hiding: surface not-found rather than admit existence + deny.
		sendAuthorityRequestError(client, op.GetClientRequestId(), acl.ErrAuthorityRequestNotFound.Error())
		return
	}

	// Reason precedence: top-level op.reason wins; fall back to create.reason
	// (which we reuse here as a cancellation reason carrier in the proto).
	reason := strings.TrimSpace(op.GetReason())
	if reason == "" {
		if c := op.GetCreate(); c != nil {
			reason = strings.TrimSpace(c.GetReason())
		}
	}

	resolved, err := s.acl.CancelOpenAuthorityRequest(ctx, requestID, reason)
	if err != nil {
		logging.Logger.Error().Err(err).Str("actor", caller.String()).Str("request_id", requestID).Msg("handleAuthorityRequestOp: cancel failed")
		sendAuthorityRequestError(client, op.GetClientRequestId(), err.Error())
		return
	}
	sendAuthorityRequestResponse(client, op.GetClientRequestId(), "authority request cancelled", resolved, nil)
	s.emitAuthorityRequestEvent(client, resolved, pb.AuthorityRequestEvent_AUTHORITY_REQUEST_EVENT_CANCELLED)
}

// =============================================================================
// Authorization helpers
// =============================================================================

// callerMayViewAuthorityRequest returns true when the caller is allowed to
// observe `req`. Visibility rules:
//   - The caller is the requesting_actor.
//   - The caller is the target_subject.
//   - The caller matches the routing principal.
//   - The caller holds the routing capability (ACL CheckAccess).
func (s *GatewayServer) callerMayViewAuthorityRequest(ctx context.Context, client *ClientSession, caller models.Identity, req *acl.AuthorityRequest) bool {
	if req == nil {
		return false
	}
	if authorityRequestPrincipalMatchesIdentity(caller, req.RequestingActor) {
		return true
	}
	if !req.TargetSubject.PrincipalRef().IsZero() && authorityRequestPrincipalMatchesIdentity(caller, req.TargetSubject) {
		return true
	}
	if req.RoutingTarget.Principal != nil && authorityRequestPrincipalMatchesIdentity(caller, *req.RoutingTarget.Principal) {
		return true
	}
	if req.RoutingTarget.Capability != "" {
		return s.callerHoldsCapability(ctx, client, caller, req.RoutingTarget.Capability, audit.OpAuthorityRequestResolve)
	}
	return false
}

// callerMayResolveAuthorityRequest enforces the resolve-authority gate:
//   - If the routing principal is set, caller's identity must match exactly.
//   - If the routing capability is set, caller must hold the capability via
//     ACL CheckAccess.
func (s *GatewayServer) callerMayResolveAuthorityRequest(ctx context.Context, client *ClientSession, caller models.Identity, req *acl.AuthorityRequest) bool {
	if req == nil {
		return false
	}
	if req.RoutingTarget.Principal != nil && !req.RoutingTarget.Principal.PrincipalRef().IsZero() {
		if authorityRequestPrincipalMatchesIdentity(caller, *req.RoutingTarget.Principal) {
			return true
		}
	}
	if req.RoutingTarget.Capability != "" {
		return s.callerHoldsCapability(ctx, client, caller, req.RoutingTarget.Capability, audit.OpAuthorityRequestResolve)
	}
	return false
}

// callerHoldsCapability runs an ACL CheckAccess against ResourceTypeCapability
// for `capability` at AccessManage. Returns true iff the decision is allow.
// Logs ACL errors but treats them as deny — capability checks must be safe
// against transient infra failures.
func (s *GatewayServer) callerHoldsCapability(ctx context.Context, client *ClientSession, caller models.Identity, capability string, operation string) bool {
	if s.acl == nil || capability == "" {
		return false
	}
	decision, err := s.acl.CheckAccess(
		ctx,
		caller,
		acl.ResourceTypeCapability,
		capability,
		operation,
		caller.Workspace,
		client.SessionUUID,
		acl.AccessManage,
	)
	if err != nil {
		logging.Logger.Debug().Err(err).Str("actor", caller.String()).Str("capability", capability).Msg("handleAuthorityRequestOp: capability check failed (treating as deny)")
		return false
	}
	if decision == nil || decision.Denied() {
		return false
	}
	return true
}

// =============================================================================
// Wire conversion helpers
// =============================================================================

// optionalPrincipalRefToIdentity returns the supplied identity from the proto
// ref, or the fallback identity when ref is nil/zero. Used by CREATE where
// requesting_actor may be omitted by clients that mean "me".
func optionalPrincipalRefToIdentity(ref *pb.PrincipalRef, fallback models.Identity) (models.Identity, error) {
	if ref == nil || (strings.TrimSpace(ref.GetPrincipalType()) == "" && strings.TrimSpace(ref.GetPrincipalId()) == "") {
		return fallback, nil
	}
	return protoPrincipalRefToIdentity(ref)
}

// protoAuthorityRequestRoutingToACL converts the proto routing target to the
// ACL layer shape, enforcing exactly-one-of (principal, capability).
func protoAuthorityRequestRoutingToACL(target *pb.AuthorityRequestRoutingTarget) (acl.AuthorityRequestRoutingTarget, error) {
	if target == nil {
		return acl.AuthorityRequestRoutingTarget{}, fmt.Errorf("routing_target is required")
	}
	hasPrincipal := target.GetPrincipal() != nil && strings.TrimSpace(target.GetPrincipal().GetPrincipalId()) != ""
	hasCapability := strings.TrimSpace(target.GetCapability()) != ""

	switch {
	case hasPrincipal && hasCapability:
		return acl.AuthorityRequestRoutingTarget{}, fmt.Errorf("routing_target must set exactly one of principal or capability")
	case !hasPrincipal && !hasCapability:
		return acl.AuthorityRequestRoutingTarget{}, fmt.Errorf("routing_target must set either principal or capability")
	case hasPrincipal:
		p, err := protoPrincipalRefToIdentity(target.GetPrincipal())
		if err != nil {
			return acl.AuthorityRequestRoutingTarget{}, fmt.Errorf("invalid routing_target.principal: %w", err)
		}
		return acl.AuthorityRequestRoutingTarget{Principal: &p}, nil
	default:
		return acl.AuthorityRequestRoutingTarget{Capability: strings.TrimSpace(target.GetCapability())}, nil
	}
}

// protoAuthorityRequestResourceScopeToACL flattens the proto repeated-entry
// shape into the map[string][]string the ACL layer uses.
func protoAuthorityRequestResourceScopeToACL(entries []*pb.AuthorityRequestResourceScopeEntry) map[string][]string {
	if len(entries) == 0 {
		return nil
	}
	result := make(map[string][]string, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		result[entry.GetResourceType()] = append([]string(nil), entry.GetPatterns()...)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// aclAuthorityRequestResourceScopeToProto is the inverse of
// protoAuthorityRequestResourceScopeToACL, used when serializing rows to the
// wire.
func aclAuthorityRequestResourceScopeToProto(scope map[string][]string) []*pb.AuthorityRequestResourceScopeEntry {
	if len(scope) == 0 {
		return nil
	}
	out := make([]*pb.AuthorityRequestResourceScopeEntry, 0, len(scope))
	for resourceType, patterns := range scope {
		out = append(out, &pb.AuthorityRequestResourceScopeEntry{
			ResourceType: resourceType,
			Patterns:     append([]string(nil), patterns...),
		})
	}
	return out
}

// accessLevelProtoToInt maps the new proto AccessLevel enum to the legacy
// int scale used throughout the ACL layer.
func accessLevelProtoToInt(level pb.AccessLevel) int {
	switch level {
	case pb.AccessLevel_ACCESS_LEVEL_NONE:
		return acl.AccessNone
	case pb.AccessLevel_ACCESS_LEVEL_READ:
		return acl.AccessRead
	case pb.AccessLevel_ACCESS_LEVEL_READWRITE:
		return acl.AccessReadWrite
	case pb.AccessLevel_ACCESS_LEVEL_MANAGE:
		return acl.AccessManage
	case pb.AccessLevel_ACCESS_LEVEL_ADMIN:
		return acl.AccessAdmin
	case pb.AccessLevel_ACCESS_LEVEL_SUPERADMIN:
		return acl.AccessSuperAdmin
	default:
		return 0
	}
}

// accessLevelIntToProto is the inverse of accessLevelProtoToInt. Unknown
// legacy values fall back to UNSPECIFIED rather than panicking — the storage
// row is the source of truth and downstream consumers can tolerate the
// sentinel.
func accessLevelIntToProto(level int) pb.AccessLevel {
	switch level {
	case acl.AccessNone:
		return pb.AccessLevel_ACCESS_LEVEL_NONE
	case acl.AccessRead:
		return pb.AccessLevel_ACCESS_LEVEL_READ
	case acl.AccessReadWrite:
		return pb.AccessLevel_ACCESS_LEVEL_READWRITE
	case acl.AccessManage:
		return pb.AccessLevel_ACCESS_LEVEL_MANAGE
	case acl.AccessAdmin:
		return pb.AccessLevel_ACCESS_LEVEL_ADMIN
	case acl.AccessSuperAdmin:
		return pb.AccessLevel_ACCESS_LEVEL_SUPERADMIN
	default:
		return pb.AccessLevel_ACCESS_LEVEL_UNSPECIFIED
	}
}

// authorityRequestStatusFromProto converts the proto enum to the ACL string
// status. Unspecified maps to empty (no filter).
func authorityRequestStatusFromProto(value pb.AuthorityRequestStatus) acl.AuthorityRequestStatus {
	switch value {
	case pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_PENDING:
		return acl.AuthorityRequestStatusPending
	case pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_APPROVED:
		return acl.AuthorityRequestStatusApproved
	case pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_DENIED:
		return acl.AuthorityRequestStatusDenied
	case pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_EXPIRED:
		return acl.AuthorityRequestStatusExpired
	case pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_CANCELLED:
		return acl.AuthorityRequestStatusCancelled
	default:
		return ""
	}
}

// authorityRequestStatusToProto is the inverse mapping.
func authorityRequestStatusToProto(value acl.AuthorityRequestStatus) pb.AuthorityRequestStatus {
	switch value {
	case acl.AuthorityRequestStatusPending:
		return pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_PENDING
	case acl.AuthorityRequestStatusApproved:
		return pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_APPROVED
	case acl.AuthorityRequestStatusDenied:
		return pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_DENIED
	case acl.AuthorityRequestStatusExpired:
		return pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_EXPIRED
	case acl.AuthorityRequestStatusCancelled:
		return pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_CANCELLED
	default:
		return pb.AuthorityRequestStatus_AUTHORITY_REQUEST_STATUS_UNSPECIFIED
	}
}

// authorityRequestToProto serializes an ACL AuthorityRequest row to the wire.
func authorityRequestToProto(req *acl.AuthorityRequest) *pb.AuthorityRequest {
	if req == nil {
		return nil
	}

	var routing *pb.AuthorityRequestRoutingTarget
	if req.RoutingTarget.Principal != nil || req.RoutingTarget.Capability != "" {
		routing = &pb.AuthorityRequestRoutingTarget{
			Capability: req.RoutingTarget.Capability,
		}
		if req.RoutingTarget.Principal != nil {
			ref := req.RoutingTarget.Principal.PrincipalRef()
			if !ref.IsZero() {
				routing.Principal = &pb.PrincipalRef{
					PrincipalType: acl.PrincipalTypeForModel(ref.Type),
					PrincipalId:   ref.ID,
				}
			}
		}
	}

	pbReq := &pb.AuthorityRequest{
		RequestId:                req.RequestID,
		Status:                   authorityRequestStatusToProto(req.Status),
		RequestingActor:          identityToProtoPrincipalRef(req.RequestingActor),
		TargetSubject:            identityToProtoPrincipalRef(req.TargetSubject),
		DesiredWorkspaceScope:    append([]string(nil), req.WorkspaceScope...),
		DesiredResourceScope:     aclAuthorityRequestResourceScopeToProto(req.ResourceScope),
		DesiredOperationScope:    append([]string(nil), req.OperationScope...),
		RequestedAccessLevel:     accessLevelIntToProto(req.RequestedAccess),
		RequestedDurationSeconds: req.DurationSeconds,
		AudienceType:             req.AudienceType,
		AudienceId:               req.AudienceID,
		RoutingTarget:            routing,
		Reason:                   req.Reason,
		TaskId:                   req.TaskID,
		Metadata:                 metadataToProtoStringMap(req.Metadata),
		CreatedAt:                req.CreatedAt.Unix(),
		ExpiresAt:                req.ExpiresAt.Unix(),
		GrantedGrantId:           req.GrantedGrantID,
		ResolutionReason:         req.ResolutionReason,
	}
	if req.ResolvedAt != nil && !req.ResolvedAt.IsZero() {
		pbReq.ResolvedAt = req.ResolvedAt.Unix()
	}
	if !req.ResolvedBy.PrincipalRef().IsZero() {
		pbReq.ResolvedBy = identityToProtoPrincipalRef(req.ResolvedBy)
	}
	return pbReq
}

// identityToProtoPrincipalRef collapses a models.Identity into the wire ref
// shape. Zero identities return nil so the wire carries an absent PrincipalRef
// rather than a half-populated one.
func identityToProtoPrincipalRef(id models.Identity) *pb.PrincipalRef {
	ref := id.PrincipalRef()
	if ref.IsZero() {
		return nil
	}
	return &pb.PrincipalRef{
		PrincipalType: acl.PrincipalTypeForModel(ref.Type),
		PrincipalId:   ref.ID,
	}
}

// authorityRequestPrincipalMatchesIdentity is true when `a` and `b` refer to
// the same principal by canonical id.
func authorityRequestPrincipalMatchesIdentity(a, b models.Identity) bool {
	aRef := a.PrincipalRef()
	bRef := b.PrincipalRef()
	if aRef.IsZero() || bRef.IsZero() {
		return false
	}
	return acl.PrincipalTypeForModel(aRef.Type) == acl.PrincipalTypeForModel(bRef.Type) &&
		aRef.ID == bRef.ID
}

// =============================================================================
// Downstream send helpers
// =============================================================================

func sendAuthorityRequestError(client *ClientSession, clientRequestID, errMsg string) {
	if client == nil {
		return
	}
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityRequestResponse{
			AuthorityRequestResponse: &pb.AuthorityRequestOperationResponse{
				Success:         false,
				Error:           errMsg,
				ClientRequestId: clientRequestID,
			},
		},
	})
}

// sendAuthorityRequestResponse is the success path: callers populate exactly
// one of (req, rows). Both nil is allowed (e.g. CANCEL acknowledgement) but
// uncommon.
func sendAuthorityRequestResponse(client *ClientSession, clientRequestID, message string, req *acl.AuthorityRequest, rows []*acl.AuthorityRequest) {
	if client == nil {
		return
	}
	resp := &pb.AuthorityRequestOperationResponse{
		Success:         true,
		ClientRequestId: clientRequestID,
	}
	if req != nil {
		resp.Request = authorityRequestToProto(req)
	}
	if rows != nil {
		protoRows := make([]*pb.AuthorityRequest, 0, len(rows))
		for _, r := range rows {
			protoRows = append(protoRows, authorityRequestToProto(r))
		}
		resp.Requests = protoRows
		resp.TotalCount = int32(len(protoRows))
	}
	// `message` is reserved for future use (e.g. logging on the client);
	// the wire response message has no separate "message" slot, so we drop
	// it after using it for server-side log breadcrumbs only.
	if message != "" {
		logging.Logger.Debug().Str("client_request_id", clientRequestID).Str("message", message).Msg("authority request op response")
	}
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityRequestResponse{
			AuthorityRequestResponse: resp,
		},
	})
}

// emitAuthorityRequestEvent pushes an AuthorityRequestEvent to the originating
// session. Stage 4 will expand the delivery surface to task-scoped topics,
// but for Stage C single-session direct delivery is the contract.
func (s *GatewayServer) emitAuthorityRequestEvent(client *ClientSession, req *acl.AuthorityRequest, eventType pb.AuthorityRequestEvent_EventType) {
	if client == nil || req == nil {
		return
	}
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityRequestEvent{
			AuthorityRequestEvent: &pb.AuthorityRequestEvent{
				EventType: eventType,
				Request:   authorityRequestToProto(req),
				EmittedAt: time.Now().Unix(),
			},
		},
	})
}
