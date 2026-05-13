package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

func (s *GatewayServer) handleAuthorityGrantOp(ctx context.Context, client *ClientSession, op *pb.AuthorityGrantOperation) {
	if s.acl == nil {
		sendAuthorityGrantError(client, op.GetRequestId(), "ACL service not configured")
		return
	}

	client.identityMu.RLock()
	actor := client.Identity
	client.identityMu.RUnlock()

	switch op.Op {
	case pb.AuthorityGrantOperation_EXCHANGE:
		if op.GetExchangeRequest() == nil {
			sendAuthorityGrantError(client, op.GetRequestId(), "exchange_request is required")
			return
		}
		grant, err := s.exchangeAuthorityGrant(ctx, client, actor, op.GetExchangeRequest())
		if err != nil {
			logging.Logger.Error().Err(err).Str("actor", actor.String()).Msg("handleAuthorityGrantOp: exchange failed")
			s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantExchange, nil, false, err.Error(), map[string]interface{}{
				"request_id": op.GetRequestId(),
			})
			sendAuthorityGrantError(client, op.GetRequestId(), err.Error())
			return
		}
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantExchange, grant, true, "", map[string]interface{}{
			"request_id": op.GetRequestId(),
		})
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_AuthorityGrant{
				AuthorityGrant: &pb.AuthorityGrantResponse{
					Success:   true,
					Message:   "authority grant exchanged",
					Grant:     aclAuthorityGrantToProto(grant),
					RequestId: op.GetRequestId(),
				},
			},
		})

	case pb.AuthorityGrantOperation_DERIVE:
		if op.GetDeriveRequest() == nil {
			sendAuthorityGrantError(client, op.GetRequestId(), "derive_request is required")
			return
		}
		grant, err := s.deriveAuthorityGrant(ctx, client, actor, op.GetDeriveRequest())
		if err != nil {
			logging.Logger.Error().Err(err).Str("actor", actor.String()).Msg("handleAuthorityGrantOp: derive failed")
			s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantDerive, nil, false, err.Error(), map[string]interface{}{
				"request_id": op.GetRequestId(),
			})
			sendAuthorityGrantError(client, op.GetRequestId(), err.Error())
			return
		}
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantDerive, grant, true, "", map[string]interface{}{
			"request_id": op.GetRequestId(),
		})
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_AuthorityGrant{
				AuthorityGrant: &pb.AuthorityGrantResponse{
					Success:   true,
					Message:   "authority grant derived",
					Grant:     aclAuthorityGrantToProto(grant),
					RequestId: op.GetRequestId(),
				},
			},
		})

	case pb.AuthorityGrantOperation_GET:
		grant, err := s.getVisibleAuthorityGrant(ctx, client, actor, op.GetGrantId())
		if err != nil {
			logging.Logger.Error().Err(err).Str("grant_id", op.GetGrantId()).Msg("handleAuthorityGrantOp: get failed")
			s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantGet, nil, false, err.Error(), map[string]interface{}{
				"request_id": op.GetRequestId(),
				"grant_id":   op.GetGrantId(),
			})
			sendAuthorityGrantError(client, op.GetRequestId(), err.Error())
			return
		}
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantGet, grant, true, "", map[string]interface{}{
			"request_id": op.GetRequestId(),
		})
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_AuthorityGrant{
				AuthorityGrant: &pb.AuthorityGrantResponse{
					Success:   true,
					Grant:     aclAuthorityGrantToProto(grant),
					RequestId: op.GetRequestId(),
				},
			},
		})

	case pb.AuthorityGrantOperation_RENEW:
		grant, err := s.renewVisibleAuthorityGrant(ctx, client, actor, op.GetRenewRequest(), op.GetGrantId())
		if err != nil {
			logging.Logger.Error().Err(err).Str("grant_id", op.GetGrantId()).Msg("handleAuthorityGrantOp: renew failed")
			s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantRenew, nil, false, err.Error(), map[string]interface{}{
				"request_id": op.GetRequestId(),
				"grant_id":   op.GetGrantId(),
			})
			sendAuthorityGrantError(client, op.GetRequestId(), err.Error())
			return
		}
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantRenew, grant, true, "", map[string]interface{}{
			"request_id": op.GetRequestId(),
		})
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_AuthorityGrant{
				AuthorityGrant: &pb.AuthorityGrantResponse{
					Success:   true,
					Message:   "authority grant renewed",
					Grant:     aclAuthorityGrantToProto(grant),
					RequestId: op.GetRequestId(),
				},
			},
		})

	case pb.AuthorityGrantOperation_REVOKE:
		grant, err := s.revokeVisibleAuthorityGrant(ctx, client, actor, op.GetGrantId())
		if err != nil {
			logging.Logger.Error().Err(err).Str("grant_id", op.GetGrantId()).Msg("handleAuthorityGrantOp: revoke failed")
			s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantRevoke, nil, false, err.Error(), map[string]interface{}{
				"request_id": op.GetRequestId(),
				"grant_id":   op.GetGrantId(),
			})
			sendAuthorityGrantError(client, op.GetRequestId(), err.Error())
			return
		}
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantRevoke, grant, true, "", map[string]interface{}{
			"request_id": op.GetRequestId(),
		})
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_AuthorityGrant{
				AuthorityGrant: &pb.AuthorityGrantResponse{
					Success:   true,
					Message:   "authority grant revoked",
					Grant:     aclAuthorityGrantToProto(grant),
					RequestId: op.GetRequestId(),
				},
			},
		})

	case pb.AuthorityGrantOperation_LIST_MY_GRANTS:
		s.handleListAuthorityGrants(ctx, client, actor, op, true /*byDelegate*/, true /*bySubject*/)

	case pb.AuthorityGrantOperation_LIST_GRANTS_ON_ME:
		s.handleListAuthorityGrants(ctx, client, actor, op, false, true)

	case pb.AuthorityGrantOperation_BATCH_EXCHANGE:
		s.handleBatchExchangeAuthorityGrants(ctx, client, actor, op)

	case pb.AuthorityGrantOperation_DERIVE_FOR_TARGET:
		s.handleDeriveAuthorityGrantForTarget(ctx, client, actor, op)

	default:
		sendAuthorityGrantError(client, op.GetRequestId(), "unknown authority grant operation")
	}
}

// handleListAuthorityGrants services LIST_MY_GRANTS and LIST_GRANTS_ON_ME by
// projecting matching rows in the actor's view to ACLAuthorityGrantInfo.
func (s *GatewayServer) handleListAuthorityGrants(ctx context.Context, client *ClientSession, actor models.Identity, op *pb.AuthorityGrantOperation, byDelegate, bySubject bool) {
	listReq := op.GetListRequest()
	filter := acl.VisibleGrantsFilter{
		Actor:      actor,
		ByDelegate: byDelegate,
		BySubject:  bySubject,
	}
	if listReq != nil {
		filter.AudienceType = listReq.GetAudienceType()
		filter.AudienceID = listReq.GetAudienceId()
		filter.IncludeRevoked = listReq.GetIncludeRevoked()
		filter.Limit = int(listReq.GetLimit())
		filter.Offset = int(listReq.GetOffset())
	}

	grants, err := s.acl.ListVisibleGrants(ctx, filter)
	if err != nil {
		logging.Logger.Error().Err(err).Str("actor", actor.String()).Msg("handleAuthorityGrantOp: list visible grants failed")
		sendAuthorityGrantError(client, op.GetRequestId(), err.Error())
		return
	}

	protoGrants := make([]*pb.ACLAuthorityGrantInfo, 0, len(grants))
	for _, grant := range grants {
		protoGrants = append(protoGrants, aclAuthorityGrantToProto(grant))
	}
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityGrant{
			AuthorityGrant: &pb.AuthorityGrantResponse{
				Success:   true,
				Grants:    protoGrants,
				Total:     int32(len(protoGrants)),
				RequestId: op.GetRequestId(),
			},
		},
	})
}

// handleBatchExchangeAuthorityGrants performs N grant exchanges for the
// actor in one round-trip. On stop_on_first_error, the first failure
// short-circuits the batch with a top-level error; otherwise per-request
// failures are surfaced via partial `grants` results and a non-empty
// `error` summary string.
func (s *GatewayServer) handleBatchExchangeAuthorityGrants(ctx context.Context, client *ClientSession, actor models.Identity, op *pb.AuthorityGrantOperation) {
	batch := op.GetBatchExchangeRequest()
	if batch == nil || len(batch.GetRequests()) == 0 {
		sendAuthorityGrantError(client, op.GetRequestId(), "batch_exchange_request with at least one request is required")
		return
	}

	stopOnError := batch.GetStopOnFirstError()
	results := make([]*pb.ACLAuthorityGrantInfo, 0, len(batch.GetRequests()))
	failures := make([]string, 0)

	for i, req := range batch.GetRequests() {
		grant, err := s.exchangeAuthorityGrant(ctx, client, actor, req)
		if err != nil {
			s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantExchange, nil, false, err.Error(), map[string]interface{}{
				"request_id":  op.GetRequestId(),
				"batch_index": i,
				"batch_op":    "BATCH_EXCHANGE",
			})
			if stopOnError {
				sendAuthorityGrantError(client, op.GetRequestId(), fmt.Sprintf("batch exchange aborted at index %d: %v", i, err))
				return
			}
			failures = append(failures, fmt.Sprintf("index %d: %v", i, err))
			continue
		}
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantExchange, grant, true, "", map[string]interface{}{
			"request_id":  op.GetRequestId(),
			"batch_index": i,
			"batch_op":    "BATCH_EXCHANGE",
		})
		results = append(results, aclAuthorityGrantToProto(grant))
	}

	resp := &pb.AuthorityGrantResponse{
		Success:   len(failures) == 0,
		Grants:    results,
		Total:     int32(len(results)),
		RequestId: op.GetRequestId(),
	}
	if len(failures) > 0 {
		resp.Error = strings.Join(failures, "; ")
	} else {
		resp.Message = "authority grants exchanged"
	}
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityGrant{
			AuthorityGrant: resp,
		},
	})
}

// handleDeriveAuthorityGrantForTarget implements idempotent derive: returns
// an existing visible derived grant if one matches (parent, target,
// audience), otherwise mints a new one via the standard derive flow.
func (s *GatewayServer) handleDeriveAuthorityGrantForTarget(ctx context.Context, client *ClientSession, actor models.Identity, op *pb.AuthorityGrantOperation) {
	req := op.GetDeriveForTargetRequest()
	if req == nil {
		sendAuthorityGrantError(client, op.GetRequestId(), "derive_for_target_request is required")
		return
	}
	if strings.TrimSpace(req.GetParentGrantId()) == "" {
		sendAuthorityGrantError(client, op.GetRequestId(), "parent_grant_id is required")
		return
	}
	if req.GetTarget() == nil {
		sendAuthorityGrantError(client, op.GetRequestId(), "target is required")
		return
	}
	if strings.TrimSpace(req.GetAudienceType()) == "" || strings.TrimSpace(req.GetAudienceId()) == "" {
		sendAuthorityGrantError(client, op.GetRequestId(), "audience_type and audience_id are required")
		return
	}

	target, err := protoPrincipalRefToIdentity(req.GetTarget())
	if err != nil {
		sendAuthorityGrantError(client, op.GetRequestId(), fmt.Sprintf("invalid target: %v", err))
		return
	}

	// Idempotent check: prefer an existing active grant if one matches
	// (parent, delegate, audience). Reuse keeps the audit trail short and
	// avoids burning hops on unnecessary mints.
	existing, err := s.acl.FindVisibleDerivedGrant(ctx, req.GetParentGrantId(), target, req.GetAudienceType(), req.GetAudienceId())
	if err == nil && existing != nil {
		// Audit as a "get" since this branch did not mutate state.
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantGet, existing, true, "", map[string]interface{}{
			"request_id":        op.GetRequestId(),
			"derive_for_target": true,
			"reused_existing":   true,
		})
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_AuthorityGrant{
				AuthorityGrant: &pb.AuthorityGrantResponse{
					Success:   true,
					Message:   "existing authority grant reused",
					Grant:     aclAuthorityGrantToProto(existing),
					RequestId: op.GetRequestId(),
				},
			},
		})
		return
	}

	deriveReq := &pb.AuthorityGrantDeriveRequest{
		ParentGrantId:  req.GetParentGrantId(),
		Delegate:       req.GetTarget(),
		OperationScope: append([]string(nil), req.GetOperationScope()...),
		MaxAccessLevel: req.GetMaxAccessLevel(),
		AudienceType:   req.GetAudienceType(),
		AudienceId:     req.GetAudienceId(),
		ExpiresAt:      req.GetExpiresAt(),
		RenewableUntil: req.GetRenewableUntil(),
		MayDelegate:    req.GetMayDelegate(),
		RemainingHops:  req.GetRemainingHops(),
		Reason:         req.GetReason(),
	}
	grant, err := s.deriveAuthorityGrant(ctx, client, actor, deriveReq)
	if err != nil {
		s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantDerive, nil, false, err.Error(), map[string]interface{}{
			"request_id":        op.GetRequestId(),
			"derive_for_target": true,
		})
		sendAuthorityGrantError(client, op.GetRequestId(), err.Error())
		return
	}
	s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantDerive, grant, true, "", map[string]interface{}{
		"request_id":        op.GetRequestId(),
		"derive_for_target": true,
	})
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityGrant{
			AuthorityGrant: &pb.AuthorityGrantResponse{
				Success:   true,
				Message:   "authority grant derived",
				Grant:     aclAuthorityGrantToProto(grant),
				RequestId: op.GetRequestId(),
			},
		},
	})
}

func (s *GatewayServer) exchangeAuthorityGrant(ctx context.Context, client *ClientSession, actor models.Identity, req *pb.AuthorityGrantExchangeRequest) (*acl.AuthorityGrant, error) {
	if req == nil {
		return nil, fmt.Errorf("exchange_request is required")
	}

	subject := actor
	metadata := protoStringMapToMetadata(req.Metadata)
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	sourceSessionID := strings.TrimSpace(req.GetSourceSessionId())
	if sourceSessionID == "" {
		if actor.Type != models.PrincipalUser {
			return nil, fmt.Errorf("source_session_id is required for non-user actors")
		}
		metadata["exchange_mode"] = "self"
	} else {
		// Resolve the source session FIRST so the permission check below can
		// be scoped to the resolved subject (workspace + canonical id).
		sourceIdentity, err := s.sessions.GetSessionIdentity(ctx, sourceSessionID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve source session: %w", err)
		}
		if sourceIdentity.Type != models.PrincipalUser {
			return nil, fmt.Errorf("source_session_id must reference an active user session")
		}

		active, err := s.sessions.IsActive(ctx, sourceIdentity.String())
		if err != nil {
			return nil, fmt.Errorf("failed to validate source session activity: %w", err)
		}
		if !active {
			return nil, fmt.Errorf("source_session_id is not active")
		}

		// Subject-scoped capability check: tries the narrow rule
		// (capability/exchange_authority_grants/{subject_workspace}/{subject_canonical_id})
		// then falls back to the broad legacy rule (capability/exchange_authority_grants).
		// Lets operators write per-(subject-population, service) rules without
		// breaking deployments still relying on the broad form.
		if err := s.checkAuthorityGrantExchangePermissionForSubject(ctx, client, actor, sourceIdentity); err != nil {
			return nil, err
		}

		subject = sourceIdentity
		metadata["exchange_mode"] = "trusted_exchange"
		metadata["source_session_id"] = sourceSessionID
	}

	audienceType, audienceID, err := resolveExchangeAudience(client, actor, req.GetAudienceType(), req.GetAudienceId(), sourceSessionID)
	if err != nil {
		return nil, err
	}

	level, err := normalizeAuthorityGrantAccessLevel(int(req.GetMaxAccessLevel()))
	if err != nil {
		return nil, fmt.Errorf("invalid max_access_level: %w", err)
	}

	// Trusted-exchange grants are forced to track the source session lifecycle:
	// when the user disconnects, the grant is no longer valid even if its
	// expires_at hasn't been reached. This closes the orphan-grant window where
	// a service could keep using/renewing a grant past user logout.
	validWhileAudienceActive := req.GetValidWhileAudienceActive()
	if metadata["exchange_mode"] == "trusted_exchange" {
		validWhileAudienceActive = true
	}

	return s.acl.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:                  subject,
		Delegate:                 actor,
		IssuedBy:                 actor,
		MayDelegate:              req.GetMayDelegate(),
		RemainingHops:            int(req.GetRemainingHops()),
		WorkspaceScope:           append([]string(nil), req.GetWorkspaceScope()...),
		ResourceScope:            protoAuthorityResourceScopeToACL(req.GetResourceScope()),
		OperationScope:           append([]string(nil), req.GetOperationScope()...),
		MaxAccessLevel:           level,
		AudienceType:             audienceType,
		AudienceID:               audienceID,
		ValidWhileAudienceActive: validWhileAudienceActive,
		ExpiresAt:                time.Unix(req.GetExpiresAt(), 0),
		RenewableUntil:           time.Unix(req.GetRenewableUntil(), 0),
		Reason:                   req.GetReason(),
		Metadata:                 metadata,
	})
}

func (s *GatewayServer) deriveAuthorityGrant(ctx context.Context, client *ClientSession, actor models.Identity, req *pb.AuthorityGrantDeriveRequest) (*acl.AuthorityGrant, error) {
	if req == nil {
		return nil, fmt.Errorf("derive_request is required")
	}
	if strings.TrimSpace(req.GetParentGrantId()) == "" {
		return nil, fmt.Errorf("parent_grant_id is required")
	}
	if req.GetDelegate() == nil {
		return nil, fmt.Errorf("delegate is required")
	}
	if strings.TrimSpace(req.GetAudienceType()) == "" || strings.TrimSpace(req.GetAudienceId()) == "" {
		return nil, fmt.Errorf("audience_type and audience_id are required")
	}

	parentGrant, err := s.acl.GetAuthorityGrant(ctx, req.GetParentGrantId())
	if err != nil {
		return nil, err
	}
	if err := s.requireCurrentDelegateAuthority(ctx, client, actor, parentGrant); err != nil {
		return nil, err
	}

	subject, err := identityFromAuthorityPrincipal(parentGrant.SubjectType, parentGrant.SubjectID)
	if err != nil {
		return nil, fmt.Errorf("invalid parent subject: %w", err)
	}
	rootSubject, err := identityFromAuthorityPrincipal(parentGrant.RootSubjectType, parentGrant.RootSubjectID)
	if err != nil {
		return nil, fmt.Errorf("invalid parent root subject: %w", err)
	}
	delegate, err := protoPrincipalRefToIdentity(req.GetDelegate())
	if err != nil {
		return nil, fmt.Errorf("invalid delegate: %w", err)
	}

	metadata := protoStringMapToMetadata(req.Metadata)
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	metadata["derived_from_grant_id"] = parentGrant.GrantID

	level, err := normalizeAuthorityGrantAccessLevel(int(req.GetMaxAccessLevel()))
	if err != nil {
		return nil, fmt.Errorf("invalid max_access_level: %w", err)
	}

	parentGrantID := parentGrant.GrantID
	return s.acl.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:                  subject,
		Delegate:                 delegate,
		IssuedBy:                 actor,
		RootSubject:              &rootSubject,
		ParentGrantID:            &parentGrantID,
		MayDelegate:              req.GetMayDelegate(),
		RemainingHops:            int(req.GetRemainingHops()),
		WorkspaceScope:           append([]string(nil), req.GetWorkspaceScope()...),
		ResourceScope:            protoAuthorityResourceScopeToACL(req.GetResourceScope()),
		OperationScope:           append([]string(nil), req.GetOperationScope()...),
		MaxAccessLevel:           level,
		AudienceType:             req.GetAudienceType(),
		AudienceID:               req.GetAudienceId(),
		ValidWhileAudienceActive: req.GetValidWhileAudienceActive(),
		ExpiresAt:                time.Unix(req.GetExpiresAt(), 0),
		RenewableUntil:           time.Unix(req.GetRenewableUntil(), 0),
		Reason:                   req.GetReason(),
		Metadata:                 metadata,
	})
}

func (s *GatewayServer) getVisibleAuthorityGrant(ctx context.Context, client *ClientSession, actor models.Identity, grantID string) (*acl.AuthorityGrant, error) {
	if strings.TrimSpace(grantID) == "" {
		return nil, fmt.Errorf("grant_id is required")
	}

	grant, err := s.acl.GetAuthorityGrant(ctx, grantID)
	if err != nil {
		return nil, err
	}
	if err := s.requireVisibleAuthorityGrant(ctx, client, actor, grant); err != nil {
		return nil, err
	}

	return grant, nil
}

func (s *GatewayServer) renewVisibleAuthorityGrant(ctx context.Context, client *ClientSession, actor models.Identity, req *pb.ACLRenewAuthorityGrantRequest, fallbackGrantID string) (*acl.AuthorityGrant, error) {
	if req == nil {
		return nil, fmt.Errorf("renew_request is required")
	}

	grantID := strings.TrimSpace(req.GetGrantId())
	if grantID == "" {
		grantID = strings.TrimSpace(fallbackGrantID)
	}
	if grantID == "" {
		return nil, fmt.Errorf("grant_id is required")
	}
	// Either expires_at OR extend_seconds must be set; extend_seconds wins
	// when both are non-zero (see RenewAuthorityGrantOpts doc).
	if req.GetExpiresAt() == 0 && req.GetExtendSeconds() <= 0 {
		return nil, fmt.Errorf("expires_at or extend_seconds is required")
	}

	grant, err := s.getVisibleAuthorityGrant(ctx, client, actor, grantID)
	if err != nil {
		return nil, err
	}

	opts := acl.RenewAuthorityGrantOpts{ExtendSeconds: int(req.GetExtendSeconds())}
	if req.GetExpiresAt() != 0 {
		opts.ExpiresAt = time.Unix(req.GetExpiresAt(), 0)
	}
	return s.acl.RenewAuthorityGrantOpts(ctx, grant.GrantID, opts)
}

func (s *GatewayServer) revokeVisibleAuthorityGrant(ctx context.Context, client *ClientSession, actor models.Identity, grantID string) (*acl.AuthorityGrant, error) {
	grant, err := s.getVisibleAuthorityGrant(ctx, client, actor, grantID)
	if err != nil {
		return nil, err
	}
	revoked, err := s.acl.RevokeAuthorityGrantCascade(ctx, grant.GrantID)
	if err != nil {
		return nil, err
	}

	// Best-effort: notify locally connected delegates so SDKs can drop
	// cached grants immediately. We don't block on send failures — the
	// grant is already revoked in the DB and the next call would fail
	// anyway. Cross-gateway delegates rely on the resolver path for
	// freshness; a Redis pub/sub fan-out would be a future refinement.
	now := time.Now().Unix()
	for _, r := range revoked {
		s.publishAuthorityGrantRevocation(r, "", now)
	}

	updatedGrant, err := s.acl.GetAuthorityGrant(ctx, grant.GrantID)
	if err != nil {
		return grant, nil
	}
	return updatedGrant, nil
}

// publishAuthorityGrantRevocation pushes an AuthorityGrantRevocation
// notification to the delegate of the supplied grant if they are connected
// to this gateway. Best-effort: send failures are logged at debug level
// only since the grant is already revoked in the DB.
func (s *GatewayServer) publishAuthorityGrantRevocation(grant acl.RevokedAuthorityGrant, reason string, revokedAt int64) {
	delegateTopic := identityTopicFromAuthorityPrincipal(grant.DelegateType, grant.DelegateID)
	if delegateTopic == "" {
		return
	}
	sessionAny, ok := s.identityIndex.Load(delegateTopic)
	if !ok {
		return
	}
	sessionID, ok := sessionAny.(string)
	if !ok || sessionID == "" {
		return
	}
	clientAny, ok := s.activeStreams.Load(sessionID)
	if !ok {
		return
	}
	client, ok := clientAny.(*ClientSession)
	if !ok || client == nil {
		return
	}
	cascade := !grant.IsRoot
	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityGrantRevocation{
			AuthorityGrantRevocation: &pb.AuthorityGrantRevocation{
				GrantId:     grant.GrantID,
				RootGrantId: grant.RootGrantID,
				Reason:      reason,
				RevokedAt:   revokedAt,
				Cascade:     cascade,
			},
		},
	}); err != nil {
		logging.Logger.Debug().Err(err).Str("grant_id", grant.GrantID).Str("delegate", delegateTopic).Msg("failed to push authority grant revocation notice")
	}
}

// identityTopicFromAuthorityPrincipal converts an authority-grant principal
// pair (type+id) into the canonical identity-topic string used by
// identityIndex. Returns an empty string for principal kinds that don't
// have a stable topic-addressable form (system principals).
func identityTopicFromAuthorityPrincipal(principalType, principalID string) string {
	if principalType == "" || principalID == "" {
		return ""
	}
	identity, err := identityFromAuthorityPrincipal(principalType, principalID)
	if err != nil {
		return ""
	}
	return identity.String()
}

func (s *GatewayServer) checkAuthorityGrantExchangePermission(ctx context.Context, client *ClientSession, actor models.Identity) error {
	decision, err := s.acl.CheckAccess(
		ctx,
		actor,
		acl.ResourceTypeCapability,
		acl.PermissionExchangeAuthorityGrants,
		audit.OpAuthorityGrantExchange,
		actor.Workspace,
		client.SessionUUID,
		acl.AccessManage,
	)
	if err != nil {
		return fmt.Errorf("authority grant exchange permission check failed: %w", err)
	}
	if decision == nil || decision.Denied() {
		return fmt.Errorf("not authorized to exchange authority grants from another session")
	}
	return nil
}

// checkAuthorityGrantExchangePermissionForSubject is the subject-scoped
// equivalent of checkAuthorityGrantExchangePermission. It first tries a
// narrow rule whose resource_id encodes the subject's workspace and canonical
// principal id (e.g. "capability/exchange_authority_grants/prod/user::cust-1"),
// and falls back to the broad legacy rule if no narrow rule matches. This lets
// operators write per-(subject-population, service) ACL rules — e.g.
// "svc::platform-server may exchange for user::cust-* in workspace prod" —
// without breaking deployments that still grant the broad capability.
//
// Without this scoping, any service holding the broad capability could mint a
// grant on behalf of any active user session in the tenant — the well-known
// weakness of the trusted-intermediary pattern. With it, grants can be confined
// to specific subject populations.
func (s *GatewayServer) checkAuthorityGrantExchangePermissionForSubject(ctx context.Context, client *ClientSession, actor models.Identity, subject models.Identity) error {
	subjectID := subject.CanonicalPrincipalID()
	subjectWorkspace := subject.Workspace
	if subjectWorkspace == "" {
		subjectWorkspace = "_no_workspace"
	}
	narrowResourceID := fmt.Sprintf("%s/%s/%s", acl.PermissionExchangeAuthorityGrants, subjectWorkspace, subjectID)

	decision, err := s.acl.CheckAccess(
		ctx,
		actor,
		acl.ResourceTypeCapability,
		narrowResourceID,
		audit.OpAuthorityGrantExchange,
		subject.Workspace,
		client.SessionUUID,
		acl.AccessManage,
	)
	if err == nil && decision != nil && !decision.Denied() {
		return nil
	}
	// Fall back to the broad legacy rule. Operators wishing to lock down can
	// remove the broad rule and rely solely on the narrow form.
	return s.checkAuthorityGrantExchangePermission(ctx, client, actor)
}

func (s *GatewayServer) requireVisibleAuthorityGrant(ctx context.Context, client *ClientSession, actor models.Identity, grant *acl.AuthorityGrant) error {
	if grant == nil {
		return fmt.Errorf("authority grant is required")
	}
	if authorityGrantPrincipalMatches(actor, grant.SubjectType, grant.SubjectID) || authorityGrantPrincipalMatches(actor, grant.IssuedByType, grant.IssuedByID) {
		return nil
	}
	return s.requireCurrentDelegateAuthority(ctx, client, actor, grant)
}

func (s *GatewayServer) requireCurrentDelegateAuthority(ctx context.Context, client *ClientSession, actor models.Identity, grant *acl.AuthorityGrant) error {
	if grant == nil {
		return fmt.Errorf("authority grant is required")
	}
	if !authorityGrantPrincipalMatches(actor, grant.DelegateType, grant.DelegateID) {
		return fmt.Errorf("not authorized to use authority grant %s", grant.GrantID)
	}

	subject, err := identityFromAuthorityPrincipal(grant.SubjectType, grant.SubjectID)
	if err != nil {
		return fmt.Errorf("invalid authority grant subject: %w", err)
	}

	_, err = s.acl.ResolveAuthority(ctx, actor, acl.RequestAuthorityContext{
		Mode:    audit.AuthorityModeOnBehalfOf,
		Subject: subject,
		GrantID: grant.GrantID,
	}, acl.GrantAudienceContext{
		SessionID:        client.SessionUUID,
		AssociatedTaskID: client.AssociatedTaskID,
		Actor:            actor,
	})
	if err != nil {
		return fmt.Errorf("authority grant is not valid for the current delegate context: %w", err)
	}

	return nil
}

// resolveExchangeAudience normalizes the requested audience for a grant exchange.
// sourceSessionID, when non-empty, identifies the user session this exchange is
// acting on behalf of (trusted-service flow). Binding the grant audience to the
// source session means the grant auto-expires when the user disconnects from
// Aether — which is the correct lifecycle for on-behalf-of delegation.
func resolveExchangeAudience(client *ClientSession, actor models.Identity, requestedType, requestedID, sourceSessionID string) (string, string, error) {
	audienceType := strings.TrimSpace(requestedType)
	audienceID := strings.TrimSpace(requestedID)
	sourceSessionID = strings.TrimSpace(sourceSessionID)

	if audienceType == "" {
		// Default: prefer the source user session (trusted exchange) so the
		// grant's lifetime tracks the user's connection; otherwise fall back
		// to the caller's own session.
		if sourceSessionID != "" {
			return acl.AuthorityAudienceSession, sourceSessionID, nil
		}
		return acl.AuthorityAudienceSession, client.SessionUUID.String(), nil
	}

	switch audienceType {
	case acl.AuthorityAudienceSession:
		callerID := client.SessionUUID.String()
		if audienceID == "" {
			// Prefer the source user session; fall back to caller's session.
			if sourceSessionID != "" {
				audienceID = sourceSessionID
			} else {
				audienceID = callerID
			}
		}
		// Accept either the caller's own session or the source user session
		// (when this is a trusted exchange). Any other value is rejected.
		if audienceID != callerID && (sourceSessionID == "" || audienceID != sourceSessionID) {
			return "", "", fmt.Errorf("session audience must match the current session or the source session")
		}
	case acl.AuthorityAudienceTask:
		if client.AssociatedTaskID == "" {
			return "", "", fmt.Errorf("task audience requires a task-associated connection")
		}
		if audienceID == "" {
			audienceID = client.AssociatedTaskID
		}
		if audienceID != client.AssociatedTaskID {
			return "", "", fmt.Errorf("task audience must match the current associated task")
		}
	case acl.AuthorityAudienceAgent:
		if actor.Type != models.PrincipalAgent {
			return "", "", fmt.Errorf("agent audience requires an agent actor")
		}
		expectedID := actor.CanonicalPrincipalID()
		if audienceID == "" {
			audienceID = expectedID
		}
		if audienceID != expectedID {
			return "", "", fmt.Errorf("agent audience must match the current actor")
		}
	case acl.AuthorityAudienceService:
		if actor.Type != models.PrincipalService {
			return "", "", fmt.Errorf("service audience requires a service actor")
		}
		expectedID := actor.CanonicalPrincipalID()
		if audienceID == "" {
			audienceID = expectedID
		}
		if audienceID != expectedID {
			return "", "", fmt.Errorf("service audience must match the current actor")
		}
	default:
		return "", "", fmt.Errorf("invalid audience_type %q", audienceType)
	}

	return audienceType, audienceID, nil
}

func normalizeAuthorityGrantAccessLevel(level int) (int, error) {
	if level == 0 {
		return 0, fmt.Errorf("max_access_level is required and must be explicitly set; zero is not a valid value")
	}
	return level, nil
}

func authorityGrantPrincipalMatches(actor models.Identity, principalType, principalID string) bool {
	return acl.PrincipalTypeForModel(actor.Type) == principalType && actor.CanonicalPrincipalID() == principalID
}

func identityFromAuthorityPrincipal(principalType, principalID string) (models.Identity, error) {
	pt, err := parsePrincipalTypeString(principalType)
	if err != nil {
		return models.Identity{}, err
	}

	// Prefer the canonical parser. ParseIdentity handles every well-known
	// identity-prefixed string format (ag/tu/ta/us/sv/br/orc/wfe/metrics).
	// If the parsed type matches the requested principal type, accept it as
	// authoritative; otherwise fall back to a minimal identity carrying the
	// raw ID so the caller still gets a typed Identity.
	if parsed, err := models.ParseIdentity(principalID); err == nil && parsed.Type == pt {
		return parsed, nil
	}

	return models.Identity{Type: pt, ID: principalID}, nil
}

func protoAuthorityResourceScopeToACL(entries []*pb.ACLAuthorityGrantResourceScopeEntry) map[string][]string {
	if len(entries) == 0 {
		return nil
	}

	result := make(map[string][]string, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		result[entry.ResourceType] = append([]string(nil), entry.Patterns...)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func aclAuthorityGrantToProto(grant *acl.AuthorityGrant) *pb.ACLAuthorityGrantInfo {
	if grant == nil {
		return nil
	}

	resourceScope := make([]*pb.ACLAuthorityGrantResourceScopeEntry, 0, len(grant.ResourceScope))
	for resourceType, patterns := range grant.ResourceScope {
		resourceScope = append(resourceScope, &pb.ACLAuthorityGrantResourceScopeEntry{
			ResourceType: resourceType,
			Patterns:     append([]string(nil), patterns...),
		})
	}

	info := &pb.ACLAuthorityGrantInfo{
		GrantId:                  grant.GrantID,
		RootGrantId:              grant.RootGrantID,
		Subject:                  aclPrincipalRefToProto(grant.SubjectType, grant.SubjectID),
		Delegate:                 aclPrincipalRefToProto(grant.DelegateType, grant.DelegateID),
		IssuedBy:                 aclPrincipalRefToProto(grant.IssuedByType, grant.IssuedByID),
		RootSubject:              aclPrincipalRefToProto(grant.RootSubjectType, grant.RootSubjectID),
		MayDelegate:              grant.MayDelegate,
		RemainingHops:            int32(grant.RemainingHops),
		WorkspaceScope:           append([]string(nil), grant.WorkspaceScope...),
		ResourceScope:            resourceScope,
		OperationScope:           append([]string(nil), grant.OperationScope...),
		MaxAccessLevel:           int32(grant.MaxAccessLevel),
		AccessLevelName:          acl.AccessLevelName(grant.MaxAccessLevel),
		AudienceType:             grant.AudienceType,
		AudienceId:               grant.AudienceID,
		ValidWhileAudienceActive: grant.ValidWhileAudienceActive,
		ExpiresAt:                grant.ExpiresAt.Unix(),
		RenewableUntil:           grant.RenewableUntil.Unix(),
		Revoked:                  grant.Revoked,
		Reason:                   grant.Reason,
		Metadata:                 metadataToProtoStringMap(grant.Metadata),
		CreatedAt:                grant.CreatedAt.Unix(),
	}
	if grant.ParentGrantID != nil {
		info.ParentGrantId = *grant.ParentGrantID
	}
	if grant.RenewedAt != nil && !grant.RenewedAt.IsZero() {
		info.RenewedAt = grant.RenewedAt.Unix()
	}
	if grant.RevokedAt != nil && !grant.RevokedAt.IsZero() {
		info.RevokedAt = grant.RevokedAt.Unix()
	}

	return info
}

func aclPrincipalRefToProto(principalType, principalID string) *pb.PrincipalRef {
	if principalType == "" || principalID == "" {
		return nil
	}
	return &pb.PrincipalRef{
		PrincipalType: principalType,
		PrincipalId:   principalID,
	}
}

func sendAuthorityGrantError(client *ClientSession, requestID, errMsg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityGrant{
			AuthorityGrant: &pb.AuthorityGrantResponse{
				Success:   false,
				Error:     errMsg,
				RequestId: requestID,
			},
		},
	})
}

func (s *GatewayServer) logAuthorityGrantLifecycle(ctx context.Context, actor models.Identity, sessionID uuid.UUID, operation string, grant *acl.AuthorityGrant, success bool, errorMessage string, metadata map[string]interface{}) {
	if s.auditLogger == nil {
		return
	}

	event := &audit.AuditEvent{
		EventType:    audit.EventTypeACL,
		ActorType:    string(actor.Type),
		ActorID:      actor.String(),
		ResourceType: "authority_grant",
		Operation:    operation,
		Workspace:    actor.Workspace,
		SessionID:    sessionID,
		Success:      success,
		ErrorMessage: errorMessage,
		Metadata:     metadata,
	}

	if grant != nil {
		event.ResourceID = grant.GrantID
		event.SubjectType = principalTypeStringFromACL(grant.SubjectType)
		event.SubjectID = grant.SubjectID
		event.RootSubjectType = principalTypeStringFromACL(grant.RootSubjectType)
		event.RootSubjectID = grant.RootSubjectID
		event.AuthorityMode = audit.AuthorityModeOnBehalfOf
		rootGrantID := grant.RootGrantID
		if rootGrantID == "" {
			rootGrantID = grant.GrantID
		}
		event.RootAuthorityGrantID = &rootGrantID
		grantID := grant.GrantID
		event.AuthorityGrantID = &grantID
		event.ParentAuthorityGrantID = grant.ParentGrantID
	}

	s.auditLogger.LogEvent(ctx, event)
}
