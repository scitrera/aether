package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/internal/audit"
	"github.com/scitrera/aether/server/internal/logging"
	"github.com/scitrera/aether/server/pkg/models"
)

const (
	maxBatchAccessChecks = 100
	defaultReceiptTTL    = 30 * time.Second
	viewBindReceiptTTL   = 2 * time.Minute
	maxReceiptTTL        = 5 * time.Minute
)

type runtimeAccessChecker interface {
	CheckAccess(context.Context, models.Identity, string, string, string, string, uuid.UUID, int) (*acl.ACLDecision, error)
	CheckAccessWithAuthority(context.Context, models.Identity, *acl.ResolvedAuthority, string, string, string, string, uuid.UUID, int) (*acl.ACLDecision, error)
}

func validateCorrelationID(value string) error {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return fmt.Errorf("correlation_id must be 1-128 characters without surrounding whitespace")
	}
	return nil
}

func validateRequestID(value string) error {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return fmt.Errorf("request_id must be 1-128 characters without surrounding whitespace")
	}
	return nil
}

func validateResourceAccessRequest(req *pb.ResourceAccessRequest) error {
	if req == nil {
		return fmt.Errorf("access request is required")
	}
	fields := []struct {
		name  string
		value string
		max   int
		req   bool
	}{
		{"resource_type", req.GetResourceType(), 128, true},
		{"resource_id", req.GetResourceId(), 512, true},
		{"operation", req.GetOperation(), 128, true},
		{"workspace", req.GetWorkspace(), 256, false},
	}
	for _, field := range fields {
		if field.req && field.value == "" {
			return fmt.Errorf("%s is required", field.name)
		}
		if len(field.value) > field.max {
			return fmt.Errorf("%s exceeds %d characters", field.name, field.max)
		}
		if strings.TrimSpace(field.value) != field.value {
			return fmt.Errorf("%s must not contain surrounding whitespace", field.name)
		}
	}
	if err := validateCorrelationID(req.GetCorrelationId()); err != nil {
		return err
	}
	if req.GetRequiredAccessLevel() <= 0 || acl.ValidateAccessLevel(int(req.GetRequiredAccessLevel())) != nil {
		return fmt.Errorf("required_access_level must be one of 10, 20, 30, 40, or 50")
	}
	return nil
}

func receiptTTL(req *pb.ResourceAccessRequest) time.Duration {
	if req.GetResourceType() == models.ResourceTypeWorkspaceExecutionView && req.GetOperation() == "bind" {
		return viewBindReceiptTTL
	}
	return defaultReceiptTTL
}

func evaluateResourceAccess(
	ctx context.Context,
	checker runtimeAccessChecker,
	actor models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	req *pb.ResourceAccessRequest,
	deliveryTarget string,
	now time.Time,
) (*pb.AccessDecisionReceipt, error) {
	if err := validateResourceAccessRequest(req); err != nil {
		return nil, err
	}
	if checker == nil {
		return nil, fmt.Errorf("ACL service is unavailable")
	}

	var (
		decision *acl.ACLDecision
		err      error
	)
	if authority == nil {
		decision, err = checker.CheckAccess(ctx, actor, req.GetResourceType(), req.GetResourceId(), req.GetOperation(), req.GetWorkspace(), sessionID, int(req.GetRequiredAccessLevel()))
	} else {
		// Exact logical-resource checks never use the message route's
		// actor-first fallback. OBO means subject ACL intersected with grant.
		decision, err = checker.CheckAccessWithAuthority(ctx, actor, authority, req.GetResourceType(), req.GetResourceId(), req.GetOperation(), req.GetWorkspace(), sessionID, int(req.GetRequiredAccessLevel()))
	}
	if err != nil {
		return nil, err
	}
	if decision == nil {
		return nil, fmt.Errorf("ACL service returned no decision")
	}

	mode := audit.AuthorityModeDirect
	receipt := &pb.AccessDecisionReceipt{
		DecisionId:           uuid.NewString(),
		Request:              req,
		Allowed:              decision.Allowed,
		Decision:             decision.Decision,
		EffectiveAccessLevel: int32(decision.EffectiveAccessLevel),
		Actor:                actorPrincipalRef(actor),
		EvaluatedAtMs:        now.UnixMilli(),
		DeliveryTarget:       deliveryTarget,
	}
	if receipt.Decision == "" {
		if receipt.Allowed {
			receipt.Decision = acl.DecisionAllow
		} else {
			receipt.Decision = acl.DecisionDeny
		}
	}
	if !receipt.Allowed {
		receipt.DenialCode = "access_denied"
	}

	expiresAt := now.Add(receiptTTL(req))
	if expiresAt.After(now.Add(maxReceiptTTL)) {
		expiresAt = now.Add(maxReceiptTTL)
	}
	if authority != nil && authority.Grant != nil {
		mode = audit.AuthorityModeOnBehalfOf
		grant := authority.Grant
		receipt.Subject = actorPrincipalRef(authority.Subject)
		receipt.GrantId = grant.GrantID
		receipt.RootGrantId = grant.RootGrantID
		if grant.RootSubjectType != "" && grant.RootSubjectID != "" {
			receipt.RootSubject = aclPrincipalRefToProto(grant.RootSubjectType, grant.RootSubjectID)
		}
		if !grant.ExpiresAt.IsZero() && grant.ExpiresAt.Before(expiresAt) {
			expiresAt = grant.ExpiresAt
		}
	}
	receipt.AuthorityMode = mode
	receipt.ExpiresAtMs = expiresAt.UnixMilli()
	return receipt, nil
}

func (s *GatewayServer) resolveAccessCheckAuthority(ctx context.Context, client *ClientSession, actor models.Identity, authz *pb.AuthorizationContext) (*acl.ResolvedAuthority, error) {
	resolved, err := s.resolveAuthorizationContext(ctx, client, actor, authz)
	if err != nil || resolved != nil || client == nil || client.AssociatedTaskID == "" {
		return resolved, err
	}
	if actor.Type != models.PrincipalAgent && actor.Type != models.PrincipalTask {
		return nil, nil
	}
	return s.loadCallerMessageAuthority(ctx, client, actor)
}

func currentClientIdentity(client *ClientSession) models.Identity {
	client.identityMu.RLock()
	defer client.identityMu.RUnlock()
	return client.Identity
}

func (s *GatewayServer) handleAccessCheck(ctx context.Context, client *ClientSession, op *pb.AccessCheckOperation) {
	response := &pb.AccessCheckResponse{}
	if op != nil {
		response.RequestId = op.GetRequestId()
	}
	if op == nil {
		response.Error = "access check operation is required"
	} else if err := validateRequestID(op.GetRequestId()); err != nil {
		response.Error = err.Error()
	} else if err := validateResourceAccessRequest(op.GetAccess()); err != nil {
		response.Error = err.Error()
	} else {
		actor := currentClientIdentity(client)
		authority, err := s.resolveAccessCheckAuthority(ctx, client, actor, op.GetAuthorization())
		if err != nil {
			response.Error = "invalid authorization context"
		} else {
			response.Decision, err = evaluateResourceAccess(ctx, s.acl, actor, authority, client.SessionUUID, op.GetAccess(), "", time.Now())
			if err != nil {
				logging.Logger.Error().Err(err).Str("actor", actor.String()).Str("request_id", op.GetRequestId()).Msg("runtime access evaluation failed")
				response.Error = "access evaluation unavailable"
			} else {
				response.Success = true
			}
		}
	}
	_ = client.SafeSend(&pb.DownstreamMessage{Payload: &pb.DownstreamMessage_AccessCheckResponse{AccessCheckResponse: response}})
}

func (s *GatewayServer) handleBatchAccessCheck(ctx context.Context, client *ClientSession, op *pb.BatchAccessCheckOperation) {
	response := &pb.BatchAccessCheckResponse{}
	if op != nil {
		response.RequestId = op.GetRequestId()
	}
	if op == nil {
		response.Error = "batch access check operation is required"
	} else if err := validateRequestID(op.GetRequestId()); err != nil {
		response.Error = err.Error()
	} else if len(op.GetAccess()) == 0 || len(op.GetAccess()) > maxBatchAccessChecks {
		response.Error = fmt.Sprintf("batch must contain 1-%d access requests", maxBatchAccessChecks)
	} else {
		// Validate the complete batch before evaluating any item. This avoids
		// partial authorization/audit side effects for malformed batches.
		for i, req := range op.GetAccess() {
			if err := validateResourceAccessRequest(req); err != nil {
				response.Error = fmt.Sprintf("access[%d]: %s", i, err)
				break
			}
		}
		if response.Error == "" {
			actor := currentClientIdentity(client)
			authority, err := s.resolveAccessCheckAuthority(ctx, client, actor, op.GetAuthorization())
			if err != nil {
				response.Error = "invalid authorization context"
			} else {
				now := time.Now()
				response.Decisions = make([]*pb.AccessDecisionReceipt, 0, len(op.GetAccess()))
				for _, req := range op.GetAccess() {
					decision, evalErr := evaluateResourceAccess(ctx, s.acl, actor, authority, client.SessionUUID, req, "", now)
					if evalErr != nil {
						logging.Logger.Error().Err(evalErr).Str("actor", actor.String()).Str("request_id", op.GetRequestId()).Msg("batch runtime access evaluation failed")
						response.Error = "access evaluation unavailable"
						response.Decisions = nil
						break
					}
					response.Decisions = append(response.Decisions, decision)
				}
				response.Success = response.Error == ""
			}
		}
	}
	_ = client.SafeSend(&pb.DownstreamMessage{Payload: &pb.DownstreamMessage_BatchAccessCheckResponse{BatchAccessCheckResponse: response}})
}
