package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/internal/audit"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
	"google.golang.org/protobuf/proto"
)

const maxDurableScheduleAuthorityTTL = 90 * 24 * time.Hour
const workflowScheduleAuthorityPolicyVersion uint32 = 1

type workflowScheduleIdentity struct {
	ID        string `json:"id"`
	Workspace string `json:"workspace"`
	Action    *struct {
		Type                            string `json:"type"`
		TargetAgentID                   string `json:"target_agent_id"`
		RequireTaskAuthority            bool   `json:"require_task_authority"`
		RequiredDownstreamAuthorityHops uint32 `json:"required_downstream_authority_hops"`
	} `json:"action"`
}

type workflowScheduleAccess struct {
	resourceID                   string
	operation                    string
	workspace                    string
	level                        int
	manage                       bool
	actionType                   string
	targeted                     bool
	requireTaskAuthority         bool
	actionRequiredDownstreamHops uint32
}

func (s *GatewayServer) prepareWorkflowOperation(ctx context.Context, client *ClientSession, incoming *pb.WorkflowOperation) (*pb.WorkflowOperation, *acl.AuthorityGrant, error) {
	if incoming == nil {
		return nil, nil, fmt.Errorf("workflow operation is required")
	}
	op := proto.Clone(incoming).(*pb.WorkflowOperation)
	op.RequestContext = nil // gateway-authored only

	client.identityMu.RLock()
	actor := client.Identity
	client.identityMu.RUnlock()
	resolved, err := s.resolveAuthorizationContext(ctx, client, actor, op.GetAuthorization())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid workflow authorization: %w", err)
	}
	subject := actor
	if resolved != nil {
		subject = resolved.Subject
	}
	op.RequestContext = &pb.WorkflowRequestContext{
		Actor: identityToProtoPrincipalRef(actor), Subject: identityToProtoPrincipalRef(subject),
		ActorSessionId: client.SessionUUID.String(),
	}

	access, isSchedule, err := workflowScheduleAccessForOperation(op)
	if err != nil {
		return nil, nil, err
	}
	if !isSchedule {
		return op, nil, nil
	}
	if s.acl == nil {
		return nil, nil, fmt.Errorf("workflow schedule authorization requires ACL service")
	}
	var decision *acl.ACLDecision
	if resolved != nil {
		decision, err = s.acl.CheckAccessWithAuthority(ctx, actor, resolved,
			acl.ResourceTypeWorkflowSchedule, access.resourceID, access.operation,
			access.workspace, client.SessionUUID, access.level)
	} else {
		decision, err = s.acl.CheckAccess(ctx, actor,
			acl.ResourceTypeWorkflowSchedule, access.resourceID, access.operation,
			access.workspace, client.SessionUUID, access.level)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("workflow schedule access check: %w", err)
	}
	if decision == nil || decision.Denied() {
		return nil, nil, fmt.Errorf("not authorized to %s workflow schedule %s", access.operation, access.resourceID)
	}

	scope := op.GetScheduleAuthorityScope()
	if scope == nil {
		if access.manage && (op.GetOp() == pb.WorkflowOperation_CREATE_SCHEDULE || op.GetOp() == pb.WorkflowOperation_UPSERT_SCHEDULE) &&
			access.actionType == "create_task" && access.requireTaskAuthority {
			return nil, nil, fmt.Errorf("schedule action requires task authority scope")
		}
		return op, nil, nil
	}
	if !access.manage || (op.GetOp() != pb.WorkflowOperation_CREATE_SCHEDULE && op.GetOp() != pb.WorkflowOperation_UPSERT_SCHEDULE) {
		return nil, nil, fmt.Errorf("schedule authority scope is valid only for create/upsert")
	}
	if access.actionType != "create_task" {
		return nil, nil, fmt.Errorf("schedule authority scope is valid only for create_task actions")
	}
	if access.actionRequiredDownstreamHops > scope.GetRequiredTaskAuthorityHops() {
		return nil, nil, fmt.Errorf("schedule action downstream authority requirement exceeds requested scope")
	}
	remainingHops := workflowScheduleGrantRemainingHops(access, scope)
	grant, err := s.mintWorkflowScheduleGrant(ctx, client, actor, resolved, op.GetId(), scope, remainingHops)
	if err != nil {
		return nil, nil, err
	}
	rootGrantID := grant.RootGrantID
	if rootGrantID == "" {
		rootGrantID = grant.GrantID
	}
	sourceGrantID := ""
	if resolved != nil && resolved.Grant != nil {
		sourceGrantID = resolved.Grant.GrantID
	}
	digest, err := workflowAuthorityPolicyDigest(scope)
	if err != nil {
		_, _ = s.acl.RevokeAuthorityGrantCascade(ctx, grant.GrantID)
		return nil, nil, err
	}
	op.RequestContext.ScheduleAuthorization = &pb.AuthorizationContext{
		AuthorityMode: audit.AuthorityModeOnBehalfOf,
		Subject:       identityToProtoPrincipalRef(subject),
		GrantId:       grant.GrantID,
	}
	op.RequestContext.RootGrantId = rootGrantID
	op.RequestContext.SourceGrantId = sourceGrantID
	op.RequestContext.ExpiresAtMs = grant.ExpiresAt.UnixMilli()
	op.RequestContext.PolicyDigest = digest
	op.RequestContext.LifetimeMode = scope.GetLifetimeMode()
	op.RequestContext.PolicyVersion = scope.GetPolicyVersion()
	return op, grant, nil
}

func workflowScheduleGrantRemainingHops(access workflowScheduleAccess, scope *pb.WorkflowScheduleAuthorityScope) int {
	remainingHops := int(scope.GetRequiredTaskAuthorityHops()) + 1 // schedule -> targeted task
	if !access.targeted {
		remainingHops++ // schedule -> pool task anchor -> selected worker
	}
	return remainingHops
}

func workflowScheduleAccessForOperation(op *pb.WorkflowOperation) (workflowScheduleAccess, bool, error) {
	if op == nil {
		return workflowScheduleAccess{}, false, fmt.Errorf("workflow operation is required")
	}
	read := op.GetOp() == pb.WorkflowOperation_LIST_SCHEDULES
	manage := op.GetOp() == pb.WorkflowOperation_CREATE_SCHEDULE ||
		op.GetOp() == pb.WorkflowOperation_UPSERT_SCHEDULE ||
		op.GetOp() == pb.WorkflowOperation_DELETE_SCHEDULE
	if !read && !manage {
		return workflowScheduleAccess{}, false, nil
	}
	workspace := strings.TrimSpace(op.GetWorkspace())
	if workspace == "" || workspace == "*" {
		return workflowScheduleAccess{}, true, fmt.Errorf("an exact workflow schedule workspace is required")
	}
	workspaceSegment, err := encodeWorkflowResourceSegment(workspace)
	if err != nil {
		return workflowScheduleAccess{}, true, fmt.Errorf("workflow schedule workspace: %w", err)
	}
	scheduleID := "*"
	var actionType string
	var targeted, requireTaskAuthority bool
	var actionRequiredDownstreamHops uint32
	if manage {
		scheduleID = strings.TrimSpace(op.GetId())
		if scheduleID == "" {
			return workflowScheduleAccess{}, true, fmt.Errorf("workflow schedule id is required")
		}
		if op.GetOp() == pb.WorkflowOperation_CREATE_SCHEDULE || op.GetOp() == pb.WorkflowOperation_UPSERT_SCHEDULE {
			var identity workflowScheduleIdentity
			if err := json.Unmarshal(op.GetData(), &identity); err != nil {
				return workflowScheduleAccess{}, true, fmt.Errorf("invalid workflow schedule JSON: %w", err)
			}
			if identity.ID != scheduleID || identity.Workspace != workspace {
				return workflowScheduleAccess{}, true, fmt.Errorf("workflow schedule operation identity does not match JSON definition")
			}
			if identity.Action != nil {
				actionType = identity.Action.Type
				targeted = strings.TrimSpace(identity.Action.TargetAgentID) != ""
				requireTaskAuthority = identity.Action.RequireTaskAuthority
				actionRequiredDownstreamHops = identity.Action.RequiredDownstreamAuthorityHops
			}
		}
	}
	scheduleSegment := scheduleID
	if scheduleID != "*" {
		scheduleSegment, err = encodeWorkflowResourceSegment(scheduleID)
		if err != nil {
			return workflowScheduleAccess{}, true, fmt.Errorf("workflow schedule id: %w", err)
		}
	}
	access := workflowScheduleAccess{
		resourceID: "workspaces/" + workspaceSegment + "/schedules/" + scheduleSegment,
		workspace:  workspace, manage: manage, actionType: actionType, targeted: targeted,
		requireTaskAuthority: requireTaskAuthority, actionRequiredDownstreamHops: actionRequiredDownstreamHops,
	}
	if read {
		access.operation = audit.OpWorkflowScheduleRead
		access.level = acl.AccessRead
	} else {
		access.operation = audit.OpWorkflowScheduleManage
		access.level = acl.AccessManage
	}
	return access, true, nil
}

func (s *GatewayServer) mintWorkflowScheduleGrant(ctx context.Context, client *ClientSession, actor models.Identity, source *acl.ResolvedAuthority, scheduleID string, scope *pb.WorkflowScheduleAuthorityScope, remainingHops int) (*acl.AuthorityGrant, error) {
	if scope == nil || len(scope.GetWorkspaceScope()) == 0 || len(scope.GetResourceScope()) == 0 || len(scope.GetOperationScope()) == 0 {
		return nil, fmt.Errorf("workflow schedule authority requires non-empty workspace, resource, and operation scope")
	}
	if scope.GetPolicyVersion() != workflowScheduleAuthorityPolicyVersion {
		return nil, fmt.Errorf("unsupported workflow schedule authority policy version %d", scope.GetPolicyVersion())
	}
	if scope.GetMaxAccessLevel() <= 0 || scope.GetRequiredTaskAuthorityHops() > 1 {
		return nil, fmt.Errorf("invalid workflow schedule access level or downstream hop requirement")
	}
	now := time.Now().UTC()
	expiresAt := time.Unix(scope.GetExpiresAt(), 0).UTC()
	renewableUntil := time.Unix(scope.GetRenewableUntil(), 0).UTC()
	if !expiresAt.After(now) || renewableUntil.Before(expiresAt) {
		return nil, fmt.Errorf("workflow schedule authority expiry and renewable ceiling are invalid")
	}
	for _, workspace := range scope.GetWorkspaceScope() {
		if workspace == "" || workspace == "*" || workspace == acl.WorkspaceScopeSubjectInherited {
			return nil, fmt.Errorf("workflow schedule authority workspace scope must be exact")
		}
	}

	subject := actor
	if source != nil {
		subject = source.Subject
	}
	delegate := models.Identity{Type: models.PrincipalWorkflowEngine}
	request := acl.CreateAuthorityGrantRequest{
		Subject: subject, Delegate: delegate, IssuedBy: actor,
		MayDelegate: true, RemainingHops: remainingHops,
		WorkspaceScope: append([]string(nil), scope.GetWorkspaceScope()...),
		ResourceScope:  protoAuthorityResourceScopeToACL(scope.GetResourceScope()),
		OperationScope: append([]string(nil), scope.GetOperationScope()...),
		MaxAccessLevel: int(scope.GetMaxAccessLevel()),
		AudienceType:   acl.AuthorityAudienceWorkflowSchedule, AudienceID: scheduleID,
		ExpiresAt: expiresAt, RenewableUntil: renewableUntil,
		Reason: "workflow-schedule:" + scheduleID,
		Metadata: map[string]interface{}{
			"workflow_schedule_id":        scheduleID,
			"workflow_authority_lifetime": scope.GetLifetimeMode().String(),
		},
	}

	switch scope.GetLifetimeMode() {
	case pb.WorkflowAuthorityLifetimeMode_WORKFLOW_AUTHORITY_LIFETIME_SOURCE_BOUND:
		if source == nil || source.Grant == nil {
			return nil, fmt.Errorf("source-bound workflow schedule authority requires OBO authority")
		}
		parentGrantID := source.Grant.GrantID
		request.ParentGrantID = &parentGrantID
		request.RootSubject = workflowRootSubject(source)
		request.Metadata["source_authority_grant_id"] = parentGrantID
	case pb.WorkflowAuthorityLifetimeMode_WORKFLOW_AUTHORITY_LIFETIME_DURABLE:
		if expiresAt.After(now.Add(maxDurableScheduleAuthorityTTL)) || !renewableUntil.Equal(expiresAt) {
			return nil, fmt.Errorf("durable workflow schedule authority must use a fixed expiry within %s", maxDurableScheduleAuthorityTTL)
		}
		if source == nil {
			if actor.Type != models.PrincipalUser {
				return nil, fmt.Errorf("durable workflow schedule authority requires a direct user or authorized OBO intermediary")
			}
		} else {
			decision, err := s.acl.CheckAccess(ctx, actor, acl.ResourceTypeCapability,
				acl.PermissionScheduleAuthority, audit.OpScheduleAuthority,
				actor.Workspace, client.SessionUUID, acl.AccessManage)
			if err != nil || decision == nil || decision.Denied() {
				return nil, fmt.Errorf("actor is not authorized to mint durable workflow schedule authority")
			}
			if err := acl.ValidateAuthorityGrantScopeAttenuation(source.Grant, request); err != nil {
				return nil, fmt.Errorf("durable workflow schedule authority exceeds source scope: %w", err)
			}
			request.Metadata["source_authority_grant_id"] = source.Grant.GrantID
		}
	default:
		return nil, fmt.Errorf("unsupported workflow schedule authority lifetime mode")
	}

	grant, err := s.acl.CreateAuthorityGrant(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("create workflow schedule authority: %w", err)
	}
	s.logAuthorityGrantLifecycle(ctx, actor, client.SessionUUID, audit.OpAuthorityGrantDerive, grant, true, "", map[string]interface{}{
		"workflow_schedule_id": scheduleID,
		"lifetime_mode":        scope.GetLifetimeMode().String(),
	})
	return grant, nil
}

func workflowRootSubject(source *acl.ResolvedAuthority) *models.Identity {
	if source == nil || source.Grant == nil {
		return nil
	}
	root, err := identityFromAuthorityPrincipal(source.Grant.RootSubjectType, source.Grant.RootSubjectID)
	if err != nil {
		copy := source.Subject
		return &copy
	}
	return &root
}

func workflowAuthorityPolicyDigest(scope *pb.WorkflowScheduleAuthorityScope) (string, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(scope)
	if err != nil {
		return "", fmt.Errorf("marshal workflow authority policy: %w", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func encodeWorkflowResourceSegment(value string) (string, error) {
	if value == "" || value == "!" || !utf8.ValidString(value) {
		return "", fmt.Errorf("invalid resource segment")
	}
	const hexUpper = "0123456789ABCDEF"
	var out strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') ||
			(b >= '0' && b <= '9') || b == '-' || b == '.' || b == '_' || b == '~' {
			out.WriteByte(b)
			continue
		}
		out.WriteByte('%')
		out.WriteByte(hexUpper[b>>4])
		out.WriteByte(hexUpper[b&0x0f])
	}
	return out.String(), nil
}

func workflowGrantID(grant *acl.AuthorityGrant) string {
	if grant == nil {
		return ""
	}
	return grant.GrantID
}

func (s *GatewayServer) revokeProvisionalWorkflowGrant(ctx context.Context, grant *acl.AuthorityGrant) {
	if grant != nil {
		s.revokeProvisionalWorkflowGrantByID(ctx, grant.GrantID)
	}
}

func (s *GatewayServer) revokeProvisionalWorkflowGrantByID(ctx context.Context, grantID string) {
	if s.acl == nil || strings.TrimSpace(grantID) == "" {
		return
	}
	_, _ = s.acl.RevokeAuthorityGrantCascade(ctx, grantID)
}

// validateWorkflowScheduleSourceAuthority enforces the extra lifetime edge for
// source-bound schedule grants. Revocation/expiry cascades are already checked
// by ResolveAuthority; this additionally treats an inactive source session or
// task as permanent invalidity without changing the public grant schema.
func (s *GatewayServer) validateWorkflowScheduleSourceAuthority(ctx context.Context, grant *acl.AuthorityGrant) error {
	if grant == nil {
		return fmt.Errorf("workflow schedule authority grant is required")
	}
	lifetime, _ := grant.Metadata["workflow_authority_lifetime"].(string)
	if lifetime != pb.WorkflowAuthorityLifetimeMode_WORKFLOW_AUTHORITY_LIFETIME_SOURCE_BOUND.String() {
		return nil
	}
	sourceGrantID, _ := grant.Metadata["source_authority_grant_id"].(string)
	if strings.TrimSpace(sourceGrantID) == "" || grant.ParentGrantID == nil || *grant.ParentGrantID != sourceGrantID {
		return fmt.Errorf("source-bound schedule authority lineage is invalid")
	}
	source, err := s.acl.GetAuthorityGrant(ctx, sourceGrantID)
	if err != nil {
		return err
	}
	if err := source.ValidateActiveAt(time.Now()); err != nil {
		return err
	}
	if !source.ValidWhileAudienceActive {
		return nil
	}
	switch source.AudienceType {
	case acl.AuthorityAudienceSession:
		sessionID, err := uuid.Parse(source.AudienceID)
		if err != nil || s.sessions == nil {
			return acl.ErrAuthorityGrantAudienceMismatch
		}
		identity, err := s.sessions.GetSessionIdentity(ctx, sessionID.String())
		if err != nil {
			return acl.ErrAuthorityGrantAudienceMismatch
		}
		active, err := s.sessions.IsActive(ctx, identity.String())
		if err != nil || !active {
			return acl.ErrAuthorityGrantAudienceMismatch
		}
	case acl.AuthorityAudienceTask:
		if s.taskStore == nil {
			return acl.ErrAuthorityGrantAudienceMismatch
		}
		task, err := s.taskStore.GetTask(ctx, source.AudienceID)
		if err != nil || task == nil || (task.Status != tasks.TaskStatusPending && task.Status != tasks.TaskStatusAssigned && task.Status != tasks.TaskStatusRunning) {
			return acl.ErrAuthorityGrantAudienceMismatch
		}
	}
	return nil
}
