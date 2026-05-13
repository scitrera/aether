package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

func (s *GatewayServer) resolveAuthorizationContext(ctx context.Context, client *ClientSession, actor models.Identity, authz *pb.AuthorizationContext) (*acl.ResolvedAuthority, error) {
	if authz == nil {
		return nil, nil
	}

	mode := strings.TrimSpace(authz.GetAuthorityMode())
	switch mode {
	case "", audit.AuthorityModeDirect:
		if authz.Subject != nil || authz.GetGrantId() != "" {
			return nil, fmt.Errorf("direct authorization context must not include subject or grant")
		}
		return nil, nil
	case audit.AuthorityModeOnBehalfOf:
		// Continue below.
	default:
		return nil, fmt.Errorf("unsupported authority mode %q", mode)
	}

	if s.acl == nil {
		return nil, fmt.Errorf("authority grants require ACL service")
	}
	if authz.Subject == nil {
		return nil, fmt.Errorf("on-behalf-of authorization requires subject")
	}
	if strings.TrimSpace(authz.GetGrantId()) == "" {
		return nil, fmt.Errorf("on-behalf-of authorization requires grant_id")
	}

	subject, err := protoPrincipalRefToIdentity(authz.Subject)
	if err != nil {
		return nil, err
	}

	return s.acl.ResolveAuthority(ctx, actor, acl.RequestAuthorityContext{
		Mode:    mode,
		Subject: subject,
		GrantID: authz.GetGrantId(),
	}, acl.GrantAudienceContext{
		SessionID:        client.SessionUUID,
		AssociatedTaskID: client.AssociatedTaskID,
		Actor:            actor,
		SessionActive: func(sessionID uuid.UUID) bool {
			// SessionRegistry.IsActive keys on the identity string (e.g.
			// "us::dev@x::com::wnd_123"), not the session UUID. Resolve the
			// identity first, then check activity against it.
			identity, err := s.sessions.GetSessionIdentity(ctx, sessionID.String())
			if err != nil {
				return false
			}
			active, err := s.sessions.IsActive(ctx, identity.String())
			return err == nil && active
		},
		TaskActive: func(taskID string) bool {
			if taskID == "" || s.taskStore == nil {
				return false
			}
			t, err := s.taskStore.GetTask(ctx, taskID)
			if err != nil || t == nil {
				return false
			}
			return t.Status == tasks.TaskStatusPending || t.Status == tasks.TaskStatusAssigned || t.Status == tasks.TaskStatusRunning
		},
	})
}

func protoPrincipalRefToIdentity(ref *pb.PrincipalRef) (models.Identity, error) {
	if ref == nil {
		return models.Identity{}, fmt.Errorf("principal ref is required")
	}

	pt, err := parsePrincipalTypeString(ref.GetPrincipalType())
	if err != nil {
		return models.Identity{}, err
	}
	if strings.TrimSpace(ref.GetPrincipalId()) == "" {
		return models.Identity{}, fmt.Errorf("principal_id is required")
	}

	identity := models.Identity{
		Type: pt,
		ID:   ref.GetPrincipalId(),
	}

	switch pt {
	case models.PrincipalAgent, models.PrincipalTask, models.PrincipalBridge, models.PrincipalService:
		if parsed, err := models.ParseIdentity(ref.GetPrincipalId()); err == nil && parsed.Type == pt {
			return parsed, nil
		}
	}

	return identity, nil
}

func parsePrincipalTypeString(value string) (models.PrincipalType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "agent":
		return models.PrincipalAgent, nil
	case "task", "unique_task", "non_unique_task":
		return models.PrincipalTask, nil
	case "user":
		return models.PrincipalUser, nil
	case "workflow_engine", "workflowengine":
		return models.PrincipalWorkflowEngine, nil
	case "metrics_bridge", "metricsbridge":
		return models.PrincipalMetricsBridge, nil
	case "orchestrator":
		return models.PrincipalOrchestrator, nil
	case "bridge":
		return models.PrincipalBridge, nil
	case "service":
		return models.PrincipalService, nil
	default:
		return "", fmt.Errorf("unknown principal type %q", value)
	}
}

func applyResolvedAuthorityToAuditEvent(event *audit.AuditEvent, authority *acl.ResolvedAuthority) {
	if event == nil || authority == nil || authority.Grant == nil {
		return
	}

	event.SubjectType = string(authority.Subject.Type)
	event.SubjectID = authority.Subject.CanonicalPrincipalID()
	event.RootSubjectType = principalTypeStringFromACL(authority.Grant.RootSubjectType)
	event.RootSubjectID = authority.Grant.RootSubjectID
	event.AuthorityMode = audit.AuthorityModeOnBehalfOf

	rootGrantID := authority.Grant.RootGrantID
	if rootGrantID == "" {
		rootGrantID = authority.Grant.GrantID
	}
	event.RootAuthorityGrantID = &rootGrantID

	grantID := authority.Grant.GrantID
	event.AuthorityGrantID = &grantID
	event.ParentAuthorityGrantID = authority.Grant.ParentGrantID
}

func principalTypeStringFromACL(value string) string {
	switch value {
	case acl.PrincipalTypeAgent:
		return string(models.PrincipalAgent)
	case acl.PrincipalTypeTask:
		return string(models.PrincipalTask)
	case acl.PrincipalTypeUser:
		return string(models.PrincipalUser)
	case acl.PrincipalTypeWorkflowEngine:
		return string(models.PrincipalWorkflowEngine)
	case acl.PrincipalTypeMetricsBridge:
		return string(models.PrincipalMetricsBridge)
	case acl.PrincipalTypeOrchestrator:
		return string(models.PrincipalOrchestrator)
	case acl.PrincipalTypeBridge:
		return string(models.PrincipalBridge)
	case acl.PrincipalTypeService:
		return string(models.PrincipalService)
	default:
		return value
	}
}

func applyResolvedAuthorityToTaskMetadata(metadata map[string]interface{}, authority *acl.ResolvedAuthority) map[string]interface{} {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	if authority == nil || authority.Grant == nil {
		return metadata
	}

	metadata["authority_mode"] = audit.AuthorityModeOnBehalfOf
	metadata["subject_type"] = string(authority.Subject.Type)
	metadata["subject_id"] = authority.Subject.CanonicalPrincipalID()
	metadata["root_subject_type"] = principalTypeStringFromACL(authority.Grant.RootSubjectType)
	metadata["root_subject_id"] = authority.Grant.RootSubjectID
	metadata["authority_grant_id"] = authority.Grant.GrantID
	rootGrantID := authority.Grant.RootGrantID
	if rootGrantID == "" {
		rootGrantID = authority.Grant.GrantID
	}
	metadata["root_authority_grant_id"] = rootGrantID
	if authority.Grant.ParentGrantID != nil {
		metadata["parent_authority_grant_id"] = *authority.Grant.ParentGrantID
	}

	return metadata
}

func normalizeAuditPrincipalTypeFilter(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	// Audit rows store actor_type/subject_type in lowercase canonical form
	// (matching acl.PrincipalTypeAgent = "agent"). Normalize filter input
	// to the same form so queries match regardless of input casing.
	if pt, err := parsePrincipalTypeString(trimmed); err == nil {
		return acl.PrincipalTypeForModel(pt)
	}
	return audit.NormalizePrincipalTypeCase(trimmed)
}

func (s *GatewayServer) checkMessageSendWithAuthority(ctx context.Context, sender models.Identity, targetTopic string, sessionID uuid.UUID, authority *acl.ResolvedAuthority) error {
	hasGrant := authority != nil && authority.Grant != nil
	subjectType := ""
	subjectID := ""
	if authority != nil {
		subjectType = string(authority.Subject.Type)
		subjectID = authority.Subject.ID
	}
	logging.Logger.Debug().
		Str("sender", sender.ToTopic()).
		Str("target", targetTopic).
		Bool("has_grant", hasGrant).
		Str("subject_type", subjectType).
		Str("subject_id", subjectID).
		Msg("checkMessageSendWithAuthority entry")

	if s.acl == nil {
		return fmt.Errorf("ACL service not available")
	}

	// Workspace-less senders (bridges, services, users) check against the
	// target workspace — they have no "home" to fall back on. Topics
	// without a workspace component (us::, br::) skip the check entirely.
	if sender.Workspace == "" {
		targetWorkspace := extractWorkspaceFromTopic(targetTopic)
		if targetWorkspace == "" {
			return nil
		}
		decision, err := s.acl.CheckAccessWithAuthority(ctx, sender, authority, acl.ResourceTypeWorkspace, targetWorkspace, "send_message", targetWorkspace, sessionID, acl.AccessReadWrite)
		if err != nil {
			return fmt.Errorf("ACL check failed: %w", err)
		}
		if decision.Denied() {
			logging.Logger.Info().
				Str("sender", sender.ToTopic()).
				Str("target", targetTopic).
				Str("reason", decision.Reason).
				Msg("checkMessageSendWithAuthority denial")
			return fmt.Errorf("access denied: %s", decision.Reason)
		}
		return nil
	}

	// Workspace-scoped senders: the workspace we gate on is the TARGET
	// workspace (when it differs from the sender's home), or the
	// sender's own workspace (the implicit same-workspace pathway).
	// This implements "explicit cross-workspace authority OR implicit
	// same-workspace" — see routing.go::enforceTopicPermissions for the
	// rationale.
	checkWorkspace := sender.Workspace
	targetWorkspace := extractWorkspaceFromTopic(targetTopic)
	if targetWorkspace != "" && targetWorkspace != sender.Workspace {
		checkWorkspace = targetWorkspace
	}

	// Order matters: try the ACTOR's direct grant first (cheaper, simpler
	// semantic — "the caller has it"), then fall back to OBO (which loads
	// the grant chain and validates the subject's authority). This way:
	//   * Same-workspace sends short-circuit via the sender's home grants
	//     without even consulting the OBO chain.
	//   * Infra agents acting cross-workspace (e.g., CoworkAgent in `_apps`
	//     pushing config to its spawned sidecar in `_sandbox`) succeed via
	//     the actor's direct workspace grant — the OBO subject (typically
	//     an end user) doesn't need a claim on the target workspace.
	//   * Genuine OBO operations (acting on a user's behalf for resources
	//     the actor itself doesn't own) succeed via the OBO check when the
	//     actor lacks direct grant but the subject has it.
	directDecision, directErr := s.acl.CheckAccess(ctx, sender, acl.ResourceTypeWorkspace, checkWorkspace, "send_message", checkWorkspace, sessionID, acl.AccessReadWrite)
	if directErr != nil {
		return fmt.Errorf("ACL check failed (direct): %w", directErr)
	}
	if directDecision != nil && !directDecision.Denied() {
		return nil // actor's own grant suffices, OBO chain not consulted
	}
	directReason := "no decision"
	if directDecision != nil {
		directReason = directDecision.Reason
	}

	// Fall back to OBO subject's grant.
	oboDecision, oboErr := s.acl.CheckAccessWithAuthority(ctx, sender, authority, acl.ResourceTypeWorkspace, checkWorkspace, "send_message", checkWorkspace, sessionID, acl.AccessReadWrite)
	if oboErr != nil {
		logging.Logger.Info().
			Str("sender", sender.ToTopic()).
			Str("target", targetTopic).
			Str("workspace_checked", checkWorkspace).
			Str("actor_reason", directReason).
			Err(oboErr).
			Msg("checkMessageSendWithAuthority OBO fallback errored")
		return fmt.Errorf("access denied (actor: %s; OBO check failed: %w)", directReason, oboErr)
	}
	if oboDecision == nil || oboDecision.Denied() {
		oboReason := "no decision"
		if oboDecision != nil {
			oboReason = oboDecision.Reason
		}
		logging.Logger.Info().
			Str("sender", sender.ToTopic()).
			Str("target", targetTopic).
			Str("workspace_checked", checkWorkspace).
			Str("actor_reason", directReason).
			Str("obo_reason", oboReason).
			Msg("checkMessageSendWithAuthority denial (both actor + OBO)")
		return fmt.Errorf("access denied: actor lacks workspace %s grant (%s) AND OBO subject lacks it (%s)", checkWorkspace, directReason, oboReason)
	}
	logging.Logger.Debug().
		Str("sender", sender.ToTopic()).
		Str("target", targetTopic).
		Str("workspace_checked", checkWorkspace).
		Str("actor_reason", directReason).
		Msg("checkMessageSendWithAuthority allowed via OBO fallback (actor lacks direct grant)")
	return nil
}
