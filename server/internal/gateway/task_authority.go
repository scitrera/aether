package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

const (
	authorityAudienceTypeKey     = "authority_audience_type"
	authorityAudienceIDKey       = "authority_audience_id"
	authorityDelegateTypeKey     = "authority_delegate_type"
	authorityDelegateIDKey       = "authority_delegate_id"
	authorityReasonKey           = "authority_reason"
	taskAuthorityTaskIDKey       = "authority_task_id"
	taskAuthorityTaskTypeKey     = "authority_task_type"
	taskAuthorityModeKey         = "authority_task_mode"
	taskAuthorityIntermediaryKey = "authority_intermediary_reroot" // true when this grant was minted via capability/authority_intermediary because the parent grant ran out of hops
	taskAuthorityCreatorPrefix   = "creator_"
	taskAuthorityRenewalLead     = 5 * time.Minute
	taskAuthorityMinLease        = 1 * time.Minute
)

func applyTaskAuthorityGrantToMetadata(metadata map[string]interface{}, grant *acl.AuthorityGrant) map[string]interface{} {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	if grant == nil {
		return metadata
	}

	preserveCreatorAuthorityMetadata(metadata)

	rootGrantID := grant.RootGrantID
	if rootGrantID == "" {
		rootGrantID = grant.GrantID
	}
	rootSubjectType := principalTypeStringFromACL(grant.RootSubjectType)
	if rootSubjectType == "" {
		rootSubjectType = principalTypeStringFromACL(grant.SubjectType)
	}
	rootSubjectID := grant.RootSubjectID
	if rootSubjectID == "" {
		rootSubjectID = grant.SubjectID
	}

	metadata["authority_mode"] = audit.AuthorityModeOnBehalfOf
	metadata["subject_type"] = principalTypeStringFromACL(grant.SubjectType)
	metadata["subject_id"] = grant.SubjectID
	metadata["root_subject_type"] = rootSubjectType
	metadata["root_subject_id"] = rootSubjectID
	metadata["authority_grant_id"] = grant.GrantID
	metadata["root_authority_grant_id"] = rootGrantID
	if grant.ParentGrantID != nil {
		metadata["parent_authority_grant_id"] = *grant.ParentGrantID
	} else {
		delete(metadata, "parent_authority_grant_id")
	}
	metadata[authorityAudienceTypeKey] = grant.AudienceType
	metadata[authorityAudienceIDKey] = grant.AudienceID
	metadata[authorityDelegateTypeKey] = principalTypeStringFromACL(grant.DelegateType)
	metadata[authorityDelegateIDKey] = grant.DelegateID
	if grant.Reason != "" {
		metadata[authorityReasonKey] = grant.Reason
	}

	return metadata
}

func preserveCreatorAuthorityMetadata(metadata map[string]interface{}) {
	if metadata == nil {
		return
	}

	keys := []string{
		"authority_mode",
		"subject_type",
		"subject_id",
		"root_subject_type",
		"root_subject_id",
		"authority_grant_id",
		"root_authority_grant_id",
		"parent_authority_grant_id",
	}
	for _, key := range keys {
		creatorKey := taskAuthorityCreatorPrefix + key
		if _, exists := metadata[creatorKey]; exists {
			continue
		}
		if value, ok := metadata[key]; ok {
			metadata[creatorKey] = value
		}
	}
}

func taskAuthorityInfoFromGrant(grant *acl.AuthorityGrant) tasks.TaskAuthorityInfo {
	if grant == nil {
		return tasks.TaskAuthorityInfo{}
	}

	rootGrantID := grant.RootGrantID
	if rootGrantID == "" {
		rootGrantID = grant.GrantID
	}
	rootSubjectType := principalTypeStringFromACL(grant.RootSubjectType)
	if rootSubjectType == "" {
		rootSubjectType = principalTypeStringFromACL(grant.SubjectType)
	}
	rootSubjectID := grant.RootSubjectID
	if rootSubjectID == "" {
		rootSubjectID = grant.SubjectID
	}

	info := tasks.TaskAuthorityInfo{
		Mode:                 audit.AuthorityModeOnBehalfOf,
		SubjectType:          principalTypeStringFromACL(grant.SubjectType),
		SubjectID:            grant.SubjectID,
		RootSubjectType:      rootSubjectType,
		RootSubjectID:        rootSubjectID,
		AuthorityGrantID:     grant.GrantID,
		RootAuthorityGrantID: rootGrantID,
		AudienceType:         grant.AudienceType,
		AudienceID:           grant.AudienceID,
		DelegateType:         principalTypeStringFromACL(grant.DelegateType),
		DelegateID:           grant.DelegateID,
	}
	if grant.ParentGrantID != nil {
		info.ParentAuthorityGrantID = *grant.ParentGrantID
	}
	return info
}

func applyTaskAuthorityGrantToTask(task *tasks.ExtendedTask, grant *acl.AuthorityGrant) {
	if task == nil || grant == nil {
		return
	}
	task.Authority = taskAuthorityInfoFromGrant(grant)
	task.Metadata = applyTaskAuthorityGrantToMetadata(cloneTaskMetadata(task.Metadata), grant)
}

func taskAuthorityRootGrantID(task *tasks.ExtendedTask) string {
	if task == nil {
		return ""
	}
	if task.Authority.RootAuthorityGrantID != "" {
		return task.Authority.RootAuthorityGrantID
	}
	if value := metadataString(task.Metadata, "root_authority_grant_id"); value != "" {
		return value
	}
	if task.Authority.AuthorityGrantID != "" {
		return task.Authority.AuthorityGrantID
	}
	return metadataString(task.Metadata, "authority_grant_id")
}

func taskAuthorityCurrentGrantID(task *tasks.ExtendedTask) string {
	if task == nil {
		return ""
	}
	if task.Authority.AuthorityGrantID != "" {
		return task.Authority.AuthorityGrantID
	}
	return metadataString(task.Metadata, "authority_grant_id")
}

func metadataString(metadata map[string]interface{}, key string) string {
	if metadata == nil {
		return ""
	}
	if value, ok := metadata[key].(string); ok {
		return value
	}
	return ""
}

func cloneTaskMetadata(metadata map[string]interface{}) map[string]interface{} {
	if metadata == nil {
		return make(map[string]interface{})
	}
	cloned := make(map[string]interface{}, len(metadata))
	for k, v := range metadata {
		cloned[k] = v
	}
	return cloned
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func cloneStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneResourceScope(values map[string][]string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(values))
	for k, patterns := range values {
		cloned[k] = cloneStringSlice(patterns)
	}
	return cloned
}

func taskGrantAnchorIdentity(taskID, workspace, taskType string) models.Identity {
	implementation := taskType
	if implementation == "" {
		implementation = "_task_authority"
	}
	return models.Identity{
		Type:           models.PrincipalTask,
		Workspace:      workspace,
		Implementation: implementation,
		ID:             taskID,
	}
}

func authorityAudienceForPrincipal(identity models.Identity, fallbackTaskID string) (string, string, error) {
	switch identity.Type {
	case models.PrincipalAgent:
		return acl.AuthorityAudienceAgent, identity.CanonicalPrincipalID(), nil
	case models.PrincipalService:
		return acl.AuthorityAudienceService, identity.CanonicalPrincipalID(), nil
	case models.PrincipalTask:
		audienceID := fallbackTaskID
		if audienceID == "" {
			audienceID = identity.CanonicalPrincipalID()
		}
		return acl.AuthorityAudienceTask, audienceID, nil
	default:
		return "", "", fmt.Errorf("unsupported authority delegate type %s", identity.Type)
	}
}

func taskAuthorityGrantUsableForDelegate(grant *acl.AuthorityGrant, delegate models.Identity, fallbackTaskID string) bool {
	if grant == nil {
		return false
	}
	if err := grant.ValidateActiveAt(time.Now()); err != nil {
		return false
	}
	if !authorityGrantPrincipalMatches(delegate, grant.DelegateType, grant.DelegateID) {
		return false
	}

	expectedAudienceType, expectedAudienceID, err := authorityAudienceForPrincipal(delegate, fallbackTaskID)
	if err != nil {
		return false
	}
	return grant.AudienceType == expectedAudienceType && grant.AudienceID == expectedAudienceID
}

func (s *GatewayServer) gatewayAuthorityIssuerIdentity() models.Identity {
	return models.Identity{
		Type:           models.PrincipalService,
		Implementation: "gateway",
		Specifier:      s.gatewayID,
	}
}

func (s *GatewayServer) createTaskAuthorityGrant(
	ctx context.Context,
	authority *acl.ResolvedAuthority,
	issuedBy models.Identity,
	delegate models.Identity,
	audienceType, audienceID, taskID, taskType, assignmentMode string,
	requireFurtherDelegation bool,
) (*acl.AuthorityGrant, error) {
	if s.acl == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	if authority == nil || authority.Grant == nil {
		return nil, fmt.Errorf("resolved authority grant is required")
	}

	rootSubjectType := authority.Grant.RootSubjectType
	rootSubjectID := authority.Grant.RootSubjectID
	if rootSubjectType == "" || rootSubjectID == "" {
		rootSubjectType = authority.Grant.SubjectType
		rootSubjectID = authority.Grant.SubjectID
	}
	rootSubject, err := identityFromAuthorityPrincipal(rootSubjectType, rootSubjectID)
	if err != nil {
		return nil, fmt.Errorf("invalid root subject: %w", err)
	}

	remainingHops := authority.Grant.RemainingHops - 1
	intermediaryReroot := false
	if remainingHops < 0 {
		// Hop budget exhausted on the parent grant. Allow trusted intermediary
		// services (sandbox-provider, etc. — anyone the operator has granted
		// capability/authority_intermediary to) to re-root the new grant from the
		// original subject. The principal chain is preserved via Subject (=
		// rootSubject by construction in OBO chains) and ParentGrantID; the
		// gateway resets RemainingHops so the new branch can derive at least
		// once more (e.g. sandbox-provider → sandbox-sidecar). Without the
		// permission, the original ErrAuthorityGrantDelegationDenied stands.
		if !s.checkAuthorityIntermediaryPermission(ctx, issuedBy) {
			return nil, acl.ErrAuthorityGrantDelegationDenied
		}
		intermediaryReroot = true
		remainingHops = 1
		logging.Logger.Info().
			Str("issued_by_type", string(issuedBy.Type)).
			Str("issued_by_id", issuedBy.CanonicalPrincipalID()).
			Str("parent_grant_id", authority.Grant.GrantID).
			Str("subject_type", string(authority.Subject.Type)).
			Str("subject_id", authority.Subject.CanonicalPrincipalID()).
			Str("audience_type", audienceType).
			Str("audience_id", audienceID).
			Str("task_id", taskID).
			Msg("authority intermediary re-root: parent grant exhausted, minting fresh hop budget under capability/authority_intermediary")
	}
	if requireFurtherDelegation && remainingHops < 1 {
		return nil, fmt.Errorf("task authority grant requires at least two remaining delegation hops")
	}

	metadata := map[string]interface{}{
		taskAuthorityTaskIDKey:   taskID,
		taskAuthorityTaskTypeKey: taskType,
		taskAuthorityModeKey:     assignmentMode,
	}
	if intermediaryReroot {
		metadata[taskAuthorityIntermediaryKey] = true
	}
	parentGrantID := authority.Grant.GrantID
	return s.acl.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:                  authority.Subject,
		Delegate:                 delegate,
		IssuedBy:                 issuedBy,
		RootSubject:              &rootSubject,
		ParentGrantID:            &parentGrantID,
		MayDelegate:              remainingHops > 0,
		RemainingHops:            remainingHops,
		WorkspaceScope:           cloneStringSlice(authority.Grant.WorkspaceScope),
		ResourceScope:            cloneResourceScope(authority.Grant.ResourceScope),
		OperationScope:           cloneStringSlice(authority.Grant.OperationScope),
		MaxAccessLevel:           authority.Grant.MaxAccessLevel,
		AudienceType:             audienceType,
		AudienceID:               audienceID,
		ValidWhileAudienceActive: authority.Grant.ValidWhileAudienceActive,
		ExpiresAt:                authority.Grant.ExpiresAt,
		RenewableUntil:           authority.Grant.RenewableUntil,
		Reason:                   fmt.Sprintf("task:%s:%s", assignmentMode, taskID),
		Metadata:                 metadata,
	})
}

// checkAuthorityIntermediaryPermission returns true when the given principal
// is authorized to act as an authority-grant intermediary — i.e. allowed to
// mint task grants that re-root from the original subject when their own
// parent grant has run out of delegation hops. The permission is granted
// blanket (per-principal) at AccessManage on the capability/authority_intermediary
// resource; layering audience-scoped variants is a future refinement.
//
// On any error or denied decision the function returns false. The caller is
// responsible for the audit trail when the re-root actually happens.
func (s *GatewayServer) checkAuthorityIntermediaryPermission(ctx context.Context, actor models.Identity) bool {
	if s.acl == nil {
		return false
	}
	decision, err := s.acl.CheckAccess(
		ctx,
		actor,
		acl.ResourceTypeCapability,
		acl.PermissionAuthorityIntermediary,
		audit.OpAuthorityIntermediary,
		actor.Workspace,
		uuid.Nil,
		acl.AccessManage,
	)
	if err != nil || decision == nil || decision.Denied() {
		return false
	}
	return true
}

// mintTaskGrantForSender creates a root-level task authority grant for a
// gateway-initiated task whose subject is a user-sender (no parent OBO grant).
//
// This is the Phase 2 OBO propagation entry point for the `triggerOrchestration`
// path: when a user sends a message to an offline pool/targeted agent, the
// gateway autonomously creates startup + delivery tasks on that user's behalf.
// establishTaskAuthorityGrant requires a non-nil ResolvedAuthority.Grant and
// therefore cannot be used when there is no upstream grant to derive from;
// this helper fills that gap by minting a fresh root grant whose Subject is
// the user-sender.
//
// sender:     the user/principal whose message triggered the task
// taskID:     the task to attach the grant to
// workspace:  the task's workspace (always included in the grant's WorkspaceScope)
// delegate:   the principal authorized to use the grant — typically the target
//
//	agent (for message_delivery) or a task-anchor identity (for
//	agent_startup, consumed by the orchestrator). Pass a zero-value
//	Identity to default to the task-anchor form.
//
// reason:     human-readable audit string
//
// The grant's WorkspaceScope is composed of:
//   - “workspace“ (the task's workspace — e.g. "_apps" for per-user cowork
//     startup tasks, where the agent itself lives), PLUS
//   - “sender.Workspace“ when non-empty and distinct (the workspace the
//     user was acting in when they triggered the task — e.g. "default" for
//     chat messages from the user's active app workspace).
//
// Including both lets spawned agents create dependent resources (sandbox
// leases, tenant-scoped tasks) in the USER'S workspace on the user's
// behalf, matching what the user themselves could do. Without the sender's
// workspace in scope, every such op is ACL-denied because the narrow task
// workspace doesn't overlap with the user's active workspace.
//
// On success the grant is persisted AND the task row's authority columns +
// metadata are updated via taskStore.UpdateTaskAuthority in one call. On
// UpdateTaskAuthority failure the freshly-created grant is revoked to avoid
// orphans.
func (s *GatewayServer) mintTaskGrantForSender(
	ctx context.Context,
	sender models.Identity,
	taskID, workspace string,
	delegate models.Identity,
	reason string,
	extraWorkspaces ...string,
) (*acl.AuthorityGrant, error) {
	if s.acl == nil {
		return nil, fmt.Errorf("ACL service not available")
	}
	if s.taskStore == nil {
		return nil, fmt.Errorf("task store not available")
	}
	if sender.Type == "" || sender.ID == "" {
		return nil, fmt.Errorf("sender identity required")
	}
	if taskID == "" || workspace == "" {
		return nil, fmt.Errorf("task_id and workspace required")
	}

	// Default delegate to a task anchor if zero-value so grant audience is
	// always resolvable (agent_startup tasks have no preassigned delegate).
	// Check only Type: structured principals (e.g. Agent) carry identity in
	// Workspace/Implementation/Specifier, not in the bare ID field.
	if delegate.Type == "" {
		delegate = taskGrantAnchorIdentity(taskID, workspace, "")
	}

	audienceType, audienceID, err := authorityAudienceForPrincipal(delegate, taskID)
	if err != nil {
		return nil, fmt.Errorf("resolve audience: %w", err)
	}

	// Compose the grant's WorkspaceScope from the task's workspace plus the
	// sender's active workspace (when present and distinct). See function
	// docstring for rationale — this allows spawned agents to create
	// dependent resources in the user's workspace on their behalf.
	workspaceScope := []string{workspace}
	for _, w := range []string{sender.Workspace} {
		if w != "" && w != workspace && !contains(workspaceScope, w) {
			workspaceScope = append(workspaceScope, w)
		}
	}
	for _, w := range extraWorkspaces {
		if w != "" && !contains(workspaceScope, w) {
			workspaceScope = append(workspaceScope, w)
		}
	}
	logging.Logger.Debug().
		Str("task_id", taskID).
		Str("task_workspace", workspace).
		Str("sender_type", string(sender.Type)).
		Str("sender_id", sender.ID).
		Str("sender_workspace", sender.Workspace).
		Strs("extra_workspaces", extraWorkspaces).
		Strs("workspace_scope", workspaceScope).
		Msg("mintTaskGrantForSender: composed workspace scope")

	now := time.Now()
	rootSubject := sender
	req := acl.CreateAuthorityGrantRequest{
		Subject:                  sender,
		Delegate:                 delegate,
		IssuedBy:                 s.gatewayAuthorityIssuerIdentity(),
		RootSubject:              &rootSubject,
		ParentGrantID:            nil,
		MayDelegate:              true,
		RemainingHops:            1, // one hop to permit per-agent grant derivation on delivery
		WorkspaceScope:           workspaceScope,
		ResourceScope:            map[string][]string{},
		OperationScope:           []string{},
		MaxAccessLevel:           acl.AccessReadWrite,
		AudienceType:             audienceType,
		AudienceID:               audienceID,
		ValidWhileAudienceActive: false,
		ExpiresAt:                now.Add(24 * time.Hour),
		RenewableUntil:           now.Add(7 * 24 * time.Hour),
		Reason:                   reason,
		Metadata: map[string]interface{}{
			taskAuthorityTaskIDKey: taskID,
			taskAuthorityModeKey:   "unilateral_task_grant",
		},
	}

	grant, err := s.acl.CreateAuthorityGrant(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create root authority grant: %w", err)
	}

	authInfo := taskAuthorityInfoFromGrant(grant)
	// Start from the task's EXISTING metadata so any fields placed by the
	// caller (e.g. trigger_timestamp_ms via createOrchestratedStartupTask)
	// survive this update. UpdateTaskAuthority writes the metadata column
	// unconditionally (store.go::UpdateTaskAuthority: "SET metadata = $1"),
	// so passing an empty map here silently wipes prior fields — which is
	// exactly the failure mode that was preventing Fix B / first-message
	// replay from working end-to-end.
	baseMeta := map[string]interface{}{}
	if existingTask, getErr := s.taskStore.GetTask(ctx, taskID); getErr == nil && existingTask != nil {
		for k, v := range existingTask.Metadata {
			baseMeta[k] = v
		}
	} else if getErr != nil {
		logging.Logger.Debug().Err(getErr).Str("task_id", taskID).Msg("mintTaskGrantForSender: could not load existing task metadata (non-fatal)")
	}
	mergedMeta := applyTaskAuthorityGrantToMetadata(baseMeta, grant)

	// taskAuthorityInfoFromGrant / applyTaskAuthorityGrantToMetadata run grant
	// principal-type fields through principalTypeStringFromACL, which maps the
	// ACL canonical lowercase form ("user", "agent") back to models.PrincipalType
	// ("User", "Agent"). The task row columns and buildTaskContext/applyAuthority
	// ToTaskContext rely on the lowercase form (matching applySubjectIdentityTo
	// Authority in the orchestration package, and taskAuthorityGrantUsableFor
	// Delegate which compares against acl.PrincipalTypeForModel output).
	// Overwrite subject-type and delegate-type fields here so this helper's
	// writes remain consistent with the lowercase convention.
	// See applyAuthorityToTaskContext which checks auth.SubjectType == "user".
	if sender.Type == models.PrincipalUser {
		authInfo.SubjectType = acl.PrincipalTypeUser
		authInfo.RootSubjectType = acl.PrincipalTypeUser
		mergedMeta["subject_type"] = acl.PrincipalTypeUser
		mergedMeta["root_subject_type"] = acl.PrincipalTypeUser
	}
	// Coerce DelegateType to ACL-canonical lowercase so the stored task row
	// matches what authorityGrantPrincipalMatches (via loadCallerMessageAuthority)
	// expects: acl.PrincipalTypeForModel(actor.Type) == grant.DelegateType.
	authInfo.DelegateType = acl.PrincipalTypeForModel(delegate.Type)

	if err := s.taskStore.UpdateTaskAuthority(ctx, taskID, authInfo, mergedMeta); err != nil {
		// Revoke the grant we just created to avoid orphans.
		_ = s.acl.RevokeAuthorityGrant(ctx, grant.GrantID)
		return nil, fmt.Errorf("update task authority: %w", err)
	}

	return grant, nil
}

func (s *GatewayServer) establishTaskAuthorityGrant(
	ctx context.Context,
	taskID string,
	taskReq *orchestration.CreateTaskRequest,
	response *orchestration.CreateTaskResponse,
	authority *acl.ResolvedAuthority,
) (map[string]interface{}, error) {
	if authority == nil || authority.Grant == nil {
		return cloneTaskMetadata(taskReq.Metadata), nil
	}
	if s.acl == nil || s.taskStore == nil {
		return nil, fmt.Errorf("task authority grants require ACL and task store services")
	}

	metadata := cloneTaskMetadata(taskReq.Metadata)

	var (
		delegate                models.Identity
		audienceType            string
		audienceID              string
		requireFurtherDelegates bool
		err                     error
	)

	switch taskReq.AssignmentMode {
	case "pool":
		delegate = taskGrantAnchorIdentity(taskID, taskReq.Workspace, taskReq.TaskType)
		audienceType = acl.AuthorityAudienceTask
		audienceID = taskID
		requireFurtherDelegates = true
	case "targeted":
		delegate, err = models.ParseIdentity(taskReq.TargetAgentID)
		if err != nil {
			return nil, fmt.Errorf("invalid task authority delegate: %w", err)
		}
		audienceType, audienceID, err = authorityAudienceForPrincipal(delegate, taskID)
		if err != nil {
			return nil, err
		}
	case "self_assign", "":
		// Prefer an explicit target_agent_id if the caller set one. The
		// AssignmentMode axis is about WHO CLAIMS the task (creator vs.
		// pool vs. directed), but for grant-delegate purposes the natural
		// owner is the targeted recipient when one was named. Without this
		// fall-through, callers who set target_agent_id but leave the mode
		// at its default (self_assign) get a grant whose delegate is the
		// creator — which fails the actor-equals-delegate check on the
		// targeted agent's side. Keeps existing target_agent_id-aware
		// callers working without a coordinated AssignmentMode flip.
		if taskReq.TargetAgentID != "" {
			delegate, err = models.ParseIdentity(taskReq.TargetAgentID)
			if err != nil {
				return nil, fmt.Errorf("invalid task authority target identity: %w", err)
			}
		} else {
			delegate = taskReq.CreatorIdentity
		}
		audienceType, audienceID, err = authorityAudienceForPrincipal(delegate, taskID)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported assignment mode %q for task authority grant", taskReq.AssignmentMode)
	}

	grant, err := s.createTaskAuthorityGrant(
		ctx,
		authority,
		taskReq.CreatorIdentity,
		delegate,
		audienceType,
		audienceID,
		taskID,
		taskReq.TaskType,
		taskReq.AssignmentMode,
		requireFurtherDelegates,
	)
	if err != nil {
		return nil, err
	}

	updated := applyTaskAuthorityGrantToMetadata(metadata, grant)
	authorityInfo := taskAuthorityInfoFromGrant(grant)
	if response != nil && response.AssignedTo != "" {
		updated["assigned_to"] = response.AssignedTo
	}
	if err := s.taskStore.UpdateTaskAuthority(ctx, taskID, authorityInfo, updated); err != nil {
		_ = s.acl.RevokeAuthorityGrant(ctx, grant.GrantID)
		return nil, fmt.Errorf("failed to persist task authority grant state: %w", err)
	}

	return updated, nil
}

func (s *GatewayServer) prepareTaskAuthorityForDelivery(ctx context.Context, task *tasks.ExtendedTask, assignee models.Identity) (string, string, error) {
	if task == nil {
		return "", "", nil
	}
	if s.acl == nil || s.taskStore == nil {
		return "", "", nil
	}

	rootGrantID := taskAuthorityRootGrantID(task)
	if rootGrantID == "" {
		return "", "", nil
	}
	currentGrantID := taskAuthorityCurrentGrantID(task)
	if currentGrantID != "" {
		currentGrant, err := s.acl.GetAuthorityGrant(ctx, currentGrantID)
		if err == nil && taskAuthorityGrantUsableForDelegate(currentGrant, assignee, task.TaskID) {
			return rootGrantID, currentGrantID, nil
		}
	}

	rootGrant, err := s.acl.GetAuthorityGrant(ctx, rootGrantID)
	if err != nil {
		return rootGrantID, currentGrantID, fmt.Errorf("failed to load task authority root grant: %w", err)
	}
	if taskAuthorityGrantUsableForDelegate(rootGrant, assignee, task.TaskID) {
		applyTaskAuthorityGrantToTask(task, rootGrant)
		if err := s.taskStore.UpdateTaskAuthority(ctx, task.TaskID, task.Authority, task.Metadata); err != nil {
			return rootGrantID, currentGrantID, err
		}
		return rootGrantID, rootGrant.GrantID, nil
	}

	if currentGrantID != "" && currentGrantID != rootGrantID {
		if err := s.acl.RevokeAuthorityGrant(ctx, currentGrantID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", task.TaskID).Str("grant_id", currentGrantID).Msg("failed to revoke previous task delivery grant")
		}
	}

	subject, err := identityFromAuthorityPrincipal(rootGrant.SubjectType, rootGrant.SubjectID)
	if err != nil {
		return rootGrantID, currentGrantID, fmt.Errorf("invalid task authority subject: %w", err)
	}
	rootSubjectType := rootGrant.RootSubjectType
	rootSubjectID := rootGrant.RootSubjectID
	if rootSubjectType == "" || rootSubjectID == "" {
		rootSubjectType = rootGrant.SubjectType
		rootSubjectID = rootGrant.SubjectID
	}
	rootSubject, err := identityFromAuthorityPrincipal(rootSubjectType, rootSubjectID)
	if err != nil {
		return rootGrantID, currentGrantID, fmt.Errorf("invalid task authority root subject: %w", err)
	}
	audienceType, audienceID, err := authorityAudienceForPrincipal(assignee, task.TaskID)
	if err != nil {
		return rootGrantID, currentGrantID, err
	}

	parentGrantID := rootGrant.GrantID
	remainingHops := rootGrant.RemainingHops - 1
	if remainingHops < 0 {
		return rootGrantID, currentGrantID, acl.ErrAuthorityGrantDelegationDenied
	}

	childGrant, err := s.acl.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
		Subject:                  subject,
		Delegate:                 assignee,
		IssuedBy:                 s.gatewayAuthorityIssuerIdentity(),
		RootSubject:              &rootSubject,
		ParentGrantID:            &parentGrantID,
		MayDelegate:              remainingHops > 0,
		RemainingHops:            remainingHops,
		WorkspaceScope:           cloneStringSlice(rootGrant.WorkspaceScope),
		ResourceScope:            cloneResourceScope(rootGrant.ResourceScope),
		OperationScope:           cloneStringSlice(rootGrant.OperationScope),
		MaxAccessLevel:           rootGrant.MaxAccessLevel,
		AudienceType:             audienceType,
		AudienceID:               audienceID,
		ValidWhileAudienceActive: rootGrant.ValidWhileAudienceActive,
		ExpiresAt:                rootGrant.ExpiresAt,
		RenewableUntil:           rootGrant.RenewableUntil,
		Reason:                   fmt.Sprintf("task_delivery:%s:%s", task.TaskID, assignee.CanonicalPrincipalID()),
		Metadata: map[string]interface{}{
			taskAuthorityTaskIDKey:   task.TaskID,
			taskAuthorityTaskTypeKey: task.TaskType,
			taskAuthorityModeKey:     string(task.AssignmentMode),
		},
	})
	if err != nil {
		return rootGrantID, currentGrantID, fmt.Errorf("failed to derive task delivery authority grant: %w", err)
	}

	applyTaskAuthorityGrantToTask(task, childGrant)
	if err := s.taskStore.UpdateTaskAuthority(ctx, task.TaskID, task.Authority, task.Metadata); err != nil {
		_ = s.acl.RevokeAuthorityGrant(ctx, childGrant.GrantID)
		return rootGrantID, currentGrantID, fmt.Errorf("failed to persist derived task delivery grant: %w", err)
	}

	return rootGrantID, childGrant.GrantID, nil
}

func (s *GatewayServer) resetTaskAuthorityGrantToRoot(ctx context.Context, task *tasks.ExtendedTask, rootGrantID, currentGrantID string) {
	if task == nil || s.acl == nil || s.taskStore == nil {
		return
	}
	if currentGrantID != "" && currentGrantID != rootGrantID {
		if err := s.acl.RevokeAuthorityGrant(ctx, currentGrantID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", task.TaskID).Str("grant_id", currentGrantID).Msg("failed to revoke derived delivery grant during reset")
		}
	}
	if rootGrantID == "" {
		return
	}
	rootGrant, err := s.acl.GetAuthorityGrant(ctx, rootGrantID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", task.TaskID).Str("grant_id", rootGrantID).Msg("failed to reload root task authority grant during reset")
		return
	}
	applyTaskAuthorityGrantToTask(task, rootGrant)
	if err := s.taskStore.UpdateTaskAuthority(ctx, task.TaskID, task.Authority, task.Metadata); err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", task.TaskID).Msg("failed to reset task authority metadata to root grant")
	}
}

func taskGrantLeaseWindow(grant *acl.AuthorityGrant) time.Duration {
	if grant == nil {
		return 0
	}
	anchor := grant.CreatedAt
	if grant.RenewedAt != nil {
		anchor = *grant.RenewedAt
	}
	lease := grant.ExpiresAt.Sub(anchor)
	if lease < taskAuthorityMinLease {
		return taskAuthorityMinLease
	}
	return lease
}

func taskGrantRenewalTarget(grant *acl.AuthorityGrant, now time.Time) (time.Time, bool) {
	if grant == nil || grant.Revoked {
		return time.Time{}, false
	}
	if !grant.ExpiresAt.After(now) || !grant.RenewableUntil.After(grant.ExpiresAt) {
		return time.Time{}, false
	}

	lease := taskGrantLeaseWindow(grant)
	lead := taskAuthorityRenewalLead
	if halfLease := lease / 2; halfLease > 0 && halfLease < lead {
		lead = halfLease
	}
	if lead <= 0 {
		lead = taskAuthorityMinLease
	}
	if grant.ExpiresAt.Sub(now) > lead {
		return time.Time{}, false
	}

	target := now.Add(lease)
	if target.After(grant.RenewableUntil) {
		target = grant.RenewableUntil
	}
	if !target.After(grant.ExpiresAt) {
		return time.Time{}, false
	}
	return target, true
}

// loadCallerTaskAuthority returns a ResolvedAuthority derived from the caller's
// currently-delivered task grant, or (nil, nil) when the caller is not acting
// under a task-bound grant. Used to auto-derive nested task authority when an
// agent calls CreateTask without supplying an explicit AuthorizationContext.
// The returned grant must still support further delegation (MayDelegate +
// RemainingHops > 0); otherwise the caller proceeds as direct.
func (s *GatewayServer) loadCallerTaskAuthority(ctx context.Context, client *ClientSession, actor models.Identity) (*acl.ResolvedAuthority, error) {
	if client == nil || client.AssociatedTaskID == "" || s.acl == nil || s.taskStore == nil {
		return nil, nil
	}

	task, err := s.taskStore.GetTask(ctx, client.AssociatedTaskID)
	if err != nil || task == nil {
		return nil, nil
	}

	grantID := taskAuthorityCurrentGrantID(task)
	if grantID == "" {
		return nil, nil
	}

	grant, err := s.acl.GetAuthorityGrant(ctx, grantID)
	if err != nil {
		return nil, nil
	}
	if !taskAuthorityGrantUsableForDelegate(grant, actor, task.TaskID) {
		return nil, nil
	}
	if !grant.CanDelegate() {
		return nil, nil
	}

	subject, err := identityFromAuthorityPrincipal(grant.SubjectType, grant.SubjectID)
	if err != nil {
		return nil, nil
	}

	return &acl.ResolvedAuthority{
		Actor:   actor,
		Subject: subject,
		Grant:   grant,
	}, nil
}

// loadCallerMessageAuthority returns a ResolvedAuthority for a caller (agent or
// task principal) that is sending a message under a session-bound task grant,
// without having attached an explicit AuthorizationContext. Differs from
// loadCallerTaskAuthority in two ways:
//   - used at message-send time, not nested CreateTask time;
//   - does NOT require the grant to CanDelegate, because the caller is the grant's
//     delegate acting within its scope (using the grant, not deriving from it).
//
// Returns (nil, nil) when the caller has no usable session-bound task grant.
func (s *GatewayServer) loadCallerMessageAuthority(ctx context.Context, client *ClientSession, actor models.Identity) (*acl.ResolvedAuthority, error) {
	if client == nil || client.AssociatedTaskID == "" || s.acl == nil || s.taskStore == nil {
		logging.Logger.Debug().
			Bool("has_client", client != nil).
			Str("task_id", func() string {
				if client != nil {
					return client.AssociatedTaskID
				}
				return ""
			}()).
			Bool("has_acl", s.acl != nil).
			Bool("has_task_store", s.taskStore != nil).
			Msg("load_caller_message_authority: preconditions not met")
		return nil, nil
	}

	task, err := s.taskStore.GetTask(ctx, client.AssociatedTaskID)
	if err != nil || task == nil {
		logging.Logger.Debug().
			Str("task_id", client.AssociatedTaskID).
			Err(err).
			Msg("load_caller_message_authority: task not found")
		return nil, nil
	}

	grantID := taskAuthorityCurrentGrantID(task)
	if grantID == "" {
		logging.Logger.Debug().
			Str("task_id", client.AssociatedTaskID).
			Msg("load_caller_message_authority: task has no grant")
		return nil, nil
	}

	grant, err := s.acl.GetAuthorityGrant(ctx, grantID)
	if err != nil {
		logging.Logger.Debug().
			Str("task_id", client.AssociatedTaskID).
			Str("grant_id", grantID).
			Err(err).
			Msg("load_caller_message_authority: grant fetch failed")
		return nil, nil
	}
	if !taskAuthorityGrantUsableForDelegate(grant, actor, task.TaskID) {
		logging.Logger.Debug().
			Str("task_id", client.AssociatedTaskID).
			Str("grant_id", grantID).
			Str("actor", actor.CanonicalPrincipalID()).
			Str("actor_type", string(actor.Type)).
			Str("grant_delegate_type", grant.DelegateType).
			Str("grant_delegate_id", grant.DelegateID).
			Msg("load_caller_message_authority: grant not usable by delegate")
		return nil, nil
	}

	subject, err := identityFromAuthorityPrincipal(grant.SubjectType, grant.SubjectID)
	if err != nil {
		logging.Logger.Debug().
			Str("task_id", client.AssociatedTaskID).
			Str("grant_id", grantID).
			Err(err).
			Msg("load_caller_message_authority: subject identity parse failed")
		return nil, nil
	}

	logging.Logger.Debug().
		Str("task_id", client.AssociatedTaskID).
		Str("grant_id", grantID).
		Str("subject_type", string(subject.Type)).
		Str("subject_id", subject.ID).
		Msg("load_caller_message_authority: resolved grant")

	return &acl.ResolvedAuthority{
		Actor:   actor,
		Subject: subject,
		Grant:   grant,
	}, nil
}

func (s *GatewayServer) maybeRenewTaskAuthorityGrants(ctx context.Context, taskID string) error {
	if taskID == "" || s.acl == nil || s.taskStore == nil {
		return nil
	}

	task, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	rootGrantID := taskAuthorityRootGrantID(task)
	currentGrantID := taskAuthorityCurrentGrantID(task)
	if rootGrantID == "" && currentGrantID == "" {
		return nil
	}

	now := time.Now()
	orderedGrantIDs := []string{}
	if rootGrantID != "" {
		orderedGrantIDs = append(orderedGrantIDs, rootGrantID)
	}
	if currentGrantID != "" && currentGrantID != rootGrantID {
		orderedGrantIDs = append(orderedGrantIDs, currentGrantID)
	}

	for _, grantID := range orderedGrantIDs {
		grant, err := s.acl.GetAuthorityGrant(ctx, grantID)
		if err != nil {
			return fmt.Errorf("load authority grant %s: %w", grantID, err)
		}
		target, ok := taskGrantRenewalTarget(grant, now)
		if !ok {
			continue
		}
		if _, err := s.acl.RenewAuthorityGrant(ctx, grant.GrantID, target); err != nil {
			return fmt.Errorf("renew authority grant %s: %w", grant.GrantID, err)
		}
	}

	return nil
}
