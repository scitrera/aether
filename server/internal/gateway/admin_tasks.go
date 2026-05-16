package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// =============================================================================
// Tasks
// =============================================================================

func (p *GatewayStateProvider) GetTasks(ctx context.Context, filter *admin.TaskFilter) ([]*admin.TaskInfo, error) {
	if p.taskStore == nil {
		return nil, fmt.Errorf("task store not available")
	}

	taskFilter := &tasks.TaskFilter{
		Limit:  100,
		Offset: 0,
	}
	if filter != nil {
		if filter.Limit > 0 {
			taskFilter.Limit = filter.Limit
		}
		if filter.Offset > 0 {
			taskFilter.Offset = filter.Offset
		}
		if filter.Status != "" {
			status := tasks.TaskStatus(filter.Status)
			taskFilter.Status = &status
		}
		taskFilter.Workspace = filter.Workspace
		taskFilter.TaskType = filter.TaskType
		taskFilter.TaskClass = filter.TaskClass
		taskFilter.ExcludeTaskClasses = filter.ExcludeTaskClasses
		taskFilter.SubjectType = filter.SubjectType
		taskFilter.SubjectID = filter.SubjectID
		taskFilter.AuthorityMode = filter.AuthorityMode
		taskFilter.AuthorityGrantID = filter.AuthorityGrantID
		taskFilter.RootAuthorityGrantID = filter.RootAuthorityGrantID
		taskFilter.ParentTaskID = filter.ParentTaskID
	}

	records, err := p.taskStore.ListTasks(ctx, taskFilter)
	if err != nil {
		return nil, err
	}

	var taskInfos []*admin.TaskInfo
	for _, r := range records {
		taskInfos = append(taskInfos, taskToAdminInfo(r))
	}

	return taskInfos, nil
}

func (p *GatewayStateProvider) GetTaskByID(ctx context.Context, taskID string) (*admin.TaskInfo, error) {
	if p.taskStore == nil {
		return nil, fmt.Errorf("task store not available")
	}

	r, err := p.taskStore.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}

	return taskToAdminInfo(r), nil
}

func taskToAdminInfo(r *tasks.Task) *admin.TaskInfo {
	info := &admin.TaskInfo{
		TaskID:                 r.TaskID,
		TaskType:               r.TaskType,
		TaskClass:              r.TaskClass,
		DisconnectedAt:         r.DisconnectedAt,
		GraceWindowMs:          r.GraceWindowMs,
		Status:                 string(r.Status),
		Workspace:              r.Workspace,
		TargetTopic:            r.TargetTopic,
		AssignedTo:             r.AssignedTo,
		CreatedAt:              r.CreatedAt,
		Attempt:                r.RetryCount,
		MaxAttempts:            r.MaxRetries,
		Metadata:               r.Metadata,
		AuthorityMode:          r.Authority.Mode,
		SubjectType:            r.Authority.SubjectType,
		SubjectID:              r.Authority.SubjectID,
		RootSubjectType:        r.Authority.RootSubjectType,
		RootSubjectID:          r.Authority.RootSubjectID,
		AuthorityGrantID:       r.Authority.AuthorityGrantID,
		RootAuthorityGrantID:   r.Authority.RootAuthorityGrantID,
		ParentAuthorityGrantID: r.Authority.ParentAuthorityGrantID,
		CreatorActorID:         r.ParentAgentID,
		ParentTaskID:           r.ParentTaskID,
	}
	if r.StartedAt != nil {
		info.StartedAt = r.StartedAt
	}
	if r.CompletedAt != nil {
		info.CompletedAt = r.CompletedAt
	}
	if r.ErrorMessage != "" {
		info.Error = r.ErrorMessage
	}
	return info
}

func (p *GatewayStateProvider) RetryTask(ctx context.Context, taskID string) error {
	if p.taskStore == nil {
		return fmt.Errorf("task store not available")
	}
	return p.taskStore.RetryTask(ctx, taskID)
}

func (p *GatewayStateProvider) CancelTask(ctx context.Context, taskID string) error {
	if p.gateway != nil && p.gateway.orchestration != nil && p.gateway.orchestration.TaskService != nil {
		return p.gateway.orchestration.TaskService.CancelTask(ctx, taskID)
	}
	if p.taskStore == nil {
		return fmt.Errorf("task store not available")
	}
	return p.taskStore.CancelTask(ctx, taskID)
}

// =============================================================================
// Agents & Orchestration
// =============================================================================

func (p *GatewayStateProvider) GetAgentRegistrations(ctx context.Context) ([]*admin.AgentRegistrationInfo, error) {
	if p.agentRegistry == nil {
		return []*admin.AgentRegistrationInfo{}, nil
	}

	regs, err := p.agentRegistry.List(ctx, "")
	if err != nil {
		return nil, err
	}

	var infos []*admin.AgentRegistrationInfo
	for _, r := range regs {
		infos = append(infos, registryToAdminAgent(r))
	}

	return infos, nil
}

// registryToAdminAgent lifts a registry.AgentRegistration into the
// admin.AgentRegistrationInfo type that the admin/gateway boundary trades in.
// Handles the launch_params copy + profile extraction + Phase 5 field
// projection in a single place so the GET/LIST paths stay in sync.
func registryToAdminAgent(r *registry.AgentRegistration) *admin.AgentRegistrationInfo {
	params := make(map[string]interface{})
	for k, v := range r.LaunchParams {
		params[k] = v
	}

	profile := ""
	if p, ok := r.LaunchParams["profile"].(string); ok {
		profile = p
	}

	info := &admin.AgentRegistrationInfo{
		Implementation:      r.Implementation,
		OrchestratorProfile: profile,
		Description:         r.Description,
		LaunchParams:        params,
		RegisteredAt:        r.CreatedAt,
		UpdatedAt:           r.UpdatedAt,
		Capabilities:        r.Capabilities,
		Extensions:          r.Extensions,
	}
	if len(r.ResourceSchema) > 0 {
		info.ResourceSchema = make([]admin.AgentResourceSchemaEntry, len(r.ResourceSchema))
		for i, e := range r.ResourceSchema {
			info.ResourceSchema[i] = admin.AgentResourceSchemaEntry{
				ResourceTypePrefix: e.ResourceTypePrefix,
				PermissionVerbs:    e.PermissionVerbs,
				ResourceIDSchema:   e.ResourceIDSchema,
			}
		}
	}
	return info
}

// adminToRegistryAgent is the inverse of registryToAdminAgent for the
// REGISTER / UPDATE write path.
func adminToRegistryAgent(implementation string, a *admin.AgentRegistrationInfo) *registry.AgentRegistration {
	reg := &registry.AgentRegistration{
		Implementation: implementation,
		Description:    a.Description,
		LaunchParams:   a.LaunchParams,
		Capabilities:   a.Capabilities,
		Extensions:     a.Extensions,
	}
	if len(a.ResourceSchema) > 0 {
		reg.ResourceSchema = make([]registry.AgentResourceSchemaEntry, len(a.ResourceSchema))
		for i, e := range a.ResourceSchema {
			reg.ResourceSchema[i] = registry.AgentResourceSchemaEntry{
				ResourceTypePrefix: e.ResourceTypePrefix,
				PermissionVerbs:    e.PermissionVerbs,
				ResourceIDSchema:   e.ResourceIDSchema,
			}
		}
	}
	return reg
}

func (p *GatewayStateProvider) GetAgentByImplementation(ctx context.Context, implementation string) (*admin.AgentRegistrationInfo, error) {
	if p.agentRegistry == nil {
		return nil, fmt.Errorf("agent registry not available")
	}

	r, err := p.agentRegistry.Get(ctx, implementation)
	if err != nil {
		return nil, err
	}

	return registryToAdminAgent(r), nil
}

func (p *GatewayStateProvider) RegisterAgent(ctx context.Context, agent *admin.AgentRegistrationInfo) error {
	if p.agentRegistry == nil {
		return fmt.Errorf("agent registry not available")
	}

	reg := adminToRegistryAgent(agent.Implementation, agent)
	if err := p.agentRegistry.Register(ctx, reg); err != nil {
		return err
	}
	// Phase 5 Stage B: refresh the in-memory prefix index after a
	// successful write so subsequent CheckAccess calls can attribute
	// resource access to this implementation. Safe to call with a nil
	// prefixIndex (Set is a no-op).
	if p.prefixIndex != nil {
		p.prefixIndex.Set(reg.Implementation, reg.ResourceSchema)
	}
	return nil
}

func (p *GatewayStateProvider) UpdateAgent(ctx context.Context, implementation string, agent *admin.AgentRegistrationInfo) error {
	if p.agentRegistry == nil {
		return fmt.Errorf("agent registry not available")
	}

	// For update, we use the same Register method which does upsert
	reg := adminToRegistryAgent(implementation, agent)
	if err := p.agentRegistry.Register(ctx, reg); err != nil {
		return err
	}
	// Phase 5 Stage B: refresh index. Set replaces any prefixes
	// previously owned by `implementation`, so dropped prefixes are
	// released for future claims.
	if p.prefixIndex != nil {
		p.prefixIndex.Set(reg.Implementation, reg.ResourceSchema)
	}
	return nil
}

func (p *GatewayStateProvider) DeleteAgent(ctx context.Context, implementation string) error {
	if p.agentRegistry == nil {
		return fmt.Errorf("agent registry not available")
	}

	if err := p.agentRegistry.Delete(ctx, implementation); err != nil {
		return err
	}
	// Phase 5 Stage B: drop any prefix entries owned by this implementation
	// so the prefixes become claimable again.
	if p.prefixIndex != nil {
		p.prefixIndex.Delete(implementation)
	}
	return nil
}

func (p *GatewayStateProvider) GetOrchestratorProfiles(ctx context.Context) ([]*admin.OrchestratorProfileInfo, error) {
	if p.profileMgr == nil {
		return []*admin.OrchestratorProfileInfo{}, nil
	}

	profiles, err := p.profileMgr.ListAllProfiles(ctx)
	if err != nil {
		return nil, err
	}

	// Group profiles by orchestrator ID
	orchProfiles := make(map[string][]string)
	orchTimes := make(map[string]time.Time)
	for _, profile := range profiles {
		orchProfiles[profile.OrchestratorID] = append(orchProfiles[profile.OrchestratorID], profile.ProfileName)
		// Track latest heartbeat as connected time
		if t, ok := orchTimes[profile.OrchestratorID]; !ok || profile.LastHeartbeat.After(t) {
			orchTimes[profile.OrchestratorID] = profile.LastHeartbeat
		}
	}

	var infos []*admin.OrchestratorProfileInfo
	for orchID, profs := range orchProfiles {
		infos = append(infos, &admin.OrchestratorProfileInfo{
			OrchestratorID: orchID,
			Profiles:       profs,
			ConnectedAt:    orchTimes[orchID],
		})
	}

	return infos, nil
}

func (p *GatewayStateProvider) LaunchAgent(ctx context.Context, req *admin.LaunchAgentRequest) (*admin.LaunchAgentResponse, error) {
	if p.agentRegistry == nil {
		return nil, fmt.Errorf("agent registry not available")
	}

	// Get the agent registration to find its profile and launch params
	reg, err := p.agentRegistry.Get(ctx, req.Implementation)
	if err != nil {
		return nil, fmt.Errorf("agent %s not found in registry: %w", req.Implementation, err)
	}

	// Extract profile from launch params
	profile, _ := reg.LaunchParams["profile"].(string)
	if profile == "" {
		return nil, fmt.Errorf("agent %s has no orchestrator profile configured", req.Implementation)
	}

	// Check if orchestration is available
	if p.gateway.orchestration == nil || p.gateway.orchestration.TaskService == nil {
		return nil, fmt.Errorf("orchestration not available")
	}

	// Build the target identity string for the agent
	targetAgentID, terr := models.AgentTopic(req.Workspace, req.Implementation, req.Specifier)
	if terr != nil {
		return nil, fmt.Errorf("invalid agent identity: %w", terr)
	}

	// Create a targeted task for the agent - if the agent is offline,
	// this will trigger orchestration automatically.
	// TaskType must be "agent_startup" to trigger the single-task code path
	// (otherwise a queued work task + startup task would both be created)
	taskReq := &orchestration.CreateTaskRequest{
		TaskType:        "agent_startup",
		Workspace:       req.Workspace,
		AssignmentMode:  "targeted",
		TargetAgentID:   targetAgentID,
		CreatorIdentity: adminIdentity,
		LaunchParamOverrides: map[string]interface{}{
			"specifier": req.Specifier,
		},
		Metadata: map[string]interface{}{
			"source":         "admin_ui",
			"implementation": req.Implementation,
			"specifier":      req.Specifier,
		},
	}

	resp, err := p.gateway.orchestration.TaskService.CreateTask(ctx, taskReq)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestration task: %w", err)
	}

	return &admin.LaunchAgentResponse{
		TaskID:  resp.TaskID,
		Message: fmt.Sprintf("Agent %s.%s launch triggered: %s", req.Implementation, req.Specifier, resp.Message),
	}, nil
}
