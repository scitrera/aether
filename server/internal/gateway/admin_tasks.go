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
		params := make(map[string]interface{})
		for k, v := range r.LaunchParams {
			params[k] = v
		}

		// Extract profile from launch params
		profile := ""
		if p, ok := r.LaunchParams["profile"].(string); ok {
			profile = p
		}

		infos = append(infos, &admin.AgentRegistrationInfo{
			Implementation:      r.Implementation,
			OrchestratorProfile: profile,
			Description:         r.Description,
			LaunchParams:        params,
			RegisteredAt:        r.CreatedAt,
			UpdatedAt:           r.UpdatedAt,
		})
	}

	return infos, nil
}

func (p *GatewayStateProvider) GetAgentByImplementation(ctx context.Context, implementation string) (*admin.AgentRegistrationInfo, error) {
	if p.agentRegistry == nil {
		return nil, fmt.Errorf("agent registry not available")
	}

	r, err := p.agentRegistry.Get(ctx, implementation)
	if err != nil {
		return nil, err
	}

	params := make(map[string]interface{})
	for k, v := range r.LaunchParams {
		params[k] = v
	}

	profile := ""
	if p, ok := r.LaunchParams["profile"].(string); ok {
		profile = p
	}

	return &admin.AgentRegistrationInfo{
		Implementation:      r.Implementation,
		OrchestratorProfile: profile,
		Description:         r.Description,
		LaunchParams:        params,
		RegisteredAt:        r.CreatedAt,
		UpdatedAt:           r.UpdatedAt,
	}, nil
}

func (p *GatewayStateProvider) RegisterAgent(ctx context.Context, agent *admin.AgentRegistrationInfo) error {
	if p.agentRegistry == nil {
		return fmt.Errorf("agent registry not available")
	}

	reg := &registry.AgentRegistration{
		Implementation: agent.Implementation,
		Description:    agent.Description,
		LaunchParams:   agent.LaunchParams,
	}

	return p.agentRegistry.Register(ctx, reg)
}

func (p *GatewayStateProvider) UpdateAgent(ctx context.Context, implementation string, agent *admin.AgentRegistrationInfo) error {
	if p.agentRegistry == nil {
		return fmt.Errorf("agent registry not available")
	}

	// For update, we use the same Register method which does upsert
	reg := &registry.AgentRegistration{
		Implementation: implementation,
		Description:    agent.Description,
		LaunchParams:   agent.LaunchParams,
	}

	return p.agentRegistry.Register(ctx, reg)
}

func (p *GatewayStateProvider) DeleteAgent(ctx context.Context, implementation string) error {
	if p.agentRegistry == nil {
		return fmt.Errorf("agent registry not available")
	}

	return p.agentRegistry.Delete(ctx, implementation)
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
