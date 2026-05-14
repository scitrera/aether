package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/state"
	regstore "github.com/scitrera/aether/internal/storage/registry"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/errors"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// SessionLivenessRegistry is the narrow surface TaskAssignmentService needs
// from a session registry: "is this identity online right now?" plus the
// context-aware reconciliation probe. Both *state.SessionRegistry (Redis)
// and *state.BadgerSessionRegistry (lite) satisfy this interface, so the
// concrete-typed field no longer forces aetherlite to pass nil.
//
// History: until 2026-05-13 this field was typed as *state.SessionRegistry,
// which aetherlite could not supply (its registry is Badger-backed). Passing
// nil compiled cleanly and the gateway started up healthy, but the first
// chat message creating an agent-targeted task crashed inside
// handleTargeted → sessionRegistry.IsOnline.
type SessionLivenessRegistry interface {
	IsOnline(identity models.Identity) bool
	IsActive(ctx context.Context, identity string) (bool, error)
}

// compile-time conformance: keep both concrete impls in sync with the interface.
var _ SessionLivenessRegistry = (*state.SessionRegistry)(nil)
var _ SessionLivenessRegistry = (*state.BadgerSessionRegistry)(nil)

// TaskAssignmentService handles task creation and assignment for orchestration patterns.
//
// As of Stage 1 of the storage-interfaces refactor, taskStore and agentRegistry
// are interface-typed (internal/storage/tasks.Store and
// internal/storage/registry.Store). The legacy AgentRegistry method `Exists`
// is part of registry.Store, so handleTargeted continues to compile unchanged.
type TaskAssignmentService struct {
	db              *sql.DB
	taskStore       taskstore.Store
	agentRegistry   regstore.Store
	sessionRegistry SessionLivenessRegistry
	queueManager    *OrchestratedQueueManager // Kept for backward compatibility
	profileManager  regstore.Store
	tokenStore      state.TokenStore
	grantService    authorityGrantService
	dispatcher      queueRetirementDispatcher
}

// queueRetirementDispatcher is the narrow interface TaskAssignmentService needs
// to retire orchestrated_task_queue rows on task terminal transitions.
type queueRetirementDispatcher interface {
	CompleteTaskByTaskID(ctx context.Context, taskID string) error
	FailTaskByTaskID(ctx context.Context, taskID, errorMsg string) error
}

type authorityGrantService interface {
	RevokeAuthorityGrant(ctx context.Context, grantID string) error
}

// NewTaskAssignmentService creates a new task assignment service.
//
// agentRegistry and profileManager are typed against the shared
// internal/storage/registry.Store interface. Production callers (full +
// lite) pass the same bundled registry.Store for both parameters; the
// two-parameter signature preserves source compatibility for tests that
// supply nil for either slot.
func NewTaskAssignmentService(
	db *sql.DB,
	taskStore taskstore.Store,
	agentRegistry regstore.Store,
	sessionRegistry SessionLivenessRegistry,
	queueManager *OrchestratedQueueManager,
	profileManager regstore.Store,
) *TaskAssignmentService {
	// Defensive: the consumer code derefs sessionRegistry unconditionally in
	// handleTargeted and createOrchestratedStartupTask. Callers that pass nil
	// would crash on the first agent-targeted task; surface the misconfiguration
	// in startup logs instead of a runtime SIGSEGV.
	if sessionRegistry == nil {
		logging.Logger.Warn().Msg("NewTaskAssignmentService: sessionRegistry is nil — agent-targeted tasks will panic; wire a real SessionLivenessRegistry impl")
	}
	return &TaskAssignmentService{
		db:              db,
		taskStore:       taskStore,
		agentRegistry:   agentRegistry,
		sessionRegistry: sessionRegistry,
		queueManager:    queueManager,
		profileManager:  profileManager,
	}
}

// SetTokenStore sets the token store for token revocation on task completion
func (tas *TaskAssignmentService) SetTokenStore(tokenStore state.TokenStore) {
	tas.tokenStore = tokenStore
}

// SetAuthorityGrantService sets the authority grant service for task lifecycle cleanup.
func (tas *TaskAssignmentService) SetAuthorityGrantService(grantService authorityGrantService) {
	tas.grantService = grantService
}

// SetOrchestratorDispatcher sets the dispatcher used to retire orchestrated_task_queue
// rows when a task reaches a terminal state. Callers that do not set this (e.g., tests
// or lightweight in-process setups) are unaffected — queue retirement is simply skipped.
func (tas *TaskAssignmentService) SetOrchestratorDispatcher(d queueRetirementDispatcher) {
	tas.dispatcher = d
}

// CreateTaskRequest represents a request to create a task
type CreateTaskRequest struct {
	TaskType             string
	TaskClass            int32 // mirrors proto TaskClass enum; 0 = UNSPECIFIED
	Workspace            string
	AssignmentMode       string                 // 'self_assign', 'targeted', 'pool'
	TargetAgentID        string                 // For targeted mode
	TargetImplementation string                 // For pool mode: target agent implementation type
	LaunchParamOverrides map[string]interface{} // For targeted tasks triggering orchestration
	Metadata             map[string]interface{}
	Payload              []byte          // Optional binary payload for task input data
	CreatorIdentity      models.Identity // Identity creating the task
	ParentTaskID         string          // Parent task ID when this CreateTask is nested inside another task's execution
	// SubjectIdentity identifies the principal on whose behalf this task is being
	// created (OBO subject). When set, the stored task's Authority.SubjectType and
	// Authority.SubjectID are populated from this identity. This is what lets
	// downstream consumers (buildTaskContext → ConfigSnapshot.task_context["user"])
	// route responses back to the originating user. Leave zero-value for internal/
	// service-initiated tasks that have no OBO subject.
	SubjectIdentity models.Identity
}

// principalTypeStringForTask maps a models.PrincipalType to the lowercase
// canonical string form used in task Authority columns ("user", "agent",
// "task", "service", etc.). These strings match the ACL canonical forms
// (acl.PrincipalTypeUser et al.) and the strings already written to the
// subject_type / root_subject_type task columns by
// applyResolvedAuthorityToTaskMetadata in the gateway package.
func principalTypeStringForTask(pt models.PrincipalType) string {
	switch pt {
	case models.PrincipalUser:
		return "user"
	case models.PrincipalAgent:
		return "agent"
	case models.PrincipalTask:
		return "task"
	case models.PrincipalWorkflowEngine:
		return "workflow_engine"
	case models.PrincipalMetricsBridge:
		return "metrics_bridge"
	case models.PrincipalOrchestrator:
		return "orchestrator"
	case models.PrincipalBridge:
		return "bridge"
	case models.PrincipalService:
		return "service"
	default:
		return strings.ToLower(string(pt))
	}
}

// applySubjectIdentityToAuthority populates Authority.SubjectType/SubjectID
// (and the root-subject fields when unset) from the given identity. Zero-value
// identity is a no-op so existing callers that don't supply a subject preserve
// the previous behavior.
func applySubjectIdentityToAuthority(task *tasks.ExtendedTask, subject models.Identity) {
	if task == nil {
		return
	}
	if subject.Type == "" || subject.ID == "" {
		return
	}
	subjectType := principalTypeStringForTask(subject.Type)
	task.Authority.SubjectType = subjectType
	task.Authority.SubjectID = subject.ID
	// First-level subject is also the root; nested-task chains can override later
	// via UpdateTaskAuthority when a grant lineage is established.
	if task.Authority.RootSubjectType == "" {
		task.Authority.RootSubjectType = subjectType
	}
	if task.Authority.RootSubjectID == "" {
		task.Authority.RootSubjectID = subject.ID
	}
}

// CreateTaskResponse represents the result of task creation
type CreateTaskResponse struct {
	TaskID     string
	Status     string // 'created', 'assigned', 'queued_for_startup', 'orchestration_triggered'
	AssignedTo string
	// StartupTaskID is the ID of the separately-created `agent_startup` task
	// when `handleTargeted` triggers orchestration for an offline agent.
	// Empty when no startup task was created (target was online, or this is a
	// self_assign / pool path that doesn't produce a startup task). When the
	// request itself has TaskType == "agent_startup", StartupTaskID equals TaskID.
	StartupTaskID    string
	QueuedForStartup bool
	Message          string
}

// CreateTask creates and routes a task based on assignment mode
func (tas *TaskAssignmentService) CreateTask(ctx context.Context, req *CreateTaskRequest) (*CreateTaskResponse, error) {
	switch req.AssignmentMode {
	case "self_assign", "":
		return tas.handleSelfAssign(ctx, req)
	case "targeted":
		return tas.handleTargeted(ctx, req)
	case "pool":
		return tas.handlePool(ctx, req)
	default:
		return nil, &errors.InvalidAssignmentModeError{Mode: req.AssignmentMode}
	}
}

// handleSelfAssign creates a self-assigned task
// Agent creates task and immediately assigns to itself
func (tas *TaskAssignmentService) handleSelfAssign(ctx context.Context, req *CreateTaskRequest) (*CreateTaskResponse, error) {
	taskID := uuid.New().String()

	task := &tasks.ExtendedTask{
		TaskID:         taskID,
		TaskType:       req.TaskType,
		TaskClass:      req.TaskClass,
		GraceWindowMs:  DefaultGraceWindowMs(req.TaskClass),
		Workspace:      req.Workspace,
		AssignmentMode: tasks.AssignmentModeSelfAssign,
		TaskCategory:   tasks.TaskCategoryRegular,
		Status:         tasks.TaskStatusPending,
		ParentAgentID:  req.CreatorIdentity.String(),
		ParentTaskID:   req.ParentTaskID,
		Metadata:       req.Metadata,
		Payload:        req.Payload,
		MaxRetries:     3,
	}
	applySubjectIdentityToAuthority(task, req.SubjectIdentity)

	// Create task in database as pending
	if err := tas.taskStore.CreateTask(ctx, task); err != nil {
		return nil, fmt.Errorf("failed to create self-assigned task: %w", err)
	}

	// Assign to self (pending -> assigned)
	if err := tas.taskStore.AssignTask(ctx, taskID, req.CreatorIdentity.String()); err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to assign self-assigned task")
	}

	logging.Logger.Info().Str("task_id", taskID).Str("identity", req.CreatorIdentity.String()).Msg("created self-assigned task")

	return &CreateTaskResponse{
		TaskID:     taskID,
		Status:     "assigned",
		AssignedTo: req.CreatorIdentity.String(),
		Message:    "Task self-assigned successfully",
	}, nil
}

// handleTargeted creates a targeted task assigned to a specific agent
// If agent is online: deliver immediately
// If agent is offline: trigger orchestration and queue task
func (tas *TaskAssignmentService) handleTargeted(ctx context.Context, req *CreateTaskRequest) (*CreateTaskResponse, error) {
	if req.TargetAgentID == "" {
		return nil, fmt.Errorf("target_agent_id required for targeted assignment")
	}

	// Parse target agent identity
	targetIdentity, err := models.ParseIdentity(req.TargetAgentID)
	if err != nil {
		return nil, fmt.Errorf("invalid target_agent_id: %w", err)
	}

	// REQUIRED: Validate target agent implementation exists in registry
	exists, err := tas.agentRegistry.Exists(ctx, targetIdentity.Implementation)
	if err != nil {
		return nil, fmt.Errorf("failed to check agent registry: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("target agent implementation '%s' not found in registry", targetIdentity.Implementation)
	}

	taskID := uuid.New().String()

	task := &tasks.ExtendedTask{
		TaskID:         taskID,
		TaskType:       req.TaskType,
		TaskClass:      req.TaskClass,
		GraceWindowMs:  DefaultGraceWindowMs(req.TaskClass),
		Workspace:      req.Workspace,
		AssignmentMode: tasks.AssignmentModeTargeted,
		TaskCategory:   tasks.TaskCategoryRegular,
		TargetAgentID:  req.TargetAgentID,
		Status:         tasks.TaskStatusPending,
		ParentAgentID:  req.CreatorIdentity.String(),
		ParentTaskID:   req.ParentTaskID,
		Metadata:       req.Metadata,
		Payload:        req.Payload,
		MaxRetries:     3,
	}
	applySubjectIdentityToAuthority(task, req.SubjectIdentity)

	// Special case: if this IS a startup task (e.g., from admin API), go directly to
	// createOrchestratedStartupTask which handles all duplicate prevention:
	// - Checks if agent is already online
	// - Checks if there's already an active startup task
	if req.TaskType == "agent_startup" {
		startupTaskID, err := tas.createOrchestratedStartupTask(ctx, targetIdentity, startupWorkspaceFor(targetIdentity, req.Workspace), req.LaunchParamOverrides, req.SubjectIdentity, req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to create startup task: %w", err)
		}

		logging.Logger.Info().Str("task_id", startupTaskID).Str("agent_id", req.TargetAgentID).Msg("created startup task")

		return &CreateTaskResponse{
			TaskID:           startupTaskID,
			StartupTaskID:    startupTaskID,
			Status:           "orchestration_triggered",
			QueuedForStartup: true,
			Message:          "Startup task sent to orchestrator",
		}, nil
	}

	// Check if target agent is online
	isOnline := tas.sessionRegistry.IsOnline(targetIdentity)

	if isOnline {
		// Agent online: create task as pending, then assign
		// This follows the proper state machine: pending -> assigned
		if err := tas.taskStore.CreateTask(ctx, task); err != nil {
			return nil, fmt.Errorf("failed to create targeted task: %w", err)
		}

		if err := tas.taskStore.AssignTask(ctx, taskID, req.TargetAgentID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to assign targeted task")
		}

		logging.Logger.Info().Str("task_id", taskID).Str("agent_id", req.TargetAgentID).Msg("created targeted task for online agent")

		return &CreateTaskResponse{
			TaskID:     taskID,
			Status:     "assigned",
			AssignedTo: req.TargetAgentID,
			Message:    "Task assigned to online agent",
		}, nil
	}

	// Agent offline: need orchestration to start it

	// Regular task for offline agent: queue the task AND trigger orchestration
	// The queued task will be delivered to the agent when it comes online
	task.QueuedForStartup = true

	if err := tas.taskStore.CreateTask(ctx, task); err != nil {
		return nil, fmt.Errorf("failed to create queued task: %w", err)
	}

	// Create separate startup task to launch the agent. The startup task's
	// workspace must be the AGENT'S home workspace (parsed from
	// target_agent_id), not the requesting task's workspace — the orchestrator
	// builds the spawned agent's identity from this workspace value, and a
	// requesting task in a different workspace would otherwise spawn an agent
	// at the wrong identity (e.g. a chat in workspace=default targeting
	// ag::_apps::CoworkAgent::user would spawn ag::default::CoworkAgent::user
	// alongside the correct one).
	startupTaskID, err := tas.createOrchestratedStartupTask(ctx, targetIdentity, startupWorkspaceFor(targetIdentity, req.Workspace), req.LaunchParamOverrides, req.SubjectIdentity, req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrated startup task: %w", err)
	}

	logging.Logger.Info().Str("task_id", taskID).Str("agent_id", req.TargetAgentID).Str("startup_task_id", startupTaskID).Msg("created task for offline agent, triggered orchestration")

	return &CreateTaskResponse{
		TaskID:           taskID,
		StartupTaskID:    startupTaskID,
		Status:           "orchestration_triggered",
		QueuedForStartup: true,
		Message:          fmt.Sprintf("Task queued, agent startup triggered (startup task: %s)", startupTaskID),
	}, nil
}

// startupWorkspaceFor returns the workspace the orchestrator should use when
// minting the startup task for a given target. The spawned agent's canonical
// identity is built from this workspace (orchestration_integration.go's
// targetAgentIdentity = AgentTopic(task.Workspace, impl, spec)), so it must
// match the agent's HOME workspace — not the requesting task's workspace.
// Falls back to the requesting workspace when the target identity carries
// none (e.g. legacy callers that pass a topic-only string).
func startupWorkspaceFor(targetIdentity models.Identity, fallback string) string {
	if targetIdentity.Workspace != "" {
		return targetIdentity.Workspace
	}
	return fallback
}

// createOrchestratedStartupTask creates a task to start an agent.
// subjectIdentity is the OBO subject who triggered this startup (e.g. the user
// whose message caused offline-agent orchestration). When non-empty, it is
// applied to the startup task's Authority lineage so downstream consumers
// (buildTaskContext → task_context["user"]) can route responses back to the
// originating user. Pass models.Identity{} for internal/system-initiated
// startups that have no OBO subject.
func (tas *TaskAssignmentService) createOrchestratedStartupTask(
	ctx context.Context,
	targetIdentity models.Identity,
	workspace string,
	launchParamOverrides map[string]interface{},
	subjectIdentity models.Identity,
	triggeringMetadata map[string]interface{},
) (string, error) {
	// Check if agent is already online
	if tas.sessionRegistry.IsOnline(targetIdentity) {
		return "", fmt.Errorf("agent %s is already online", targetIdentity.String())
	}

	// Check for existing active startup task for this target. The specifier
	// dimension lets per-user singleton agents (e.g. CoworkAgent with
	// workspace="_apps", specifier=<user_id>) coexist without colliding on the
	// (implementation, workspace) key.
	hasActive, existingTaskID, err := tas.taskStore.HasActiveStartupTask(ctx, targetIdentity.Implementation, workspace, targetIdentity.Specifier)
	if err != nil {
		return "", fmt.Errorf("failed to check for existing startup tasks: %w", err)
	}
	if hasActive {
		return "", fmt.Errorf("startup task %s already exists for %s/%s in workspace %s",
			existingTaskID, targetIdentity.Implementation, targetIdentity.Specifier, workspace)
	}

	// Get default launch params from registry
	defaultParams, err := tas.agentRegistry.GetLaunchParams(ctx, targetIdentity.Implementation)
	if err != nil {
		return "", fmt.Errorf("failed to get launch params: %w", err)
	}

	// Merge with overrides
	effectiveParams := registry.MergeLaunchParams(defaultParams, launchParamOverrides)

	// Extract profile
	profile, ok := effectiveParams["profile"].(string)
	if !ok || profile == "" {
		return "", fmt.Errorf("launch params missing 'profile' field")
	}

	// Create orchestrated task
	startupTaskID := uuid.New().String()

	// Propagate select metadata keys from the triggering request onto the
	// startup task so downstream consumers that key off the startup task
	// (e.g. ``subscription.setupClientSubscriptions`` → ``lookupTriggerTimestampMs``
	// for first-message replay) have what they need. The triggering task
	// itself (``message_delivery`` et al.) is a different row; the connecting
	// agent's ``AssociatedTaskID`` resolves to this startup task.
	//
	// Default trigger_timestamp_ms to "now" when the caller didn't supply one
	// (e.g. TARGETED task creation that triggers orchestration via this path
	// rather than via routing.go::triggerOrchestration). Without this, the
	// agent's first-connect subscription has no resume hint and starts at
	// "next message", silently dropping any messages published between task
	// creation and agent connect — symptom: chat task created, agent comes
	// online, but on_user_message never fires because the CHAT envelope was
	// before the consumer offset.
	startupMetadata := make(map[string]interface{}, 1)
	if triggeringMetadata != nil {
		if v, ok := triggeringMetadata["trigger_timestamp_ms"]; ok {
			startupMetadata["trigger_timestamp_ms"] = v
		}
	}
	if _, ok := startupMetadata["trigger_timestamp_ms"]; !ok {
		startupMetadata["trigger_timestamp_ms"] = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}

	task := &tasks.ExtendedTask{
		TaskID:               startupTaskID,
		TaskType:             "agent_startup",
		Workspace:            workspace,
		AssignmentMode:       tasks.AssignmentModePool, // Orchestrators consume from pool
		TaskCategory:         tasks.TaskCategoryOrchestrated,
		TargetImplementation: targetIdentity.Implementation,
		TargetSpecifier:      targetIdentity.Specifier,
		LaunchParams:         effectiveParams,
		Metadata:             startupMetadata,
		Status:               tasks.TaskStatusPending,
		MaxRetries:           3,
	}
	applySubjectIdentityToAuthority(task, subjectIdentity)

	if err := tas.taskStore.CreateTask(ctx, task); err != nil {
		return "", fmt.Errorf("failed to create orchestrated task: %w", err)
	}

	// Insert into orchestrated_task_queue table (triggers PostgreSQL NOTIFY)
	queueID := uuid.New().String()
	launchParamsJSON, err := json.Marshal(effectiveParams)
	if err != nil {
		return "", fmt.Errorf("failed to marshal launch params: %w", err)
	}

	insertQuery := `
		INSERT INTO orchestrated_task_queue
		(queue_id, task_id, target_implementation, workspace, profile, launch_params, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending')
	`

	_, err = tas.db.ExecContext(ctx, insertQuery,
		queueID,
		startupTaskID,
		targetIdentity.Implementation,
		workspace,
		profile,
		launchParamsJSON,
	)
	if err != nil {
		return "", fmt.Errorf("failed to insert orchestrated task into queue: %w", err)
	}

	logging.Logger.Info().Str("task_id", startupTaskID).Str("queue_id", queueID).Str("implementation", targetIdentity.Implementation).Str("profile", profile).Msg("created orchestrated startup task")

	return startupTaskID, nil
}

// DeliverQueuedTasks delivers all queued tasks to an agent when it connects
func (tas *TaskAssignmentService) DeliverQueuedTasks(ctx context.Context, agentIdentity models.Identity) ([]*tasks.ExtendedTask, error) {
	agentID := agentIdentity.String()

	// Get all queued tasks for this agent
	queuedTasks, err := tas.taskStore.GetQueuedTasksForAgent(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("failed to get queued tasks: %w", err)
	}

	if len(queuedTasks) == 0 {
		return nil, nil
	}

	var deliveredTasks []*tasks.ExtendedTask

	for _, task := range queuedTasks {
		// Assign task to agent
		if err := tas.taskStore.AssignTask(ctx, task.TaskID, agentID); err != nil {
			logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Str("agent_id", agentID).Msg("failed to assign queued task")
			continue
		}

		// Clear queued flag
		if err := tas.taskStore.MarkTaskNotQueued(ctx, task.TaskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", task.TaskID).Msg("failed to mark task as not queued")
		}

		deliveredTasks = append(deliveredTasks, task)
	}

	logging.Logger.Info().Int("count", len(deliveredTasks)).Str("agent_id", agentID).Msg("delivered queued tasks to agent")

	return deliveredTasks, nil
}

// handlePool creates a pool task that can be claimed by any matching worker.
func (tas *TaskAssignmentService) handlePool(ctx context.Context, req *CreateTaskRequest) (*CreateTaskResponse, error) {
	if req.TargetImplementation == "" {
		return nil, fmt.Errorf("target_implementation required for pool assignment")
	}

	taskID := uuid.New().String()

	task := &tasks.ExtendedTask{
		TaskID:               taskID,
		TaskType:             req.TaskType,
		TaskClass:            req.TaskClass,
		GraceWindowMs:        DefaultGraceWindowMs(req.TaskClass),
		Workspace:            req.Workspace,
		AssignmentMode:       tasks.AssignmentModePool,
		TaskCategory:         tasks.TaskCategoryRegular,
		TargetImplementation: req.TargetImplementation,
		Status:               tasks.TaskStatusPending,
		QueuedForStartup:     true,
		ParentAgentID:        req.CreatorIdentity.String(),
		ParentTaskID:         req.ParentTaskID,
		Metadata:             req.Metadata,
		Payload:              req.Payload,
		MaxRetries:           3,
	}
	applySubjectIdentityToAuthority(task, req.SubjectIdentity)

	if err := tas.taskStore.CreateTask(ctx, task); err != nil {
		return nil, fmt.Errorf("failed to create pool task: %w", err)
	}

	logging.Logger.Info().Str("task_id", taskID).Str("implementation", req.TargetImplementation).Str("workspace", req.Workspace).Msg("created pool task")

	return &CreateTaskResponse{
		TaskID:  taskID,
		Status:  "pending_pool",
		Message: "Pool task created, awaiting worker",
	}, nil
}

// DeliverPoolTasks claims and returns pending pool tasks for a connecting agent.
func (tas *TaskAssignmentService) DeliverPoolTasks(ctx context.Context, agentIdentity models.Identity) ([]*tasks.ExtendedTask, error) {
	pendingTasks, err := tas.taskStore.GetPendingPoolTasks(ctx, agentIdentity.Implementation, agentIdentity.Workspace)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending pool tasks: %w", err)
	}

	if len(pendingTasks) == 0 {
		return nil, nil
	}

	var claimed []*tasks.ExtendedTask
	for _, task := range pendingTasks {
		ok, err := tas.taskStore.ClaimPoolTask(ctx, task.TaskID, agentIdentity.String())
		if err != nil {
			logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Msg("error claiming pool task")
			continue
		}
		if ok {
			claimed = append(claimed, task)
		}
	}

	if len(claimed) > 0 {
		logging.Logger.Info().Int("count", len(claimed)).Str("agent", agentIdentity.String()).Msg("claimed pool tasks for connecting agent")
	}

	return claimed, nil
}

// GetTaskStatus retrieves task status
func (tas *TaskAssignmentService) GetTaskStatus(ctx context.Context, taskID string) (*tasks.ExtendedTask, error) {
	return tas.taskStore.GetTask(ctx, taskID)
}

// StartingTask marks a task as starting (orchestrator is launching the job)
// The task must already be in "assigned" state. This indicates the orchestrator
// has picked up the task and is in the process of launching it, but the task
// has not yet connected.
func (tas *TaskAssignmentService) StartingTask(ctx context.Context, taskID string) error {
	return tas.taskStore.StartingTask(ctx, taskID)
}

// StartTask marks a task as running (e.g., when an orchestrated agent connects)
// The task must be in "assigned", "starting", or "running" (reconnect) state.
func (tas *TaskAssignmentService) StartTask(ctx context.Context, taskID string) error {
	return tas.taskStore.StartTask(ctx, taskID)
}

// StartTaskWithAgent marks a task as running and records the agent identity.
// This is preferred over StartTask for orchestrated agents as it enables
// reconciliation to detect orphaned tasks when the agent disconnects unexpectedly.
func (tas *TaskAssignmentService) StartTaskWithAgent(ctx context.Context, taskID, agentIdentity string) error {
	if err := tas.taskStore.StartTaskWithAgent(ctx, taskID, agentIdentity); err != nil {
		return err
	}
	if tas.dispatcher != nil {
		if err := tas.dispatcher.CompleteTaskByTaskID(ctx, taskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to retire orchestrated_task_queue row on task start (non-fatal)")
		}
	}
	return nil
}

// MarkTaskDisconnected stamps disconnected_at on a running task without
// touching status, tokens, or authority grants. Used when the assigned
// worker's gRPC stream closes (other than worker-graceful EOF, which is
// the explicit "I'm done" signal). The disconnect reaper fails the task
// if no reconnect happens within the grace window.
func (tas *TaskAssignmentService) MarkTaskDisconnected(ctx context.Context, taskID string, when time.Time) error {
	return tas.taskStore.MarkTaskDisconnected(ctx, taskID, when)
}

// ClearTaskDisconnected removes the disconnect marker. Called when a worker
// reconnects and re-establishes its task association.
func (tas *TaskAssignmentService) ClearTaskDisconnected(ctx context.Context, taskID string) error {
	return tas.taskStore.ClearTaskDisconnected(ctx, taskID)
}

// DefaultGraceWindowMs returns the per-class default reconnect-grace window.
// Connection-as-heartbeat: workers reap if disconnected_at exceeds this many
// milliseconds without reconnect.
//
// Class values mirror the proto TaskClass enum:
//
//	1 = INTERACTIVE (and 0 = UNSPECIFIED, treated as INTERACTIVE)
//	2 = BACKGROUND
//	3 = BATCH
func DefaultGraceWindowMs(class int32) int64 {
	switch class {
	case 2: // BACKGROUND
		return 600000 // 10 min — sandbox leases, idle reapers, infra workers
	case 3: // BATCH
		return 300000 // 5 min — long-running user-initiated jobs
	default: // INTERACTIVE / UNSPECIFIED
		return 30000 // 30s — short-lived foreground tasks
	}
}

// CompleteTask marks a task as completed and revokes associated tokens
func (tas *TaskAssignmentService) CompleteTask(ctx context.Context, taskID string) error {
	// Revoke tokens before completing (allows agent to reconnect until task is marked complete)
	if tas.tokenStore != nil {
		if err := tas.tokenStore.RevokeTokensForTask(ctx, taskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to revoke tokens for completed task")
			// Non-fatal - continue with completion
		}
	}

	tas.revokeTaskAuthorityGrant(ctx, taskID)

	if err := tas.taskStore.CompleteTask(ctx, taskID); err != nil {
		return err
	}
	if tas.dispatcher != nil {
		if err := tas.dispatcher.CompleteTaskByTaskID(ctx, taskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to retire orchestrated_task_queue row on task complete (non-fatal)")
		}
	}
	return nil
}

// FailTask marks a task as failed and revokes associated tokens
func (tas *TaskAssignmentService) FailTask(ctx context.Context, taskID, errorMsg string) error {
	// Revoke tokens on failure too
	if tas.tokenStore != nil {
		if err := tas.tokenStore.RevokeTokensForTask(ctx, taskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to revoke tokens for failed task")
			// Non-fatal - continue with failure handling
		}
	}

	tas.revokeTaskAuthorityGrant(ctx, taskID)

	if err := tas.taskStore.FailTask(ctx, taskID, errorMsg); err != nil {
		return err
	}
	if tas.dispatcher != nil {
		if err := tas.dispatcher.FailTaskByTaskID(ctx, taskID, errorMsg); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to retire orchestrated_task_queue row on task fail (non-fatal)")
		}
	}
	return nil
}

// CancelTask marks a task as cancelled and revokes associated tokens and authority grants.
func (tas *TaskAssignmentService) CancelTask(ctx context.Context, taskID string) error {
	if tas.tokenStore != nil {
		if err := tas.tokenStore.RevokeTokensForTask(ctx, taskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to revoke tokens for cancelled task")
		}
	}

	tas.revokeTaskAuthorityGrant(ctx, taskID)

	if err := tas.taskStore.CancelTask(ctx, taskID); err != nil {
		return err
	}
	if tas.dispatcher != nil {
		if err := tas.dispatcher.FailTaskByTaskID(ctx, taskID, "task cancelled"); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to retire orchestrated_task_queue row on task cancel (non-fatal)")
		}
	}
	return nil
}

func (tas *TaskAssignmentService) revokeTaskAuthorityGrant(ctx context.Context, taskID string) {
	if tas.grantService == nil || tas.taskStore == nil {
		return
	}

	task, err := tas.taskStore.GetTask(ctx, taskID)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to load task for authority grant cleanup")
		return
	}

	// Revoke the task's OWN authority grant only — never the lineage root.
	// For tasks created via an OBO-derived grant, the root in the lineage
	// is the caller's session grant (e.g. a user's exchanged authority);
	// revoking that as a side effect of task termination would invalidate
	// every other in-flight or future operation under that same session.
	// taskAuthorityCurrentGrantID returns the per-task child grant minted
	// by establishTaskAuthorityGrant — that's the only grant whose lifetime
	// is bounded by this task. Self-grant tasks (no OBO parent) record the
	// same id under both AuthorityGrantID and RootAuthorityGrantID, so the
	// current accessor still returns the correct value for them.
	grantID := taskAuthorityCurrentGrantID(task)
	if grantID == "" {
		return
	}

	if err := tas.grantService.RevokeAuthorityGrant(ctx, grantID); err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", taskID).Str("grant_id", grantID).Msg("failed to revoke task authority grant")
	}
}

func taskAuthorityCurrentGrantID(task *tasks.ExtendedTask) string {
	if task == nil {
		return ""
	}
	if task.Authority.AuthorityGrantID != "" {
		return task.Authority.AuthorityGrantID
	}
	if task.Metadata == nil {
		return ""
	}
	if value, ok := task.Metadata["authority_grant_id"].(string); ok {
		return value
	}
	return ""
}

// reconcileSkip is a sentinel returned by a getIdentity closure to indicate
// that the task should be skipped silently (no failure, no log).
const reconcileSkip = "\x00skip"

// reconcileTasksByStatus reconciles tasks in a given status, failing those whose
// responsible entity (agent or orchestrator) is no longer online.
//
// getIdentity extracts the responsible entity's identity string from a task.
// Return values have special meaning:
//   - reconcileSkip: skip the task silently (no action taken)
//   - "": entity identity is missing; fail the task with emptyIdentityFailReason
//   - any other string: check session liveness; fail with offlineFailReason if inactive
//
// entityLogKey is the zerolog field name used in log output (e.g. "agent_id").
func (tas *TaskAssignmentService) reconcileTasksByStatus(
	ctx context.Context,
	filter *tasks.TaskFilter,
	getIdentity func(*tasks.ExtendedTask) string,
	emptyIdentityFailReason string,
	offlineFailReason string,
	entityLogKey string,
) (int, error) {
	taskList, err := tas.taskStore.ListTasks(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to list %s tasks: %w", *filter.Status, err)
	}

	reconciled := 0
	for _, task := range taskList {
		identity := getIdentity(task)

		if identity == reconcileSkip {
			continue
		}

		if identity == "" {
			if err := tas.FailTask(ctx, task.TaskID, emptyIdentityFailReason); err != nil {
				logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Msg("reconcile: failed to mark task as failed (no identity)")
			} else {
				logging.Logger.Info().Str("task_id", task.TaskID).Msgf("reconcile: marked task as failed (no %s)", entityLogKey)
				reconciled++
			}
			continue
		}

		active, err := tas.sessionRegistry.IsActive(ctx, identity)
		if err != nil {
			logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Str(entityLogKey, identity).Msg("reconcile: error checking lock for task")
			continue
		}

		if !active {
			if err := tas.FailTask(ctx, task.TaskID, offlineFailReason); err != nil {
				logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Msg("reconcile: failed to mark task as failed")
			} else {
				logging.Logger.Info().Str("task_id", task.TaskID).Str(entityLogKey, identity).Msg("reconcile: marked orphaned task as failed (no lock)")
				reconciled++
			}
		}
	}
	return reconciled, nil
}

// agentIdentityOrSkip extracts TargetAgentID for reconciliation.
// For tasks with no TargetAgentID that are not orchestrated/agent_startup, returns
// reconcileSkip so the helper skips them silently.
func agentIdentityOrSkip(task *tasks.ExtendedTask) string {
	if task.TargetAgentID != "" {
		return task.TargetAgentID
	}
	if task.TaskCategory == tasks.TaskCategoryOrchestrated || task.TaskType == "agent_startup" {
		return "" // trigger empty-identity failure path
	}
	return reconcileSkip
}

// ReconcileOrphanedTasks finds tasks in 'running', 'starting', or 'assigned' state
// that are orphaned, and marks them as failed.
// This handles cases where:
// - Gateway crashed/restarted and defer cleanup never ran
// - Redis lock expired or was manually cleared
// - Old stale tasks from before proper lifecycle tracking (no TargetAgentID set)
// - Orchestrator disconnected before agent connected (task stuck in assigned)
// Returns the number of tasks reconciled.
func (tas *TaskAssignmentService) ReconcileOrphanedTasks(ctx context.Context) (int, error) {
	reconciled := 0

	// Check running tasks
	runningStatus := tasks.TaskStatusRunning
	n, err := tas.reconcileTasksByStatus(ctx,
		&tasks.TaskFilter{Status: &runningStatus, Limit: 1000},
		agentIdentityOrSkip,
		"orphaned task with no agent tracking (reconciliation)",
		"agent disconnected (reconciliation)",
		"agent_id",
	)
	if err != nil {
		return 0, err
	}
	reconciled += n

	// Check starting tasks
	startingStatus := tasks.TaskStatusStarting
	n, err = tas.reconcileTasksByStatus(ctx,
		&tasks.TaskFilter{Status: &startingStatus, Limit: 1000},
		agentIdentityOrSkip,
		"orphaned starting task with no agent tracking (reconciliation)",
		"agent failed to start (reconciliation)",
		"agent_id",
	)
	if err != nil {
		return reconciled, err
	}
	reconciled += n

	// Check assigned tasks (orchestrator picked up but agent never connected).
	// Only reconcile agent_startup tasks; regular assigned tasks are not checked.
	assignedStatus := tasks.TaskStatusAssigned
	n, err = tas.reconcileTasksByStatus(ctx,
		&tasks.TaskFilter{Status: &assignedStatus, TaskType: "agent_startup", Limit: 1000},
		func(task *tasks.ExtendedTask) string { return task.AssignedTo },
		"assigned task with no assignee (reconciliation)",
		"orchestrator disconnected before agent started (reconciliation)",
		"orchestrator_id",
	)
	if err != nil {
		return reconciled, err
	}
	reconciled += n

	// Check assigned pool tasks — if the worker is offline, re-queue (not fail).
	poolMode := tasks.AssignmentModePool
	poolAssigned, err := tas.taskStore.ListTasks(ctx, &tasks.TaskFilter{
		Status:         &assignedStatus,
		AssignmentMode: &poolMode,
		Limit:          1000,
	})
	if err != nil {
		return reconciled, fmt.Errorf("failed to list assigned pool tasks: %w", err)
	}
	for _, task := range poolAssigned {
		if task.AssignedTo == "" {
			if err := tas.taskStore.UnassignPoolTask(ctx, task.TaskID); err != nil {
				logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Msg("reconcile: failed to unassign pool task (no assignee)")
			} else {
				logging.Logger.Info().Str("task_id", task.TaskID).Msg("reconcile: re-queued pool task (no assignee)")
				reconciled++
			}
			continue
		}
		active, err := tas.sessionRegistry.IsActive(ctx, task.AssignedTo)
		if err != nil {
			logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Msg("reconcile: error checking lock for pool task")
			continue
		}
		if !active {
			if err := tas.taskStore.UnassignPoolTask(ctx, task.TaskID); err != nil {
				logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Msg("reconcile: failed to unassign pool task")
			} else {
				logging.Logger.Info().Str("task_id", task.TaskID).Str("worker", task.AssignedTo).Msg("reconcile: re-queued orphaned pool task (worker offline)")
				reconciled++
			}
		}
	}

	return reconciled, nil
}
