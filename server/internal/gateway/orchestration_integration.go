package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/metering"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/state"
	regstore "github.com/scitrera/aether/internal/storage/registry"
	regpg "github.com/scitrera/aether/internal/storage/registry/postgres"
	taskpg "github.com/scitrera/aether/internal/storage/tasks/postgres"
	"github.com/scitrera/aether/pkg/errors"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

// OrchestrationServices holds orchestration components.
//
// Per the storage-interfaces Stage 1 plan (§14.1 nil-tolerance), the registry
// surface is now a single `Registry` field of interface type
// (internal/storage/registry.Store) bundling the former AgentRegistry +
// ProfileManager method sets. Both full mode (Redis-backed) and lite mode
// (Badger-backed) construct a non-nil Registry — there is no defensible
// nil/opt-out path, so consumer code can dereference Registry methods without
// a nil guard.
type OrchestrationServices struct {
	// Registry bundles the agent-implementation catalog and the orchestrator
	// profile fleet. Required non-nil.
	Registry    regstore.Store
	TaskService *orchestration.TaskAssignmentService
	QueueCloser io.Closer // Closes the underlying queue connection (AMQP or in-process)
	Dispatcher  orchestration.TaskDispatcher
	TokenStore  state.TokenStore
}

// InitializeOrchestrationServices creates Orchestration services with proper dependency injection.
// This constructor creates the Redis/PostgreSQL-backed implementations. For lite mode,
// construct OrchestrationServices directly with in-process implementations.
func InitializeOrchestrationServices(
	db *sql.DB,
	redisClient redis.UniversalClient,
	amqpURL string,
	postgresConnStr string,
	sessionRegistry *state.SessionRegistry,
) (*OrchestrationServices, error) {
	if db == nil {
		return nil, &errors.InitializationError{
			Component: "OrchestrationServices",
			Err:       fmt.Errorf("database connection required"),
		}
	}

	if redisClient == nil {
		return nil, &errors.InitializationError{
			Component: "OrchestrationServices",
			Err:       fmt.Errorf("redis client required"),
		}
	}

	if sessionRegistry == nil {
		return nil, &errors.InitializationError{
			Component: "OrchestrationServices",
			Err:       fmt.Errorf("session registry required"),
		}
	}

	// Registry (bundles AgentRegistry + OrchestratorProfileManager). The
	// underlying ProfileStateStore is the Redis-backed impl shipped with the
	// legacy internal/registry package — keeps the round-robin counter
	// gateway-fleet-coherent.
	profileStateStore := registry.NewRedisProfileStateStore(redisClient)
	registryStore := regpg.New(db, profileStateStore)
	logging.Logger.Debug().Msg("registry store initialized")

	// Queue Manager
	queueManager, err := orchestration.NewOrchestratedQueueManager(amqpURL)
	if err != nil {
		return nil, &errors.InitializationError{
			Component: "OrchestratedQueueManager",
			Err:       err,
		}
	}
	logging.Logger.Debug().Msg("orchestrated queue manager initialized")

	// Extended task store
	orchestrationTaskStore := taskpg.New(db)
	logging.Logger.Debug().Msg("orchestration task store initialized")

	// Task Assignment Service (fully wired with dependencies)
	taskService := orchestration.NewTaskAssignmentService(
		orchestrationTaskStore,
		registryStore,
		sessionRegistry,
		queueManager,
		registryStore,
	)
	logging.Logger.Debug().Msg("task assignment service initialized")

	// NotifyTaskDispatcher (callback set by gateway server later)
	dispatcher, err := orchestration.NewNotifyTaskDispatcher(
		orchestrationTaskStore,
		postgresConnStr,
		10*time.Second, // Poll interval
		nil,            // Callback set by gateway server
	)
	if err != nil {
		return nil, &errors.InitializationError{
			Component: "NotifyTaskDispatcher",
			Err:       err,
		}
	}
	logging.Logger.Debug().Msg("notify task dispatcher initialized")

	// Token Store for orchestrated agent authentication
	tokenStore := state.NewRedisTokenStore(redisClient)
	logging.Logger.Debug().Msg("orchestration token store initialized")

	// Wire up token store to task service for revocation on completion
	taskService.SetTokenStore(tokenStore)

	// Wire up dispatcher to task service so terminal task transitions retire the
	// corresponding orchestrated_task_queue row, preventing stale-claim recovery
	// from re-dispatching already-running tasks.
	taskService.SetOrchestratorDispatcher(dispatcher)

	return &OrchestrationServices{
		Registry:    registryStore,
		TaskService: taskService,
		QueueCloser: queueManager,
		Dispatcher:  dispatcher,
		TokenStore:  tokenStore,
	}, nil
}

// handleOrchestratorConnection processes orchestrator InitConnection
func (s *GatewayServer) handleOrchestratorConnection(
	ctx context.Context,
	identity models.Identity,
	init *pb.InitConnection,
) error {
	if s.orchestration == nil || s.orchestration.Registry == nil {
		logging.Logger.Debug().Msg("orchestration not initialized, skipping orchestrator profile registration")
		return nil
	}

	// Get supported profiles from the orchestrator identity
	orchestratorInit, ok := init.ClientType.(*pb.InitConnection_Orchestrator)
	if !ok {
		return nil // Not an orchestrator
	}

	supportedProfiles := orchestratorInit.Orchestrator.SupportedProfiles
	if len(supportedProfiles) == 0 {
		return nil // No profiles declared
	}

	orchestratorID := identity.String()
	workspace := identity.Workspace
	if workspace == "" {
		workspace = models.SystemWorkspace
	}

	// Register profiles
	err := s.orchestration.Registry.RegisterProfiles(
		ctx,
		orchestratorID,
		supportedProfiles,
		workspace,
	)
	if err != nil {
		logging.Logger.Error().Err(err).Str("orchestrator_id", orchestratorID).Msg("failed to register orchestrator profiles")
		return err
	}

	logging.Logger.Info().Str("orchestrator_id", orchestratorID).Strs("profiles", supportedProfiles).Msg("registered orchestrator profiles")
	return nil
}

// deliverQueuedTasksToAgent delivers queued tasks when agent connects
func (s *GatewayServer) deliverQueuedTasksToAgent(
	ctx context.Context,
	identity models.Identity,
	client *ClientSession,
) error {
	if s.orchestration == nil || s.orchestration.TaskService == nil {
		return nil // orchestration not initialized
	}

	if identity.Type != models.PrincipalAgent {
		return nil // Only for agents
	}

	// Get queued tasks
	queuedTasks, err := s.orchestration.TaskService.DeliverQueuedTasks(ctx, identity)
	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Msg("failed to deliver queued tasks")
		return err
	}

	if len(queuedTasks) == 0 && s.orchestration.TaskService == nil {
		return nil
	}

	if len(queuedTasks) > 0 {
		logging.Logger.Info().Int("count", len(queuedTasks)).Str("identity", identity.String()).Msg("delivering queued tasks to agent")

		// Send each task as TaskAssignment message
		for _, task := range queuedTasks {
			if _, _, err := s.prepareTaskAuthorityForDelivery(ctx, task, identity); err != nil {
				logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Str("identity", identity.String()).Msg("failed to prepare queued task authority")
				continue
			}
			assignment := &pb.TaskAssignment{
				TaskId:     task.TaskID,
				TaskType:   task.TaskType,
				TaskClass:  pb.TaskClass(task.TaskClass),
				AssignedTo: identity.String(),
				Metadata:   convertMetadataToString(task.Metadata),
				AssignedAt: task.CreatedAt.Unix(),
				Payload:    task.Payload,
			}

			err := client.SafeSend(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_TaskAssignment{
					TaskAssignment: assignment,
				},
			})

			if err != nil {
				logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Str("identity", identity.String()).Msg("failed to send task assignment")
				continue
			}

			logging.Logger.Info().Str("task_id", task.TaskID).Str("identity", identity.String()).Msg("delivered queued task")
		}
	}

	// Deliver pending pool tasks matching this agent's implementation
	poolTasks, err := s.orchestration.TaskService.DeliverPoolTasks(ctx, identity)
	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Msg("failed to deliver pool tasks")
	} else if len(poolTasks) > 0 {
		logging.Logger.Info().Int("count", len(poolTasks)).Str("identity", identity.String()).Msg("delivering pool tasks to agent")

		for _, task := range poolTasks {
			rootGrantID, currentGrantID, err := s.prepareTaskAuthorityForDelivery(ctx, task, identity)
			if err != nil {
				logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Str("identity", identity.String()).Msg("failed to prepare pool task authority")
				if unErr := s.taskStore.UnassignPoolTask(ctx, task.TaskID); unErr != nil {
					logging.Logger.Error().Err(unErr).Str("task_id", task.TaskID).Msg("failed to unassign pool task after authority preparation failure")
				}
				continue
			}
			assignment := &pb.TaskAssignment{
				TaskId:     task.TaskID,
				TaskType:   task.TaskType,
				TaskClass:  pb.TaskClass(task.TaskClass),
				AssignedTo: identity.String(),
				Metadata:   convertMetadataToString(task.Metadata),
				AssignedAt: task.CreatedAt.Unix(),
				Payload:    task.Payload,
			}

			if sendErr := client.SafeSend(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_TaskAssignment{
					TaskAssignment: assignment,
				},
			}); sendErr != nil {
				logging.Logger.Error().Err(sendErr).Str("task_id", task.TaskID).Msg("failed to send pool task assignment, unassigning")
				s.resetTaskAuthorityGrantToRoot(ctx, task, rootGrantID, currentGrantID)
				if unErr := s.taskStore.UnassignPoolTask(ctx, task.TaskID); unErr != nil {
					logging.Logger.Error().Err(unErr).Str("task_id", task.TaskID).Msg("failed to unassign pool task")
				}
				continue
			}

			client.activePoolTasks.Add(1)
			logging.Logger.Info().Str("task_id", task.TaskID).Str("identity", identity.String()).Msg("delivered pool task")
		}
	}

	return nil
}

// handleCreateTask processes CreateTaskRequest messages
func (s *GatewayServer) handleCreateTask(
	ctx context.Context,
	client *ClientSession,
	identity models.Identity,
	req *pb.CreateTaskRequest,
) error {
	taskWorkspace := req.Workspace
	if taskWorkspace == "" {
		taskWorkspace = identity.Workspace
	}

	// Extract correlation ID for optional response path.
	requestID := req.GetRequestId()

	// mintedTaskToken is populated by maybeIssueTaskToken below (only on the
	// success path, only when the caller passed target_identity AND the
	// issue-token ACL gate allowed it). The closure below captures this
	// variable so all call sites stay 6-arg and failure paths emit empty
	// task_token for free.
	var mintedTaskToken string

	// mintedAuthorityGrantID is populated after establishTaskAuthorityGrant
	// runs successfully (only when the caller passed an OBO AuthorizationContext).
	// Captured by the closure so every response carries the per-task grant
	// when one was minted, letting the creator forward it to a downstream
	// worker. Empty on failure paths and direct-call (no-OBO) creates.
	var mintedAuthorityGrantID string

	// sendCreateTaskResponse sends a CreateTaskResponse downstream only when
	// a request_id was provided, keeping fire-and-forget callers unchanged.
	sendCreateTaskResponse := func(success bool, taskID, status, errCode, errMsg, assignedTo string) {
		if requestID == "" {
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_CreateTask{
				CreateTask: &pb.CreateTaskResponse{
					Success:          success,
					TaskId:           taskID,
					Status:           status,
					ErrorCode:        errCode,
					ErrorMessage:     errMsg,
					RequestId:        requestID,
					AssignedTo:       assignedTo,
					TaskToken:        mintedTaskToken,
					AuthorityGrantId: mintedAuthorityGrantID,
				},
			},
		})
	}

	if s.orchestration == nil || s.orchestration.TaskService == nil {
		s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, "", false, "orchestration task assignment not enabled", buildTaskCreateAuditMetadata(req, "", taskWorkspace), nil)
		sendClientError(client, "ERR_TASKS_NOT_ENABLED", "orchestration task assignment not enabled")
		sendCreateTaskResponse(false, "", "", "ERR_TASKS_NOT_ENABLED", "orchestration task assignment not enabled", "")
		return &errors.InitializationError{
			Component: "TaskAssignmentService",
			Err:       fmt.Errorf("orchestration task assignment not enabled"),
		}
	}

	// Convert protobuf assignment mode to string
	var assignmentMode string
	switch req.AssignmentMode {
	case pb.TaskAssignmentMode_SELF_ASSIGN:
		assignmentMode = "self_assign"
	case pb.TaskAssignmentMode_TARGETED:
		assignmentMode = "targeted"
	case pb.TaskAssignmentMode_POOL:
		assignmentMode = "pool"
	default:
		assignmentMode = "self_assign"
	}

	// Convert launch param overrides
	launchParamOverrides := make(map[string]interface{})
	for k, v := range req.LaunchParamOverrides {
		launchParamOverrides[k] = v
	}

	// Convert metadata
	metadata := make(map[string]interface{})
	for k, v := range req.Metadata {
		metadata[k] = v
	}

	// Validate payload size
	if len(req.Payload) > 0 {
		maxSize := s.quotaEnforcer.getMaxTaskPayloadSize()
		if len(req.Payload) > maxSize {
			errMsg := fmt.Sprintf("task payload size %d exceeds maximum %d bytes", len(req.Payload), maxSize)
			s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, "", false, errMsg, buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace), nil)
			sendCreateTaskResponse(false, "", "", "TASK_PAYLOAD_TOO_LARGE", errMsg, "")
			_ = client.SafeSend(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_Error{
					Error: &pb.ErrorResponse{
						Code:    "TASK_PAYLOAD_TOO_LARGE",
						Message: fmt.Sprintf("task payload size %d exceeds maximum %d bytes", len(req.Payload), maxSize),
					},
				},
			})
			return fmt.Errorf("task payload size %d exceeds maximum %d bytes", len(req.Payload), maxSize)
		}
	}

	if taskWorkspace == "" {
		s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, "", false, "task workspace is required", buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace), nil)
		sendClientError(client, "ERR_INVALID_ARGUMENT", "task workspace is required")
		sendCreateTaskResponse(false, "", "", "ERR_INVALID_ARGUMENT", "task workspace is required", "")
		return nil
	}

	resolvedAuthority, err := s.resolveAuthorizationContext(ctx, client, identity, req.GetAuthorization())
	if err != nil {
		s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, "", false, "invalid authorization context: "+err.Error(), buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace), nil)
		sendClientError(client, "ERR_PERMISSION_DENIED", "invalid authorization context")
		sendCreateTaskResponse(false, "", "", "ERR_PERMISSION_DENIED", "invalid authorization context", "")
		return nil
	}

	// Nested task creation: when an agent currently delivering a task under a
	// task-bound authority grant calls CreateTask without an explicit
	// AuthorizationContext, auto-derive from its task grant so the new task
	// inherits the subject, root subject, and grant lineage.
	if resolvedAuthority == nil {
		inherited, inheritedErr := s.loadCallerTaskAuthority(ctx, client, identity)
		if inheritedErr != nil {
			logging.Logger.Warn().Err(inheritedErr).Str("identity", identity.String()).Msg("failed to load caller task authority for nested CreateTask")
		}
		resolvedAuthority = inherited
	}

	if s.acl != nil {
		var decision *acl.ACLDecision
		if resolvedAuthority != nil {
			decision, err = s.acl.CheckAccessWithAuthority(ctx, identity, resolvedAuthority, acl.ResourceTypeWorkspace, taskWorkspace, audit.OpTaskCreate, taskWorkspace, client.SessionUUID, acl.AccessReadWrite)
		} else {
			decision, err = s.acl.CheckAccess(ctx, identity, acl.ResourceTypeWorkspace, taskWorkspace, audit.OpTaskCreate, taskWorkspace, client.SessionUUID, acl.AccessReadWrite)
		}
		if err != nil {
			s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, "", false, "task creation ACL check failed: "+err.Error(), buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace), resolvedAuthority)
			sendClientError(client, "ERR_INTERNAL", "task creation ACL check failed")
			sendCreateTaskResponse(false, "", "", "ERR_INTERNAL", "task creation ACL check failed", "")
			return err
		}
		if decision == nil || decision.Denied() {
			reason := "not authorized to create tasks in workspace " + taskWorkspace
			if decision != nil && decision.Reason != "" {
				reason = decision.Reason
			}
			s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, "", false, reason, buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace), resolvedAuthority)
			sendClientError(client, "ERR_PERMISSION_DENIED", fmt.Sprintf("not authorized to create tasks in workspace %s", taskWorkspace))
			sendCreateTaskResponse(false, "", "", "ERR_PERMISSION_DENIED", reason, "")
			return nil
		}
	}

	// Create task request
	metadata = applyResolvedAuthorityToTaskMetadata(metadata, resolvedAuthority)
	taskReq := &orchestration.CreateTaskRequest{
		TaskType:             req.TaskType,
		TaskClass:            int32(req.TaskClass),
		Workspace:            taskWorkspace,
		AssignmentMode:       assignmentMode,
		TargetAgentID:        req.TargetAgentId,
		TargetImplementation: req.TargetImplementation,
		LaunchParamOverrides: launchParamOverrides,
		Metadata:             metadata,
		Payload:              req.Payload,
		CreatorIdentity:      identity,
		ParentTaskID:         client.AssociatedTaskID,
	}
	// Fix AA: seed the task's Authority.SubjectType/SubjectID from the resolved
	// OBO subject so downstream consumers (buildTaskContext →
	// task_context["user"]) can route responses back to the originating user
	// even before establishTaskAuthorityGrant refines the lineage with grant
	// IDs. Non-OBO direct calls leave SubjectIdentity zero-value and preserve
	// the previous no-authority-at-creation behavior.
	if resolvedAuthority != nil {
		taskReq.SubjectIdentity = models.Identity{
			Type: resolvedAuthority.Subject.Type,
			ID:   resolvedAuthority.Subject.CanonicalPrincipalID(),
		}
	}

	// Create task
	response, err := s.orchestration.TaskService.CreateTask(ctx, taskReq)
	if err != nil {
		s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, "", false, err.Error(), buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace), resolvedAuthority)
		sendClientError(client, "ERR_TASK_CREATE_FAILED", "failed to create task")
		sendCreateTaskResponse(false, "", "", "ERR_TASK_CREATE_FAILED", "failed to create task", "")
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Msg("failed to create task")
		return errors.WrapError(err, "failed to create task")
	}

	if resolvedAuthority != nil {
		taskReq.Metadata, err = s.establishTaskAuthorityGrant(ctx, response.TaskID, taskReq, response, resolvedAuthority)
		if err != nil {
			if s.orchestration != nil && s.orchestration.TaskService != nil {
				_ = s.orchestration.TaskService.CancelTask(ctx, response.TaskID)
			} else if s.taskStore != nil {
				_ = s.taskStore.CancelTask(ctx, response.TaskID)
			}
			s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, response.TaskID, false, "task authority grant setup failed: "+err.Error(), buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace), resolvedAuthority)
			sendClientError(client, "ERR_PERMISSION_DENIED", "unable to delegate task authority")
			sendCreateTaskResponse(false, "", "", "ERR_PERMISSION_DENIED", "unable to delegate task authority", "")
			return err
		}
		// Capture the freshly-minted grant ID so the response can echo it.
		// applyTaskAuthorityGrantToMetadata stamps "authority_grant_id" with
		// the GrantID of the derived child grant; that's the exact value the
		// caller needs to forward to a downstream worker (audience=task) so
		// the worker can act with this task's per-task workspace/scope.
		if v, ok := taskReq.Metadata["authority_grant_id"].(string); ok {
			mintedAuthorityGrantID = v
		}
	}

	// If the caller declared a target_identity, mint a per-task auth token
	// for the future-spawned worker. Best-effort: failures here do NOT roll
	// back the task, they just leave CreateTaskResponse.task_token empty so
	// the caller can detect denial. The mint helper enforces workspace
	// cohesion, an AccessManage gate, and a platform-workspace blocklist.
	mintedTaskToken = s.maybeIssueTaskToken(ctx, client, identity, req, response.TaskID, taskWorkspace, resolvedAuthority)

	assignmentMetadata := convertMetadataToString(taskReq.Metadata)

	successMetadata := buildTaskCreateAuditMetadata(req, assignmentMode, taskWorkspace)
	successMetadata["status"] = response.Status
	successMetadata["queued_for_startup"] = response.QueuedForStartup
	if response.AssignedTo != "" {
		successMetadata["assigned_to"] = response.AssignedTo
	}
	if response.Message != "" {
		successMetadata["message"] = response.Message
	}
	s.logTaskCreateAudit(ctx, identity, client.SessionUUID, taskWorkspace, response.TaskID, true, "", successMetadata, resolvedAuthority)

	logging.Logger.Info().Str("task_id", response.TaskID).Str("status", string(response.Status)).Str("mode", assignmentMode).Str("creator", identity.String()).Msg("task created")
	metering.TaskOperations.WithLabelValues(taskWorkspace, "create").Inc()

	// For pool tasks, try immediate delivery to an online worker
	if assignmentMode == "pool" && response.Status == "pending_pool" {
		if s.deliverPoolTaskToWorker(ctx, response.TaskID, req.TargetImplementation, taskWorkspace, req.TaskType, assignmentMetadata, req.Payload) {
			logging.Logger.Info().Str("task_id", response.TaskID).Msg("pool task delivered immediately")
		} else {
			logging.Logger.Info().Str("task_id", response.TaskID).Msg("pool task pending, no matching worker online")
		}
		sendCreateTaskResponse(true, response.TaskID, string(response.Status), "", "", response.AssignedTo)
		return nil
	}

	// For targeted tasks to online agents, send TaskAssignment immediately
	if assignmentMode == "targeted" && response.Status == "assigned" && response.AssignedTo != "" {
		// Parse target identity
		targetIdentity, err := models.ParseIdentity(response.AssignedTo)
		if err != nil {
			logging.Logger.Error().Err(err).Str("target", response.AssignedTo).Msg("failed to parse target identity")
			sendCreateTaskResponse(true, response.TaskID, string(response.Status), "", "", response.AssignedTo)
			return nil
		}

		// Get target client session
		if targetClient := s.getClientByIdentity(targetIdentity); targetClient != nil {
			assignment := &pb.TaskAssignment{
				TaskId:     response.TaskID,
				TaskType:   req.TaskType,
				TaskClass:  req.TaskClass,
				AssignedTo: response.AssignedTo,
				Metadata:   assignmentMetadata,
				AssignedAt: time.Now().Unix(),
				Payload:    req.Payload,
			}

			err := targetClient.SafeSend(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_TaskAssignment{
					TaskAssignment: assignment,
				},
			})

			if err != nil {
				logging.Logger.Error().Err(err).Str("target", response.AssignedTo).Msg("failed to send task assignment")
			} else {
				logging.Logger.Info().Str("task_id", response.TaskID).Str("target", response.AssignedTo).Msg("sent task assignment")
			}
		}
		sendCreateTaskResponse(true, response.TaskID, string(response.Status), "", "", response.AssignedTo)
		return nil
	}

	// Default success path (self_assign, pending, or other statuses)
	sendCreateTaskResponse(true, response.TaskID, string(response.Status), "", "", response.AssignedTo)
	return nil
}

func buildTaskCreateAuditMetadata(req *pb.CreateTaskRequest, assignmentMode, workspace string) map[string]interface{} {
	metadata := map[string]interface{}{
		"task_type":        req.TaskType,
		"assignment_mode":  assignmentMode,
		"workspace":        workspace,
		"payload_size":     len(req.Payload),
		"metadata_entries": len(req.Metadata),
	}
	if req.TargetAgentId != "" {
		metadata["target_agent_id"] = req.TargetAgentId
	}
	if req.TargetImplementation != "" {
		metadata["target_implementation"] = req.TargetImplementation
	}
	if len(req.LaunchParamOverrides) > 0 {
		metadata["launch_param_overrides"] = len(req.LaunchParamOverrides)
	}
	return metadata
}

func (s *GatewayServer) logTaskCreateAudit(
	ctx context.Context,
	actor models.Identity,
	sessionID uuid.UUID,
	workspace string,
	taskID string,
	success bool,
	errorMsg string,
	metadata map[string]interface{},
	authority *acl.ResolvedAuthority,
) {
	if s.auditLogger == nil {
		return
	}

	event := audit.NewTaskEvent(
		string(actor.Type),
		actor.String(),
		audit.OpTaskCreate,
		taskID,
		workspace,
		sessionID,
		success,
		errorMsg,
		metadata,
	)
	applyResolvedAuthorityToAuditEvent(event, authority)
	s.auditLogger.LogEvent(ctx, event)
}

// platformBlockedWorkspacesForTokenIssue lists workspaces that are NEVER
// allowed as token-issue targets, regardless of any ACL grant. Tokens
// minted via CreateTask.target_identity authenticate AS the declared
// identity, which is too dangerous to permit for platform-control
// workspaces even if a manage grant exists.
var platformBlockedWorkspacesForTokenIssue = map[string]struct{}{
	models.SystemWorkspace: {}, // "_system"
}

// maybeIssueTaskToken returns the plaintext task token to ship in
// CreateTaskResponse.task_token, or "" if no token should be issued.
//
// Issuance happens only when ALL of the following hold:
//  1. req.target_identity is non-empty AND parses as a known identity form;
//  2. the orchestration token store is wired;
//  3. the caller passes the ACL gate appropriate to the target principal type:
//     - Agent / Task / Bridge (workspace-having identities): caller has at
//     least AccessManage on the target's workspace under OpTaskTokenIssue,
//     and the workspace is not on the platform-blocklist.
//     - Service (workspace-less, sv::impl::spec): caller has at least
//     AccessManage on resource_type=service_impl, resource_id=<impl>
//     under OpTaskTokenIssue. Prevents arbitrary actors from forging
//     tokens for service implementations they don't own (e.g.,
//     impersonating sandbox-sidecar / memorylayer / etc.).
//
// Failures are non-fatal: the surrounding task remains created, and the
// caller learns of the denial via empty task_token + audit log.
func (s *GatewayServer) maybeIssueTaskToken(
	ctx context.Context,
	client *ClientSession,
	identity models.Identity,
	req *pb.CreateTaskRequest,
	taskID string,
	taskWorkspace string,
	resolvedAuthority *acl.ResolvedAuthority,
) string {
	targetIdentity := strings.TrimSpace(req.GetTargetIdentity())
	if targetIdentity == "" {
		return ""
	}

	if s.orchestration == nil || s.orchestration.TokenStore == nil {
		s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, "token store not wired", resolvedAuthority)
		return ""
	}

	parsed, err := models.ParseIdentity(targetIdentity)
	if err != nil {
		s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, "target_identity parse failed: "+err.Error(), resolvedAuthority)
		return ""
	}

	// Pick the ACL resource appropriate to the target principal type.
	// Workspace-having principals (Agent/Task/Bridge) gate on the target's
	// workspace. Service principals — which are workspace-less by design —
	// gate on resource_type=service_impl, resource_id=<implementation>:
	// any actor wanting to mint a Service token must hold Manage on the
	// implementation, preventing forgery of well-known service identities.
	var aclResourceType, aclResourceID string
	switch parsed.Type {
	case models.PrincipalService:
		if parsed.Implementation == "" {
			s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, "service target_identity must include an implementation", resolvedAuthority)
			return ""
		}
		aclResourceType = acl.ResourceTypeServiceImpl
		aclResourceID = parsed.Implementation
	default:
		// The workspace we gate on is the TARGET identity's workspace —
		// i.e., "the workspace this token will let the future-spawned
		// worker authenticate IN". The surrounding task's workspace can
		// legitimately differ (e.g., cowork creates a sandbox_lease task
		// in the user's workspace `default` for OBO attribution, but the
		// spawned sidecar agent lives in the system workspace
		// `_sandbox`). Earlier revisions required parsed.Workspace ==
		// taskWorkspace; that mistakenly ruled out the legitimate
		// cross-workspace use case while preserving the actual security
		// property only by accident (the manage check below is what
		// actually matters).
		targetWorkspace := parsed.Workspace
		if targetWorkspace == "" {
			s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, "target_identity must include a workspace component", resolvedAuthority)
			return ""
		}
		if _, blocked := platformBlockedWorkspacesForTokenIssue[targetWorkspace]; blocked {
			s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, "workspace is on the platform-blocklist for token issuance", resolvedAuthority)
			return ""
		}
		aclResourceType = acl.ResourceTypeWorkspace
		aclResourceID = targetWorkspace
	}

	if s.acl != nil {
		// Token issuance is an ACTOR capability ("can this caller mint a
		// credential for a third identity?"), not an on-behalf-of resource
		// access. Use plain CheckAccess on the actor's own grants — even
		// when the surrounding task carries OBO authority, the OBO
		// subject (typically an end user) shouldn't need Manage on the
		// target resource just because their request triggered some
		// infra agent to spawn a worker. The audit event still records
		// the OBO chain.
		decision, aclErr := s.acl.CheckAccess(ctx, identity, aclResourceType, aclResourceID, audit.OpTaskTokenIssue, taskWorkspace, client.SessionUUID, acl.AccessManage)
		if aclErr != nil {
			s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, "ACL check failed: "+aclErr.Error(), resolvedAuthority)
			return ""
		}
		if decision == nil || decision.Denied() {
			reason := fmt.Sprintf("AccessManage required on %s=%q to mint task token", aclResourceType, aclResourceID)
			if decision != nil && decision.Reason != "" {
				reason = decision.Reason
			}
			s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, reason, resolvedAuthority)
			return ""
		}
	}

	token, err := s.orchestration.TokenStore.GenerateToken(ctx, taskID, targetIdentity, taskWorkspace, identity.String())
	if err != nil {
		s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, false, "token store error: "+err.Error(), resolvedAuthority)
		return ""
	}

	s.logTaskTokenIssueAudit(ctx, identity, client.SessionUUID, taskWorkspace, taskID, targetIdentity, true, "", resolvedAuthority)
	return token.Token
}

// logTaskTokenIssueAudit records a task-token-mint attempt in the audit log,
// success OR failure. Failures are visible separately from OpTaskCreate so
// reviewers can spot escalation attempts (e.g., target_identity in a
// workspace the caller can write to but not manage) without trawling
// unrelated task-create denials.
func (s *GatewayServer) logTaskTokenIssueAudit(
	ctx context.Context,
	actor models.Identity,
	sessionID uuid.UUID,
	workspace string,
	taskID string,
	targetIdentity string,
	success bool,
	errorMsg string,
	authority *acl.ResolvedAuthority,
) {
	if s.auditLogger == nil {
		return
	}

	metadata := map[string]interface{}{
		"workspace":       workspace,
		"target_identity": targetIdentity,
	}
	event := audit.NewTaskEvent(
		string(actor.Type),
		actor.String(),
		audit.OpTaskTokenIssue,
		taskID,
		workspace,
		sessionID,
		success,
		errorMsg,
		metadata,
	)
	applyResolvedAuthorityToAuditEvent(event, authority)
	s.auditLogger.LogEvent(ctx, event)
}

// getClientByIdentity retrieves active client by identity using the O(1) identity index.
func (s *GatewayServer) getClientByIdentity(identity models.Identity) *ClientSession {
	identityStr := identity.String()
	sessionID, ok := s.identityIndex.Load(identityStr)
	if !ok {
		return nil
	}
	client, ok := s.activeStreams.Load(sessionID)
	if !ok {
		// Stale index entry - clean up and return nil
		s.identityIndex.Delete(identityStr)
		return nil
	}
	return client.(*ClientSession)
}

// convertMetadataToString converts map[string]interface{} to map[string]string
func convertMetadataToString(metadata map[string]interface{}) map[string]string {
	if metadata == nil {
		return nil
	}
	result := make(map[string]string)
	for k, v := range metadata {
		if str, ok := v.(string); ok {
			result[k] = str
		}
	}
	return result
}

// configureOrchestratorDispatcher sets up the callback for the orchestrator task dispatcher
func (s *GatewayServer) configureOrchestratorDispatcher() {
	if s.orchestration == nil || s.orchestration.Dispatcher == nil {
		return
	}

	// Set the callback to deliver tasks to connected orchestrators
	s.orchestration.Dispatcher.SetCallback(func(task *orchestration.OrchestrationTaskNotification) {
		s.deliverTaskToOrchestrator(task)
	})

	logging.Logger.Debug().Msg("orchestrator task dispatcher callback configured")
}

// deliverTaskToOrchestrator delivers an orchestrated task to a matching orchestrator.
// This method is safe to call from multiple gateways - only one will successfully claim and deliver.
func (s *GatewayServer) deliverTaskToOrchestrator(task *orchestration.OrchestrationTaskNotification) {
	if task == nil {
		return
	}

	ctx := context.Background()

	// Step 1: Try to claim the task FIRST (before any other work)
	// This is the distributed lock - only one gateway will succeed
	orchestratorClient := s.findOrchestratorByProfile(task.Profile, task.Workspace)
	if orchestratorClient == nil {
		// No matching orchestrator on this gateway - another gateway might have one
		// Don't log as error since this is expected in multi-gateway setup
		logging.Logger.Debug().Str("profile", task.Profile).Str("task_id", task.TaskID).Msg("no local orchestrator for profile, another gateway may handle it")
		return
	}

	// Step 2: Attempt to claim the task atomically
	err := s.orchestration.Dispatcher.ClaimTask(ctx, task.QueueID, orchestratorClient.Identity.String())
	if err == orchestration.ErrTaskAlreadyClaimed {
		// Another gateway already claimed this task - this is normal in multi-gateway setup
		logging.Logger.Debug().Str("task_id", task.TaskID).Msg("task already claimed by another gateway")
		return
	}
	if err != nil {
		logging.Logger.Error().Err(err).Str("queue_id", task.QueueID).Msg("failed to claim task")
		return
	}

	// Step 3: We have the claim - now get full task details
	taskDetails, err := s.orchestration.Dispatcher.GetTaskDetails(ctx, task.QueueID)
	if err != nil {
		logging.Logger.Error().Err(err).Str("queue_id", task.QueueID).Msg("failed to get task details")
		// Unclaim so another gateway can try. The error path here is
		// non-fatal; the claim TTL also expires it, so we just log.
		if unclaimErr := s.orchestration.Dispatcher.UnclaimTask(ctx, task.QueueID); unclaimErr != nil {
			logging.Logger.Warn().Err(unclaimErr).Str("queue_id", task.QueueID).Msg("failed to unclaim task after GetTaskDetails error; relying on claim TTL")
		}
		return
	}

	// Step 4: Convert launch params to string map for protobuf
	launchParamsStr := make(map[string]string)
	for k, v := range taskDetails.LaunchParams {
		if str, ok := v.(string); ok {
			launchParamsStr[k] = str
		}
	}

	// Step 5: Resolve the target specifier for the agent.
	//
	// For per-principal singleton agents (e.g. per-user CoworkAgent instances),
	// the specifier is stamped on the Task row as TargetSpecifier at creation
	// time (see createOrchestratedStartupTask). The orchestrated_task_queue
	// table does not carry specifier, so we read it from the Task record.
	//
	// Fallback order:
	//   1. Task.TargetSpecifier (stamped by createOrchestratedStartupTask)
	//   2. launch_params["specifier"] (legacy / admin-supplied override)
	//   3. "default" (legacy single-agent-per-workspace behavior)
	specifier := "default"
	if taskRow, terr := s.taskStore.GetTask(ctx, task.TaskID); terr == nil && taskRow != nil && taskRow.TargetSpecifier != "" {
		specifier = taskRow.TargetSpecifier
	} else if spec, ok := launchParamsStr["specifier"]; ok && spec != "" {
		specifier = spec
	}
	targetAgentIdentity, terr := models.AgentTopic(task.Workspace, task.TargetImplementation, specifier)
	if terr != nil {
		logging.Logger.Error().Err(terr).Str("task_id", task.TaskID).Str("workspace", task.Workspace).Str("implementation", task.TargetImplementation).Str("specifier", specifier).Msg("invalid target agent identity; unclaiming task")
		if unclaimErr := s.orchestration.Dispatcher.UnclaimTask(ctx, task.QueueID); unclaimErr != nil {
			logging.Logger.Warn().Err(unclaimErr).Str("queue_id", task.QueueID).Msg("failed to unclaim task after invalid target identity; relying on claim TTL")
		}
		return
	}

	// Step 5.5: Generate auth token for the agent that will be launched
	if s.orchestration.TokenStore != nil {
		token, err := s.orchestration.TokenStore.GenerateToken(
			ctx,
			task.TaskID,
			targetAgentIdentity,
			task.Workspace,
			orchestratorClient.Identity.String(),
		)
		if err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", task.TaskID).Msg("failed to generate auth token")
			// Continue without token - token auth is optional layer
		} else {
			// Inject token into launch params - orchestrator will pass to agent
			launchParamsStr["auth_token"] = token.Token
			logging.Logger.Info().Str("agent", targetAgentIdentity).Str("task_id", task.TaskID).Msg("generated auth token for agent")
		}
	}

	// Step 6: Create TaskAssignment message for the orchestrator
	assignment := &pb.TaskAssignment{
		TaskId:               task.TaskID,
		TaskType:             "agent_startup",
		AssignedTo:           orchestratorClient.Identity.String(),
		Profile:              task.Profile,
		TargetImplementation: task.TargetImplementation,
		Workspace:            task.Workspace,
		Specifier:            specifier,
		LaunchParams:         launchParamsStr,
		AssignedAt:           time.Now().Unix(),
	}

	// Step 7: Send to orchestrator via gRPC stream
	err = orchestratorClient.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskAssignment{
			TaskAssignment: assignment,
		},
	})

	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", task.TaskID).Str("orchestrator", orchestratorClient.Identity.String()).Msg("failed to send task to orchestrator")
		// Unclaim the task so another gateway/orchestrator can retry
		if unclaimErr := s.orchestration.Dispatcher.UnclaimTask(ctx, task.QueueID); unclaimErr != nil {
			logging.Logger.Error().Err(unclaimErr).Str("queue_id", task.QueueID).Msg("failed to unclaim task for retry")
			// Mark as failed if we can't unclaim. If FailTask itself errors,
			// the claim will still expire via TTL and the row stays in the
			// failed/retry pipeline.
			if failErr := s.orchestration.Dispatcher.FailTask(ctx, task.QueueID, "delivery failed, unclaim failed: "+err.Error()); failErr != nil {
				logging.Logger.Error().Err(failErr).Str("queue_id", task.QueueID).Msg("failed to mark task failed after unclaim failure; relying on claim TTL")
			}
		}
		return
	}

	logging.Logger.Info().Str("task_id", task.TaskID).Str("orchestrator", orchestratorClient.Identity.String()).Str("profile", task.Profile).Msg("delivered orchestrated task")

	// Step 8: Mark task as assigned to the orchestrator
	if err := s.taskStore.AssignTask(ctx, task.TaskID, orchestratorClient.Identity.String()); err != nil {
		logging.Logger.Warn().Err(err).Str("task_id", task.TaskID).Msg("failed to mark task as assigned")
		// Non-fatal - task was delivered successfully
	}
}

// registerOrchestratorInIndex adds an orchestrator to the orchestratorIndex for all
// profiles it supports. profiles must be the same list passed to ProfileManager.RegisterProfiles.
// The orchestrator's supported profiles are also stored on the session for clean removal.
func (s *GatewayServer) registerOrchestratorInIndex(cs *ClientSession, profiles []string) {
	if len(profiles) == 0 {
		return
	}
	cs.orchestratorProfiles = profiles

	workspace := cs.Identity.Workspace
	if workspace == "" {
		workspace = models.SystemWorkspace
	}

	s.orchestratorIndexMu.Lock()
	for _, profile := range profiles {
		key := workspace + ":" + profile
		s.orchestratorIndex[key] = append(s.orchestratorIndex[key], cs)
	}
	s.orchestratorIndexMu.Unlock()
}

// unregisterOrchestratorFromIndex removes an orchestrator from the orchestratorIndex.
func (s *GatewayServer) unregisterOrchestratorFromIndex(cs *ClientSession) {
	if len(cs.orchestratorProfiles) == 0 {
		return
	}

	workspace := cs.Identity.Workspace
	if workspace == "" {
		workspace = models.SystemWorkspace
	}

	s.orchestratorIndexMu.Lock()
	for _, profile := range cs.orchestratorProfiles {
		key := workspace + ":" + profile
		clients := s.orchestratorIndex[key]
		for i, c := range clients {
			if c == cs {
				s.orchestratorIndex[key] = append(clients[:i], clients[i+1:]...)
				break
			}
		}
		if len(s.orchestratorIndex[key]) == 0 {
			delete(s.orchestratorIndex, key)
		}
	}
	s.orchestratorIndexMu.Unlock()
}

// findOrchestratorByProfile finds a connected orchestrator that supports the given profile.
// Checks the orchestratorIndex for O(1) lookup first. For global orchestrators (workspace "_system"),
// also checks the system-workspace index when no workspace-specific match is found.
// Falls back to the full O(n) scan if the index yields no live session (stale entry guard).
func (s *GatewayServer) findOrchestratorByProfile(profile, workspace string) *ClientSession {
	// Helper: look up a live orchestrator from the index for a given workspace key.
	lookupInIndex := func(ws string) *ClientSession {
		key := ws + ":" + profile
		s.orchestratorIndexMu.RLock()
		clients := s.orchestratorIndex[key]
		// Copy slice under lock to avoid holding the lock during identity checks.
		snapshot := make([]*ClientSession, len(clients))
		copy(snapshot, clients)
		s.orchestratorIndexMu.RUnlock()

		for _, c := range snapshot {
			if c != nil {
				return c
			}
		}
		return nil
	}

	// 1. Try workspace-specific orchestrators first.
	if cs := lookupInIndex(workspace); cs != nil {
		return cs
	}

	// 2. Try global orchestrators (system workspace can handle any workspace).
	if workspace != models.SystemWorkspace {
		if cs := lookupInIndex(models.SystemWorkspace); cs != nil {
			return cs
		}
	}

	// 3. Fallback: full O(n) scan (safety net for stale index or unindexed orchestrators).
	var matchingOrchestrator *ClientSession
	s.activeStreams.Range(func(key, value interface{}) bool {
		client, ok := value.(*ClientSession)
		if !ok {
			return true
		}
		if client.Identity.Type != models.PrincipalOrchestrator {
			return true
		}

		orchWorkspace := client.Identity.Workspace
		isGlobalOrchestrator := orchWorkspace == "" || orchWorkspace == models.SystemWorkspace
		if !isGlobalOrchestrator && orchWorkspace != workspace {
			return true
		}

		ctx := context.Background()
		supports, err := s.orchestration.Registry.OrchestratorSupportsProfile(
			ctx,
			client.Identity.String(),
			profile,
		)
		if err != nil {
			logging.Logger.Error().Err(err).Str("identity", client.Identity.String()).Msg("error checking profile support")
			return true
		}
		if supports {
			matchingOrchestrator = client
			return false
		}
		return true
	})
	return matchingOrchestrator
}

// =============================================================================
// Implementation Index (for pool task routing)
// =============================================================================

// addToImplIndex adds an agent client to the implementation index.
func (s *GatewayServer) addToImplIndex(identity models.Identity, client *ClientSession) {
	key := identity.Workspace + ":" + identity.Implementation
	s.implIndexMu.Lock()
	s.implementationIndex[key] = append(s.implementationIndex[key], client)
	s.implIndexMu.Unlock()
}

// removeFromImplIndex removes an agent client from the implementation index.
func (s *GatewayServer) removeFromImplIndex(identity models.Identity, client *ClientSession) {
	key := identity.Workspace + ":" + identity.Implementation
	s.implIndexMu.Lock()
	clients := s.implementationIndex[key]
	for i, c := range clients {
		if c == client {
			s.implementationIndex[key] = append(clients[:i], clients[i+1:]...)
			break
		}
	}
	if len(s.implementationIndex[key]) == 0 {
		delete(s.implementationIndex, key)
	}
	s.implIndexMu.Unlock()
}

// findWorkerByImplementation finds an online agent matching the given implementation
// in the specified workspace. Uses power-of-two-choices load balancing: picks 2 random
// candidates and selects the one with fewer active pool tasks.
func (s *GatewayServer) findWorkerByImplementation(implementation, workspace string) *ClientSession {
	key := workspace + ":" + implementation
	s.implIndexMu.RLock()
	clients := s.implementationIndex[key]
	n := len(clients)
	if n == 0 {
		s.implIndexMu.RUnlock()
		return nil
	}
	// Copy references under lock, then release
	if n == 1 {
		c := clients[0]
		s.implIndexMu.RUnlock()
		return c
	}
	// Power-of-two-choices: pick 2 random candidates
	i := rand.IntN(n)
	j := rand.IntN(n - 1)
	if j >= i {
		j++
	}
	c1, c2 := clients[i], clients[j]
	s.implIndexMu.RUnlock()

	if c1.activePoolTasks.Load() <= c2.activePoolTasks.Load() {
		return c1
	}
	return c2
}

// =============================================================================
// Pool Task Delivery
// =============================================================================

// deliverPoolTaskToWorker attempts to find an online worker and deliver a pool task.
// Returns true if the task was successfully delivered.
func (s *GatewayServer) deliverPoolTaskToWorker(ctx context.Context, taskID, targetImpl, workspace, taskType string, metadata map[string]string, payload []byte) bool {
	worker := s.findWorkerByImplementation(targetImpl, workspace)
	if worker == nil {
		return false
	}

	// Atomically claim
	claimed, err := s.taskStore.ClaimPoolTask(ctx, taskID, worker.Identity.String())
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to claim pool task")
		return false
	}
	if !claimed {
		return false // another gateway claimed it
	}

	task := &tasks.ExtendedTask{
		TaskID:         taskID,
		TaskType:       taskType,
		Workspace:      workspace,
		AssignmentMode: tasks.AssignmentModePool,
		Metadata:       make(map[string]interface{}, len(metadata)),
		Payload:        payload,
	}
	for k, v := range metadata {
		task.Metadata[k] = v
	}
	rootGrantID, currentGrantID, err := s.prepareTaskAuthorityForDelivery(ctx, task, worker.Identity)
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Str("worker", worker.Identity.String()).Msg("failed to prepare pool task authority")
		if unErr := s.taskStore.UnassignPoolTask(ctx, taskID); unErr != nil {
			logging.Logger.Error().Err(unErr).Str("task_id", taskID).Msg("failed to unassign pool task")
		}
		return false
	}

	assignment := &pb.TaskAssignment{
		TaskId:     taskID,
		TaskType:   taskType,
		AssignedTo: worker.Identity.String(),
		Metadata:   convertMetadataToString(task.Metadata),
		AssignedAt: time.Now().Unix(),
		Payload:    payload,
	}

	err = worker.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskAssignment{
			TaskAssignment: assignment,
		},
	})
	if err != nil {
		logging.Logger.Error().Err(err).Str("task_id", taskID).Str("worker", worker.Identity.String()).Msg("failed to send pool task assignment, unassigning")
		s.resetTaskAuthorityGrantToRoot(ctx, task, rootGrantID, currentGrantID)
		if unErr := s.taskStore.UnassignPoolTask(ctx, taskID); unErr != nil {
			logging.Logger.Error().Err(unErr).Str("task_id", taskID).Msg("failed to unassign pool task")
		}
		return false
	}

	worker.activePoolTasks.Add(1)
	logging.Logger.Info().Str("task_id", taskID).Str("worker", worker.Identity.String()).Msg("delivered pool task to worker")
	return true
}

// CleanupOrchestration closes orchestration resources
func (s *GatewayServer) CleanupOrchestration() {
	if s.orchestration != nil {
		if s.orchestration.Dispatcher != nil {
			s.orchestration.Dispatcher.Stop()
		}
		if s.orchestration.QueueCloser != nil {
			s.orchestration.QueueCloser.Close()
		}
	}
}
