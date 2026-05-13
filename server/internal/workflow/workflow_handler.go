package workflow

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
)

// handleWorkflowOperation processes a WorkflowOperation forwarded from the gateway.
// It delegates to existing store/engine methods and returns a WorkflowResponse.
func (s *Server) handleWorkflowOperation(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	switch op.Op {
	// ---- Rules ----
	case pb.WorkflowOperation_LIST_RULES:
		return s.handleListRules(ctx, op)
	case pb.WorkflowOperation_GET_RULE:
		return s.handleGetRule(ctx, op)
	case pb.WorkflowOperation_CREATE_RULE:
		return s.handleCreateRule(ctx, op)
	case pb.WorkflowOperation_UPDATE_RULE:
		return s.handleUpdateRule(ctx, op)
	case pb.WorkflowOperation_DELETE_RULE:
		return s.handleDeleteRule(ctx, op)

	// ---- Workflow Definitions ----
	case pb.WorkflowOperation_LIST_WORKFLOWS:
		return s.handleListWorkflows(ctx, op)
	case pb.WorkflowOperation_GET_WORKFLOW:
		return s.handleGetWorkflow(ctx, op)
	case pb.WorkflowOperation_CREATE_WORKFLOW:
		return s.handleCreateWorkflow(ctx, op)
	case pb.WorkflowOperation_DELETE_WORKFLOW:
		return s.handleDeleteWorkflow(ctx, op)

	// ---- Schedules ----
	case pb.WorkflowOperation_LIST_SCHEDULES:
		return s.handleListSchedules(ctx, op)
	case pb.WorkflowOperation_CREATE_SCHEDULE:
		return s.handleCreateSchedule(ctx, op)
	case pb.WorkflowOperation_DELETE_SCHEDULE:
		return s.handleDeleteSchedule(ctx, op)
	case pb.WorkflowOperation_UPSERT_SCHEDULE:
		return s.handleUpsertSchedule(ctx, op)

	// ---- Executions ----
	case pb.WorkflowOperation_LIST_EXECUTIONS:
		return s.handleListExecutions(ctx, op)
	case pb.WorkflowOperation_GET_EXECUTION:
		return s.handleGetExecution(ctx, op)
	case pb.WorkflowOperation_CANCEL_EXECUTION:
		return s.handleCancelExecution(ctx, op)

	// ---- State Machines ----
	case pb.WorkflowOperation_LIST_STATE_MACHINES:
		return s.handleListStateMachines(ctx, op)
	case pb.WorkflowOperation_GET_STATE_MACHINE:
		return s.handleGetStateMachine(ctx, op)
	case pb.WorkflowOperation_CREATE_STATE_MACHINE:
		return s.handleCreateStateMachine(ctx, op)
	case pb.WorkflowOperation_DELETE_STATE_MACHINE:
		return s.handleDeleteStateMachine(ctx, op)

	// ---- State Machine Instances ----
	case pb.WorkflowOperation_LIST_SM_INSTANCES:
		return s.handleListSMInstances(ctx, op)
	case pb.WorkflowOperation_GET_SM_INSTANCE:
		return s.handleGetSMInstance(ctx, op)
	case pb.WorkflowOperation_CREATE_SM_INSTANCE:
		return s.handleCreateSMInstance(ctx, op)
	case pb.WorkflowOperation_SEND_SM_EVENT:
		return s.handleSendSMEvent(ctx, op)

	default:
		return &pb.WorkflowResponse{
			Success:   false,
			Error:     "unknown workflow operation",
			RequestId: op.RequestId,
		}, nil
	}
}

// jsonResponse is a helper that marshals data into a WorkflowResponse.
func jsonResponse(requestID string, data any) (*pb.WorkflowResponse, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return &pb.WorkflowResponse{
			Success:   false,
			Error:     "failed to marshal response: " + err.Error(),
			RequestId: requestID,
		}, nil
	}
	return &pb.WorkflowResponse{
		Success:   true,
		Data:      b,
		RequestId: requestID,
	}, nil
}

func errResponse(requestID, msg string) *pb.WorkflowResponse {
	return &pb.WorkflowResponse{
		Success:   false,
		Error:     msg,
		RequestId: requestID,
	}
}

func okResponse(requestID, msg string) *pb.WorkflowResponse {
	return &pb.WorkflowResponse{
		Success:   true,
		Message:   msg,
		RequestId: requestID,
	}
}

// =============================================================================
// Rules
// =============================================================================

func (s *Server) handleListRules(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	workspace := op.Workspace
	if workspace == "" {
		workspace = "*"
	}
	rules, err := s.store.ListRules(ctx, workspace)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if rules == nil {
		rules = []Rule{}
	}
	return jsonResponse(op.RequestId, rules)
}

func (s *Server) handleGetRule(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	id, err := strconv.Atoi(op.Id)
	if err != nil {
		return errResponse(op.RequestId, "invalid rule ID"), nil
	}
	rule, err := s.store.GetRule(ctx, id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if rule == nil {
		return errResponse(op.RequestId, "rule not found"), nil
	}
	return jsonResponse(op.RequestId, rule)
}

func (s *Server) handleCreateRule(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	var rule Rule
	if err := json.Unmarshal(op.Data, &rule); err != nil {
		return errResponse(op.RequestId, "invalid JSON: "+err.Error()), nil
	}
	if rule.RuleName == "" || rule.SourceEvent == "" || rule.DestinationTemplate == "" {
		return errResponse(op.RequestId, "rule_name, source_event, and destination_template are required"), nil
	}
	if rule.SourceAgent == "" {
		rule.SourceAgent = "*"
	}
	if rule.Workspace == "" {
		rule.Workspace = "*"
	}
	if rule.TransformationStyle == "" {
		rule.TransformationStyle = "template-yaml"
	}
	rule.Active = true

	if err := s.store.CreateRule(ctx, &rule); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	s.router.InvalidateCache()
	return jsonResponse(op.RequestId, rule)
}

func (s *Server) handleUpdateRule(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	id, err := strconv.Atoi(op.Id)
	if err != nil {
		return errResponse(op.RequestId, "invalid rule ID"), nil
	}
	var rule Rule
	if err := json.Unmarshal(op.Data, &rule); err != nil {
		return errResponse(op.RequestId, "invalid JSON: "+err.Error()), nil
	}
	rule.ID = id
	if err := s.store.UpdateRule(ctx, &rule); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	s.router.InvalidateCache()
	return jsonResponse(op.RequestId, rule)
}

func (s *Server) handleDeleteRule(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	id, err := strconv.Atoi(op.Id)
	if err != nil {
		return errResponse(op.RequestId, "invalid rule ID"), nil
	}
	if err := s.store.DeleteRule(ctx, id); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	s.router.InvalidateCache()
	return okResponse(op.RequestId, "rule deleted"), nil
}

// =============================================================================
// Workflow Definitions
// =============================================================================

func (s *Server) handleListWorkflows(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	workspace := op.Workspace
	if workspace == "" {
		workspace = "*"
	}
	defs, err := s.store.GetWorkflowDefinitionsForTrigger(ctx, workspace)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if defs == nil {
		defs = []WorkflowDefinition{}
	}
	return jsonResponse(op.RequestId, defs)
}

func (s *Server) handleGetWorkflow(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	def, err := s.store.GetWorkflowDefinition(ctx, op.Id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if def == nil {
		return errResponse(op.RequestId, "workflow not found"), nil
	}
	return jsonResponse(op.RequestId, def)
}

func (s *Server) handleCreateWorkflow(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	var def WorkflowDefinition
	if err := json.Unmarshal(op.Data, &def); err != nil {
		return errResponse(op.RequestId, "invalid JSON: "+err.Error()), nil
	}
	if def.ID == "" {
		return errResponse(op.RequestId, "id is required"), nil
	}
	if len(def.Definition) == 0 {
		return errResponse(op.RequestId, "definition is required"), nil
	}
	var dagDef DAGDefinition
	if err := json.Unmarshal(def.Definition, &dagDef); err != nil {
		return errResponse(op.RequestId, "invalid DAG definition: "+err.Error()), nil
	}
	if def.Workspace == "" {
		def.Workspace = "*"
	}
	if def.Version == 0 {
		def.Version = 1
	}
	def.Active = true

	if err := s.store.CreateWorkflowDefinition(ctx, &def); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return jsonResponse(op.RequestId, def)
}

func (s *Server) handleDeleteWorkflow(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	if err := s.store.DeactivateWorkflowDefinition(ctx, op.Id); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return okResponse(op.RequestId, "workflow deactivated"), nil
}

// =============================================================================
// Schedules
// =============================================================================

func (s *Server) handleListSchedules(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	workspace := op.Workspace
	if workspace == "" {
		workspace = "*"
	}
	schedules, err := s.store.ListSchedules(ctx, workspace)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if schedules == nil {
		schedules = []Schedule{}
	}
	return jsonResponse(op.RequestId, schedules)
}

func (s *Server) handleCreateSchedule(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	var sc Schedule
	if err := json.Unmarshal(op.Data, &sc); err != nil {
		return errResponse(op.RequestId, "invalid JSON: "+err.Error()), nil
	}
	if sc.ID == "" || sc.Name == "" || sc.ScheduleType == "" || sc.ScheduleExpr == "" {
		return errResponse(op.RequestId, "id, name, schedule_type, and schedule_expr are required"), nil
	}
	if len(sc.Action) == 0 && sc.WorkflowID == "" {
		return errResponse(op.RequestId, "action or workflow_id is required"), nil
	}
	if sc.Workspace == "" {
		sc.Workspace = "*"
	}
	if sc.MissPolicy == "" {
		sc.MissPolicy = "skip"
	}
	sc.Enabled = true

	nextFire, err := s.scheduler.ComputeInitialNextFire(sc.ScheduleType, sc.ScheduleExpr)
	if err != nil {
		return errResponse(op.RequestId, "invalid schedule expression: "+err.Error()), nil
	}
	sc.NextFireAt = nextFire

	if err := s.store.CreateSchedule(ctx, &sc); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return jsonResponse(op.RequestId, sc)
}

func (s *Server) handleDeleteSchedule(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	if err := s.store.DeleteSchedule(ctx, op.Id); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return okResponse(op.RequestId, "schedule deleted"), nil
}

func (s *Server) handleUpsertSchedule(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	var sc Schedule
	if err := json.Unmarshal(op.Data, &sc); err != nil {
		return errResponse(op.RequestId, "invalid JSON: "+err.Error()), nil
	}
	if sc.ID == "" || sc.Name == "" || sc.ScheduleType == "" || sc.ScheduleExpr == "" {
		return errResponse(op.RequestId, "id, name, schedule_type, and schedule_expr are required"), nil
	}
	if len(sc.Action) == 0 && sc.WorkflowID == "" {
		return errResponse(op.RequestId, "action or workflow_id is required"), nil
	}
	if sc.Workspace == "" {
		sc.Workspace = "*"
	}
	if sc.MissPolicy == "" {
		sc.MissPolicy = "skip"
	}
	sc.Enabled = true

	nextFire, err := s.scheduler.ComputeInitialNextFire(sc.ScheduleType, sc.ScheduleExpr)
	if err != nil {
		return errResponse(op.RequestId, "invalid schedule expression: "+err.Error()), nil
	}
	sc.NextFireAt = nextFire

	if err := s.store.UpsertSchedule(ctx, &sc); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return jsonResponse(op.RequestId, sc)
}

// =============================================================================
// Executions
// =============================================================================

func (s *Server) handleListExecutions(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	statusFilter := op.StatusFilter
	var execs []WorkflowExecution
	var err error
	if statusFilter == "running" || statusFilter == "" {
		execs, err = s.store.GetRunningExecutions(ctx)
	} else {
		execs, err = s.store.GetExecutionsByStatus(ctx, statusFilter)
	}
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if execs == nil {
		execs = []WorkflowExecution{}
	}
	return jsonResponse(op.RequestId, execs)
}

func (s *Server) handleGetExecution(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	exec, err := s.store.GetExecution(ctx, op.Id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if exec == nil {
		return errResponse(op.RequestId, "execution not found"), nil
	}
	steps, err := s.store.GetStepStates(ctx, op.Id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return jsonResponse(op.RequestId, map[string]any{
		"execution": exec,
		"steps":     steps,
	})
}

func (s *Server) handleCancelExecution(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	exec, err := s.store.GetExecution(ctx, op.Id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if exec == nil {
		return errResponse(op.RequestId, "execution not found"), nil
	}
	if exec.Status != ExecStatusRunning {
		return errResponse(op.RequestId, "execution is not running"), nil
	}
	if err := s.store.UpdateExecutionStatus(ctx, op.Id, ExecStatusCancelled, "cancelled via gRPC stream"); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return okResponse(op.RequestId, "execution cancelled"), nil
}

// =============================================================================
// State Machines
// =============================================================================

func (s *Server) handleListStateMachines(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	workspace := op.Workspace
	if workspace == "" {
		workspace = "*"
	}
	machines, err := s.store.ListStateMachines(ctx, workspace)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if machines == nil {
		machines = []StateMachineDef{}
	}
	return jsonResponse(op.RequestId, machines)
}

func (s *Server) handleGetStateMachine(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	sm, err := s.store.GetStateMachine(ctx, op.Id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if sm == nil {
		return errResponse(op.RequestId, "state machine not found"), nil
	}
	return jsonResponse(op.RequestId, sm)
}

func (s *Server) handleCreateStateMachine(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	var sm StateMachineDef
	if err := json.Unmarshal(op.Data, &sm); err != nil {
		return errResponse(op.RequestId, "invalid JSON: "+err.Error()), nil
	}
	if sm.ID == "" || len(sm.Definition) == 0 {
		return errResponse(op.RequestId, "id and definition are required"), nil
	}
	var def StateMachineDefinition
	if err := json.Unmarshal(sm.Definition, &def); err != nil {
		return errResponse(op.RequestId, "invalid state machine definition: "+err.Error()), nil
	}
	if def.InitialState == "" {
		return errResponse(op.RequestId, "initial_state is required in definition"), nil
	}
	if sm.Workspace == "" {
		sm.Workspace = "*"
	}
	sm.Active = true

	if err := s.store.CreateStateMachine(ctx, &sm); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return jsonResponse(op.RequestId, sm)
}

func (s *Server) handleDeleteStateMachine(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	if err := s.store.DeactivateStateMachine(ctx, op.Id); err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return okResponse(op.RequestId, "state machine deactivated"), nil
}

// =============================================================================
// State Machine Instances
// =============================================================================

func (s *Server) handleListSMInstances(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	instances, err := s.store.ListStateMachineInstances(ctx, op.Id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if instances == nil {
		instances = []StateMachineInstance{}
	}
	return jsonResponse(op.RequestId, instances)
}

func (s *Server) handleGetSMInstance(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	instance, err := s.store.GetStateMachineInstance(ctx, op.Id)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	if instance == nil {
		return errResponse(op.RequestId, "instance not found"), nil
	}
	return jsonResponse(op.RequestId, instance)
}

func (s *Server) handleCreateSMInstance(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	var body struct {
		InstanceID string          `json:"instance_id"`
		MachineID  string          `json:"machine_id"`
		Workspace  string          `json:"workspace"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(op.Data, &body); err != nil {
		return errResponse(op.RequestId, "invalid JSON: "+err.Error()), nil
	}
	if body.InstanceID == "" {
		return errResponse(op.RequestId, "instance_id is required"), nil
	}
	machineID := body.MachineID
	if machineID == "" {
		machineID = op.Id // fallback to op.Id for the machine ID
	}
	if body.Workspace == "" {
		body.Workspace = "*"
	}

	instance, err := s.stateMach.CreateInstance(ctx, machineID, body.InstanceID, body.Workspace, body.Data)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	return jsonResponse(op.RequestId, instance)
}

func (s *Server) handleSendSMEvent(ctx context.Context, op *pb.WorkflowOperation) (*pb.WorkflowResponse, error) {
	instanceID := op.Id
	eventName := op.SecondaryId

	// Also support JSON body with event and data
	var eventData json.RawMessage
	if len(op.Data) > 0 {
		var body struct {
			Event string          `json:"event"`
			Data  json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(op.Data, &body); err == nil {
			if body.Event != "" {
				eventName = body.Event
			}
			eventData = body.Data
		}
	}

	if eventName == "" {
		return errResponse(op.RequestId, "event name is required (set secondary_id or event in data)"), nil
	}

	instance, err := s.stateMach.SendEvent(ctx, instanceID, eventName, eventData)
	if err != nil {
		return errResponse(op.RequestId, err.Error()), nil
	}
	log.Debug().Str("instance_id", instanceID).Str("event", eventName).Msg("state machine event sent via gRPC stream")
	return jsonResponse(op.RequestId, instance)
}
