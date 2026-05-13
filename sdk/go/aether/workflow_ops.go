// Package aether workflow operations for the Go SDK.
//
// This file provides WorkflowOps, a helper for managing workflow resources
// (rules, workflow definitions, schedules, executions, state machines) via
// the Aether gateway's WorkflowOperation protocol.
//
// Operations can be performed in two modes:
//   - Async: Fire-and-forget via SendOp; responses delivered to OnWorkflowResponse
//   - Sync: Blocking via SendOpSync; waits for correlated response with timeout

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultWorkflowTimeout is the default timeout for synchronous workflow operations.
const DefaultWorkflowTimeout = 10 * time.Second

// WorkflowOps provides workflow management operations on a client.
type WorkflowOps struct {
	client *BaseClient
	syncMu sync.Mutex // serializes synchronous workflow operations
}

// newWorkflowOps creates a new WorkflowOps helper for a client.
func newWorkflowOps(client *BaseClient) *WorkflowOps {
	return &WorkflowOps{client: client}
}

// =============================================================================
// Async Operation
// =============================================================================

// SendOp sends a workflow operation asynchronously.
// The response is delivered via the OnWorkflowResponse handler callback.
func (w *WorkflowOps) SendOp(op *pb.WorkflowOperation) error {
	return w.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_WorkflowOp{WorkflowOp: op},
	})
}

// =============================================================================
// Synchronous Operation
// =============================================================================

// SendOpSync sends a workflow operation and waits for the correlated response.
func (w *WorkflowOps) SendOpSync(ctx context.Context, op *pb.WorkflowOperation, timeout time.Duration) (*WorkflowResponse, error) {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultWorkflowTimeout
	}

	requestID := w.client.NextRequestID()
	op.RequestId = requestID
	ch := w.client.RegisterPendingWorkflowRequest(requestID)
	defer w.client.pendingWorkflowRequests.Delete(requestID)

	if err := w.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("workflow operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// =============================================================================
// Convenience Methods - Rules
// =============================================================================

// ListRules lists all workflow rules for a workspace.
func (w *WorkflowOps) ListRules(ctx context.Context, workspace string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:        pb.WorkflowOperation_LIST_RULES,
		Workspace: workspace,
	}, 0)
}

// GetRule retrieves a specific workflow rule by ID.
func (w *WorkflowOps) GetRule(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_GET_RULE,
		Id: id,
	}, 0)
}

// CreateRule creates a new workflow rule from JSON data.
func (w *WorkflowOps) CreateRule(ctx context.Context, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:   pb.WorkflowOperation_CREATE_RULE,
		Data: data,
	}, 0)
}

// UpdateRule updates an existing workflow rule by ID with JSON data.
func (w *WorkflowOps) UpdateRule(ctx context.Context, id string, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:   pb.WorkflowOperation_UPDATE_RULE,
		Id:   id,
		Data: data,
	}, 0)
}

// DeleteRule deletes a workflow rule by ID.
func (w *WorkflowOps) DeleteRule(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_DELETE_RULE,
		Id: id,
	}, 0)
}

// =============================================================================
// Convenience Methods - Workflow Definitions
// =============================================================================

// ListWorkflows lists all workflow definitions for a workspace.
func (w *WorkflowOps) ListWorkflows(ctx context.Context, workspace string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:        pb.WorkflowOperation_LIST_WORKFLOWS,
		Workspace: workspace,
	}, 0)
}

// GetWorkflow retrieves a specific workflow definition by ID.
func (w *WorkflowOps) GetWorkflow(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_GET_WORKFLOW,
		Id: id,
	}, 0)
}

// CreateWorkflow creates a new workflow definition from JSON data.
func (w *WorkflowOps) CreateWorkflow(ctx context.Context, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:   pb.WorkflowOperation_CREATE_WORKFLOW,
		Data: data,
	}, 0)
}

// DeleteWorkflow deletes a workflow definition by ID.
func (w *WorkflowOps) DeleteWorkflow(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_DELETE_WORKFLOW,
		Id: id,
	}, 0)
}

// =============================================================================
// Convenience Methods - Schedules
// =============================================================================

// ListSchedules lists all schedules for a workspace.
func (w *WorkflowOps) ListSchedules(ctx context.Context, workspace string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:        pb.WorkflowOperation_LIST_SCHEDULES,
		Workspace: workspace,
	}, 0)
}

// CreateSchedule creates a new schedule from JSON data.
func (w *WorkflowOps) CreateSchedule(ctx context.Context, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:   pb.WorkflowOperation_CREATE_SCHEDULE,
		Data: data,
	}, 0)
}

// UpsertSchedule creates or updates a schedule idempotently from JSON data.
// If a schedule with the given ID exists, its configuration is updated but
// next_fire_at and last_fired_at are preserved.
func (w *WorkflowOps) UpsertSchedule(ctx context.Context, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:   pb.WorkflowOperation_UPSERT_SCHEDULE,
		Data: data,
	}, 0)
}

// DeleteSchedule deletes a schedule by ID.
func (w *WorkflowOps) DeleteSchedule(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_DELETE_SCHEDULE,
		Id: id,
	}, 0)
}

// =============================================================================
// Convenience Methods - Executions
// =============================================================================

// ListExecutions lists workflow executions for a workspace, optionally filtered by status.
func (w *WorkflowOps) ListExecutions(ctx context.Context, workspace, statusFilter string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:           pb.WorkflowOperation_LIST_EXECUTIONS,
		Workspace:    workspace,
		StatusFilter: statusFilter,
	}, 0)
}

// GetExecution retrieves a specific workflow execution by ID.
func (w *WorkflowOps) GetExecution(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_GET_EXECUTION,
		Id: id,
	}, 0)
}

// CancelExecution cancels a running workflow execution by ID.
func (w *WorkflowOps) CancelExecution(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_CANCEL_EXECUTION,
		Id: id,
	}, 0)
}

// =============================================================================
// Convenience Methods - State Machines
// =============================================================================

// ListStateMachines lists all state machine definitions for a workspace.
func (w *WorkflowOps) ListStateMachines(ctx context.Context, workspace string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:        pb.WorkflowOperation_LIST_STATE_MACHINES,
		Workspace: workspace,
	}, 0)
}

// GetStateMachine retrieves a specific state machine definition by ID.
func (w *WorkflowOps) GetStateMachine(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_GET_STATE_MACHINE,
		Id: id,
	}, 0)
}

// CreateStateMachine creates a new state machine definition from JSON data.
func (w *WorkflowOps) CreateStateMachine(ctx context.Context, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:   pb.WorkflowOperation_CREATE_STATE_MACHINE,
		Data: data,
	}, 0)
}

// DeleteStateMachine deletes a state machine definition by ID.
func (w *WorkflowOps) DeleteStateMachine(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_DELETE_STATE_MACHINE,
		Id: id,
	}, 0)
}

// =============================================================================
// Convenience Methods - State Machine Instances
// =============================================================================

// ListSMInstances lists all state machine instances for a definition.
func (w *WorkflowOps) ListSMInstances(ctx context.Context, definitionID string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_LIST_SM_INSTANCES,
		Id: definitionID,
	}, 0)
}

// GetSMInstance retrieves a specific state machine instance by ID.
func (w *WorkflowOps) GetSMInstance(ctx context.Context, id string) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op: pb.WorkflowOperation_GET_SM_INSTANCE,
		Id: id,
	}, 0)
}

// CreateSMInstance creates a new state machine instance from JSON data.
func (w *WorkflowOps) CreateSMInstance(ctx context.Context, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:   pb.WorkflowOperation_CREATE_SM_INSTANCE,
		Data: data,
	}, 0)
}

// SendSMEvent sends an event to a state machine instance.
// instanceID is the instance to target, eventName is the event (in SecondaryId), data is optional payload.
func (w *WorkflowOps) SendSMEvent(ctx context.Context, instanceID, eventName string, data []byte) (*WorkflowResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkflowOperation{
		Op:          pb.WorkflowOperation_SEND_SM_EVENT,
		Id:          instanceID,
		SecondaryId: eventName,
		Data:        data,
	}, 0)
}
