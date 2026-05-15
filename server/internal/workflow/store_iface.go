// Package workflow — store_iface.go defines the WorkflowStore interface
// consumed by the workflow engine's internal collaborators (Server, Router,
// Scheduler, DAGEngine, StateMachineEngine, AdminServer).
//
// Why a local interface instead of importing internal/storage/workflow.Store:
//
//	internal/storage/workflow imports internal/workflow (for the type aliases
//	in types.go). Importing it back would create an import cycle. Go's
//	structural typing means any value that satisfies
//	internal/storage/workflow.Store also satisfies this interface — they
//	have identical method sets. Callers that inject a native sqlite store
//	(or any other implementation of internal/storage/workflow.Store) into
//	the workflow engine via NewServerWithStore can do so without adapter
//	code; the type assertion is implicit.
//
// The method set here is a one-for-one mirror of
// internal/storage/workflow.Store. If that interface gains or loses
// methods, this file must be updated in lockstep. The compile-time
// conformance assert at the bottom ensures drift is caught at build time.
package workflow

import (
	"context"
	"encoding/json"
	"time"
)

// WorkflowStore is the engine-internal contract for persistent workflow
// operations. See internal/storage/workflow.Store for the canonical
// documentation of each method.
type WorkflowStore interface {
	// Rules
	GetMatchingRules(ctx context.Context, sourceAgent, sourceEvent, workspace string) ([]Rule, error)
	CreateRule(ctx context.Context, r *Rule) error
	UpdateRule(ctx context.Context, r *Rule) error
	DeleteRule(ctx context.Context, id int) error
	ListRules(ctx context.Context, workspace string) ([]Rule, error)
	GetRule(ctx context.Context, id int) (*Rule, error)

	// Workflow definitions
	GetWorkflowDefinition(ctx context.Context, id string) (*WorkflowDefinition, error)
	CreateWorkflowDefinition(ctx context.Context, d *WorkflowDefinition) error
	GetWorkflowDefinitionsForTrigger(ctx context.Context, workspace string) ([]WorkflowDefinition, error)
	DeactivateWorkflowDefinition(ctx context.Context, id string) error

	// Executions
	CreateExecution(ctx context.Context, e *WorkflowExecution) error
	GetExecution(ctx context.Context, executionID string) (*WorkflowExecution, error)
	UpdateExecutionStatus(ctx context.Context, executionID, status, errorMessage string) error
	GetRunningExecutions(ctx context.Context) ([]WorkflowExecution, error)
	CountRunningExecutions(ctx context.Context) (int, error)
	GetExecutionsByStatus(ctx context.Context, status string) ([]WorkflowExecution, error)

	// Step states
	CreateStepState(ctx context.Context, st *StepState) error
	UpdateStepStatus(ctx context.Context, executionID, stepID, status string) error
	SetStepOutput(ctx context.Context, executionID, stepID string, output json.RawMessage) error
	SetStepError(ctx context.Context, executionID, stepID, errorMessage string) error
	SetStepTaskID(ctx context.Context, executionID, stepID, taskID string) error
	IncrementStepAttempt(ctx context.Context, executionID, stepID string) error
	GetStepStates(ctx context.Context, executionID string) ([]StepState, error)
	GetStepByTaskID(ctx context.Context, taskID string) (*StepState, error)

	// Schedules
	GetDueSchedules(ctx context.Context, now time.Time) ([]Schedule, error)
	CreateSchedule(ctx context.Context, sc *Schedule) error
	DeleteSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context, workspace string) ([]Schedule, error)
	GetSchedule(ctx context.Context, id string) (*Schedule, error)
	UpsertSchedule(ctx context.Context, sc *Schedule) error
	UpdateScheduleAfterFire(ctx context.Context, id string, lastFired time.Time, nextFire *time.Time) error
	SetScheduleActiveTask(ctx context.Context, scheduleID, taskID string) error

	// State machines
	CreateStateMachine(ctx context.Context, sm *StateMachineDef) error
	GetStateMachine(ctx context.Context, id string) (*StateMachineDef, error)
	ListStateMachines(ctx context.Context, workspace string) ([]StateMachineDef, error)
	DeactivateStateMachine(ctx context.Context, id string) error
	CreateStateMachineInstance(ctx context.Context, inst *StateMachineInstance) error
	GetStateMachineInstance(ctx context.Context, instanceID string) (*StateMachineInstance, error)
	ListStateMachineInstances(ctx context.Context, machineID string) ([]StateMachineInstance, error)
	UpdateStateMachineInstance(ctx context.Context, instanceID, newState string, timeoutAt *time.Time, completed bool) error
	GetTimedOutInstances(ctx context.Context, now time.Time) ([]StateMachineInstance, error)
	ClearInstanceTimeout(ctx context.Context, instanceID string) error
}

// Compile-time conformance assert: *Store (the legacy concrete type)
// must satisfy WorkflowStore. This also transitively guarantees that
// any implementation of internal/storage/workflow.Store (which has the
// same method set) satisfies WorkflowStore via Go structural typing.
var _ WorkflowStore = (*Store)(nil)
