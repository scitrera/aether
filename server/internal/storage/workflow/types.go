package workflow

// This file re-exports the shared workflow types and constants from the
// legacy internal/workflow package under the new internal/storage/workflow
// interface namespace. The legacy package remains the source of truth
// during Stage 1 of the storage-interfaces refactor; Stage 2 will
// introduce a native sqlite sibling and may eventually let us collapse
// the legacy types into this package. For now, downstream callers can
//
//	import "github.com/scitrera/aether/internal/storage/workflow"
//
// and find every type and constant they need to read or write workflow
// rows — no double-import of the legacy package required.

import (
	legacy "github.com/scitrera/aether/internal/workflow"
)

// Core types — aliased so a single import gets callers everything they
// need to interact with the workflow Store.
type (
	// Rule is a workflow_rules row. See legacy.Rule for field docs.
	Rule = legacy.Rule
	// WorkflowDefinition is a workflow_definitions row.
	WorkflowDefinition = legacy.WorkflowDefinition
	// WorkflowExecution is a workflow_executions row.
	WorkflowExecution = legacy.WorkflowExecution
	// StepState is a workflow_step_states row.
	StepState = legacy.StepState
	// Schedule is a workflow_schedules row.
	Schedule = legacy.Schedule
	// StateMachineDef is a workflow_state_machines row.
	StateMachineDef = legacy.StateMachineDef
	// StateMachineInstance is a workflow_state_machine_instances row.
	StateMachineInstance = legacy.StateMachineInstance
)

// Execution status values — values that land in
// workflow_executions.status. Mirrors the legacy package's canonical set.
const (
	ExecStatusRunning   = legacy.ExecStatusRunning
	ExecStatusCompleted = legacy.ExecStatusCompleted
	ExecStatusFailed    = legacy.ExecStatusFailed
	ExecStatusCancelled = legacy.ExecStatusCancelled
)

// Step status values — values that land in workflow_step_states.status.
const (
	StepStatusPending   = legacy.StepStatusPending
	StepStatusRunning   = legacy.StepStatusRunning
	StepStatusCompleted = legacy.StepStatusCompleted
	StepStatusFailed    = legacy.StepStatusFailed
	StepStatusSkipped   = legacy.StepStatusSkipped
)

// Schedule type values — values that land in
// workflow_schedules.schedule_type.
const (
	ScheduleTypeCron         = legacy.ScheduleTypeCron
	ScheduleTypeInterval     = legacy.ScheduleTypeInterval
	ScheduleTypeOnce         = legacy.ScheduleTypeOnce
	ScheduleTypeEventDelayed = legacy.ScheduleTypeEventDelayed
)
