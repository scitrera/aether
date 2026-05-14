// Package workflow defines the storage interface for the workflow subsystem
// (rules, definitions, executions, step states, schedules, state machines).
//
// Stage 1 consumers (callers that depend on this interface today):
//   - internal/workflow/server.go — constructs the underlying *workflow.Store
//     via NewStore(db, isSQLite) inside workflow.Server.initialize(); the
//     gateway and aetherlite reach the workflow subsystem through
//     workflow.NewServer, never the store directly. Stage 1 is therefore an
//     in-package re-home with no construction-site churn at the cmd/* layer.
//   - internal/workflow/{scheduler,executor,statemachine,admin,...}.go —
//     internal collaborators that take *workflow.Store today; the new
//     interface is added alongside without renaming the concrete handle so
//     none of those collaborators have to change in this stage.
//
// The interface mirrors the legacy *internal/workflow.Store method set
// one-for-one. This is the mechanical-extraction phase of the storage
// refactor described in `.slop/20260513_native-storage-interfaces.md`
// §2/§3/§13 and the Stage 0 decisions in
// `.slop/20260514_storage_interfaces_stage0.md` §11.
//
// Why workflow is the trailing Stage 1 domain (§13.5): the legacy package
// was already "partly factored" — workflow.db is a separate SQLite file in
// lite mode, the package owns its own migrations under
// `internal/workflow/migrations/` (postgres) and
// `internal/workflow/migrations/sqlite/` (sqlite), and the existing
// constructor `workflow.NewStore(db *sql.DB, isSQLite bool)` already
// branches on backend internally. The Store interface in this file
// preserves that ergonomic exactly; Stage 2 is what eventually splits the
// concrete impl into postgres + sqlite siblings that each drop the
// isSQLite flag.
//
// Note on the isSQLite bool: the underlying legacy.Store uses the flag for
// inline dialect branching (notably the `FOR UPDATE SKIP LOCKED` suffix on
// GetDueSchedules, which postgres supports and sqlite does not). Stage 1
// keeps the flag on postgres.New for that reason — postgres callers pass
// `false`, lite callers pass `true`, and dbcompat continues to translate
// the postgres-flavored SQL the impl emits. Stage 2 introduces sibling
// packages `internal/storage/workflow/postgres` and
// `internal/storage/workflow/sqlite` that each construct the same store
// without needing the flag; the interface contract here is unchanged
// across that transition.
package workflow

import (
	"context"
	"encoding/json"
	"time"
)

// Store is the workflow-subsystem surface consumed by the workflow.Server
// and its internal collaborators (scheduler, executor, statemachine,
// admin). It groups every persistent operation the workflow engine needs:
// CRUD on rules and definitions, execution and per-step state machines,
// cron/interval/once/event-delayed schedules, and standalone state-machine
// instances.
//
// Nil-tolerance policy (§14.1 of the storage-interfaces plan): callers
// MUST pass a non-nil implementation. The workflow.Server holds the Store
// for the lifetime of the process and dereferences it from many code
// paths (scheduler tick, executor step transitions, admin HTTP handlers).
// A nil Store at runtime is a class-A crash — there is no defensible
// opt-out mode for workflow. No NoOp impl is provided in this domain.
//
// Methods are grouped to match the legacy package's organization, which
// also matches the underlying table set:
//   - workflow_rules               → Rules
//   - workflow_definitions         → Workflow definitions
//   - workflow_executions          → Executions
//   - workflow_step_states         → Step states
//   - workflow_schedules           → Schedules
//   - workflow_state_machines      → State machines (definitions)
//   - workflow_state_machine_instances → State machines (instances)
type Store interface {
	// =========================================================================
	// Rules — workflow_rules table
	// =========================================================================

	// GetMatchingRules returns the active rules whose trigger matches the
	// given (sourceAgent, sourceEvent, workspace) tuple, ordered by
	// priority DESC then id ASC. Wildcard '*' values for source_agent and
	// workspace participate in the match, matching the legacy semantics.
	GetMatchingRules(ctx context.Context, sourceAgent, sourceEvent, workspace string) ([]Rule, error)

	// CreateRule inserts a new rule row and populates r.ID, r.CreatedAt,
	// r.UpdatedAt from the RETURNING clause.
	CreateRule(ctx context.Context, r *Rule) error

	// UpdateRule writes r's mutable columns back to the row identified by
	// r.ID. CreatedAt/UpdatedAt are not modified by this call (UpdatedAt
	// is maintained by a DB trigger).
	UpdateRule(ctx context.Context, r *Rule) error

	// DeleteRule removes the rule row with the given id. Idempotent
	// against missing rows (DELETE returns 0 affected; not an error).
	DeleteRule(ctx context.Context, id int) error

	// ListRules returns every rule visible to the given workspace —
	// workspace-scoped rules plus wildcard '*' rules — ordered by
	// priority DESC then id ASC.
	ListRules(ctx context.Context, workspace string) ([]Rule, error)

	// GetRule fetches a single rule by id. Returns (nil, nil) when the row
	// is absent (sql.ErrNoRows is intentionally not surfaced).
	GetRule(ctx context.Context, id int) (*Rule, error)

	// =========================================================================
	// Workflow definitions — workflow_definitions table
	// =========================================================================

	// GetWorkflowDefinition returns the highest-version active definition
	// for the given id, or (nil, nil) when no active row exists.
	GetWorkflowDefinition(ctx context.Context, id string) (*WorkflowDefinition, error)

	// CreateWorkflowDefinition inserts a new definition version and
	// populates d.CreatedAt and d.UpdatedAt from the RETURNING clause.
	CreateWorkflowDefinition(ctx context.Context, d *WorkflowDefinition) error

	// GetWorkflowDefinitionsForTrigger returns the latest active version
	// of every definition visible to the given workspace (DISTINCT ON id,
	// ORDER BY id, version DESC).
	GetWorkflowDefinitionsForTrigger(ctx context.Context, workspace string) ([]WorkflowDefinition, error)

	// DeactivateWorkflowDefinition sets active=false on every version of
	// the given id; the row is retained for audit / historical replay.
	DeactivateWorkflowDefinition(ctx context.Context, id string) error

	// =========================================================================
	// Executions — workflow_executions table
	// =========================================================================

	// CreateExecution inserts a new execution row and populates
	// e.StartedAt from the RETURNING clause.
	CreateExecution(ctx context.Context, e *WorkflowExecution) error

	// GetExecution returns the execution with the given executionID, or
	// (nil, nil) when the row is absent.
	GetExecution(ctx context.Context, executionID string) (*WorkflowExecution, error)

	// UpdateExecutionStatus transitions an execution to the given status.
	// Terminal statuses (completed, failed, cancelled) also stamp
	// completed_at to NOW(); non-terminal transitions leave it untouched.
	UpdateExecutionStatus(ctx context.Context, executionID, status, errorMessage string) error

	// GetRunningExecutions returns every execution in the 'running'
	// status, ordered by started_at ASC. Used by the executor on startup
	// to resume in-flight work.
	GetRunningExecutions(ctx context.Context) ([]WorkflowExecution, error)

	// CountRunningExecutions returns the number of executions currently
	// in the 'running' status. Used for admission control by the executor.
	CountRunningExecutions(ctx context.Context) (int, error)

	// GetExecutionsByStatus returns up to 100 executions in the given
	// status, ordered by started_at DESC. Used by the admin API.
	GetExecutionsByStatus(ctx context.Context, status string) ([]WorkflowExecution, error)

	// =========================================================================
	// Step states — workflow_step_states table
	// =========================================================================

	// CreateStepState inserts a new step-state row for an execution.
	// Callers populate ExecutionID, StepID, Status, InputData, Attempt;
	// timestamps and output/error are filled by subsequent transitions.
	CreateStepState(ctx context.Context, st *StepState) error

	// UpdateStepStatus transitions a step's status and stamps the
	// appropriate timestamp column (started_at on Running; completed_at
	// on Completed/Failed/Skipped; neither on other states).
	UpdateStepStatus(ctx context.Context, executionID, stepID, status string) error

	// SetStepOutput marks a step as completed, stamping completed_at and
	// writing the output payload in a single UPDATE.
	SetStepOutput(ctx context.Context, executionID, stepID string, output json.RawMessage) error

	// SetStepError marks a step as failed, stamping completed_at and
	// writing the error message in a single UPDATE.
	SetStepError(ctx context.Context, executionID, stepID, errorMessage string) error

	// SetStepTaskID associates the orchestrated task spawned for this
	// step with the step row, so the dispatcher can later look up the
	// step by task id (see GetStepByTaskID).
	SetStepTaskID(ctx context.Context, executionID, stepID, taskID string) error

	// IncrementStepAttempt resets a step row for retry: attempt++,
	// status='pending', started_at/completed_at/error_message all NULL.
	IncrementStepAttempt(ctx context.Context, executionID, stepID string) error

	// GetStepStates returns every step-state row for the given execution,
	// ordered by step_id.
	GetStepStates(ctx context.Context, executionID string) ([]StepState, error)

	// GetStepByTaskID returns the step-state row tied to the given task
	// id, or (nil, nil) when no row matches.
	GetStepByTaskID(ctx context.Context, taskID string) (*StepState, error)

	// =========================================================================
	// Schedules — workflow_schedules table
	// =========================================================================

	// GetDueSchedules returns every enabled schedule whose next_fire_at
	// is <= now, ordered by next_fire_at ASC. On postgres the query is
	// suffixed with `FOR UPDATE SKIP LOCKED` so multiple gateway
	// instances can shard the work safely; on sqlite the suffix is
	// omitted (single-writer).
	GetDueSchedules(ctx context.Context, now time.Time) ([]Schedule, error)

	// CreateSchedule inserts a new schedule row and populates sc.CreatedAt
	// and sc.UpdatedAt from the RETURNING clause.
	CreateSchedule(ctx context.Context, sc *Schedule) error

	// DeleteSchedule removes the schedule row with the given id.
	// Idempotent against missing rows.
	DeleteSchedule(ctx context.Context, id string) error

	// ListSchedules returns every schedule visible to the given workspace
	// — workspace-scoped plus wildcard '*' — ordered by name ASC.
	ListSchedules(ctx context.Context, workspace string) ([]Schedule, error)

	// GetSchedule returns the schedule with the given id, or (nil, nil)
	// when the row is absent.
	GetSchedule(ctx context.Context, id string) (*Schedule, error)

	// UpsertSchedule inserts a new schedule row, or updates the existing
	// row with the same id, preserving created_at across updates.
	// Populates sc.CreatedAt and sc.UpdatedAt from the RETURNING clause.
	UpsertSchedule(ctx context.Context, sc *Schedule) error

	// UpdateScheduleAfterFire stamps last_fired_at and rolls next_fire_at
	// forward (or to NULL for one-shot schedules) after a successful fire.
	UpdateScheduleAfterFire(ctx context.Context, id string, lastFired time.Time, nextFire *time.Time) error

	// SetScheduleActiveTask records the task id currently running for the
	// given schedule (NULL-equivalent when taskID is ""). Used by the
	// max_concurrent=1 enforcement path.
	SetScheduleActiveTask(ctx context.Context, scheduleID, taskID string) error

	// =========================================================================
	// State machines — workflow_state_machines + workflow_state_machine_instances
	// =========================================================================

	// CreateStateMachine inserts a new state-machine definition and
	// populates sm.CreatedAt and sm.UpdatedAt from the RETURNING clause.
	CreateStateMachine(ctx context.Context, sm *StateMachineDef) error

	// GetStateMachine returns the active state-machine definition for the
	// given id, or (nil, nil) when no active row exists.
	GetStateMachine(ctx context.Context, id string) (*StateMachineDef, error)

	// ListStateMachines returns every active state-machine definition
	// visible to the given workspace — workspace-scoped plus wildcard
	// '*' — ordered by id.
	ListStateMachines(ctx context.Context, workspace string) ([]StateMachineDef, error)

	// DeactivateStateMachine sets active=false on the given state-machine
	// definition; the row is retained for audit / historical replay.
	DeactivateStateMachine(ctx context.Context, id string) error

	// CreateStateMachineInstance inserts a new running instance of a
	// state machine and populates inst.CreatedAt and inst.UpdatedAt from
	// the RETURNING clause.
	CreateStateMachineInstance(ctx context.Context, inst *StateMachineInstance) error

	// GetStateMachineInstance returns the instance with the given
	// instanceID, or (nil, nil) when the row is absent.
	GetStateMachineInstance(ctx context.Context, instanceID string) (*StateMachineInstance, error)

	// ListStateMachineInstances returns up to 100 instances of the given
	// machine, ordered by created_at DESC.
	ListStateMachineInstances(ctx context.Context, machineID string) ([]StateMachineInstance, error)

	// UpdateStateMachineInstance writes a state transition: sets
	// current_state and timeout_at, and stamps completed_at to NOW() if
	// the completed flag is true.
	UpdateStateMachineInstance(ctx context.Context, instanceID, newState string, timeoutAt *time.Time, completed bool) error

	// GetTimedOutInstances returns every non-completed instance whose
	// timeout_at is <= now, ordered by timeout_at ASC. Used by the
	// timeout sweeper.
	GetTimedOutInstances(ctx context.Context, now time.Time) ([]StateMachineInstance, error)

	// ClearInstanceTimeout sets timeout_at = NULL on the given instance.
	// Used when a transition removes the timeout obligation.
	ClearInstanceTimeout(ctx context.Context, instanceID string) error
}
