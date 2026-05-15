package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// =============================================================================
// DAG Definition Types (parsed from JSONB)
// =============================================================================

type DAGDefinition struct {
	Name    string     `json:"name" yaml:"name"`
	Version int        `json:"version" yaml:"version"`
	Trigger DAGTrigger `json:"trigger" yaml:"trigger"`
	Steps   []DAGStep  `json:"steps" yaml:"steps"`
}

type DAGTrigger struct {
	Event       string `json:"event" yaml:"event"`
	SourceAgent string `json:"source_agent" yaml:"source_agent"`
	Condition   string `json:"condition" yaml:"condition"`
}

type DAGStep struct {
	ID        string      `json:"id" yaml:"id"`
	Action    ActionDef   `json:"action" yaml:"action"`
	OnSuccess string      `json:"on_success" yaml:"on_success"`
	OnFailure string      `json:"on_failure" yaml:"on_failure"`
	DependsOn []string    `json:"depends_on" yaml:"depends_on"`
	Timeout   string      `json:"timeout" yaml:"timeout"`
	Retry     RetryConfig `json:"retry" yaml:"retry"`
}

type RetryConfig struct {
	MaxAttempts int    `json:"max_attempts" yaml:"max_attempts"`
	Backoff     string `json:"backoff" yaml:"backoff"` // constant, linear, exponential
}

// =============================================================================
// DAG Engine
// =============================================================================

type DAGEngine struct {
	store            WorkflowStore
	expr             *ExprEngine
	tmpl             *TemplateEngine
	executor         *Executor
	stepTimeout      time.Duration
	dagTimeout       time.Duration
	maxConcurrentExe int
}

func NewDAGEngine(store WorkflowStore, expr *ExprEngine, tmpl *TemplateEngine, executor *Executor, cfg *WorkflowConfig) *DAGEngine {
	return &DAGEngine{
		store:            store,
		expr:             expr,
		tmpl:             tmpl,
		executor:         executor,
		stepTimeout:      cfg.GetStepDefaultTimeout(),
		dagTimeout:       cfg.GetDAGDefaultTimeout(),
		maxConcurrentExe: cfg.GetMaxConcurrentExecutions(),
	}
}

// StartExecution creates a new DAG execution from a workflow definition.
func (d *DAGEngine) StartExecution(ctx context.Context, workflowID, workspace string, triggerData json.RawMessage) (string, error) {
	// Check concurrent execution limit
	count, err := d.store.CountRunningExecutions(ctx)
	if err != nil {
		return "", fmt.Errorf("count running executions: %w", err)
	}
	if count >= d.maxConcurrentExe {
		return "", fmt.Errorf("max concurrent executions reached (%d)", d.maxConcurrentExe)
	}

	// Get workflow definition
	def, err := d.store.GetWorkflowDefinition(ctx, workflowID)
	if err != nil {
		return "", fmt.Errorf("get workflow definition: %w", err)
	}
	if def == nil {
		return "", fmt.Errorf("workflow definition not found: %s", workflowID)
	}

	// Parse DAG definition
	var dagDef DAGDefinition
	if err := json.Unmarshal(def.Definition, &dagDef); err != nil {
		return "", fmt.Errorf("parse DAG definition: %w", err)
	}

	// Create execution record
	executionID := uuid.New().String()
	exec := &WorkflowExecution{
		ExecutionID:     executionID,
		WorkflowID:      workflowID,
		WorkflowVersion: def.Version,
		Workspace:       workspace,
		Status:          ExecStatusRunning,
		TriggerData:     triggerData,
	}
	if err := d.store.CreateExecution(ctx, exec); err != nil {
		return "", fmt.Errorf("create execution: %w", err)
	}

	// Create initial step states
	for _, step := range dagDef.Steps {
		st := &StepState{
			ExecutionID: executionID,
			StepID:      step.ID,
			Status:      StepStatusPending,
			Attempt:     1,
		}
		if err := d.store.CreateStepState(ctx, st); err != nil {
			return "", fmt.Errorf("create step state for %s: %w", step.ID, err)
		}
	}

	log.Info().
		Str("execution_id", executionID).
		Str("workflow_id", workflowID).
		Int("steps", len(dagDef.Steps)).
		Msg("DAG execution started")

	// Advance: run initial steps (those with no dependencies)
	if err := d.advanceExecution(ctx, executionID, &dagDef, triggerData); err != nil {
		log.Error().Err(err).Str("execution_id", executionID).Msg("failed to advance initial steps")
	}

	return executionID, nil
}

// ProcessStepCompletion handles a step completing (success or failure)
// and advances the DAG to the next steps.
func (d *DAGEngine) ProcessStepCompletion(ctx context.Context, executionID, stepID string, success bool, output json.RawMessage, errorMsg string) error {
	if success {
		if err := d.store.SetStepOutput(ctx, executionID, stepID, output); err != nil {
			return fmt.Errorf("set step output: %w", err)
		}
	} else {
		if err := d.store.SetStepError(ctx, executionID, stepID, errorMsg); err != nil {
			return fmt.Errorf("set step error: %w", err)
		}
	}

	// Get execution to find workflow definition
	exec, err := d.store.GetExecution(ctx, executionID)
	if err != nil {
		return fmt.Errorf("get execution: %w", err)
	}
	if exec == nil || exec.Status != ExecStatusRunning {
		return nil // Execution already completed or cancelled
	}

	def, err := d.store.GetWorkflowDefinition(ctx, exec.WorkflowID)
	if err != nil {
		return fmt.Errorf("get workflow definition: %w", err)
	}
	if def == nil {
		return fmt.Errorf("workflow definition not found: %s", exec.WorkflowID)
	}

	var dagDef DAGDefinition
	if err := json.Unmarshal(def.Definition, &dagDef); err != nil {
		return fmt.Errorf("parse DAG definition: %w", err)
	}

	// Handle failure with retry
	if !success {
		step := d.findStep(&dagDef, stepID)
		if step != nil && step.Retry.MaxAttempts > 0 {
			stepStates, _ := d.store.GetStepStates(ctx, executionID)
			for _, ss := range stepStates {
				if ss.StepID == stepID && ss.Attempt < step.Retry.MaxAttempts {
					log.Info().
						Str("execution_id", executionID).
						Str("step_id", stepID).
						Int("attempt", ss.Attempt+1).
						Msg("retrying step")
					if err := d.store.IncrementStepAttempt(ctx, executionID, stepID); err != nil {
						log.Warn().Err(err).Str("execution_id", executionID).Str("step_id", stepID).Msg("failed to increment step attempt; retry counter may drift")
					}
					return d.advanceExecution(ctx, executionID, &dagDef, exec.TriggerData)
				}
			}
		}

		// Check for on_failure handler
		if step != nil && step.OnFailure != "" {
			return d.advanceExecution(ctx, executionID, &dagDef, exec.TriggerData)
		}

		// No retry, no handler: fail the execution
		log.Warn().
			Str("execution_id", executionID).
			Str("step_id", stepID).
			Str("error", errorMsg).
			Msg("step failed, failing execution")
		return d.store.UpdateExecutionStatus(ctx, executionID, ExecStatusFailed, fmt.Sprintf("step %s failed: %s", stepID, errorMsg))
	}

	return d.advanceExecution(ctx, executionID, &dagDef, exec.TriggerData)
}

// MonitorExecutions checks running executions for timeouts and advances stalled DAGs.
func (d *DAGEngine) MonitorExecutions(ctx context.Context) error {
	execs, err := d.store.GetRunningExecutions(ctx)
	if err != nil {
		return err
	}

	for _, exec := range execs {
		// Check DAG-level timeout
		if time.Since(exec.StartedAt) > d.dagTimeout {
			log.Warn().
				Str("execution_id", exec.ExecutionID).
				Dur("elapsed", time.Since(exec.StartedAt)).
				Msg("DAG execution timed out")
			if err := d.store.UpdateExecutionStatus(ctx, exec.ExecutionID, ExecStatusFailed, "DAG execution timed out"); err != nil {
				log.Warn().Err(err).Str("execution_id", exec.ExecutionID).Msg("failed to mark DAG execution timed-out; will retry on next monitor tick")
			}
			continue
		}

		// Check for step-level timeouts
		steps, err := d.store.GetStepStates(ctx, exec.ExecutionID)
		if err != nil {
			log.Error().Err(err).Str("execution_id", exec.ExecutionID).Msg("failed to get step states")
			continue
		}

		for _, step := range steps {
			if step.Status == StepStatusRunning && step.StartedAt != nil {
				if time.Since(*step.StartedAt) > d.stepTimeout {
					log.Warn().
						Str("execution_id", exec.ExecutionID).
						Str("step_id", step.StepID).
						Msg("step timed out")
					if err := d.store.SetStepError(ctx, exec.ExecutionID, step.StepID, "step timed out"); err != nil {
						log.Warn().Err(err).Str("execution_id", exec.ExecutionID).Str("step_id", step.StepID).Msg("failed to record step timeout; will retry on next monitor tick")
					}
				}
			}
		}

		// Check if execution is complete
		d.checkExecutionComplete(ctx, exec.ExecutionID, steps)
	}

	return nil
}

// advanceExecution finds ready steps and dispatches their actions.
func (d *DAGEngine) advanceExecution(ctx context.Context, executionID string, dagDef *DAGDefinition, triggerData json.RawMessage) error {
	steps, err := d.store.GetStepStates(ctx, executionID)
	if err != nil {
		return err
	}

	stepMap := make(map[string]*StepState, len(steps))
	for i := range steps {
		stepMap[steps[i].StepID] = &steps[i]
	}

	// Build step outputs map for template interpolation. Both Unmarshal
	// calls treat malformed JSON as "no value": template expressions then
	// see a nil under that step, which the template engine renders as
	// empty. We log on failure so the operator can correlate template
	// misses with bad upstream payloads.
	stepOutputs := make(map[string]any)
	for _, s := range steps {
		if s.Status == StepStatusCompleted && len(s.OutputData) > 0 {
			var out any
			if err := json.Unmarshal(s.OutputData, &out); err != nil {
				log.Warn().Err(err).Str("execution_id", executionID).Str("step_id", s.StepID).Msg("step output not valid JSON; template interpolation will see nil")
			}
			stepOutputs[s.StepID] = map[string]any{"result": out}
		}
	}

	var triggerMap any
	if len(triggerData) > 0 {
		if err := json.Unmarshal(triggerData, &triggerMap); err != nil {
			log.Warn().Err(err).Str("execution_id", executionID).Msg("trigger payload not valid JSON; template interpolation will see nil")
		}
	}

	for _, dagStep := range dagDef.Steps {
		ss := stepMap[dagStep.ID]
		if ss == nil || ss.Status != StepStatusPending {
			continue
		}

		// Check if dependencies are satisfied
		if !d.dependenciesMet(dagStep, stepMap, dagDef) {
			continue
		}

		// Mark step as running
		if err := d.store.UpdateStepStatus(ctx, executionID, dagStep.ID, StepStatusRunning); err != nil {
			log.Error().Err(err).Str("step_id", dagStep.ID).Msg("failed to mark step running")
			continue
		}

		// Build template data
		tmplData := map[string]any{
			"trigger": triggerMap,
			"steps":   stepOutputs,
		}

		// Resolve action arguments through templates
		action := d.resolveAction(dagStep.Action, tmplData)

		log.Info().
			Str("execution_id", executionID).
			Str("step_id", dagStep.ID).
			Str("agent", action.Agent).
			Msg("dispatching DAG step")

		// Create Aether task for this step
		metadata := map[string]string{
			"execution_id": executionID,
			"step_id":      dagStep.ID,
			"workflow":     "true",
		}
		payload, _ := json.Marshal(action)

		if err := d.executor.CreateTask(action.Workspace, action.Agent, metadata, payload); err != nil {
			log.Error().Err(err).
				Str("execution_id", executionID).
				Str("step_id", dagStep.ID).
				Msg("failed to create task for step")
			if storeErr := d.store.SetStepError(ctx, executionID, dagStep.ID, "failed to create task: "+err.Error()); storeErr != nil {
				log.Warn().Err(storeErr).Str("execution_id", executionID).Str("step_id", dagStep.ID).Msg("failed to record step error; step state may stay in 'running' until the next monitor tick")
			}
		}
	}

	// Check if all steps are done
	d.checkExecutionComplete(ctx, executionID, steps)

	return nil
}

func (d *DAGEngine) dependenciesMet(step DAGStep, states map[string]*StepState, dagDef *DAGDefinition) bool {
	// Explicit depends_on
	for _, dep := range step.DependsOn {
		ss := states[dep]
		if ss == nil || ss.Status != StepStatusCompleted {
			return false
		}
	}

	// Implicit: check if this step is the on_success target of another step
	for _, other := range dagDef.Steps {
		if other.OnSuccess == step.ID {
			ss := states[other.ID]
			if ss == nil || ss.Status != StepStatusCompleted {
				return false
			}
		}
		if other.OnFailure == step.ID {
			ss := states[other.ID]
			if ss == nil || ss.Status != StepStatusFailed {
				return false
			}
		}
	}

	return true
}

func (d *DAGEngine) resolveAction(action ActionDef, data map[string]any) ActionDef {
	resolved := ActionDef{
		Agent:     action.Agent,
		ToolName:  action.ToolName,
		Workspace: action.Workspace,
		Metadata:  action.Metadata,
		Arguments: make(map[string]any, len(action.Arguments)),
	}

	for k, v := range action.Arguments {
		str, ok := v.(string)
		if !ok {
			resolved.Arguments[k] = v
			continue
		}
		// Check if value contains template syntax
		if len(str) > 4 && str[:2] == "{{" {
			rendered, err := d.tmpl.TransformRaw(str, data)
			if err == nil && len(rendered) > 0 {
				for _, rv := range rendered {
					resolved.Arguments[k] = rv
					break
				}
			} else {
				resolved.Arguments[k] = str
			}
		} else {
			resolved.Arguments[k] = str
		}
	}

	if resolved.Workspace == "" {
		resolved.Workspace = d.executor.defaultWorkspace
	}

	return resolved
}

func (d *DAGEngine) checkExecutionComplete(ctx context.Context, executionID string, steps []StepState) {
	allDone := true
	anyFailed := false
	for _, s := range steps {
		switch s.Status {
		case StepStatusPending, StepStatusRunning:
			allDone = false
		case StepStatusFailed:
			anyFailed = true
		}
	}

	if !allDone {
		return
	}

	if anyFailed {
		if err := d.store.UpdateExecutionStatus(ctx, executionID, ExecStatusFailed, "one or more steps failed"); err != nil {
			log.Warn().Err(err).Str("execution_id", executionID).Msg("failed to mark DAG execution failed; will retry on next monitor tick")
		}
	} else {
		if err := d.store.UpdateExecutionStatus(ctx, executionID, ExecStatusCompleted, ""); err != nil {
			log.Warn().Err(err).Str("execution_id", executionID).Msg("failed to mark DAG execution completed; will retry on next monitor tick")
		}
	}

	log.Info().
		Str("execution_id", executionID).
		Bool("success", !anyFailed).
		Msg("DAG execution completed")
}

func (d *DAGEngine) findStep(dagDef *DAGDefinition, stepID string) *DAGStep {
	for i := range dagDef.Steps {
		if dagDef.Steps[i].ID == stepID {
			return &dagDef.Steps[i]
		}
	}
	return nil
}

// TryTriggerFromEvent checks if an event should trigger any DAG executions.
func (d *DAGEngine) TryTriggerFromEvent(ctx context.Context, sourceAgent, workspace string, eventNames []string, eventData any) error {
	defs, err := d.store.GetWorkflowDefinitionsForTrigger(ctx, workspace)
	if err != nil {
		return err
	}

	for _, def := range defs {
		var dagDef DAGDefinition
		if err := json.Unmarshal(def.Definition, &dagDef); err != nil {
			log.Warn().Err(err).Str("workflow_id", def.ID).Msg("failed to parse DAG definition")
			continue
		}

		if dagDef.Trigger.Event == "" {
			continue
		}

		// Check if event name matches
		matched := false
		for _, name := range eventNames {
			if name == dagDef.Trigger.Event {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		// Check source agent filter
		if dagDef.Trigger.SourceAgent != "" && dagDef.Trigger.SourceAgent != "*" && dagDef.Trigger.SourceAgent != sourceAgent {
			continue
		}

		// Evaluate condition
		if dagDef.Trigger.Condition != "" {
			env := map[string]any{
				"input": eventData,
				"source": map[string]any{
					"agent":     sourceAgent,
					"workspace": workspace,
				},
			}
			ok, err := d.expr.Evaluate(dagDef.Trigger.Condition, env)
			if err != nil {
				log.Warn().Err(err).Str("workflow_id", def.ID).Msg("trigger condition eval failed")
				continue
			}
			if !ok {
				continue
			}
		}

		triggerData, _ := json.Marshal(eventData)
		executionID, err := d.StartExecution(ctx, def.ID, workspace, triggerData)
		if err != nil {
			log.Error().Err(err).Str("workflow_id", def.ID).Msg("failed to start DAG execution from event")
			continue
		}

		log.Info().
			Str("workflow_id", def.ID).
			Str("execution_id", executionID).
			Str("trigger_event", dagDef.Trigger.Event).
			Msg("DAG execution triggered by event")
	}

	return nil
}
