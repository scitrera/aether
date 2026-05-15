package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// =============================================================================
// State Machine Definition Types
// =============================================================================

// StateMachineDefinition is the parsed JSONB definition of a state machine.
type StateMachineDefinition struct {
	Name         string                    `json:"name" yaml:"name"`
	InitialState string                    `json:"initial_state" yaml:"initial_state"`
	States       map[string]StateDefConfig `json:"states" yaml:"states"`
}

type StateDefConfig struct {
	On            map[string]string `json:"on" yaml:"on"`                         // event → next state
	EntryAction   *ActionDef        `json:"entry_action" yaml:"entry_action"`     // action on state entry
	ExitAction    *ActionDef        `json:"exit_action" yaml:"exit_action"`       // action on state exit
	Terminal      bool              `json:"terminal" yaml:"terminal"`             // no further transitions
	Timeout       string            `json:"timeout" yaml:"timeout"`               // duration string e.g. "24h"
	TimeoutAction string            `json:"timeout_action" yaml:"timeout_action"` // state to transition to on timeout
}

// =============================================================================
// State Machine Engine
// =============================================================================

type StateMachineEngine struct {
	store    WorkflowStore
	executor *Executor
}

func NewStateMachineEngine(store WorkflowStore, executor *Executor) *StateMachineEngine {
	return &StateMachineEngine{
		store:    store,
		executor: executor,
	}
}

// CreateInstance creates a new state machine instance starting at the initial state.
func (e *StateMachineEngine) CreateInstance(ctx context.Context, machineID, instanceID, workspace string, data json.RawMessage) (*StateMachineInstance, error) {
	sm, err := e.store.GetStateMachine(ctx, machineID)
	if err != nil {
		return nil, fmt.Errorf("get state machine: %w", err)
	}
	if sm == nil {
		return nil, fmt.Errorf("state machine not found: %s", machineID)
	}

	var def StateMachineDefinition
	if err := json.Unmarshal(sm.Definition, &def); err != nil {
		return nil, fmt.Errorf("parse state machine definition: %w", err)
	}

	if _, ok := def.States[def.InitialState]; !ok {
		return nil, fmt.Errorf("initial state %q not defined", def.InitialState)
	}

	// Calculate timeout if initial state has one
	var timeoutAt *time.Time
	if stCfg, ok := def.States[def.InitialState]; ok && stCfg.Timeout != "" {
		if d, err := time.ParseDuration(stCfg.Timeout); err == nil {
			t := time.Now().Add(d)
			timeoutAt = &t
		}
	}

	instance := &StateMachineInstance{
		InstanceID:   instanceID,
		MachineID:    machineID,
		Workspace:    workspace,
		CurrentState: def.InitialState,
		Data:         data,
		TimeoutAt:    timeoutAt,
	}

	if err := e.store.CreateStateMachineInstance(ctx, instance); err != nil {
		return nil, fmt.Errorf("create instance: %w", err)
	}

	// Fire entry action for initial state
	if stCfg := def.States[def.InitialState]; stCfg.EntryAction != nil {
		action := *stCfg.EntryAction
		if action.Workspace == "" {
			action.Workspace = workspace
		}
		if err := e.executor.DispatchAction(&action); err != nil {
			log.Error().Err(err).
				Str("instance_id", instanceID).
				Str("state", def.InitialState).
				Msg("failed to dispatch entry action")
		}
	}

	log.Info().
		Str("machine_id", machineID).
		Str("instance_id", instanceID).
		Str("initial_state", def.InitialState).
		Msg("state machine instance created")

	return instance, nil
}

// SendEvent transitions a state machine instance based on an event.
func (e *StateMachineEngine) SendEvent(ctx context.Context, instanceID, event string, eventData json.RawMessage) (*StateMachineInstance, error) {
	instance, err := e.store.GetStateMachineInstance(ctx, instanceID)
	if err != nil {
		return nil, fmt.Errorf("get instance: %w", err)
	}
	if instance == nil {
		return nil, fmt.Errorf("instance not found: %s", instanceID)
	}
	if instance.CompletedAt != nil {
		return nil, fmt.Errorf("instance %s is already completed", instanceID)
	}

	sm, err := e.store.GetStateMachine(ctx, instance.MachineID)
	if err != nil {
		return nil, fmt.Errorf("get state machine: %w", err)
	}
	if sm == nil {
		return nil, fmt.Errorf("state machine not found: %s", instance.MachineID)
	}

	var def StateMachineDefinition
	if err := json.Unmarshal(sm.Definition, &def); err != nil {
		return nil, fmt.Errorf("parse definition: %w", err)
	}

	currentStateCfg, ok := def.States[instance.CurrentState]
	if !ok {
		return nil, fmt.Errorf("current state %q not defined", instance.CurrentState)
	}

	if currentStateCfg.Terminal {
		return nil, fmt.Errorf("state %q is terminal, no transitions allowed", instance.CurrentState)
	}

	nextState, ok := currentStateCfg.On[event]
	if !ok {
		return nil, fmt.Errorf("no transition for event %q from state %q", event, instance.CurrentState)
	}

	nextStateCfg, ok := def.States[nextState]
	if !ok {
		return nil, fmt.Errorf("target state %q not defined", nextState)
	}

	return e.transition(ctx, instance, &def, instance.CurrentState, nextState, &currentStateCfg, &nextStateCfg)
}

func (e *StateMachineEngine) transition(ctx context.Context, instance *StateMachineInstance, def *StateMachineDefinition, fromState, toState string, fromCfg, toCfg *StateDefConfig) (*StateMachineInstance, error) {
	log.Info().
		Str("instance_id", instance.InstanceID).
		Str("from", fromState).
		Str("to", toState).
		Msg("state machine transition")

	// Fire exit action for current state
	if fromCfg.ExitAction != nil {
		action := *fromCfg.ExitAction
		if action.Workspace == "" {
			action.Workspace = instance.Workspace
		}
		if err := e.executor.DispatchAction(&action); err != nil {
			log.Error().Err(err).
				Str("instance_id", instance.InstanceID).
				Str("state", fromState).
				Msg("failed to dispatch exit action")
		}
	}

	// Update state
	var timeoutAt *time.Time
	if toCfg.Timeout != "" {
		if d, err := time.ParseDuration(toCfg.Timeout); err == nil {
			t := time.Now().Add(d)
			timeoutAt = &t
		}
	}

	completed := toCfg.Terminal
	if err := e.store.UpdateStateMachineInstance(ctx, instance.InstanceID, toState, timeoutAt, completed); err != nil {
		return nil, fmt.Errorf("update instance state: %w", err)
	}

	// Fire entry action for new state
	if toCfg.EntryAction != nil {
		action := *toCfg.EntryAction
		if action.Workspace == "" {
			action.Workspace = instance.Workspace
		}
		if err := e.executor.DispatchAction(&action); err != nil {
			log.Error().Err(err).
				Str("instance_id", instance.InstanceID).
				Str("state", toState).
				Msg("failed to dispatch entry action")
		}
	}

	// Return updated instance
	instance.CurrentState = toState
	instance.TimeoutAt = timeoutAt
	if completed {
		now := time.Now()
		instance.CompletedAt = &now
	}
	return instance, nil
}

// MonitorTimeouts checks for state machine instances that have timed out
// and transitions them to their timeout_action state.
func (e *StateMachineEngine) MonitorTimeouts(ctx context.Context) error {
	instances, err := e.store.GetTimedOutInstances(ctx, time.Now())
	if err != nil {
		return err
	}

	for _, inst := range instances {
		sm, err := e.store.GetStateMachine(ctx, inst.MachineID)
		if err != nil {
			log.Error().Err(err).Str("machine_id", inst.MachineID).Msg("failed to get state machine for timeout")
			continue
		}
		if sm == nil {
			continue
		}

		var def StateMachineDefinition
		if err := json.Unmarshal(sm.Definition, &def); err != nil {
			log.Error().Err(err).Str("machine_id", inst.MachineID).Msg("failed to parse definition for timeout")
			continue
		}

		currentCfg, ok := def.States[inst.CurrentState]
		if !ok || currentCfg.TimeoutAction == "" {
			// No timeout action defined; just clear the timeout. Best-effort:
			// a stale timeout row simply causes the next monitor tick to
			// re-evaluate and clear it then.
			if err := e.store.ClearInstanceTimeout(ctx, inst.InstanceID); err != nil {
				log.Warn().Err(err).Str("instance_id", inst.InstanceID).Msg("failed to clear instance timeout; will retry on next monitor tick")
			}
			continue
		}

		targetState := currentCfg.TimeoutAction
		targetCfg, ok := def.States[targetState]
		if !ok {
			log.Error().
				Str("instance_id", inst.InstanceID).
				Str("timeout_action", targetState).
				Msg("timeout target state not defined")
			continue
		}

		log.Warn().
			Str("instance_id", inst.InstanceID).
			Str("from", inst.CurrentState).
			Str("to", targetState).
			Msg("state machine instance timed out")

		if _, err := e.transition(ctx, &inst, &def, inst.CurrentState, targetState, &currentCfg, &targetCfg); err != nil {
			log.Error().Err(err).Str("instance_id", inst.InstanceID).Msg("failed to process timeout transition")
		}
	}

	return nil
}
