package workflow

import (
	"context"
	"encoding/json"
	"testing"
)

// ---- DAGEngine.dependenciesMet ----

func TestDAGEngine_dependenciesMet_noDepsMeansReady(t *testing.T) {
	d := &DAGEngine{}
	step := DAGStep{ID: "step-a"}
	dagDef := &DAGDefinition{
		Steps: []DAGStep{step},
	}
	states := map[string]*StepState{
		"step-a": {StepID: "step-a", Status: StepStatusPending},
	}

	if !d.dependenciesMet(step, states, dagDef) {
		t.Error("dependenciesMet() = false for step with no deps, want true")
	}
}

func TestDAGEngine_dependenciesMet_pendingExplicitDepBlocksStep(t *testing.T) {
	d := &DAGEngine{}
	step := DAGStep{ID: "step-b", DependsOn: []string{"step-a"}}
	dagDef := &DAGDefinition{
		Steps: []DAGStep{{ID: "step-a"}, step},
	}
	states := map[string]*StepState{
		"step-a": {StepID: "step-a", Status: StepStatusPending},
		"step-b": {StepID: "step-b", Status: StepStatusPending},
	}

	if d.dependenciesMet(step, states, dagDef) {
		t.Error("dependenciesMet() = true when dep is still pending, want false")
	}
}

func TestDAGEngine_dependenciesMet_completedExplicitDepAllowsStep(t *testing.T) {
	d := &DAGEngine{}
	step := DAGStep{ID: "step-b", DependsOn: []string{"step-a"}}
	dagDef := &DAGDefinition{
		Steps: []DAGStep{{ID: "step-a"}, step},
	}
	states := map[string]*StepState{
		"step-a": {StepID: "step-a", Status: StepStatusCompleted},
		"step-b": {StepID: "step-b", Status: StepStatusPending},
	}

	if !d.dependenciesMet(step, states, dagDef) {
		t.Error("dependenciesMet() = false when dep is completed, want true")
	}
}

func TestDAGEngine_dependenciesMet_onSuccessImplicitDepRequiresCompletion(t *testing.T) {
	d := &DAGEngine{}
	stepA := DAGStep{ID: "step-a", OnSuccess: "step-b"}
	stepB := DAGStep{ID: "step-b"}
	dagDef := &DAGDefinition{Steps: []DAGStep{stepA, stepB}}
	// step-a is running, so step-b (its on_success target) should not run yet
	states := map[string]*StepState{
		"step-a": {StepID: "step-a", Status: StepStatusRunning},
		"step-b": {StepID: "step-b", Status: StepStatusPending},
	}

	if d.dependenciesMet(stepB, states, dagDef) {
		t.Error("dependenciesMet() = true when on_success predecessor is still running, want false")
	}
}

func TestDAGEngine_dependenciesMet_onSuccessImplicitDepMetWhenComplete(t *testing.T) {
	d := &DAGEngine{}
	stepA := DAGStep{ID: "step-a", OnSuccess: "step-b"}
	stepB := DAGStep{ID: "step-b"}
	dagDef := &DAGDefinition{Steps: []DAGStep{stepA, stepB}}
	states := map[string]*StepState{
		"step-a": {StepID: "step-a", Status: StepStatusCompleted},
		"step-b": {StepID: "step-b", Status: StepStatusPending},
	}

	if !d.dependenciesMet(stepB, states, dagDef) {
		t.Error("dependenciesMet() = false when on_success predecessor is complete, want true")
	}
}

func TestDAGEngine_dependenciesMet_onFailureImplicitDepRequiresFailure(t *testing.T) {
	d := &DAGEngine{}
	stepA := DAGStep{ID: "step-a", OnFailure: "step-err"}
	stepErr := DAGStep{ID: "step-err"}
	dagDef := &DAGDefinition{Steps: []DAGStep{stepA, stepErr}}
	// step-a completed successfully → on_failure target should not run
	states := map[string]*StepState{
		"step-a":   {StepID: "step-a", Status: StepStatusCompleted},
		"step-err": {StepID: "step-err", Status: StepStatusPending},
	}

	if d.dependenciesMet(stepErr, states, dagDef) {
		t.Error("dependenciesMet() = true for on_failure target when predecessor succeeded, want false")
	}
}

func TestDAGEngine_dependenciesMet_onFailureMetWhenPredecessorFailed(t *testing.T) {
	d := &DAGEngine{}
	stepA := DAGStep{ID: "step-a", OnFailure: "step-err"}
	stepErr := DAGStep{ID: "step-err"}
	dagDef := &DAGDefinition{Steps: []DAGStep{stepA, stepErr}}
	states := map[string]*StepState{
		"step-a":   {StepID: "step-a", Status: StepStatusFailed},
		"step-err": {StepID: "step-err", Status: StepStatusPending},
	}

	if !d.dependenciesMet(stepErr, states, dagDef) {
		t.Error("dependenciesMet() = false for on_failure target when predecessor failed, want true")
	}
}

// ---- DAGEngine.findStep ----

func TestDAGEngine_findStep_returnsStepByID(t *testing.T) {
	d := &DAGEngine{}
	dagDef := &DAGDefinition{
		Steps: []DAGStep{
			{ID: "alpha"},
			{ID: "beta"},
			{ID: "gamma"},
		},
	}

	step := d.findStep(dagDef, "beta")
	if step == nil {
		t.Fatal("findStep() = nil, want non-nil for existing step ID")
	}
	if step.ID != "beta" {
		t.Errorf("findStep() returned step ID = %q, want %q", step.ID, "beta")
	}
}

func TestDAGEngine_findStep_returnsNilForUnknownID(t *testing.T) {
	d := &DAGEngine{}
	dagDef := &DAGDefinition{
		Steps: []DAGStep{{ID: "alpha"}},
	}

	step := d.findStep(dagDef, "nonexistent")
	if step != nil {
		t.Errorf("findStep() = %v, want nil for unknown step ID", step)
	}
}

func TestDAGEngine_findStep_returnsNilForEmptyDef(t *testing.T) {
	d := &DAGEngine{}
	dagDef := &DAGDefinition{}

	step := d.findStep(dagDef, "any")
	if step != nil {
		t.Errorf("findStep() = %v, want nil for empty definition", step)
	}
}

// ---- DAGEngine.checkExecutionComplete ----

func TestDAGEngine_checkExecutionComplete_doesNotMarkCompleteWhenStepsRunning(t *testing.T) {
	// We can't easily observe the store call without a DB, but we can ensure
	// the function does not panic with mixed step states.
	d := &DAGEngine{store: &Store{}} // nil DB — method won't be called
	steps := []StepState{
		{StepID: "a", Status: StepStatusCompleted},
		{StepID: "b", Status: StepStatusRunning},
	}
	// Should not panic; actual DB call skipped because allDone=false.
	// Pass context.TODO so SA1012 (nil context) is not flagged — the
	// receiver never touches the context in this allDone=false path.
	d.checkExecutionComplete(context.TODO(), "exec-1", steps)
}

// ---- StateMachineEngine definition validation ----

func TestStateMachineDefinition_parseRoundTrip(t *testing.T) {
	rawJSON := `{
		"name": "order-flow",
		"initial_state": "pending",
		"states": {
			"pending": {
				"on": {"approve": "approved", "reject": "rejected"},
				"terminal": false
			},
			"approved": {
				"terminal": false,
				"timeout": "24h",
				"timeout_action": "expired"
			},
			"rejected": {"terminal": true},
			"expired":  {"terminal": true}
		}
	}`

	var def StateMachineDefinition
	if err := json.Unmarshal([]byte(rawJSON), &def); err != nil {
		t.Fatalf("json.Unmarshal StateMachineDefinition error = %v", err)
	}
	if def.Name != "order-flow" {
		t.Errorf("Name = %q, want %q", def.Name, "order-flow")
	}
	if def.InitialState != "pending" {
		t.Errorf("InitialState = %q, want %q", def.InitialState, "pending")
	}
	if len(def.States) != 4 {
		t.Errorf("States count = %d, want 4", len(def.States))
	}

	pending := def.States["pending"]
	if pending.Terminal {
		t.Error("pending.Terminal = true, want false")
	}
	if pending.On["approve"] != "approved" {
		t.Errorf("pending.On[approve] = %q, want %q", pending.On["approve"], "approved")
	}

	approved := def.States["approved"]
	if approved.Timeout != "24h" {
		t.Errorf("approved.Timeout = %q, want %q", approved.Timeout, "24h")
	}
	if approved.TimeoutAction != "expired" {
		t.Errorf("approved.TimeoutAction = %q, want %q", approved.TimeoutAction, "expired")
	}

	if !def.States["rejected"].Terminal {
		t.Error("rejected.Terminal = false, want true")
	}
}

func TestStateMachineDefinition_stateWithEntryAndExitActions(t *testing.T) {
	raw := `{
		"name": "with-actions",
		"initial_state": "idle",
		"states": {
			"idle": {
				"on": {"start": "active"},
				"entry_action": {"agent": "monitor", "tool_name": "on_entry"},
				"exit_action":  {"agent": "monitor", "tool_name": "on_exit"}
			},
			"active": {"terminal": true}
		}
	}`

	var def StateMachineDefinition
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}

	idle := def.States["idle"]
	if idle.EntryAction == nil {
		t.Fatal("EntryAction = nil, want non-nil")
	}
	if idle.EntryAction.Agent != "monitor" {
		t.Errorf("EntryAction.Agent = %q, want %q", idle.EntryAction.Agent, "monitor")
	}
	if idle.EntryAction.ToolName != "on_entry" {
		t.Errorf("EntryAction.ToolName = %q, want %q", idle.EntryAction.ToolName, "on_entry")
	}
	if idle.ExitAction == nil {
		t.Fatal("ExitAction = nil, want non-nil")
	}
	if idle.ExitAction.ToolName != "on_exit" {
		t.Errorf("ExitAction.ToolName = %q, want %q", idle.ExitAction.ToolName, "on_exit")
	}
}

// ---- DAGDefinition round-trip ----

func TestDAGDefinition_parseRoundTrip(t *testing.T) {
	raw := `{
		"name": "build-pipeline",
		"version": 2,
		"trigger": {
			"event": "push",
			"source_agent": "git-agent",
			"condition": "input.branch == \"main\""
		},
		"steps": [
			{
				"id": "build",
				"action": {"agent": "builder", "tool_name": "build"},
				"on_success": "test"
			},
			{
				"id": "test",
				"action": {"agent": "tester", "tool_name": "run_tests"},
				"depends_on": ["build"],
				"retry": {"max_attempts": 3, "backoff": "exponential"}
			}
		]
	}`

	var def DAGDefinition
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		t.Fatalf("unmarshal DAGDefinition error = %v", err)
	}
	if def.Name != "build-pipeline" {
		t.Errorf("Name = %q, want %q", def.Name, "build-pipeline")
	}
	if def.Version != 2 {
		t.Errorf("Version = %d, want 2", def.Version)
	}
	if def.Trigger.Event != "push" {
		t.Errorf("Trigger.Event = %q, want %q", def.Trigger.Event, "push")
	}
	if def.Trigger.SourceAgent != "git-agent" {
		t.Errorf("Trigger.SourceAgent = %q, want %q", def.Trigger.SourceAgent, "git-agent")
	}
	if len(def.Steps) != 2 {
		t.Fatalf("Steps count = %d, want 2", len(def.Steps))
	}

	build := def.Steps[0]
	if build.ID != "build" {
		t.Errorf("Steps[0].ID = %q, want %q", build.ID, "build")
	}
	if build.OnSuccess != "test" {
		t.Errorf("Steps[0].OnSuccess = %q, want %q", build.OnSuccess, "test")
	}

	testStep := def.Steps[1]
	if testStep.Retry.MaxAttempts != 3 {
		t.Errorf("Steps[1].Retry.MaxAttempts = %d, want 3", testStep.Retry.MaxAttempts)
	}
	if testStep.Retry.Backoff != "exponential" {
		t.Errorf("Steps[1].Retry.Backoff = %q, want %q", testStep.Retry.Backoff, "exponential")
	}
	if len(testStep.DependsOn) != 1 || testStep.DependsOn[0] != "build" {
		t.Errorf("Steps[1].DependsOn = %v, want [build]", testStep.DependsOn)
	}
}
