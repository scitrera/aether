package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
)

// ActionDef defines an action to dispatch to an agent via Aether.
type ActionDef struct {
	Type      string            `json:"type,omitempty" yaml:"type,omitempty"` // "message" (default), "create_task"
	Agent     string            `json:"agent,omitempty" yaml:"agent,omitempty"`
	ToolName  string            `json:"tool_name,omitempty" yaml:"tool_name,omitempty"`
	Arguments map[string]any    `json:"arguments,omitempty" yaml:"arguments,omitempty"`
	Workspace string            `json:"workspace,omitempty" yaml:"workspace,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	// create_task fields
	TaskType             string `json:"task_type,omitempty" yaml:"task_type,omitempty"`
	TargetImplementation string `json:"target_implementation,omitempty" yaml:"target_implementation,omitempty"`
	Payload              any    `json:"payload,omitempty" yaml:"payload,omitempty"`
}

// ToolCallPayload is the JSON structure sent as the message payload
// when dispatching a tool call to an agent.
type ToolCallPayload struct {
	ToolName  string         `json:"tool_name"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// Executor dispatches actions to agents via the Aether SDK.
type Executor struct {
	client           *aether.WorkflowEngineClient
	defaultWorkspace string
}

func NewExecutor(client *aether.WorkflowEngineClient, defaultWorkspace string) *Executor {
	return &Executor{
		client:           client,
		defaultWorkspace: defaultWorkspace,
	}
}

// DispatchAction routes an action based on its Type field.
func (e *Executor) DispatchAction(action *ActionDef) error {
	switch action.Type {
	case "create_task":
		return e.dispatchCreateTask(action)
	case "", "message":
		return e.dispatchMessage(action)
	default:
		return fmt.Errorf("unknown action type: %s", action.Type)
	}
}

// dispatchMessage sends a tool call message to the target agent.
func (e *Executor) dispatchMessage(action *ActionDef) error {
	if action.Agent == "" {
		return fmt.Errorf("action agent is required")
	}

	workspace := action.Workspace
	if workspace == "" {
		workspace = e.defaultWorkspace
	}

	payload := ToolCallPayload{
		ToolName:  action.ToolName,
		Arguments: action.Arguments,
	}
	if len(action.Metadata) > 0 {
		meta := make(map[string]any, len(action.Metadata))
		for k, v := range action.Metadata {
			meta[k] = v
		}
		payload.Metadata = meta
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal tool call payload: %w", err)
	}

	log.Debug().
		Str("agent", action.Agent).
		Str("workspace", workspace).
		Str("tool", action.ToolName).
		Msg("dispatching action")

	return e.client.SendCommandToAgent(workspace, action.Agent, "default", data)
}

// dispatchCreateTask creates an Aether task from a schedule action.
func (e *Executor) dispatchCreateTask(action *ActionDef) error {
	if action.TaskType == "" {
		return fmt.Errorf("task_type is required for create_task action")
	}

	workspace := action.Workspace
	if workspace == "" {
		workspace = e.defaultWorkspace
	}

	var payload []byte
	if action.Payload != nil {
		var err error
		payload, err = msgpack.Marshal(action.Payload)
		if err != nil {
			return fmt.Errorf("msgpack marshal create_task payload: %w", err)
		}
	}

	targetImpl := action.TargetImplementation
	metadata := action.Metadata

	log.Debug().
		Str("task_type", action.TaskType).
		Str("workspace", workspace).
		Str("target_impl", targetImpl).
		Msg("dispatching create_task action")

	return e.CreateTaskWithType(workspace, action.TaskType, targetImpl, metadata, payload)
}

// CreateTaskWithType creates an Aether task with the given task type.
func (e *Executor) CreateTaskWithType(workspace, taskType, targetImpl string, metadata map[string]string, payload []byte) error {
	log.Debug().
		Str("workspace", workspace).
		Str("task_type", taskType).
		Str("target_impl", targetImpl).
		Msg("creating task")

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CreateTask{
			CreateTask: &pb.CreateTaskRequest{
				TaskType:             taskType,
				Workspace:            workspace,
				AssignmentMode:       pb.TaskAssignmentMode_POOL,
				TargetImplementation: targetImpl,
				Metadata:             metadata,
				Payload:              payload,
			},
		},
	}

	return e.client.Send(msg)
}

// DispatchActionToTopic sends a tool call message to an arbitrary topic.
func (e *Executor) DispatchActionToTopic(topic string, action *ActionDef) error {
	payload := ToolCallPayload{
		ToolName:  action.ToolName,
		Arguments: action.Arguments,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal tool call payload: %w", err)
	}

	log.Debug().
		Str("topic", topic).
		Str("tool", action.ToolName).
		Msg("dispatching action to topic")

	return e.client.SendToolCallMessage(topic, data)
}

// DispatchTransformResult sends the result of a template transformation.
func (e *Executor) DispatchTransformResult(result *TransformResult) error {
	action := &ActionDef{
		Agent:     result.Agent,
		ToolName:  result.ToolName,
		Arguments: result.Arguments,
		Workspace: result.Workspace,
		Metadata:  result.Metadata,
	}
	return e.DispatchAction(action)
}

// CreateTask creates an Aether task targeting an agent for DAG step execution.
// Uses raw Send() since WorkflowEngineClient doesn't expose CreateTask directly.
func (e *Executor) CreateTask(workspace, agentImpl string, metadata map[string]string, payload []byte) error {
	log.Debug().
		Str("workspace", workspace).
		Str("agent_impl", agentImpl).
		Msg("creating task for DAG step")

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CreateTask{
			CreateTask: &pb.CreateTaskRequest{
				TaskType:             "workflow",
				Workspace:            workspace,
				AssignmentMode:       pb.TaskAssignmentMode_POOL,
				TargetImplementation: agentImpl,
				Metadata:             metadata,
				Payload:              payload,
			},
		},
	}

	return e.client.Send(msg)
}
