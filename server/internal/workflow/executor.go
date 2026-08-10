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
	// TargetAgentID selects TARGETED assignment when a schedule must run on one
	// concrete worker (for example, a worker-authoritative filesystem view).
	// Empty preserves the historical implementation-pooled assignment.
	TargetAgentID string `json:"target_agent_id,omitempty" yaml:"target_agent_id,omitempty"`
	// TargetOfflinePolicy controls exact-target behavior while that identity is
	// disconnected: orchestrate, queue, or reject. Empty preserves the released
	// orchestration behavior.
	TargetOfflinePolicy string `json:"target_offline_policy,omitempty" yaml:"target_offline_policy,omitempty"`
	Payload             any    `json:"payload,omitempty" yaml:"payload,omitempty"`
	// PayloadEncoding controls how Payload becomes CreateTaskRequest.payload.
	// Empty or "msgpack" preserves the historical wire encoding; "json" is for
	// versioned task envelopes shared with non-msgpack consumers.
	PayloadEncoding string `json:"payload_encoding,omitempty" yaml:"payload_encoding,omitempty"`
	// Optional retry policy for create_task actions. When set, the task
	// store re-pends the task with a policy-driven next_retry_at on
	// FailTask. Omitted = legacy hard-coded max_retries=3 behavior.
	Retry *RetryConfig `json:"retry,omitempty" yaml:"retry,omitempty"`
	// emit_event field: the event name to publish onto the event plane (Type ==
	// "emit_event"). Payload carries the event data. Enables join on_complete to
	// chain into further rules/joins.
	EventName string `json:"event_name,omitempty" yaml:"event_name,omitempty"`
	// IdempotencyKey, when set on a create_task action, is forwarded to the
	// gateway which dedups creation on it (exactly-once downstream). Joins set it
	// to their instance key so a completion and a timeout sweep yield one task.
	IdempotencyKey string `json:"idempotency_key,omitempty" yaml:"idempotency_key,omitempty"`
	// CorrelationID stamps the spawned task with a fan-out/fan-in correlation id
	// (the barrier/group id a downstream join matches on).
	CorrelationID string `json:"correlation_id,omitempty" yaml:"correlation_id,omitempty"`
	// CompletionEvent opts the spawned task into "feed B": it emits a domain
	// event onto the event plane at its terminal status, which a join can gather.
	CompletionEvent *CompletionEventConfig `json:"completion_event,omitempty" yaml:"completion_event,omitempty"`
}

// CompletionEventConfig is the create_task-destination form of a task's feed-B
// opt-in (a subset of the proto TaskCompletionEvent; on_statuses defaults to all
// terminal statuses server-side).
type CompletionEventConfig struct {
	Enabled   bool   `json:"enabled" yaml:"enabled"`
	EventName string `json:"event_name,omitempty" yaml:"event_name,omitempty"`
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
	request, err := buildCreateTaskRequest(action, e.defaultWorkspace)
	if err != nil {
		return err
	}

	log.Debug().
		Str("task_type", request.TaskType).
		Str("workspace", request.Workspace).
		Str("target_impl", request.TargetImplementation).
		Str("target_agent", request.TargetAgentId).
		Msg("dispatching create_task action")
	return e.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CreateTask{CreateTask: request},
	})
}

func buildCreateTaskRequest(action *ActionDef, defaultWorkspace string) (*pb.CreateTaskRequest, error) {
	if action == nil {
		return nil, fmt.Errorf("create_task action is required")
	}
	if action.TaskType == "" {
		return nil, fmt.Errorf("task_type is required for create_task action")
	}
	workspace := action.Workspace
	if workspace == "" {
		workspace = defaultWorkspace
	}
	var payload []byte
	if action.Payload != nil {
		var err error
		switch action.PayloadEncoding {
		case "", "msgpack":
			payload, err = msgpack.Marshal(action.Payload)
			if err != nil {
				return nil, fmt.Errorf("msgpack marshal create_task payload: %w", err)
			}
		case "json":
			payload, err = json.Marshal(action.Payload)
			if err != nil {
				return nil, fmt.Errorf("JSON marshal create_task payload: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported create_task payload_encoding %q", action.PayloadEncoding)
		}
	}
	assignmentMode := pb.TaskAssignmentMode_POOL
	targetImplementation := action.TargetImplementation
	if action.TargetAgentID != "" {
		assignmentMode = pb.TaskAssignmentMode_TARGETED
		targetImplementation = ""
	}
	offlinePolicy, err := targetOfflinePolicyToProto(action.TargetOfflinePolicy)
	if err != nil {
		return nil, err
	}
	if offlinePolicy != pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_UNSPECIFIED && assignmentMode != pb.TaskAssignmentMode_TARGETED {
		return nil, fmt.Errorf("target_offline_policy requires target_agent_id")
	}
	var completion *pb.TaskCompletionEvent
	if action.CompletionEvent != nil {
		completion = &pb.TaskCompletionEvent{
			Enabled:   action.CompletionEvent.Enabled,
			EventName: action.CompletionEvent.EventName,
		}
	}
	return &pb.CreateTaskRequest{
		TaskType:             action.TaskType,
		Workspace:            workspace,
		AssignmentMode:       assignmentMode,
		TargetImplementation: targetImplementation,
		TargetAgentId:        action.TargetAgentID,
		Metadata:             action.Metadata,
		Payload:              payload,
		RetryPolicy:          retryConfigToProto(action.Retry),
		IdempotencyKey:       action.IdempotencyKey,
		CorrelationId:        action.CorrelationID,
		CompletionEvent:      completion,
		TaskClass:            pb.TaskClass_TASK_CLASS_BACKGROUND,
		TargetOfflinePolicy:  offlinePolicy,
	}, nil
}

func targetOfflinePolicyToProto(value string) (pb.TargetOfflinePolicy, error) {
	switch value {
	case "":
		return pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_UNSPECIFIED, nil
	case "orchestrate":
		return pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_ORCHESTRATE, nil
	case "queue":
		return pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_QUEUE, nil
	case "reject":
		return pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_REJECT, nil
	default:
		return pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_UNSPECIFIED, fmt.Errorf("unsupported target_offline_policy %q", value)
	}
}

// EmitEvent publishes a synthetic event onto the event plane (event.*) as a
// MessageType_EVENT message, mirroring an SDK client's SendEvent. Used by a
// join's on_complete (Type == "emit_event") to chain into further rules/joins.
func (e *Executor) EmitEvent(action *ActionDef) error {
	if action.EventName == "" {
		return fmt.Errorf("emit_event requires event_name")
	}
	workspace := action.Workspace
	if workspace == "" {
		workspace = e.defaultWorkspace
	}
	payload := map[string]any{
		"source_agent": "workflow-engine",
		"workspace":    workspace,
		"event_names":  []string{action.EventName},
		"data":         action.Payload,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal emit_event payload: %w", err)
	}
	log.Debug().Str("event", action.EventName).Str("workspace", workspace).Msg("emitting event")
	return e.client.SendMessage(aether.EventWildcardTopic(), data, pb.MessageType_EVENT)
}

// CreateTaskWithType creates an Aether task with the given task type. When
// retry is non-nil, it is translated to a proto RetryPolicy and attached to
// the request so the task store handles backoff scheduling on FailTask.
// Pass nil to keep the legacy hard-coded max_retries=3 behavior.
func (e *Executor) CreateTaskWithType(workspace, taskType, targetImpl string, metadata map[string]string, payload []byte, retry *RetryConfig, idempotencyKey, correlationID string, completion *pb.TaskCompletionEvent) error {
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
				RetryPolicy:          retryConfigToProto(retry),
				IdempotencyKey:       idempotencyKey,
				CorrelationId:        correlationID,
				CompletionEvent:      completion,
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
//
// Carries the create_task fields through so an event-triggered rule can spawn
// an Aether POOL task (Type == "create_task"). When Type is empty the action
// falls through to the historical "message" (tool-call) dispatch.
func (e *Executor) DispatchTransformResult(result *TransformResult) error {
	action := &ActionDef{
		Type:                 result.Type,
		Agent:                result.Agent,
		ToolName:             result.ToolName,
		Arguments:            result.Arguments,
		Workspace:            result.Workspace,
		Metadata:             result.Metadata,
		TaskType:             result.TaskType,
		TargetImplementation: result.TargetImplementation,
		TargetAgentID:        result.TargetAgentID,
		PayloadEncoding:      result.PayloadEncoding,
		Payload:              result.Payload,
		CorrelationID:        result.CorrelationID,
		CompletionEvent:      result.CompletionEvent,
	}
	return e.DispatchAction(action)
}

// CreateTask creates an Aether task targeting an agent for DAG step execution.
// Uses raw Send() since WorkflowEngineClient doesn't expose CreateTask directly.
//
// When retry is non-nil, it is translated to a proto RetryPolicy and
// attached to the request so the task store handles backoff scheduling
// on FailTask. The WE no longer drives backoff itself.
func (e *Executor) CreateTask(workspace, agentImpl string, metadata map[string]string, payload []byte, retry *RetryConfig) error {
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
				RetryPolicy:          retryConfigToProto(retry),
			},
		},
	}

	return e.client.Send(msg)
}

// retryConfigToProto translates a DAG-step RetryConfig into the proto
// RetryPolicy understood by the task store. Returns nil when the step did
// not declare retries (preserving legacy default-attempts behavior).
func retryConfigToProto(cfg *RetryConfig) *pb.RetryPolicy {
	if cfg == nil || cfg.MaxAttempts <= 0 {
		return nil
	}
	backoff := pb.BackoffStrategy_BACKOFF_STRATEGY_EXPONENTIAL
	switch cfg.Backoff {
	case "constant", "fixed":
		backoff = pb.BackoffStrategy_BACKOFF_STRATEGY_FIXED
	case "exponential", "":
		backoff = pb.BackoffStrategy_BACKOFF_STRATEGY_EXPONENTIAL
	case "linear":
		// Linear isn't a first-class strategy in the store; map to
		// EXPONENTIAL with no max cap so attempts scale up similarly.
		backoff = pb.BackoffStrategy_BACKOFF_STRATEGY_EXPONENTIAL
	}
	return &pb.RetryPolicy{
		MaxAttempts:    int32(cfg.MaxAttempts),
		Backoff:        backoff,
		InitialDelayMs: 1000, // 1s base; DAG step authors can refine when the schema grows.
		JitterFactor:   0.1,
	}
}
