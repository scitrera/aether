package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
)

func TestBuildCreateTaskRequestTargetsExactAgentWithJSONPayload(t *testing.T) {
	action := &ActionDef{
		Type:                "create_task",
		TaskType:            "agent-harness.scheduled-turn.v1",
		TargetAgentID:       "ag::default::agent-harness::worker-1",
		TargetOfflinePolicy: "queue",
		PayloadEncoding:     "json",
		Payload: map[string]any{
			"schema": "agent-harness.scheduled-turn.v1",
			"binding": map[string]any{
				"workspace_id": "project-a",
				"view_id":      "view-a",
			},
		},
		Metadata: map[string]string{"scitrera.schedule_id": "daily-review"},
	}

	request, err := buildCreateTaskRequest(action, "default")
	if err != nil {
		t.Fatal(err)
	}
	if request.AssignmentMode != pb.TaskAssignmentMode_TARGETED {
		t.Fatalf("assignment mode = %v", request.AssignmentMode)
	}
	if request.TargetAgentId != action.TargetAgentID || request.TargetImplementation != "" {
		t.Fatalf("target agent=%q implementation=%q", request.TargetAgentId, request.TargetImplementation)
	}
	if request.TargetOfflinePolicy != pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_QUEUE {
		t.Fatalf("target offline policy = %v", request.TargetOfflinePolicy)
	}
	if request.TaskClass != pb.TaskClass_TASK_CLASS_BACKGROUND {
		t.Fatalf("task class = %v", request.TaskClass)
	}
	var decoded map[string]any
	if err := json.Unmarshal(request.Payload, &decoded); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if decoded["schema"] != "agent-harness.scheduled-turn.v1" {
		t.Fatalf("payload = %#v", decoded)
	}
}

func TestBuildCreateTaskRequestPreservesPoolMsgpackDefaults(t *testing.T) {
	request, err := buildCreateTaskRequest(&ActionDef{
		Type: "create_task", TaskType: "legacy", TargetImplementation: "worker", Payload: map[string]any{"x": "y"},
	}, "default")
	if err != nil {
		t.Fatal(err)
	}
	if request.AssignmentMode != pb.TaskAssignmentMode_POOL || request.TargetImplementation != "worker" || request.TargetAgentId != "" {
		t.Fatalf("legacy routing changed: %#v", request)
	}
	if len(request.Payload) == 0 || json.Valid(request.Payload) {
		t.Fatalf("legacy payload did not retain msgpack encoding: %x", request.Payload)
	}
}

func TestBuildCreateTaskRequestRejectsUnknownPayloadEncoding(t *testing.T) {
	_, err := buildCreateTaskRequest(&ActionDef{
		Type: "create_task", TaskType: "bad", PayloadEncoding: "yaml", Payload: map[string]any{"x": "y"},
	}, "default")
	if err == nil {
		t.Fatal("unknown payload encoding was accepted")
	}
}

func TestBuildCreateTaskRequestRejectsInvalidOfflinePolicy(t *testing.T) {
	for name, action := range map[string]*ActionDef{
		"unknown": {
			Type: "create_task", TaskType: "bad", TargetAgentID: "ag::default::worker::one",
			TargetOfflinePolicy: "eventually",
		},
		"without exact target": {
			Type: "create_task", TaskType: "bad", TargetImplementation: "worker",
			TargetOfflinePolicy: "queue",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := buildCreateTaskRequest(action, "default"); err == nil {
				t.Fatal("invalid target offline policy was accepted")
			}
		})
	}
}

func TestDispatchScheduledActionConfirmsTaskCreation(t *testing.T) {
	var gotOptions sdk.CreateTaskOptions
	executor := &Executor{
		defaultWorkspace: "default",
		createScheduledTaskSync: func(_ context.Context, taskType, workspace string, options sdk.CreateTaskOptions, timeout time.Duration) (*sdk.CreateTaskResponse, error) {
			if taskType != "scheduled" || workspace != "workspace-a" || timeout != scheduledTaskCreateTimeout {
				t.Fatalf("create args = %q %q %v", taskType, workspace, timeout)
			}
			gotOptions = options
			return &sdk.CreateTaskResponse{Success: true, TaskID: "task-1"}, nil
		},
	}
	action := &ActionDef{
		Type: "create_task", TaskType: "scheduled", Workspace: "workspace-a",
		TargetAgentID: "ag::workspace-a::worker::one", TargetOfflinePolicy: "queue",
		PayloadEncoding: "json", Payload: map[string]string{"run": "one"},
		Metadata: map[string]string{"scheduled_for": "now"}, IdempotencyKey: "occurrence-1",
	}
	if err := executor.DispatchScheduledAction(context.Background(), action); err != nil {
		t.Fatal(err)
	}
	if gotOptions.AssignmentMode != sdk.TaskAssignmentTargeted ||
		gotOptions.TargetAgentID != action.TargetAgentID ||
		gotOptions.TargetOfflinePolicy != pb.TargetOfflinePolicy_TARGET_OFFLINE_POLICY_QUEUE ||
		gotOptions.IdempotencyKey != action.IdempotencyKey ||
		gotOptions.TaskClass != pb.TaskClass_TASK_CLASS_BACKGROUND ||
		!json.Valid(gotOptions.Payload) {
		t.Fatalf("confirmed create options = %#v", gotOptions)
	}
}

func TestDispatchScheduledActionDoesNotConfirmRejectedOrUncertainCreation(t *testing.T) {
	for name, create := range map[string]func(context.Context, string, string, sdk.CreateTaskOptions, time.Duration) (*sdk.CreateTaskResponse, error){
		"rejected": func(context.Context, string, string, sdk.CreateTaskOptions, time.Duration) (*sdk.CreateTaskResponse, error) {
			return &sdk.CreateTaskResponse{Success: false, ErrorMessage: "denied"}, nil
		},
		"response lost": func(context.Context, string, string, sdk.CreateTaskOptions, time.Duration) (*sdk.CreateTaskResponse, error) {
			return nil, errors.New("timeout")
		},
	} {
		t.Run(name, func(t *testing.T) {
			executor := &Executor{defaultWorkspace: "default", createScheduledTaskSync: create}
			err := executor.DispatchScheduledAction(context.Background(), &ActionDef{Type: "create_task", TaskType: "scheduled"})
			if err == nil || (name == "rejected" && !strings.Contains(err.Error(), "denied")) {
				t.Fatalf("dispatch error = %v", err)
			}
		})
	}
}
