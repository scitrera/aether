package workflow

import (
	"encoding/json"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
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
