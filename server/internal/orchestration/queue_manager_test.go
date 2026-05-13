package orchestration

import (
	"encoding/json"
	"testing"
)

// TestOrchestratedTaskPayload_JSONRoundtrip verifies that OrchestratedTaskPayload
// marshals and unmarshals correctly — covering the JSON codec path used in
// PublishOrchestratedTask and the consumer goroutine.
func TestOrchestratedTaskPayload_JSONRoundtrip(t *testing.T) {
	original := &OrchestratedTaskPayload{
		TaskID:               "task-abc-123",
		TargetImplementation: "my-agent",
		Workspace:            "production",
		Profile:              "kubernetes",
		LaunchParams: map[string]interface{}{
			"image":    "my-agent:latest",
			"replicas": float64(1),
		},
		Metadata: map[string]interface{}{
			"created_by": "orchestrator",
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var decoded OrchestratedTaskPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.TaskID != original.TaskID {
		t.Errorf("TaskID = %q, want %q", decoded.TaskID, original.TaskID)
	}
	if decoded.TargetImplementation != original.TargetImplementation {
		t.Errorf("TargetImplementation = %q, want %q", decoded.TargetImplementation, original.TargetImplementation)
	}
	if decoded.Workspace != original.Workspace {
		t.Errorf("Workspace = %q, want %q", decoded.Workspace, original.Workspace)
	}
	if decoded.Profile != original.Profile {
		t.Errorf("Profile = %q, want %q", decoded.Profile, original.Profile)
	}
	if decoded.LaunchParams["image"] != original.LaunchParams["image"] {
		t.Errorf("LaunchParams[image] = %v, want %v", decoded.LaunchParams["image"], original.LaunchParams["image"])
	}
	if decoded.Metadata["created_by"] != original.Metadata["created_by"] {
		t.Errorf("Metadata[created_by] = %v, want %v", decoded.Metadata["created_by"], original.Metadata["created_by"])
	}
}

func TestOrchestratedTaskPayload_NilMetadata(t *testing.T) {
	// Metadata is omitempty — nil should round-trip as nil.
	payload := &OrchestratedTaskPayload{
		TaskID:               "task-no-meta",
		TargetImplementation: "agent",
		Workspace:            "ws",
		Profile:              "docker",
		LaunchParams:         map[string]interface{}{"key": "val"},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var decoded OrchestratedTaskPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if decoded.Metadata != nil {
		t.Errorf("Metadata = %v, want nil (omitempty)", decoded.Metadata)
	}
}

// TestGetQueueName verifies the queue naming convention via a stub that
// calls the unexported helper through a throwaway manager value (no real AMQP).
func TestGetQueueName(t *testing.T) {
	// We can instantiate the struct directly without connecting to AMQP
	// because getQueueName only uses the workspace string.
	oqm := &OrchestratedQueueManager{}

	tests := []struct {
		workspace string
		want      string
	}{
		{"production", "queue:orchestrated:production"},
		{"staging", "queue:orchestrated:staging"},
		{"my-workspace", "queue:orchestrated:my-workspace"},
		// Empty workspace falls back to SystemWorkspace
		{"", "queue:orchestrated:_system"},
	}

	for _, tc := range tests {
		got := oqm.getQueueName(tc.workspace)
		if got != tc.want {
			t.Errorf("getQueueName(%q) = %q, want %q", tc.workspace, got, tc.want)
		}
	}
}

// TestListOrchestratedQueues_ReturnsEmpty verifies that ListOrchestratedQueues
// returns an empty (non-nil capable) slice and no error when called without
// a real RabbitMQ connection (it is a stub by design).
func TestListOrchestratedQueues_ReturnsEmpty(t *testing.T) {
	oqm := &OrchestratedQueueManager{}
	queues, err := oqm.ListOrchestratedQueues()
	if err != nil {
		t.Fatalf("ListOrchestratedQueues() unexpected error: %v", err)
	}
	if len(queues) != 0 {
		t.Errorf("ListOrchestratedQueues() = %v, want empty slice", queues)
	}
}

// TestNewOrchestratedQueueManager_FailsWithInvalidURL verifies that passing
// an unreachable AMQP URL returns a wrapped error rather than panicking.
func TestNewOrchestratedQueueManager_FailsWithInvalidURL(t *testing.T) {
	_, err := NewOrchestratedQueueManager("amqp://localhost:1") // port 1 always refused
	if err == nil {
		t.Fatal("NewOrchestratedQueueManager() expected error with unreachable URL, got nil")
	}
}
