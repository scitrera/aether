package aether

import (
	"testing"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// ReportProgress Tests (AgentClient)
// =============================================================================

func TestAgentClient_ReportProgress_HappyPath(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.ReportProgress(ReportProgressOptions{
		TaskID:     "task-123",
		State:      "running",
		Completion: 0.5,
		Summary:    "halfway done",
	})
	if err != nil {
		t.Fatalf("ReportProgress() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		prog := msg.GetProgress()
		if prog == nil {
			t.Fatal("expected ProgressReport in message")
		}
		if prog.TaskId != "task-123" {
			t.Errorf("TaskId = %q, want %q", prog.TaskId, "task-123")
		}
		if prog.State != "running" {
			t.Errorf("State = %q, want %q", prog.State, "running")
		}
		if prog.Completion != 0.5 {
			t.Errorf("Completion = %v, want 0.5", prog.Completion)
		}
		if prog.Summary != "halfway done" {
			t.Errorf("Summary = %q, want %q", prog.Summary, "halfway done")
		}
		if prog.Step != nil {
			t.Error("Step should be nil when StepName is empty")
		}
	default:
		t.Error("message should be in queue")
	}
}

func TestAgentClient_ReportProgress_WithStep(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.ReportProgress(ReportProgressOptions{
		TaskID:       "task-456",
		State:        "running",
		Completion:   0.25,
		StepName:     "Extracting text",
		StepDetail:   "Processing page 1 of 4",
		StepSequence: 1,
		StepTotal:    4,
		StepType:     "processing",
	})
	if err != nil {
		t.Fatalf("ReportProgress() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		prog := msg.GetProgress()
		if prog == nil {
			t.Fatal("expected ProgressReport in message")
		}
		if prog.Step == nil {
			t.Fatal("Step should not be nil when StepName is set")
		}
		if prog.Step.Name != "Extracting text" {
			t.Errorf("Step.Name = %q, want %q", prog.Step.Name, "Extracting text")
		}
		if prog.Step.Sequence != 1 {
			t.Errorf("Step.Sequence = %d, want 1", prog.Step.Sequence)
		}
		if prog.Step.TotalSteps != 4 {
			t.Errorf("Step.TotalSteps = %d, want 4", prog.Step.TotalSteps)
		}
		if prog.Step.StepType != "processing" {
			t.Errorf("Step.StepType = %q, want %q", prog.Step.StepType, "processing")
		}
	default:
		t.Error("message should be in queue")
	}
}

func TestAgentClient_ReportProgress_WithKind(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.ReportProgress(ReportProgressOptions{
		TaskID: "task-789",
		State:  "running",
		Kind:   pb.ProgressKind_PROGRESS_KIND_CHAT,
	})
	if err != nil {
		t.Fatalf("ReportProgress() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		prog := msg.GetProgress()
		if prog == nil {
			t.Fatal("expected ProgressReport in message")
		}
		if prog.Kind != pb.ProgressKind_PROGRESS_KIND_CHAT {
			t.Errorf("Kind = %v, want PROGRESS_KIND_CHAT", prog.Kind)
		}
	default:
		t.Error("message should be in queue")
	}
}

func TestAgentClient_ReportProgress_WithRecipientAndRequestID(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.ReportProgress(ReportProgressOptions{
		TaskID:    "task-abc",
		State:     "finishing",
		Recipient: "us::user-1::win-1",
		RequestID: "req-xyz",
		Metadata:  map[string]string{"thread_id": "thread-1"},
	})
	if err != nil {
		t.Fatalf("ReportProgress() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		prog := msg.GetProgress()
		if prog == nil {
			t.Fatal("expected ProgressReport in message")
		}
		if prog.Recipient != "us::user-1::win-1" {
			t.Errorf("Recipient = %q, want %q", prog.Recipient, "us::user-1::win-1")
		}
		if prog.RequestId != "req-xyz" {
			t.Errorf("RequestId = %q, want %q", prog.RequestId, "req-xyz")
		}
		if prog.Metadata["thread_id"] != "thread-1" {
			t.Errorf("Metadata[thread_id] = %q, want %q", prog.Metadata["thread_id"], "thread-1")
		}
	default:
		t.Error("message should be in queue")
	}
}

func TestAgentClient_ReportProgress_NotRunning(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	// Do NOT set running.Store(true)

	err = client.ReportProgress(ReportProgressOptions{
		TaskID: "task-123",
		State:  "running",
	})
	if err == nil {
		t.Error("ReportProgress() should fail when client is not running")
	}
}

// =============================================================================
// ReportProgress Tests (TaskClient)
// =============================================================================

func TestTaskClient_ReportProgress_HappyPath(t *testing.T) {
	client, err := NewTaskClient(TaskOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewTaskClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.ReportProgress(ReportProgressOptions{
		TaskID:     "task-999",
		State:      "running",
		Completion: -1.0,
		Summary:    "indeterminate",
	})
	if err != nil {
		t.Fatalf("ReportProgress() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		prog := msg.GetProgress()
		if prog == nil {
			t.Fatal("expected ProgressReport in message")
		}
		if prog.TaskId != "task-999" {
			t.Errorf("TaskId = %q, want %q", prog.TaskId, "task-999")
		}
		if prog.Completion != -1.0 {
			t.Errorf("Completion = %v, want -1.0", prog.Completion)
		}
	default:
		t.Error("message should be in queue")
	}
}

// =============================================================================
// SwitchWorkspace Tests (AgentClient)
// =============================================================================

func TestAgentClient_SwitchWorkspace(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	client.running.Store(true)

	newWS := "new-workspace"
	err = client.SwitchWorkspace(newWS)
	if err != nil {
		t.Fatalf("SwitchWorkspace() error = %v", err)
	}

	// Verify the upstream message was queued
	select {
	case msg := <-client.RequestQueue():
		sw := msg.GetSwitchWorkspace()
		if sw == nil {
			t.Fatal("expected SwitchWorkspace in message")
		}
		if sw.NewWorkspaceId != newWS {
			t.Errorf("NewWorkspaceId = %q, want %q", sw.NewWorkspaceId, newWS)
		}
	default:
		t.Error("SwitchWorkspace message should be in queue")
	}

	// Verify local workspace was updated
	if got := client.Workspace(); got != newWS {
		t.Errorf("Workspace() = %q, want %q", got, newWS)
	}
}

func TestAgentClient_SwitchWorkspace_EmptyWorkspace(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.SwitchWorkspace("")
	if err == nil {
		t.Error("SwitchWorkspace() should fail with empty workspace")
	}
}

func TestAgentClient_SwitchWorkspace_NotRunning(t *testing.T) {
	client, err := NewAgentClient(AgentOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewAgentClient() error = %v", err)
	}
	// Do NOT set running.Store(true)

	err = client.SwitchWorkspace("other-ws")
	if err == nil {
		t.Error("SwitchWorkspace() should fail when client is not running")
	}
}

// =============================================================================
// SwitchWorkspace Tests (TaskClient)
// =============================================================================

func TestTaskClient_SwitchWorkspace(t *testing.T) {
	client, err := NewTaskClient(TaskOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewTaskClient() error = %v", err)
	}
	client.running.Store(true)

	newWS := "switched-workspace"
	err = client.SwitchWorkspace(newWS)
	if err != nil {
		t.Fatalf("SwitchWorkspace() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		sw := msg.GetSwitchWorkspace()
		if sw == nil {
			t.Fatal("expected SwitchWorkspace in message")
		}
		if sw.NewWorkspaceId != newWS {
			t.Errorf("NewWorkspaceId = %q, want %q", sw.NewWorkspaceId, newWS)
		}
	default:
		t.Error("SwitchWorkspace message should be in queue")
	}

	if got := client.Workspace(); got != newWS {
		t.Errorf("Workspace() = %q, want %q", got, newWS)
	}
}

func TestTaskClient_SwitchWorkspace_EmptyWorkspace(t *testing.T) {
	client, err := NewTaskClient(TaskOptions{
		ClientOptions:  ClientOptions{ServerAddr: TestServerAddr},
		Workspace:      TestWorkspace,
		Implementation: TestImplementation,
		Specifier:      TestSpecifier,
	})
	if err != nil {
		t.Fatalf("NewTaskClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.SwitchWorkspace("")
	if err == nil {
		t.Error("SwitchWorkspace() should fail with empty workspace")
	}
}
