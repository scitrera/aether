package aether

import (
	"testing"
)

// =============================================================================
// UserClient Tests
// =============================================================================

func TestUserClient_SwitchWorkspace(t *testing.T) {
	opts := UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: TestServerAddr,
		},
		UserID:   TestUserID,
		WindowID: TestWindowID,
	}

	client, err := NewUserClient(opts)
	if err != nil {
		t.Fatalf("NewUserClient() error = %v", err)
	}
	client.running.Store(true)

	// Verify initial workspace is empty
	if client.Workspace() != "" {
		t.Errorf("initial Workspace() = %q, want empty", client.Workspace())
	}

	err = client.SwitchWorkspace(TestWorkspace)
	if err != nil {
		t.Errorf("SwitchWorkspace() error = %v", err)
	}

	// Verify local workspace was updated
	if client.Workspace() != TestWorkspace {
		t.Errorf("Workspace() = %q, want %q", client.Workspace(), TestWorkspace)
	}

	// Verify the correct proto message was queued
	select {
	case msg := <-client.RequestQueue():
		sw := msg.GetSwitchWorkspace()
		if sw == nil {
			t.Fatal("Expected SwitchWorkspace in message")
		}
		if sw.NewWorkspaceId != TestWorkspace {
			t.Errorf("NewWorkspaceId = %q, want %q", sw.NewWorkspaceId, TestWorkspace)
		}
	default:
		t.Error("SwitchWorkspace message should be in queue")
	}
}

func TestUserClient_SwitchWorkspace_UpdatesLocal(t *testing.T) {
	opts := UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: TestServerAddr,
		},
		UserID:   TestUserID,
		WindowID: TestWindowID,
	}

	client, err := NewUserClient(opts)
	if err != nil {
		t.Fatalf("NewUserClient() error = %v", err)
	}
	client.running.Store(true)

	// Switch to first workspace
	_ = client.SwitchWorkspace("workspace-1")
	<-client.RequestQueue() // drain

	if client.Workspace() != "workspace-1" {
		t.Errorf("Workspace() = %q, want %q", client.Workspace(), "workspace-1")
	}

	// Switch to second workspace
	_ = client.SwitchWorkspace("workspace-2")
	<-client.RequestQueue() // drain

	if client.Workspace() != "workspace-2" {
		t.Errorf("Workspace() = %q after second switch, want %q", client.Workspace(), "workspace-2")
	}
}

func TestUserClient_SwitchWorkspace_NotRunning(t *testing.T) {
	opts := UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: TestServerAddr,
		},
		UserID:   TestUserID,
		WindowID: TestWindowID,
	}

	client, err := NewUserClient(opts)
	if err != nil {
		t.Fatalf("NewUserClient() error = %v", err)
	}
	// client.running is false by default

	err = client.SwitchWorkspace(TestWorkspace)
	if err == nil {
		t.Error("SwitchWorkspace() should fail when client is not running")
	}

	// Workspace should not have been updated on failure
	if client.Workspace() == TestWorkspace {
		t.Error("Workspace should not be updated when Send fails")
	}
}

func TestUserClient_Identity(t *testing.T) {
	opts := UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: TestServerAddr,
		},
		UserID:    TestUserID,
		WindowID:  TestWindowID,
		Workspace: TestWorkspace,
	}

	client, err := NewUserClient(opts)
	if err != nil {
		t.Fatalf("NewUserClient() error = %v", err)
	}

	if client.UserID() != TestUserID {
		t.Errorf("UserID() = %q, want %q", client.UserID(), TestUserID)
	}
	if client.WindowID() != TestWindowID {
		t.Errorf("WindowID() = %q, want %q", client.WindowID(), TestWindowID)
	}
	if client.Workspace() != TestWorkspace {
		t.Errorf("Workspace() = %q, want %q", client.Workspace(), TestWorkspace)
	}
}

func TestUserClient_Topic(t *testing.T) {
	opts := UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: TestServerAddr,
		},
		UserID:   TestUserID,
		WindowID: TestWindowID,
	}

	client, err := NewUserClient(opts)
	if err != nil {
		t.Fatalf("NewUserClient() error = %v", err)
	}

	expected := UserTopic(TestUserID, TestWindowID)
	if client.Topic() != expected {
		t.Errorf("Topic() = %q, want %q", client.Topic(), expected)
	}
}

func TestUserClient_WorkspaceTopic(t *testing.T) {
	opts := UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: TestServerAddr,
		},
		UserID:    TestUserID,
		WindowID:  TestWindowID,
		Workspace: TestWorkspace,
	}

	client, err := NewUserClient(opts)
	if err != nil {
		t.Fatalf("NewUserClient() error = %v", err)
	}

	expected := UserWorkspaceTopic(TestUserID, TestWorkspace)
	if client.WorkspaceTopic() != expected {
		t.Errorf("WorkspaceTopic() = %q, want %q", client.WorkspaceTopic(), expected)
	}
}

func TestUserClient_WorkspaceTopic_Empty(t *testing.T) {
	opts := UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: TestServerAddr,
		},
		UserID:   TestUserID,
		WindowID: TestWindowID,
		// No workspace set
	}

	client, err := NewUserClient(opts)
	if err != nil {
		t.Fatalf("NewUserClient() error = %v", err)
	}

	if client.WorkspaceTopic() != "" {
		t.Errorf("WorkspaceTopic() = %q, want empty when no workspace set", client.WorkspaceTopic())
	}
}
