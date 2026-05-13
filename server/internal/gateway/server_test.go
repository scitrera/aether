package gateway

import (
	"testing"

	"github.com/scitrera/aether/pkg/models"
)

// Test helper types for identity resolution

type testAgent struct {
	workspace      string
	implementation string
	specifier      string
}

type testTask struct {
	workspace       string
	implementation  string
	uniqueSpecifier string
}

type testUser struct {
	userId   string
	windowId string
}

// Tests

func TestIdentityResolution_Agent(t *testing.T) {
	agent := testAgent{workspace: "ws1", implementation: "impl", specifier: "spec"}

	ident := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      agent.workspace,
		Implementation: agent.implementation,
		Specifier:      agent.specifier,
	}

	if ident.Type != models.PrincipalAgent {
		t.Errorf("Expected type PrincipalAgent, got %s", ident.Type)
	}
	if ident.Workspace != "ws1" {
		t.Errorf("Expected workspace 'ws1', got '%s'", ident.Workspace)
	}
	if ident.Implementation != "impl" {
		t.Errorf("Expected implementation 'impl', got '%s'", ident.Implementation)
	}
	if ident.Specifier != "spec" {
		t.Errorf("Expected specifier 'spec', got '%s'", ident.Specifier)
	}
}

func TestIdentityResolution_UniqueTask(t *testing.T) {
	task := testTask{workspace: "ws1", implementation: "impl", uniqueSpecifier: "unique-id"}

	ident := models.Identity{
		Type:           models.PrincipalTask,
		Workspace:      task.workspace,
		Implementation: task.implementation,
		Specifier:      task.uniqueSpecifier,
	}

	if ident.Type != models.PrincipalTask {
		t.Errorf("Expected type PrincipalTask, got %s", ident.Type)
	}
	if ident.Specifier != "unique-id" {
		t.Errorf("Expected specifier 'unique-id', got '%s'", ident.Specifier)
	}
}

func TestIdentityResolution_NonUniqueTask(t *testing.T) {
	task := testTask{workspace: "ws1", implementation: "impl", uniqueSpecifier: ""}

	ident := models.Identity{
		Type:           models.PrincipalTask,
		Workspace:      task.workspace,
		Implementation: task.implementation,
		Specifier:      task.uniqueSpecifier,
	}

	if ident.Specifier != "" {
		t.Errorf("Expected empty specifier for non-unique task, got '%s'", ident.Specifier)
	}
}

func TestIdentityResolution_User(t *testing.T) {
	user := testUser{userId: "user-123", windowId: "window-1"}

	ident := models.Identity{
		Type:      models.PrincipalUser,
		ID:        user.userId,
		Specifier: user.windowId,
	}

	if ident.Type != models.PrincipalUser {
		t.Errorf("Expected type PrincipalUser, got %s", ident.Type)
	}
	if ident.ID != "user-123" {
		t.Errorf("Expected userId 'user-123', got '%s'", ident.ID)
	}
	if ident.Specifier != "window-1" {
		t.Errorf("Expected windowId 'window-1', got '%s'", ident.Specifier)
	}
}

func TestClientSession_Fields(t *testing.T) {
	session := &ClientSession{
		ID:       "test-session-id",
		Identity: models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
	}

	if session.ID != "test-session-id" {
		t.Errorf("Expected ID test-session-id, got %s", session.ID)
	}
	if session.Identity.Type != models.PrincipalAgent {
		t.Errorf("Expected type Agent, got %s", session.Identity.Type)
	}
}
