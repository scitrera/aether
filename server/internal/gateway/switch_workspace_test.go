package gateway

// Tests for handleSwitchWorkspace() covering:
//   - Non-user principal is rejected with ERR_INVALID_PRINCIPAL
//   - Same workspace is a no-op (no messages sent)
//   - Successful workspace switch (old topics unsubscribed, new subscribed)
//   - ACL nil path (no ACL check → switch proceeds)

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// newSwitchWorkspaceServer returns a GatewayServer with mock dependencies
// and no ACL service (nil acl → switch allowed for any user).
func newSwitchWorkspaceServer(router *mockMessageRouter) *GatewayServer {
	return &GatewayServer{
		router:        router,
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
		// acl is nil → ACL check skipped
	}
}

// newUserClient builds a ClientSession for a user principal.
func newUserClient(stream *mockStream, userID, windowID, workspace string) *ClientSession {
	return &ClientSession{
		ID: "user-session",
		Identity: models.Identity{
			Type:      models.PrincipalUser,
			ID:        userID,
			Specifier: windowID,
			Workspace: workspace,
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}
}

// ---------------------------------------------------------------------------
// Non-user principal rejected
// ---------------------------------------------------------------------------

func TestHandleSwitchWorkspace_AgentPrincipal_SendsInvalidPrincipalError(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	client := &ClientSession{
		ID: "agent-session",
		Identity: models.Identity{
			Type:      models.PrincipalAgent,
			Workspace: "ws1",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws2"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for non-user switch workspace attempt")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil {
		t.Fatal("expected DownstreamMessage_Error payload")
	}
	if errResp.Code != "ERR_INVALID_PRINCIPAL" {
		t.Errorf("expected ERR_INVALID_PRINCIPAL, got %q", errResp.Code)
	}
}

func TestHandleSwitchWorkspace_TaskPrincipal_SendsInvalidPrincipalError(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	client := &ClientSession{
		ID: "task-session",
		Identity: models.Identity{
			Type:      models.PrincipalTask,
			Workspace: "ws1",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws2"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response for task switch workspace attempt")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil || errResp.Code != "ERR_INVALID_PRINCIPAL" {
		t.Errorf("expected ERR_INVALID_PRINCIPAL for task principal, got %v", errResp)
	}
}

// ---------------------------------------------------------------------------
// Same workspace → no-op
// ---------------------------------------------------------------------------

func TestHandleSwitchWorkspace_SameWorkspace_NoMessageSent(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	client := newUserClient(stream, "alice", "win-1", "ws1")

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws1"} // same as current
	s.handleSwitchWorkspace(context.Background(), client, sw)

	if stream.sentCount() != 0 {
		t.Errorf("expected no messages for same-workspace switch, got %d", stream.sentCount())
	}
}

func TestHandleSwitchWorkspace_SameWorkspace_WorkspaceUnchanged(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	client := newUserClient(stream, "alice", "win-1", "ws1")

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws1"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	client.identityMu.RLock()
	ws := client.Identity.Workspace
	client.identityMu.RUnlock()

	if ws != "ws1" {
		t.Errorf("expected workspace to remain 'ws1', got %q", ws)
	}
}

// ---------------------------------------------------------------------------
// Successful workspace switch (no ACL, old workspace subscriptions removed)
// ---------------------------------------------------------------------------

func TestHandleSwitchWorkspace_ValidUser_UpdatesWorkspaceOnIdentity(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	client := newUserClient(stream, "bob", "win-2", "ws1")

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws2"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	client.identityMu.RLock()
	ws := client.Identity.Workspace
	client.identityMu.RUnlock()

	if ws != "ws2" {
		t.Errorf("expected workspace to be updated to 'ws2', got %q", ws)
	}
}

func TestHandleSwitchWorkspace_ValidUser_NoErrorSentToClient(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	client := newUserClient(stream, "bob", "win-2", "ws1")

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws2"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	stream.mu.Lock()
	var hasError bool
	for _, m := range stream.sent {
		if m.GetError() != nil {
			hasError = true
		}
	}
	stream.mu.Unlock()

	if hasError {
		t.Error("expected no error response for valid workspace switch")
	}
}

func TestHandleSwitchWorkspace_ValidUser_OldWorkspaceTopicsUnsubscribed(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	// Pre-subscribe to old workspace topics.
	client := newUserClient(stream, "carol", "win-3", "ws1")

	oldTopicUnsubCalled := false
	client.AddSubscription("gu::ws1", func() { oldTopicUnsubCalled = true })
	client.AddSubscription("uw::carol::ws1", func() {})

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws2"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	if !oldTopicUnsubCalled {
		t.Error("expected old workspace topic 'gu::ws1' to be unsubscribed on workspace switch")
	}
	if client.HasSubscription("gu::ws1") {
		t.Error("expected 'gu::ws1' to be removed from subscriptions after workspace switch")
	}
}

func TestHandleSwitchWorkspace_ValidUser_NewWorkspaceTopicsSubscribed(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	client := newUserClient(stream, "dave", "win-4", "ws1")

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws3"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	// After switch, user should be subscribed to gu.ws3 and uw.dave.ws3
	if !router.hasSharedTopic("gu::ws3") {
		t.Error("expected shared subscription for 'gu::ws3' after workspace switch")
	}
	if !router.hasSharedTopic("uw::dave::ws3") {
		t.Error("expected shared subscription for 'uw::dave::ws3' after workspace switch")
	}
}

// ---------------------------------------------------------------------------
// Switch from empty workspace (first workspace assignment)
// ---------------------------------------------------------------------------

func TestHandleSwitchWorkspace_FromEmptyWorkspace_SubscribesToNewWorkspace(t *testing.T) {
	router := newMockMessageRouter()
	s := newSwitchWorkspaceServer(router)
	stream := &mockStream{}

	// User starts with no workspace
	client := newUserClient(stream, "eve", "win-5", "")

	sw := &pb.SwitchWorkspace{NewWorkspaceId: "ws-new"}
	s.handleSwitchWorkspace(context.Background(), client, sw)

	client.identityMu.RLock()
	ws := client.Identity.Workspace
	client.identityMu.RUnlock()

	if ws != "ws-new" {
		t.Errorf("expected workspace 'ws-new', got %q", ws)
	}
}
