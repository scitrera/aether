package gateway

// Tests for cleanupSession() and resolveIdentity().
//
// cleanupSession: verifies session unregistration, lock release, and that
// quota decrement is called when a quotaManager is present.
//
// resolveIdentity: verifies that each principal type (Agent, Task unique,
// Task non-unique, User, Orchestrator, WorkflowEngine, MetricsBridge,
// Bridge, Service) is resolved correctly from an InitConnection message,
// and that an unknown type returns an error.

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// cleanupSession tests
// ---------------------------------------------------------------------------

// buildCleanupCS builds a connectionState and wires a ClientSession into it
// so that cleanupSession can call client.UnsubscribeAll() without panicking.
func buildCleanupCS(identity models.Identity, sessionID string) (*connectionState, *ClientSession) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &ClientSession{
		ID:            sessionID,
		Identity:      identity,
		subscriptions: make(map[string]func()),
	}
	cs := &connectionState{
		identity:      identity,
		sessionID:     sessionID,
		sessionCtx:    ctx,
		sessionCancel: cancel,
		client:        client,
	}
	return cs, client
}

func TestCleanupSession_UnregistersSession(t *testing.T) {
	sessions := newMockSessionManager()
	s := newTestGatewayWithMocks(sessions, newMockMessageRouter(), newMockKVReadWriter(), newMockCheckpointManager())

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	cs, client := buildCleanupCS(identity, "cleanup-session-1")
	s.activeStreams.Store(cs.sessionID, client)
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.cleanupSession(cs, true)

	sessions.mu.Lock()
	unregLen := len(sessions.unregisterCalls)
	sessions.mu.Unlock()

	if unregLen == 0 {
		t.Error("expected UnregisterSession to be called during cleanupSession")
	}
	if sessions.unregisterCalls[0] != "cleanup-session-1" {
		t.Errorf("expected unregister called with session ID 'cleanup-session-1', got %q", sessions.unregisterCalls[0])
	}
}

func TestCleanupSession_ReleasesLock(t *testing.T) {
	sessions := newMockSessionManager()
	s := newTestGatewayWithMocks(sessions, newMockMessageRouter(), newMockKVReadWriter(), newMockCheckpointManager())

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	cs, client := buildCleanupCS(identity, "cleanup-session-2")
	s.activeStreams.Store(cs.sessionID, client)
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.cleanupSession(cs, true)

	sessions.mu.Lock()
	releaseLen := len(sessions.releaseCalls)
	sessions.mu.Unlock()

	if releaseLen == 0 {
		t.Error("expected ReleaseLock to be called during cleanupSession")
	}
}

func TestCleanupSession_RemovesFromActiveStreams(t *testing.T) {
	sessions := newMockSessionManager()
	s := newTestGatewayWithMocks(sessions, newMockMessageRouter(), newMockKVReadWriter(), newMockCheckpointManager())

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	cs, client := buildCleanupCS(identity, "cleanup-session-3")
	s.activeStreams.Store(cs.sessionID, client)
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.cleanupSession(cs, true)

	if _, ok := s.activeStreams.Load(cs.sessionID); ok {
		t.Error("expected session to be removed from activeStreams after cleanupSession")
	}
}

func TestCleanupSession_RemovesFromIdentityIndex(t *testing.T) {
	sessions := newMockSessionManager()
	s := newTestGatewayWithMocks(sessions, newMockMessageRouter(), newMockKVReadWriter(), newMockCheckpointManager())

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	cs, client := buildCleanupCS(identity, "cleanup-session-4")
	s.activeStreams.Store(cs.sessionID, client)
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.cleanupSession(cs, true)

	if _, ok := s.identityIndex.Load(identity.String()); ok {
		t.Error("expected identity to be removed from identityIndex after cleanupSession")
	}
}

func TestCleanupSession_UnsubscribesAllTopics(t *testing.T) {
	sessions := newMockSessionManager()
	s := newTestGatewayWithMocks(sessions, newMockMessageRouter(), newMockKVReadWriter(), newMockCheckpointManager())

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	cs, client := buildCleanupCS(identity, "cleanup-session-5")

	unsubCalled := false
	client.AddSubscription("ag::ws1::impl::spec", func() { unsubCalled = true })

	s.activeStreams.Store(cs.sessionID, client)
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.cleanupSession(cs, true)

	if !unsubCalled {
		t.Error("expected subscriptions to be unsubscribed during cleanupSession")
	}
}

func TestCleanupSession_GracefulExit_CallsBothUnregisterAndRelease(t *testing.T) {
	sessions := newMockSessionManager()
	s := newTestGatewayWithMocks(sessions, newMockMessageRouter(), newMockKVReadWriter(), newMockCheckpointManager())

	identity := models.Identity{Type: models.PrincipalTask, Workspace: "ws2"}
	cs, client := buildCleanupCS(identity, "graceful-exit-session")
	s.activeStreams.Store(cs.sessionID, client)
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.cleanupSession(cs, true) // gracefulExit=true

	sessions.mu.Lock()
	unregLen := len(sessions.unregisterCalls)
	releaseLen := len(sessions.releaseCalls)
	sessions.mu.Unlock()

	if unregLen == 0 {
		t.Error("expected UnregisterSession called on graceful exit")
	}
	if releaseLen == 0 {
		t.Error("expected ReleaseLock called on graceful exit")
	}
}

func TestCleanupSession_UngracefulExit_CallsBothUnregisterAndRelease(t *testing.T) {
	sessions := newMockSessionManager()
	s := newTestGatewayWithMocks(sessions, newMockMessageRouter(), newMockKVReadWriter(), newMockCheckpointManager())

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws3"}
	cs, client := buildCleanupCS(identity, "crash-exit-session")
	s.activeStreams.Store(cs.sessionID, client)
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.cleanupSession(cs, false) // gracefulExit=false

	sessions.mu.Lock()
	unregLen := len(sessions.unregisterCalls)
	releaseLen := len(sessions.releaseCalls)
	sessions.mu.Unlock()

	if unregLen == 0 {
		t.Error("expected UnregisterSession called on ungraceful exit")
	}
	if releaseLen == 0 {
		t.Error("expected ReleaseLock called on ungraceful exit")
	}
}

// ---------------------------------------------------------------------------
// resolveIdentity tests
// ---------------------------------------------------------------------------

func TestResolveIdentity_Agent_ReturnsAgentIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "prod",
				Implementation: "classifier",
				Specifier:      "v2",
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalAgent {
		t.Errorf("expected PrincipalAgent, got %s", ident.Type)
	}
	if ident.Workspace != "prod" {
		t.Errorf("expected Workspace='prod', got %q", ident.Workspace)
	}
	if ident.Implementation != "classifier" {
		t.Errorf("expected Implementation='classifier', got %q", ident.Implementation)
	}
	if ident.Specifier != "v2" {
		t.Errorf("expected Specifier='v2', got %q", ident.Specifier)
	}
}

func TestResolveIdentity_UniqueTask_ReturnsTaskIdentityWithSpecifier(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Task{
			Task: &pb.TaskIdentity{
				Workspace:       "staging",
				Implementation:  "etl",
				UniqueSpecifier: "daily-run",
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalTask {
		t.Errorf("expected PrincipalTask, got %s", ident.Type)
	}
	if ident.Specifier != "daily-run" {
		t.Errorf("expected Specifier='daily-run', got %q", ident.Specifier)
	}
	if ident.ID != "" {
		t.Errorf("expected ID to be empty for unique task, got %q", ident.ID)
	}
}

func TestResolveIdentity_NonUniqueTask_GeneratesID(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Task{
			Task: &pb.TaskIdentity{
				Workspace:       "prod",
				Implementation:  "worker",
				UniqueSpecifier: "", // empty → non-unique task
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalTask {
		t.Errorf("expected PrincipalTask, got %s", ident.Type)
	}
	if ident.Specifier != "" {
		t.Errorf("expected empty Specifier for non-unique task, got %q", ident.Specifier)
	}
	if ident.ID == "" {
		t.Error("expected a generated ID for non-unique task, got empty string")
	}
}

func TestResolveIdentity_NonUniqueTasks_GenerateDistinctIDs(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	makeInit := func() *pb.InitConnection {
		return &pb.InitConnection{
			ClientType: &pb.InitConnection_Task{
				Task: &pb.TaskIdentity{
					Workspace:       "prod",
					Implementation:  "worker",
					UniqueSpecifier: "",
				},
			},
		}
	}

	id1, _ := h.resolveIdentity(makeInit())
	id2, _ := h.resolveIdentity(makeInit())

	if id1.ID == id2.ID {
		t.Errorf("expected distinct IDs for two non-unique tasks, both got %q", id1.ID)
	}
}

func TestResolveIdentity_User_ReturnsUserIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_User{
			User: &pb.UserIdentity{
				UserId:   "alice",
				WindowId: "win-42",
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalUser {
		t.Errorf("expected PrincipalUser, got %s", ident.Type)
	}
	if ident.ID != "alice" {
		t.Errorf("expected ID='alice', got %q", ident.ID)
	}
	if ident.Specifier != "win-42" {
		t.Errorf("expected Specifier='win-42', got %q", ident.Specifier)
	}
}

func TestResolveIdentity_Orchestrator_UsesProvidedSpecifier(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Orchestrator{
			Orchestrator: &pb.OrchestratorIdentity{
				Implementation: "k8s",
				Specifier:      "primary",
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalOrchestrator {
		t.Errorf("expected PrincipalOrchestrator, got %s", ident.Type)
	}
	if ident.Implementation != "k8s" {
		t.Errorf("expected Implementation='k8s', got %q", ident.Implementation)
	}
	if ident.Specifier != "primary" {
		t.Errorf("expected Specifier='primary', got %q", ident.Specifier)
	}
}

func TestResolveIdentity_Orchestrator_EmptySpecifier_GeneratesShortID(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Orchestrator{
			Orchestrator: &pb.OrchestratorIdentity{
				Implementation: "k8s",
				Specifier:      "", // empty → auto-generate
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Specifier == "" {
		t.Error("expected auto-generated Specifier for orchestrator with empty specifier")
	}
}

func TestResolveIdentity_WorkflowEngine_ReturnsWorkflowEngineIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_WorkflowEngine{
			WorkflowEngine: &pb.WorkflowEngineIdentity{},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalWorkflowEngine {
		t.Errorf("expected PrincipalWorkflowEngine, got %s", ident.Type)
	}
}

func TestResolveIdentity_MetricsBridge_ReturnsMetricsBridgeIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_MetricsBridge{
			MetricsBridge: &pb.MetricsBridgeIdentity{},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalMetricsBridge {
		t.Errorf("expected PrincipalMetricsBridge, got %s", ident.Type)
	}
}

func TestResolveIdentity_Bridge_ReturnsBridgeIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Bridge{
			Bridge: &pb.BridgeIdentity{
				Implementation: "aether-msgbridge",
				Specifier:      "discord-1",
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalBridge {
		t.Errorf("expected PrincipalBridge, got %s", ident.Type)
	}
	if ident.Implementation != "aether-msgbridge" {
		t.Errorf("expected Implementation='aether-msgbridge', got %q", ident.Implementation)
	}
	if ident.Specifier != "discord-1" {
		t.Errorf("expected Specifier='discord-1', got %q", ident.Specifier)
	}
	if ident.Workspace != "" {
		t.Errorf("expected Workspace to be empty for bridge identity, got %q", ident.Workspace)
	}
}

func TestResolveIdentity_Service_ReturnsServiceIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Service{
			Service: &pb.ServiceIdentity{
				Implementation: "frontend-api",
				Specifier:      "pod-1",
			},
		},
	}

	ident, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Type != models.PrincipalService {
		t.Errorf("expected PrincipalService, got %s", ident.Type)
	}
	if ident.Implementation != "frontend-api" {
		t.Errorf("expected Implementation='frontend-api', got %q", ident.Implementation)
	}
	if ident.Specifier != "pod-1" {
		t.Errorf("expected Specifier='pod-1', got %q", ident.Specifier)
	}
	if ident.Workspace != "" {
		t.Errorf("expected Workspace to be empty for service identity, got %q", ident.Workspace)
	}
}

func TestResolveIdentity_UnknownType_ReturnsError(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	// An InitConnection with no ClientType set produces an empty identity type.
	init := &pb.InitConnection{}

	_, err := h.resolveIdentity(init)
	if err == nil {
		t.Fatal("expected error for unknown principal type, got nil")
	}
}

// ---------------------------------------------------------------------------
// resolveConnectionIdentity: strict vs relaxed mode
// ---------------------------------------------------------------------------

func TestResolveConnectionIdentity_StrictMode_WithCertificate_UsesCertIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	certIdentity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "cert-ws",
		Implementation: "cert-impl",
		Specifier:      "cert-spec",
	}

	// Even if InitConnection claims different values, cert identity wins in strict mode.
	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "different-ws",
				Implementation: "different-impl",
				Specifier:      "different-spec",
			},
		},
	}

	ident, err := h.resolveConnectionIdentity(context.Background(), init, certIdentity, "", true, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Workspace != "cert-ws" {
		t.Errorf("expected cert workspace 'cert-ws', got %q", ident.Workspace)
	}
}

func TestResolveConnectionIdentity_StrictMode_NoCertificate_MTLSNotRequired_UsesInitIdentity(t *testing.T) {
	h := newAuthHandler(nil, false /* mtlsRequired=false */, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "init-ws",
				Implementation: "init-impl",
				Specifier:      "init-spec",
			},
		},
	}

	ident, err := h.resolveConnectionIdentity(context.Background(), init, models.Identity{}, "", false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ident.Workspace != "init-ws" {
		t.Errorf("expected 'init-ws', got %q", ident.Workspace)
	}
}

func TestResolveConnectionIdentity_StrictMode_NoCertificate_MTLSRequired_ReturnsUnauthenticated(t *testing.T) {
	h := newAuthHandler(nil, true /* mtlsRequired=true */, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{Workspace: "ws1"},
		},
	}

	_, err := h.resolveConnectionIdentity(context.Background(), init, models.Identity{}, "", false, false)
	if err == nil {
		t.Fatal("expected Unauthenticated error when mTLS required but no cert provided")
	}
}

func TestResolveConnectionIdentity_RelaxedMode_NoCertificate_MTLSNotRequired_UsesInitIdentity(t *testing.T) {
	h := newAuthHandler(nil, false, MTLSModeRelaxed, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "relax-ws",
				Implementation: "relax-impl",
				Specifier:      "relax-spec",
			},
		},
	}

	ident, err := h.resolveConnectionIdentity(context.Background(), init, models.Identity{}, "", false, false)
	if err != nil {
		t.Fatalf("unexpected error in relaxed mode without cert: %v", err)
	}
	if ident.Workspace != "relax-ws" {
		t.Errorf("expected 'relax-ws', got %q", ident.Workspace)
	}
}

func TestResolveConnectionIdentity_RelaxedMode_NoCertificate_MTLSRequired_ReturnsUnauthenticated(t *testing.T) {
	h := newAuthHandler(nil, true, MTLSModeRelaxed, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{Workspace: "ws1"},
		},
	}

	_, err := h.resolveConnectionIdentity(context.Background(), init, models.Identity{}, "", false, false)
	if err == nil {
		t.Fatal("expected Unauthenticated error when mTLS required in relaxed mode but no cert")
	}
}

// ---------------------------------------------------------------------------
// resolveIdentity: charset validation boundary tests
//
// These tests verify that the identval validation is wired into the
// resolveIdentity path.  They rely on the default strict mode (true) which
// is active when AETHER_STRICT_IDENTIFIER_CHARSET is unset.  Strict-mode-off
// behaviour is covered in identval/identval_test.go.
// ---------------------------------------------------------------------------

func TestResolveIdentity_InvalidWorkspace_Wildcard_ReturnsError(t *testing.T) {
	// Default strict mode (env var unset) rejects '*'.
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "bad*workspace",
				Implementation: "my-impl",
				Specifier:      "spec1",
			},
		},
	}

	_, err := h.resolveIdentity(init)
	if err == nil {
		t.Fatal("expected error for workspace containing '*', got nil")
	}
}

func TestResolveIdentity_InvalidImpl_DoubleSeparator_ReturnsError(t *testing.T) {
	// Default strict mode rejects '::' inside a token.
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "prod",
				Implementation: "impl::bad",
				Specifier:      "spec1",
			},
		},
	}

	_, err := h.resolveIdentity(init)
	if err == nil {
		t.Fatal("expected error for implementation containing '::', got nil")
	}
}

func TestResolveIdentity_ValidReverseDNSImpl_Succeeds(t *testing.T) {
	// Default strict mode must accept dotted reverse-DNS impl names.
	h := newAuthHandler(nil, false, MTLSModeStrict, nil, nil)

	init := &pb.InitConnection{
		ClientType: &pb.InitConnection_Agent{
			Agent: &pb.AgentIdentity{
				Workspace:      "prod",
				Implementation: "com.example.chat-agent",
				Specifier:      "instance-1",
			},
		},
	}

	_, err := h.resolveIdentity(init)
	if err != nil {
		t.Fatalf("expected success for reverse-DNS impl, got: %v", err)
	}
}
