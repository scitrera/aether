package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Test helpers
// =============================================================================

// resolveFirstPendingAdmin drains the request queue and resolves the first
// pending admin-query request with the given response.
func resolveFirstPendingAdmin(client *BaseClient, resp *AdminResponse) {
	time.Sleep(10 * time.Millisecond)
	<-client.RequestQueue()
	client.pendingAdminRequests.Range(func(key, val any) bool {
		ch := val.(chan *AdminResponse)
		client.pendingAdminRequests.Delete(key)
		ch <- resp
		return false
	})
}

// resolveFirstPendingSession drains the request queue and resolves the first
// pending session-operation request with the given response.
func resolveFirstPendingSession(client *BaseClient, resp *SessionOperationResponse) {
	time.Sleep(10 * time.Millisecond)
	<-client.RequestQueue()
	client.pendingSessionRequests.Range(func(key, val any) bool {
		ch := val.(chan *SessionOperationResponse)
		client.pendingSessionRequests.Delete(key)
		ch <- resp
		return false
	})
}

// newTestAdminClient builds an AdminClient backed by a BaseClient that has
// been put into the "running" state so SendOp queues messages instead of
// failing.
func newTestAdminClient(t *testing.T) *AdminClient {
	t.Helper()
	bc, err := NewBaseClient(BaseClientConfig{ServerAddr: TestServerAddr})
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	bc.running.Store(true)
	return NewAdminClientFromBase(bc)
}

// =============================================================================
// Construction / validation
// =============================================================================

func TestAdminClient_NewAdminClient_MissingServerAddr(t *testing.T) {
	_, err := NewAdminClient(AdminOptions{
		UserID:   "admin",
		WindowID: "w1",
	})
	if err == nil {
		t.Fatal("NewAdminClient should fail without ServerAddr")
	}
}

func TestAdminClient_NewAdminClient_MissingUserID(t *testing.T) {
	_, err := NewAdminClient(AdminOptions{
		ClientOptions: ClientOptions{ServerAddr: TestServerAddr},
		WindowID:      "w1",
	})
	if err == nil {
		t.Fatal("NewAdminClient should fail without UserID")
	}
}

func TestAdminClient_NewAdminClient_MissingWindowID(t *testing.T) {
	_, err := NewAdminClient(AdminOptions{
		ClientOptions: ClientOptions{ServerAddr: TestServerAddr},
		UserID:        "admin",
	})
	if err == nil {
		t.Fatal("NewAdminClient should fail without WindowID")
	}
}

func TestAdminClient_NewAdminClient_HappyPath(t *testing.T) {
	c, err := NewAdminClient(AdminOptions{
		ClientOptions: ClientOptions{ServerAddr: TestServerAddr},
		UserID:        "admin",
		WindowID:      "w1",
		Workspace:     "ops",
	})
	if err != nil {
		t.Fatalf("NewAdminClient() error = %v", err)
	}
	if c.BaseClient() == nil {
		t.Fatal("BaseClient should not be nil after construction")
	}
}

func TestAdminClient_NewAdminClientFromBase(t *testing.T) {
	bc, err := NewBaseClient(BaseClientConfig{ServerAddr: TestServerAddr})
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	c := NewAdminClientFromBase(bc)
	if c.BaseClient() != bc {
		t.Fatal("BaseClient() should return the wrapped *BaseClient")
	}
}

// =============================================================================
// Workspace round-trip
// =============================================================================

func TestAdminClient_ListWorkspaces_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)
	go resolveFirstPendingWorkspace(admin.BaseClient(), &WorkspaceResponse{Success: true, TotalCount: 7})

	resp, err := admin.ListWorkspaces(context.Background(), ListWorkspacesOptions{
		Limit: 50,
	})
	if err != nil {
		t.Fatalf("ListWorkspaces() error = %v", err)
	}
	if !resp.Success || resp.TotalCount != 7 {
		t.Errorf("got %+v, want Success=true TotalCount=7", resp)
	}
}

func TestAdminClient_CreateWorkspace_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		wsOp := msg.GetWorkspaceOp()
		if wsOp == nil || wsOp.Op != pb.WorkspaceOperation_CREATE {
			return
		}
		if wsOp.GetWorkspace().GetWorkspaceId() != "staging" {
			return
		}
		admin.BaseClient().pendingWorkspaceRequests.Range(func(key, val any) bool {
			ch := val.(chan *WorkspaceResponse)
			admin.BaseClient().pendingWorkspaceRequests.Delete(key)
			ch <- &WorkspaceResponse{Success: true}
			return false
		})
	}()

	resp, err := admin.CreateWorkspace(context.Background(), CreateWorkspaceOptions{
		WorkspaceID: "staging",
		DisplayName: "Staging",
	})
	if err != nil {
		t.Fatalf("CreateWorkspace() error = %v", err)
	}
	if !resp.Success {
		t.Error("CreateWorkspace response should be successful")
	}
}

// =============================================================================
// Agent round-trip
// =============================================================================

func TestAdminClient_ListAgents_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)
	go resolveFirstPendingAgent(admin.BaseClient(), &AgentResponse{Success: true, TotalCount: 2})

	resp, err := admin.ListAgents(context.Background(), ListAgentsOptions{})
	if err != nil {
		t.Fatalf("ListAgents() error = %v", err)
	}
	if !resp.Success || resp.TotalCount != 2 {
		t.Errorf("got %+v, want Success=true TotalCount=2", resp)
	}
}

func TestAdminClient_GetAgent_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		agOp := msg.GetAgentOp()
		if agOp == nil || agOp.Op != pb.AgentOperation_GET || agOp.Implementation != "data-processor" {
			return
		}
		admin.BaseClient().pendingAgentRequests.Range(func(key, val any) bool {
			ch := val.(chan *AgentResponse)
			admin.BaseClient().pendingAgentRequests.Delete(key)
			ch <- &AgentResponse{Success: true, Agent: &AgentRegistrationInfo{Implementation: "data-processor"}}
			return false
		})
	}()

	resp, err := admin.GetAgent(context.Background(), GetAgentOptions{
		Implementation: "data-processor",
	})
	if err != nil {
		t.Fatalf("GetAgent() error = %v", err)
	}
	if resp.Agent == nil || resp.Agent.Implementation != "data-processor" {
		t.Errorf("got %+v, want agent with Implementation=data-processor", resp)
	}
}

// =============================================================================
// ACL round-trip
// =============================================================================

func TestAdminClient_CreateACLRule_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		aclOp := msg.GetAclOp()
		if aclOp == nil || aclOp.Op != pb.ACLOperation_GRANT {
			return
		}
		gr := aclOp.GetGrantRequest()
		if gr == nil || gr.GetAccessLevel() != 20 || gr.GetPrincipalId() != "ag.test.impl.spec" {
			return
		}
		admin.BaseClient().pendingACLRequests.Range(func(key, val any) bool {
			ch := val.(chan *ACLResponse)
			admin.BaseClient().pendingACLRequests.Delete(key)
			ch <- &ACLResponse{Success: true, Rule: &ACLRuleInfo{RuleID: "rule-1"}}
			return false
		})
	}()

	resp, err := admin.CreateACLRule(context.Background(), CreateACLRuleOptions{
		PrincipalType: "agent",
		PrincipalID:   "ag.test.impl.spec",
		ResourceType:  "workspace",
		ResourceID:    TestWorkspace,
		AccessLevel:   20, // READWRITE
		GrantedBy:     "ops",
		Reason:        "test",
	})
	if err != nil {
		t.Fatalf("CreateACLRule() error = %v", err)
	}
	if !resp.Success || resp.Rule == nil || resp.Rule.RuleID != "rule-1" {
		t.Errorf("got %+v, want Success=true Rule.RuleID=rule-1", resp)
	}
}

func TestAdminClient_DeleteACLRule_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		aclOp := msg.GetAclOp()
		if aclOp == nil || aclOp.Op != pb.ACLOperation_REVOKE || aclOp.RuleId != "rule-2" {
			return
		}
		admin.BaseClient().pendingACLRequests.Range(func(key, val any) bool {
			ch := val.(chan *ACLResponse)
			admin.BaseClient().pendingACLRequests.Delete(key)
			ch <- &ACLResponse{Success: true}
			return false
		})
	}()

	resp, err := admin.DeleteACLRule(context.Background(), DeleteACLRuleOptions{RuleID: "rule-2"})
	if err != nil {
		t.Fatalf("DeleteACLRule() error = %v", err)
	}
	if !resp.Success {
		t.Error("DeleteACLRule response should be successful")
	}
}

// =============================================================================
// Token round-trip
// =============================================================================

func TestAdminClient_CreateToken_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		tokenOp := msg.GetTokenOp()
		if tokenOp == nil || tokenOp.Op != pb.TokenOperation_CREATE {
			return
		}
		req := tokenOp.GetCreateRequest()
		if req == nil || req.GetName() != "ci-token" || req.GetPrincipalType() != "agent" {
			return
		}
		admin.BaseClient().pendingTokenRequests.Range(func(key, val any) bool {
			ch := val.(chan *TokenResponse)
			admin.BaseClient().pendingTokenRequests.Delete(key)
			ch <- &TokenResponse{
				Success:        true,
				PlaintextToken: "secret-abc",
				CreatedToken:   &TokenInfo{ID: "tok-1", Name: req.GetName()},
			}
			return false
		})
	}()

	resp, err := admin.CreateToken(context.Background(), CreateTokenOptions{
		Name:             "ci-token",
		PrincipalType:    "agent",
		ExpiresInSeconds: 7200, // 2h
	})
	if err != nil {
		t.Fatalf("CreateToken() error = %v", err)
	}
	if resp.PlaintextToken != "secret-abc" {
		t.Errorf("PlaintextToken = %q, want %q", resp.PlaintextToken, "secret-abc")
	}
}

func TestAdminClient_ListTokens_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)
	go resolveFirstPendingToken(admin.BaseClient(), &TokenResponse{Success: true, TotalCount: 1})

	resp, err := admin.ListTokens(context.Background(), ListTokensOptions{IncludeRevoked: true})
	if err != nil {
		t.Fatalf("ListTokens() error = %v", err)
	}
	if !resp.Success || resp.TotalCount != 1 {
		t.Errorf("got %+v, want Success=true TotalCount=1", resp)
	}
}

// =============================================================================
// Workflow round-trip (covered separately by workflow_ops; here we exercise
// the AdminClient surface for parity).
// =============================================================================

func TestAdminClient_RevokeToken_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		tokenOp := msg.GetTokenOp()
		if tokenOp == nil || tokenOp.Op != pb.TokenOperation_REVOKE || tokenOp.TokenId != "tok-zzz" {
			return
		}
		admin.BaseClient().pendingTokenRequests.Range(func(key, val any) bool {
			ch := val.(chan *TokenResponse)
			admin.BaseClient().pendingTokenRequests.Delete(key)
			ch <- &TokenResponse{Success: true, Message: "revoked"}
			return false
		})
	}()

	resp, err := admin.RevokeToken(context.Background(), RevokeTokenOptions{TokenID: "tok-zzz"})
	if err != nil {
		t.Fatalf("RevokeToken() error = %v", err)
	}
	if !resp.Success {
		t.Error("RevokeToken response should be successful")
	}
}

// =============================================================================
// Admin queries (AdminQuery / AdminResponse)
// =============================================================================

func TestAdminClient_GetHealth_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)
	go resolveFirstPendingAdmin(admin.BaseClient(), &AdminResponse{
		Success: true,
		Health:  &pb.HealthInfo{Timestamp: 12345},
	})

	resp, err := admin.GetHealth(context.Background(), 0)
	if err != nil {
		t.Fatalf("GetHealth() error = %v", err)
	}
	if !resp.Success || resp.Health == nil || resp.Health.GetTimestamp() != 12345 {
		t.Errorf("got %+v, want Success=true Health.Timestamp=12345", resp)
	}
}

func TestAdminClient_GetConnections_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		q := msg.GetAdminQuery()
		if q == nil || q.Op != pb.AdminQuery_LIST_CONNECTIONS {
			return
		}
		filter := q.GetFilter()
		if filter == nil || filter.GetWorkspace() != "production" {
			return
		}
		admin.BaseClient().pendingAdminRequests.Range(func(key, val any) bool {
			ch := val.(chan *AdminResponse)
			admin.BaseClient().pendingAdminRequests.Delete(key)
			ch <- &AdminResponse{
				Success:     true,
				TotalCount:  3,
				Connections: []*pb.ConnectionInfo{{SessionId: "s1"}, {SessionId: "s2"}, {SessionId: "s3"}},
			}
			return false
		})
	}()

	resp, err := admin.GetConnections(context.Background(), ListConnectionsOptions{
		Workspace: "production",
	})
	if err != nil {
		t.Fatalf("GetConnections() error = %v", err)
	}
	if !resp.Success || resp.TotalCount != 3 || len(resp.Connections) != 3 {
		t.Errorf("got %+v, want 3 connections", resp)
	}
}

// =============================================================================
// Session operations (DisconnectSession)
// =============================================================================

func TestAdminClient_DisconnectSession_RoundTrip(t *testing.T) {
	admin := newTestAdminClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-admin.BaseClient().RequestQueue()
		sop := msg.GetSessionOp()
		if sop == nil || sop.Op != pb.SessionOperation_DISCONNECT || sop.SessionId != "sess-789" {
			return
		}
		admin.BaseClient().pendingSessionRequests.Range(func(key, val any) bool {
			ch := val.(chan *SessionOperationResponse)
			admin.BaseClient().pendingSessionRequests.Delete(key)
			ch <- &SessionOperationResponse{Success: true, Message: "disconnected"}
			return false
		})
	}()

	resp, err := admin.DisconnectSession(context.Background(), DisconnectSessionOptions{
		SessionID: "sess-789",
		Reason:    "maintenance",
	})
	if err != nil {
		t.Fatalf("DisconnectSession() error = %v", err)
	}
	if !resp.Success {
		t.Error("DisconnectSession response should be successful")
	}
}

// =============================================================================
// Dispatch wiring sanity check: an inbound AdminResponse with a known
// request_id resolves the pending sync caller.
// =============================================================================

func TestBaseClient_DispatchResponse_AdminResponse(t *testing.T) {
	bc, err := NewBaseClient(BaseClientConfig{ServerAddr: TestServerAddr})
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	bc.running.Store(true)

	requestID := bc.NextRequestID()
	ch := bc.RegisterPendingAdminRequest(requestID)

	protoResp := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Admin{
			Admin: &pb.AdminResponse{
				Success:    true,
				TotalCount: 42,
				RequestId:  requestID,
			},
		},
	}
	if err := bc.dispatchResponse(context.Background(), protoResp); err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	select {
	case got := <-ch:
		if !got.Success || got.TotalCount != 42 {
			t.Errorf("got %+v, want Success=true TotalCount=42", got)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatchResponse did not resolve pending admin request")
	}
}
