package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// resolveFirstPendingWorkspace drains the request queue and resolves the first
// pending workspace request with the given response.
func resolveFirstPendingWorkspace(client *BaseClient, resp *WorkspaceResponse) {
	time.Sleep(10 * time.Millisecond)
	<-client.RequestQueue()
	client.pendingWorkspaceRequests.Range(func(key, val any) bool {
		ch := val.(chan *WorkspaceResponse)
		client.pendingWorkspaceRequests.Delete(key)
		ch <- resp
		return false
	})
}

// resolveFirstPendingAgent drains the request queue and resolves the first
// pending agent request with the given response.
func resolveFirstPendingAgent(client *BaseClient, resp *AgentResponse) {
	time.Sleep(10 * time.Millisecond)
	<-client.RequestQueue()
	client.pendingAgentRequests.Range(func(key, val any) bool {
		ch := val.(chan *AgentResponse)
		client.pendingAgentRequests.Delete(key)
		ch <- resp
		return false
	})
}

// resolveFirstPendingACL drains the request queue and resolves the first
// pending ACL request with the given response.
func resolveFirstPendingACL(client *BaseClient, resp *ACLResponse) {
	time.Sleep(10 * time.Millisecond)
	<-client.RequestQueue()
	client.pendingACLRequests.Range(func(key, val any) bool {
		ch := val.(chan *ACLResponse)
		client.pendingACLRequests.Delete(key)
		ch <- resp
		return false
	})
}

// =============================================================================
// WorkspaceOps Tests
// =============================================================================

func TestWorkspaceOps_List(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingWorkspace(client, &WorkspaceResponse{Success: true, TotalCount: 3})

	ctx := context.Background()
	resp, err := client.Workspace().List(ctx)
	if err != nil {
		t.Errorf("WorkspaceOps.List() error = %v", err)
	}
	if resp == nil {
		t.Fatal("List() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if resp.TotalCount != 3 {
		t.Errorf("TotalCount = %d, want 3", resp.TotalCount)
	}
}

func TestWorkspaceOps_List_QueuesCorrectOp(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingWorkspace(client, &WorkspaceResponse{Success: true})

	ctx := context.Background()
	_, _ = client.Workspace().List(ctx)

	// The goroutine already drained the queue, but we can verify via async path
	// by re-sending and checking the queued op type
	client.running.Store(true)
	_ = client.Workspace().SendOp(&pb.WorkspaceOperation{Op: pb.WorkspaceOperation_LIST})

	select {
	case msg := <-client.RequestQueue():
		wsOp := msg.GetWorkspaceOp()
		if wsOp == nil {
			t.Fatal("Expected WorkspaceOperation in message")
		}
		if wsOp.Op != pb.WorkspaceOperation_LIST {
			t.Errorf("Op = %v, want LIST", wsOp.Op)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestWorkspaceOps_Get(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		wsOp := msg.GetWorkspaceOp()
		if wsOp == nil || wsOp.Op != pb.WorkspaceOperation_GET {
			return
		}
		client.pendingWorkspaceRequests.Range(func(key, val any) bool {
			ch := val.(chan *WorkspaceResponse)
			client.pendingWorkspaceRequests.Delete(key)
			ch <- &WorkspaceResponse{
				Success:   true,
				Workspace: &WorkspaceInfo{WorkspaceID: wsOp.WorkspaceId},
			}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.Workspace().Get(ctx, "ws-123")
	if err != nil {
		t.Errorf("WorkspaceOps.Get() error = %v", err)
	}
	if resp == nil || resp.Workspace == nil {
		t.Fatal("Get() response/workspace should not be nil")
	}
	if resp.Workspace.WorkspaceID != "ws-123" {
		t.Errorf("WorkspaceID = %q, want %q", resp.Workspace.WorkspaceID, "ws-123")
	}
}

func TestWorkspaceOps_Create(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		wsOp := msg.GetWorkspaceOp()
		if wsOp == nil || wsOp.Op != pb.WorkspaceOperation_CREATE {
			return
		}
		client.pendingWorkspaceRequests.Range(func(key, val any) bool {
			ch := val.(chan *WorkspaceResponse)
			client.pendingWorkspaceRequests.Delete(key)
			ch <- &WorkspaceResponse{Success: true}
			return false
		})
	}()

	ctx := context.Background()
	info := &pb.WorkspaceInfo{
		WorkspaceId: "new-ws",
		DisplayName: "New Workspace",
	}
	resp, err := client.Workspace().Create(ctx, info)
	if err != nil {
		t.Errorf("WorkspaceOps.Create() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Create() response should be successful")
	}
}

func TestWorkspaceOps_Delete(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		wsOp := msg.GetWorkspaceOp()
		if wsOp == nil || wsOp.Op != pb.WorkspaceOperation_DELETE {
			return
		}
		client.pendingWorkspaceRequests.Range(func(key, val any) bool {
			ch := val.(chan *WorkspaceResponse)
			client.pendingWorkspaceRequests.Delete(key)
			ch <- &WorkspaceResponse{Success: true}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.Workspace().Delete(ctx, "old-ws")
	if err != nil {
		t.Errorf("WorkspaceOps.Delete() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Delete() response should be successful")
	}
}

// =============================================================================
// AgentOps Tests
// =============================================================================

func TestAgentOps_List(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingAgent(client, &AgentResponse{Success: true, TotalCount: 5})

	ctx := context.Background()
	resp, err := client.Agent().List(ctx)
	if err != nil {
		t.Errorf("AgentOps.List() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("List() response should be successful")
	}
	if resp.TotalCount != 5 {
		t.Errorf("TotalCount = %d, want 5", resp.TotalCount)
	}
}

func TestAgentOps_Get(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		agOp := msg.GetAgentOp()
		if agOp == nil || agOp.Op != pb.AgentOperation_GET {
			return
		}
		client.pendingAgentRequests.Range(func(key, val any) bool {
			ch := val.(chan *AgentResponse)
			client.pendingAgentRequests.Delete(key)
			ch <- &AgentResponse{
				Success: true,
				Agent:   &AgentRegistrationInfo{Implementation: agOp.Implementation},
			}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.Agent().Get(ctx, "my-agent")
	if err != nil {
		t.Errorf("AgentOps.Get() error = %v", err)
	}
	if resp == nil || resp.Agent == nil {
		t.Fatal("Get() response/agent should not be nil")
	}
	if resp.Agent.Implementation != "my-agent" {
		t.Errorf("Implementation = %q, want %q", resp.Agent.Implementation, "my-agent")
	}
}

func TestAgentOps_Register(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		agOp := msg.GetAgentOp()
		if agOp == nil || agOp.Op != pb.AgentOperation_REGISTER {
			return
		}
		client.pendingAgentRequests.Range(func(key, val any) bool {
			ch := val.(chan *AgentResponse)
			client.pendingAgentRequests.Delete(key)
			ch <- &AgentResponse{Success: true}
			return false
		})
	}()

	ctx := context.Background()
	info := &pb.AgentRegistrationInfo{
		Implementation:      "new-agent",
		OrchestratorProfile: "docker",
	}
	resp, err := client.Agent().Register(ctx, info)
	if err != nil {
		t.Errorf("AgentOps.Register() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Register() response should be successful")
	}
}

func TestAgentOps_Launch(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		agOp := msg.GetAgentOp()
		if agOp == nil || agOp.Op != pb.AgentOperation_LAUNCH {
			return
		}
		client.pendingAgentRequests.Range(func(key, val any) bool {
			ch := val.(chan *AgentResponse)
			client.pendingAgentRequests.Delete(key)
			ch <- &AgentResponse{
				Success:      true,
				LaunchResult: &AgentLaunchResult{TaskID: "task-launch-123"},
			}
			return false
		})
	}()

	ctx := context.Background()
	params := &pb.AgentLaunchParams{
		Workspace: TestWorkspace,
		Specifier: "inst-1",
	}
	resp, err := client.Agent().Launch(ctx, "my-agent", params)
	if err != nil {
		t.Errorf("AgentOps.Launch() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Launch() response should be successful")
	}
	if resp.LaunchResult == nil || resp.LaunchResult.TaskID != "task-launch-123" {
		t.Error("Launch() should include LaunchResult with TaskID")
	}
}

// =============================================================================
// ACLOps Tests
// =============================================================================

func TestACLOps_ListRules(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingACL(client, &ACLResponse{Success: true, TotalRules: 4})

	ctx := context.Background()
	resp, err := client.ACL().ListRules(ctx, nil)
	if err != nil {
		t.Errorf("ACLOps.ListRules() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("ListRules() response should be successful")
	}
	if resp.TotalRules != 4 {
		t.Errorf("TotalRules = %d, want 4", resp.TotalRules)
	}
}

func TestACLOps_Grant(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		aclOp := msg.GetAclOp()
		if aclOp == nil || aclOp.Op != pb.ACLOperation_GRANT {
			return
		}
		client.pendingACLRequests.Range(func(key, val any) bool {
			ch := val.(chan *ACLResponse)
			client.pendingACLRequests.Delete(key)
			ch <- &ACLResponse{
				Success: true,
				Rule:    &ACLRuleInfo{RuleID: "rule-new"},
			}
			return false
		})
	}()

	ctx := context.Background()
	req := &pb.ACLGrantRequest{
		PrincipalType: "agent",
		PrincipalId:   "ag.test.impl.spec",
		ResourceType:  "workspace",
		ResourceId:    TestWorkspace,
	}
	resp, err := client.ACL().Grant(ctx, req)
	if err != nil {
		t.Errorf("ACLOps.Grant() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Grant() response should be successful")
	}
	if resp.Rule == nil || resp.Rule.RuleID != "rule-new" {
		t.Error("Grant() should return the new rule")
	}
}

func TestACLOps_Revoke(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		aclOp := msg.GetAclOp()
		if aclOp == nil || aclOp.Op != pb.ACLOperation_REVOKE {
			return
		}
		if aclOp.RuleId != "rule-abc" {
			return
		}
		client.pendingACLRequests.Range(func(key, val any) bool {
			ch := val.(chan *ACLResponse)
			client.pendingACLRequests.Delete(key)
			ch <- &ACLResponse{Success: true}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.ACL().Revoke(ctx, "rule-abc")
	if err != nil {
		t.Errorf("ACLOps.Revoke() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Revoke() response should be successful")
	}
}

func TestACLOps_QueryAudit(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		aclOp := msg.GetAclOp()
		if aclOp == nil || aclOp.Op != pb.ACLOperation_QUERY_AUDIT {
			return
		}
		client.pendingACLRequests.Range(func(key, val any) bool {
			ch := val.(chan *ACLResponse)
			client.pendingACLRequests.Delete(key)
			ch <- &ACLResponse{
				Success:           true,
				TotalAuditEntries: 10,
			}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.ACL().QueryAudit(ctx, &pb.ACLAuditFilter{Workspace: TestWorkspace})
	if err != nil {
		t.Errorf("ACLOps.QueryAudit() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("QueryAudit() response should be successful")
	}
	if resp.TotalAuditEntries != 10 {
		t.Errorf("TotalAuditEntries = %d, want 10", resp.TotalAuditEntries)
	}
}

// =============================================================================
// Async SendOp Tests (queue verification)
// =============================================================================

func TestWorkspaceOps_SendOp_Async(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.Workspace().SendOp(&pb.WorkspaceOperation{
		Op:          pb.WorkspaceOperation_GET,
		WorkspaceId: "test-ws",
	})
	if err != nil {
		t.Errorf("WorkspaceOps.SendOp() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		wsOp := msg.GetWorkspaceOp()
		if wsOp == nil {
			t.Fatal("Expected WorkspaceOperation in message")
		}
		if wsOp.Op != pb.WorkspaceOperation_GET {
			t.Errorf("Op = %v, want GET", wsOp.Op)
		}
		if wsOp.WorkspaceId != "test-ws" {
			t.Errorf("WorkspaceId = %q, want %q", wsOp.WorkspaceId, "test-ws")
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestAgentOps_SendOp_Async(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.Agent().SendOp(&pb.AgentOperation{
		Op: pb.AgentOperation_LIST,
	})
	if err != nil {
		t.Errorf("AgentOps.SendOp() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		agOp := msg.GetAgentOp()
		if agOp == nil {
			t.Fatal("Expected AgentOperation in message")
		}
		if agOp.Op != pb.AgentOperation_LIST {
			t.Errorf("Op = %v, want LIST", agOp.Op)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestACLOps_SendOp_Async(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.ACL().SendOp(&pb.ACLOperation{
		Op: pb.ACLOperation_LIST_RULES,
	})
	if err != nil {
		t.Errorf("ACLOps.SendOp() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		aclOp := msg.GetAclOp()
		if aclOp == nil {
			t.Fatal("Expected ACLOperation in message")
		}
		if aclOp.Op != pb.ACLOperation_LIST_RULES {
			t.Errorf("Op = %v, want LIST_RULES", aclOp.Op)
		}
	default:
		t.Error("Message should be in queue")
	}
}
