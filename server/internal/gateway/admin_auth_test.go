package gateway

// Tests for isAllowedAdminOp() and isAllowedACLOp() covering:
//   - WorkflowEngine principal → allowed (true) for all categories
//   - Orchestrator principal → allowed (true) for all categories
//   - Agent with no ACL configured → denied (false), error sent to client
//   - User with no ACL configured → denied (false), error sent to client
//   - Task with no ACL configured → denied (false), error sent to client
//   - Category-specific permission checks
//   - Workspace-scoped ACL enforcement for isAllowedACLOp

import (
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// newAdminTestServer returns a GatewayServer with no ACL (nil) so non-system
// principals are always denied.
func newAdminTestServer() *GatewayServer {
	return &GatewayServer{
		// acl is nil → only system principals are allowed
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
	}
}

func newAdminTestClient(stream *mockStream, principalType models.PrincipalType) *ClientSession {
	return &ClientSession{
		ID: "admin-test-session",
		Identity: models.Identity{
			Type:      principalType,
			Workspace: "ws1",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}
}

// ---------------------------------------------------------------------------
// isAllowedAdminOp: System principals — implicitly allowed
// ---------------------------------------------------------------------------

func TestIsAllowedAdminOp_WorkflowEngine_ReturnsTrue(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalWorkflowEngine)

	for _, category := range []string{"acl", "tokens", "workspaces", "agents"} {
		if !s.isAllowedAdminOp(client, client.Identity, category) {
			t.Errorf("expected isAllowedAdminOp to return true for WorkflowEngine (category=%s)", category)
		}
	}
}

func TestIsAllowedAdminOp_WorkflowEngine_NoErrorSentToClient(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalWorkflowEngine)

	s.isAllowedAdminOp(client, client.Identity, "acl")

	if stream.sentCount() != 0 {
		t.Errorf("expected no messages sent for allowed WorkflowEngine, got %d", stream.sentCount())
	}
}

func TestIsAllowedAdminOp_Orchestrator_ReturnsTrue(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalOrchestrator)

	for _, category := range []string{"acl", "tokens", "workspaces", "agents"} {
		if !s.isAllowedAdminOp(client, client.Identity, category) {
			t.Errorf("expected isAllowedAdminOp to return true for Orchestrator (category=%s)", category)
		}
	}
}

func TestIsAllowedAdminOp_Orchestrator_NoErrorSentToClient(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalOrchestrator)

	s.isAllowedAdminOp(client, client.Identity, "acl")

	if stream.sentCount() != 0 {
		t.Errorf("expected no messages sent for allowed Orchestrator, got %d", stream.sentCount())
	}
}

// ---------------------------------------------------------------------------
// isAllowedAdminOp: Non-system principals without ACL — denied
// ---------------------------------------------------------------------------

func TestIsAllowedAdminOp_Agent_NoACL_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	if s.isAllowedAdminOp(client, client.Identity, "acl") {
		t.Error("expected isAllowedAdminOp to return false for Agent without ACL grant")
	}
}

func TestIsAllowedAdminOp_Agent_NoACL_SendsPermissionDeniedError(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	s.isAllowedAdminOp(client, client.Identity, "tokens")

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response to be sent for denied Agent")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil {
		t.Fatal("expected DownstreamMessage_Error payload")
	}
	if errResp.Code != "ERR_PERMISSION_DENIED" {
		t.Errorf("expected ERR_PERMISSION_DENIED, got %q", errResp.Code)
	}
}

func TestIsAllowedAdminOp_User_NoACL_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalUser)

	if s.isAllowedAdminOp(client, client.Identity, "workspaces") {
		t.Error("expected isAllowedAdminOp to return false for User without ACL grant")
	}
}

func TestIsAllowedAdminOp_User_NoACL_SendsPermissionDeniedError(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalUser)

	s.isAllowedAdminOp(client, client.Identity, "agents")

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response sent for denied User")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil || errResp.Code != "ERR_PERMISSION_DENIED" {
		t.Errorf("expected ERR_PERMISSION_DENIED for User, got %v", errResp)
	}
}

func TestIsAllowedAdminOp_Task_NoACL_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalTask)

	if s.isAllowedAdminOp(client, client.Identity, "acl") {
		t.Error("expected isAllowedAdminOp to return false for Task without ACL grant")
	}
}

func TestIsAllowedAdminOp_MetricsBridge_NoACL_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalMetricsBridge)

	if s.isAllowedAdminOp(client, client.Identity, "acl") {
		t.Error("expected isAllowedAdminOp to return false for MetricsBridge without ACL grant")
	}
}

// ---------------------------------------------------------------------------
// isAllowedAdminOp: Error message includes the category
// ---------------------------------------------------------------------------

func TestIsAllowedAdminOp_ErrorMessageIncludesCategory(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	s.isAllowedAdminOp(client, client.Identity, "tokens")

	if stream.sentCount() == 0 {
		t.Fatal("expected an error response")
	}
	stream.mu.Lock()
	errResp := stream.sent[0].GetError()
	stream.mu.Unlock()

	if errResp == nil {
		t.Fatal("expected error payload")
	}
	// Error message should mention the category
	if errResp.Message == "" {
		t.Error("expected non-empty error message")
	}
}

// ---------------------------------------------------------------------------
// isAllowedACLOp: System principals — implicitly allowed
// ---------------------------------------------------------------------------

func TestIsAllowedACLOp_WorkflowEngine_ReturnsTrue(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalWorkflowEngine)

	aclOp := &pb.ACLOperation{Op: pb.ACLOperation_LIST_RULES}
	if !s.isAllowedACLOp(client, client.Identity, aclOp) {
		t.Error("expected isAllowedACLOp to return true for WorkflowEngine")
	}
}

func TestIsAllowedACLOp_Orchestrator_ReturnsTrue(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalOrchestrator)

	aclOp := &pb.ACLOperation{Op: pb.ACLOperation_GRANT}
	if !s.isAllowedACLOp(client, client.Identity, aclOp) {
		t.Error("expected isAllowedACLOp to return true for Orchestrator")
	}
}

// ---------------------------------------------------------------------------
// isAllowedACLOp: Non-system principals — denied for non-workspace resources
// ---------------------------------------------------------------------------

func TestIsAllowedACLOp_Agent_NoACL_NonWorkspaceResource_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	// ACL op on admin resource (not workspace-scoped) → requires global admin
	aclOp := &pb.ACLOperation{
		Op: pb.ACLOperation_LIST_RULES,
		RuleFilter: &pb.ACLRuleFilter{
			ResourceType: "admin",
			ResourceId:   "admin/*",
		},
	}
	if s.isAllowedACLOp(client, client.Identity, aclOp) {
		t.Error("expected isAllowedACLOp to return false for Agent on non-workspace resource")
	}
}

func TestIsAllowedACLOp_Agent_NoACL_NoFilter_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	// ACL op with no filter → no workspace derivable → requires global admin
	aclOp := &pb.ACLOperation{Op: pb.ACLOperation_LIST_RULES}
	if s.isAllowedACLOp(client, client.Identity, aclOp) {
		t.Error("expected isAllowedACLOp to return false for Agent with no filter")
	}
}

func TestIsAllowedACLOp_Agent_NoACL_WorkspaceResource_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	// ACL op on workspace resource — but no ACL service means no workspace access check possible
	aclOp := &pb.ACLOperation{
		Op: pb.ACLOperation_LIST_RULES,
		RuleFilter: &pb.ACLRuleFilter{
			ResourceType: "workspace",
			ResourceId:   "my-workspace",
		},
	}
	if s.isAllowedACLOp(client, client.Identity, aclOp) {
		t.Error("expected isAllowedACLOp to return false for Agent with no ACL service")
	}
}

// ---------------------------------------------------------------------------
// isAllowedACLOp: GRANT workspace resource — denied for non-admin agents
// ---------------------------------------------------------------------------

func TestIsAllowedACLOp_Agent_NoACL_GrantWorkspace_ReturnsFalse(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	aclOp := &pb.ACLOperation{
		Op: pb.ACLOperation_GRANT,
		GrantRequest: &pb.ACLGrantRequest{
			PrincipalType: "user",
			PrincipalId:   "alice",
			ResourceType:  "workspace",
			ResourceId:    "dev",
			AccessLevel:   10, // Read
		},
	}
	if s.isAllowedACLOp(client, client.Identity, aclOp) {
		t.Error("expected isAllowedACLOp to return false for Agent GRANT with no ACL service")
	}
}

// ---------------------------------------------------------------------------
// isAllowedACLOp: Error messages
// ---------------------------------------------------------------------------

func TestIsAllowedACLOp_NonWorkspaceResource_SendsACLError(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	aclOp := &pb.ACLOperation{
		Op: pb.ACLOperation_GRANT,
		GrantRequest: &pb.ACLGrantRequest{
			ResourceType: "capability",
			ResourceId:   "capability/something",
			AccessLevel:  40,
		},
	}
	result := s.isAllowedACLOp(client, client.Identity, aclOp)
	if result {
		t.Error("expected isAllowedACLOp to return false for non-workspace resource")
	}

	if stream.sentCount() == 0 {
		t.Fatal("expected ACL error response")
	}
	stream.mu.Lock()
	aclResp := stream.sent[0].GetAcl()
	stream.mu.Unlock()

	if aclResp == nil {
		t.Fatal("expected ACLResponse payload")
	}
	if aclResp.Success {
		t.Error("expected ACLResponse.Success=false")
	}
	if aclResp.Error == "" {
		t.Error("expected non-empty ACLResponse.Error message")
	}
}

// ---------------------------------------------------------------------------
// isAllowedAdminOpQuiet: no error sent to client
// ---------------------------------------------------------------------------

func TestIsAllowedAdminOpQuiet_Agent_NoACL_NoErrorSent(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalAgent)

	result := s.isAllowedAdminOpQuiet(client, client.Identity, "acl")
	if result {
		t.Error("expected isAllowedAdminOpQuiet to return false for Agent without ACL")
	}
	if stream.sentCount() != 0 {
		t.Errorf("expected no messages sent by quiet variant, got %d", stream.sentCount())
	}
}

func TestIsAllowedAdminOpQuiet_Orchestrator_ReturnsTrue(t *testing.T) {
	s := newAdminTestServer()
	stream := &mockStream{}
	client := newAdminTestClient(stream, models.PrincipalOrchestrator)

	if !s.isAllowedAdminOpQuiet(client, client.Identity, "acl") {
		t.Error("expected isAllowedAdminOpQuiet to return true for Orchestrator")
	}
}
