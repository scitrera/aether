// Package aether admin operations for the Go SDK.
//
// This file provides WorkspaceOps, AgentOps, and ACLOps helpers for managing
// workspace, agent, and ACL resources via the Aether gateway protocol.
//
// Operations can be performed in two modes:
//   - Async: Fire-and-forget via Send*Op; responses delivered to handler callbacks
//   - Sync: Blocking via Send*OpSync; waits for correlated response with timeout

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultAdminTimeout is the default timeout for synchronous admin operations.
const DefaultAdminTimeout = 10 * time.Second

// =============================================================================
// WorkspaceOps
// =============================================================================

// WorkspaceOps provides workspace management operations on a client.
type WorkspaceOps struct {
	client *BaseClient
	syncMu sync.Mutex
}

// newWorkspaceOps creates a new WorkspaceOps helper for a client.
func newWorkspaceOps(client *BaseClient) *WorkspaceOps {
	return &WorkspaceOps{client: client}
}

// SendOp sends a workspace operation asynchronously.
// The response is delivered via the OnWorkspaceResponse handler callback.
func (w *WorkspaceOps) SendOp(op *pb.WorkspaceOperation) error {
	return w.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_WorkspaceOp{WorkspaceOp: op},
	})
}

// SendOpSync sends a workspace operation and waits for the correlated response.
func (w *WorkspaceOps) SendOpSync(ctx context.Context, op *pb.WorkspaceOperation, timeout time.Duration) (*WorkspaceResponse, error) {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultAdminTimeout
	}

	requestID := w.client.NextRequestID()
	ch := w.client.RegisterPendingWorkspaceRequest(requestID)
	defer w.client.pendingWorkspaceRequests.Delete(requestID)

	if err := w.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("workspace operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// List lists all workspaces.
func (w *WorkspaceOps) List(ctx context.Context) (*WorkspaceResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkspaceOperation{Op: pb.WorkspaceOperation_LIST}, 0)
}

// Get retrieves a specific workspace by ID.
func (w *WorkspaceOps) Get(ctx context.Context, workspaceID string) (*WorkspaceResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkspaceOperation{
		Op:          pb.WorkspaceOperation_GET,
		WorkspaceId: workspaceID,
	}, 0)
}

// Create creates a new workspace.
func (w *WorkspaceOps) Create(ctx context.Context, workspace *pb.WorkspaceInfo) (*WorkspaceResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkspaceOperation{
		Op:        pb.WorkspaceOperation_CREATE,
		Workspace: workspace,
	}, 0)
}

// Update updates an existing workspace by ID.
func (w *WorkspaceOps) Update(ctx context.Context, workspaceID string, workspace *pb.WorkspaceInfo) (*WorkspaceResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkspaceOperation{
		Op:          pb.WorkspaceOperation_UPDATE,
		WorkspaceId: workspaceID,
		Workspace:   workspace,
	}, 0)
}

// Delete deletes a workspace by ID.
func (w *WorkspaceOps) Delete(ctx context.Context, workspaceID string) (*WorkspaceResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkspaceOperation{
		Op:          pb.WorkspaceOperation_DELETE,
		WorkspaceId: workspaceID,
	}, 0)
}

// GetMessageFlow retrieves the message flow graph for a workspace.
func (w *WorkspaceOps) GetMessageFlow(ctx context.Context, workspaceID string) (*WorkspaceResponse, error) {
	return w.SendOpSync(ctx, &pb.WorkspaceOperation{
		Op:          pb.WorkspaceOperation_GET_MESSAGE_FLOW,
		WorkspaceId: workspaceID,
	}, 0)
}

// Workspace returns the WorkspaceOps helper for this client.
func (c *BaseClient) Workspace() *WorkspaceOps {
	c.workspaceOnce.Do(func() {
		c.workspaceInstance = newWorkspaceOps(c)
	})
	return c.workspaceInstance
}

// =============================================================================
// AgentOps
// =============================================================================

// AgentOps provides agent management operations on a client.
type AgentOps struct {
	client *BaseClient
	syncMu sync.Mutex
}

// newAgentOps creates a new AgentOps helper for a client.
func newAgentOps(client *BaseClient) *AgentOps {
	return &AgentOps{client: client}
}

// SendOp sends an agent operation asynchronously.
// The response is delivered via the OnAgentResponse handler callback.
func (a *AgentOps) SendOp(op *pb.AgentOperation) error {
	return a.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_AgentOp{AgentOp: op},
	})
}

// SendOpSync sends an agent operation and waits for the correlated response.
func (a *AgentOps) SendOpSync(ctx context.Context, op *pb.AgentOperation, timeout time.Duration) (*AgentResponse, error) {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultAdminTimeout
	}

	requestID := a.client.NextRequestID()
	ch := a.client.RegisterPendingAgentRequest(requestID)
	defer a.client.pendingAgentRequests.Delete(requestID)

	if err := a.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("agent operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// List lists all agent registrations.
func (a *AgentOps) List(ctx context.Context) (*AgentResponse, error) {
	return a.SendOpSync(ctx, &pb.AgentOperation{Op: pb.AgentOperation_LIST}, 0)
}

// Get retrieves a specific agent registration by implementation name.
func (a *AgentOps) Get(ctx context.Context, implementation string) (*AgentResponse, error) {
	return a.SendOpSync(ctx, &pb.AgentOperation{
		Op:             pb.AgentOperation_GET,
		Implementation: implementation,
	}, 0)
}

// Register registers a new agent type.
func (a *AgentOps) Register(ctx context.Context, agent *pb.AgentRegistrationInfo) (*AgentResponse, error) {
	return a.SendOpSync(ctx, &pb.AgentOperation{
		Op:    pb.AgentOperation_REGISTER,
		Agent: agent,
	}, 0)
}

// Update updates an existing agent registration.
func (a *AgentOps) Update(ctx context.Context, implementation string, agent *pb.AgentRegistrationInfo) (*AgentResponse, error) {
	return a.SendOpSync(ctx, &pb.AgentOperation{
		Op:             pb.AgentOperation_UPDATE,
		Implementation: implementation,
		Agent:          agent,
	}, 0)
}

// Delete deletes an agent registration by implementation name.
func (a *AgentOps) Delete(ctx context.Context, implementation string) (*AgentResponse, error) {
	return a.SendOpSync(ctx, &pb.AgentOperation{
		Op:             pb.AgentOperation_DELETE,
		Implementation: implementation,
	}, 0)
}

// Launch launches an agent via orchestration.
func (a *AgentOps) Launch(ctx context.Context, implementation string, params *pb.AgentLaunchParams) (*AgentResponse, error) {
	return a.SendOpSync(ctx, &pb.AgentOperation{
		Op:             pb.AgentOperation_LAUNCH,
		Implementation: implementation,
		LaunchParams:   params,
	}, 0)
}

// ListOrchestrators lists connected orchestrators with their profiles.
func (a *AgentOps) ListOrchestrators(ctx context.Context) (*AgentResponse, error) {
	return a.SendOpSync(ctx, &pb.AgentOperation{Op: pb.AgentOperation_LIST_ORCHESTRATORS}, 0)
}

// Agent returns the AgentOps helper for this client.
func (c *BaseClient) Agent() *AgentOps {
	c.agentOnce.Do(func() {
		c.agentInstance = newAgentOps(c)
	})
	return c.agentInstance
}

// =============================================================================
// ACLOps
// =============================================================================

// ACLOps provides ACL management operations on a client.
type ACLOps struct {
	client *BaseClient
	syncMu sync.Mutex
}

// newACLOps creates a new ACLOps helper for a client.
func newACLOps(client *BaseClient) *ACLOps {
	return &ACLOps{client: client}
}

// SendOp sends an ACL operation asynchronously.
// The response is delivered via the OnACLResponse handler callback.
func (a *ACLOps) SendOp(op *pb.ACLOperation) error {
	return a.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_AclOp{AclOp: op},
	})
}

// SendOpSync sends an ACL operation and waits for the correlated response.
func (a *ACLOps) SendOpSync(ctx context.Context, op *pb.ACLOperation, timeout time.Duration) (*ACLResponse, error) {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultAdminTimeout
	}

	requestID := a.client.NextRequestID()
	ch := a.client.RegisterPendingACLRequest(requestID)
	defer a.client.pendingACLRequests.Delete(requestID)

	if err := a.SendOp(op); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("ACL operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// ListRules lists ACL rules with optional filters.
func (a *ACLOps) ListRules(ctx context.Context, filter *pb.ACLRuleFilter) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:         pb.ACLOperation_LIST_RULES,
		RuleFilter: filter,
	}, 0)
}

// GetRule retrieves a specific ACL rule by ID.
func (a *ACLOps) GetRule(ctx context.Context, ruleID string) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:     pb.ACLOperation_GET_RULE,
		RuleId: ruleID,
	}, 0)
}

// Grant creates a new ACL rule to grant access.
func (a *ACLOps) Grant(ctx context.Context, req *pb.ACLGrantRequest) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:           pb.ACLOperation_GRANT,
		GrantRequest: req,
	}, 0)
}

// Revoke deletes an ACL rule by ID.
func (a *ACLOps) Revoke(ctx context.Context, ruleID string) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:     pb.ACLOperation_REVOKE,
		RuleId: ruleID,
	}, 0)
}

// QueryAudit queries the ACL audit log.
func (a *ACLOps) QueryAudit(ctx context.Context, filter *pb.ACLAuditFilter) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:          pb.ACLOperation_QUERY_AUDIT,
		AuditFilter: filter,
	}, 0)
}

// GetFallbackPolicy retrieves a fallback policy by rule category.
func (a *ACLOps) GetFallbackPolicy(ctx context.Context, ruleCategory string) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:           pb.ACLOperation_GET_FALLBACK_POLICY,
		RuleCategory: ruleCategory,
	}, 0)
}

// SetFallbackPolicy sets or updates a fallback policy.
func (a *ACLOps) SetFallbackPolicy(ctx context.Context, req *pb.ACLSetFallbackRequest) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:              pb.ACLOperation_SET_FALLBACK_POLICY,
		FallbackRequest: req,
	}, 0)
}

// CleanupExpired removes expired ACL rules.
func (a *ACLOps) CleanupExpired(ctx context.Context) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{Op: pb.ACLOperation_CLEANUP_EXPIRED}, 0)
}

// CleanupAuditLogs removes old audit log entries.
func (a *ACLOps) CleanupAuditLogs(ctx context.Context, retentionDays int32) (*ACLResponse, error) {
	return a.SendOpSync(ctx, &pb.ACLOperation{
		Op:            pb.ACLOperation_CLEANUP_AUDIT_LOGS,
		RetentionDays: retentionDays,
	}, 0)
}

// ACL returns the ACLOps helper for this client.
func (c *BaseClient) ACL() *ACLOps {
	c.aclOnce.Do(func() {
		c.aclInstance = newACLOps(c)
	})
	return c.aclInstance
}
