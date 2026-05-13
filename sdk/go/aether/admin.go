// Package aether AdminClient implementation.
//
// AdminClient wraps a connected BaseClient (typically obtained from an
// AgentClient, TaskClient, or UserClient) and exposes named helper methods
// for the administrative operations available through the gRPC streaming
// protocol: token management, ACL rules, workspace CRUD, agent registry,
// gateway health/info/stats queries, and session management.
//
// The wrapped BaseClient must already be connected before any AdminClient
// method is invoked. All methods return typed responses; they block on the
// correlated server response or return an error on timeout.
//
// This file mirrors the TypeScript SDK's AdminClient (sdk/typescript/src/admin.ts)
// method-for-method, with Go idioms (context as first arg, typed Options
// structs, error returns). The underlying transport reuses the existing
// WorkspaceOps / AgentOps / ACLOps / TokenOps helpers plus a new
// AdminQueryOps / SessionOps surface for gateway-level admin queries.

package aether

import (
	"context"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// AdminClient
// =============================================================================

// AdminClient provides named helper methods for administrative operations
// on the Aether gateway. It wraps an existing BaseClient and delegates each
// method to the corresponding *Ops helper plus the AdminQuery / SessionOp
// surface added by this file.
//
// Construction: see NewAdminClient (creates an internal user-shaped
// BaseClient) or NewAdminClientFromBase (wraps an existing client such as an
// AgentClient or UserClient).
type AdminClient struct {
	base *BaseClient
}

// NewAdminClientFromBase wraps an existing connected BaseClient (or any
// embedding type such as *AgentClient) in an AdminClient facade. The
// supplied client must already be connected before the AdminClient is used.
//
// This is the preferred constructor when the caller already has a connected
// principal-typed client (Agent, Task, User, etc.).
func NewAdminClientFromBase(base *BaseClient) *AdminClient {
	return &AdminClient{base: base}
}

// AdminOptions configures a standalone AdminClient created via NewAdminClient.
//
// When the caller does not already have a principal-typed client they want
// to reuse, NewAdminClient creates an internal UserClient using these
// options. The Workspace field is optional — if empty, the user is created
// without an initial workspace (admin operations are workspace-agnostic).
type AdminOptions struct {
	ClientOptions

	// UserID is the user/admin identity opening the admin connection.
	// Required.
	UserID string

	// WindowID is the window/session identifier. Required.
	WindowID string

	// Workspace is the optional initial workspace context. Most admin
	// operations are workspace-agnostic; leave empty unless a method that
	// depends on the connection's workspace is being called.
	Workspace string
}

// Validate checks that required fields are set on AdminOptions.
func (o *AdminOptions) Validate() error {
	if err := o.ClientOptions.Validate(); err != nil {
		return err
	}
	if o.UserID == "" {
		return NewInvalidArgumentError("user ID is required", "UserID")
	}
	if o.WindowID == "" {
		return NewInvalidArgumentError("window ID is required", "WindowID")
	}
	return nil
}

// NewAdminClient creates a new standalone AdminClient with its own
// underlying UserClient. The returned client is not connected; the caller
// must call Connect on the embedded *UserClient (accessible via the
// UserClient() accessor) before invoking admin methods.
//
// For most uses, prefer NewAdminClientFromBase wrapping an existing
// connected client.
func NewAdminClient(opts AdminOptions) (*AdminClient, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	uc, err := NewUserClient(UserOptions{
		ClientOptions: opts.ClientOptions,
		UserID:        opts.UserID,
		WindowID:      opts.WindowID,
		Workspace:     opts.Workspace,
	})
	if err != nil {
		return nil, err
	}
	return &AdminClient{base: uc.BaseClient}, nil
}

// BaseClient returns the underlying BaseClient. Useful when callers need to
// register event handlers (OnConnect, OnDisconnect, etc.) or call Connect
// on the embedded transport.
func (a *AdminClient) BaseClient() *BaseClient {
	return a.base
}

// =============================================================================
// Shared option types
// =============================================================================

// AdminTimeoutOption captures the timeout field shared by every admin
// method. A zero value falls back to DefaultAdminTimeout.
type AdminTimeoutOption struct {
	// Timeout is the operation timeout. Default: 10s (DefaultAdminTimeout).
	Timeout time.Duration
}

// =============================================================================
// Token Operations
// =============================================================================

// CreateTokenOptions configures CreateToken.
type CreateTokenOptions struct {
	AdminTimeoutOption

	// Name is a human-readable label for the token. Required.
	Name string

	// PrincipalType this token authenticates as (e.g. "agent", "user").
	PrincipalType string

	// WorkspacePatterns are glob patterns for workspaces this token may
	// access.
	WorkspacePatterns []string

	// Scopes are the permission scopes granted by this token.
	Scopes []string

	// ExpiresInSeconds: token expiry as seconds from now (0 = no expiry).
	// Stored on the server as a duration; mirrors the TS field name.
	ExpiresInSeconds int64

	// CreatedBy records who minted the token (audit trail).
	CreatedBy string
}

// CreateToken creates a new API token and returns the plaintext token
// (only available at creation time) in the response.
func (a *AdminClient) CreateToken(ctx context.Context, opts CreateTokenOptions) (*TokenResponse, error) {
	// ExpiresInSeconds maps to TokenCreateRequest.expires_in_hours (the
	// server enforces hour granularity). Convert by ceiling-rounding to
	// preserve "non-zero means has expiry" semantics.
	var expiresInHours int32
	if opts.ExpiresInSeconds > 0 {
		hours := opts.ExpiresInSeconds / 3600
		if opts.ExpiresInSeconds%3600 != 0 {
			hours++
		}
		expiresInHours = int32(hours)
	}
	return a.base.Tokens().SendOpSync(ctx, &pb.TokenOperation{
		Op: pb.TokenOperation_CREATE,
		CreateRequest: &pb.TokenCreateRequest{
			Name:              opts.Name,
			PrincipalType:     opts.PrincipalType,
			WorkspacePatterns: opts.WorkspacePatterns,
			Scopes:            opts.Scopes,
			ExpiresInHours:    expiresInHours,
			CreatedBy:         opts.CreatedBy,
		},
	}, opts.Timeout)
}

// RevokeTokenOptions configures RevokeToken.
type RevokeTokenOptions struct {
	AdminTimeoutOption

	// TokenID is the token to revoke. Required.
	TokenID string
}

// RevokeToken revokes an API token by ID.
func (a *AdminClient) RevokeToken(ctx context.Context, opts RevokeTokenOptions) (*TokenResponse, error) {
	return a.base.Tokens().SendOpSync(ctx, &pb.TokenOperation{
		Op:      pb.TokenOperation_REVOKE,
		TokenId: opts.TokenID,
	}, opts.Timeout)
}

// ListTokensOptions configures ListTokens.
//
// NOTE: PrincipalType is intentionally absent — the server's TokenFilter
// proto does not include a principal-type filter (see
// api/proto/aether.proto::TokenFilter). The TypeScript SDK exposes the
// field but it is silently dropped on the server side; we omit it here for
// honesty.
type ListTokensOptions struct {
	AdminTimeoutOption

	// IncludeRevoked, when true, returns revoked tokens in results.
	IncludeRevoked bool

	// Limit caps the number of results (0 = server default).
	Limit int32

	// Offset is the pagination offset.
	Offset int32
}

// ListTokens lists API tokens with optional filters.
func (a *AdminClient) ListTokens(ctx context.Context, opts ListTokensOptions) (*TokenResponse, error) {
	return a.base.Tokens().SendOpSync(ctx, &pb.TokenOperation{
		Op: pb.TokenOperation_LIST,
		Filter: &pb.TokenFilter{
			IncludeRevoked: opts.IncludeRevoked,
			Limit:          opts.Limit,
			Offset:         opts.Offset,
		},
	}, opts.Timeout)
}

// =============================================================================
// ACL Operations
// =============================================================================

// CreateACLRuleOptions configures CreateACLRule.
//
// NOTE: The TypeScript SDK accepts a free-form `permission` string and a
// `metadata` map. The Go proto (ACLGrantRequest) instead uses a numeric
// `AccessLevel` tier (0=NONE, 10=READ, 20=READWRITE, 30=MANAGE, 40=ADMIN,
// 50=SUPERADMIN) and a free-form `Reason` for audit. Metadata is not
// part of the proto wire format.
type CreateACLRuleOptions struct {
	AdminTimeoutOption

	// PrincipalType being granted access. Required.
	PrincipalType string

	// PrincipalID being granted access. Required.
	PrincipalID string

	// ResourceType the access applies to (e.g. "workspace"). Required.
	ResourceType string

	// ResourceID the access applies to. Required.
	ResourceID string

	// AccessLevel tier: 0=NONE, 10=READ, 20=READWRITE, 30=MANAGE,
	// 40=ADMIN, 50=SUPERADMIN.
	AccessLevel int32

	// GrantedBy is the identity recording who granted the access.
	GrantedBy string

	// Reason is a free-form audit string.
	Reason string

	// ExpiresAt as Unix timestamp seconds (0 = no expiry).
	ExpiresAt int64
}

// CreateACLRule grants an ACL rule.
func (a *AdminClient) CreateACLRule(ctx context.Context, opts CreateACLRuleOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op: pb.ACLOperation_GRANT,
		GrantRequest: &pb.ACLGrantRequest{
			PrincipalType: opts.PrincipalType,
			PrincipalId:   opts.PrincipalID,
			ResourceType:  opts.ResourceType,
			ResourceId:    opts.ResourceID,
			AccessLevel:   opts.AccessLevel,
			GrantedBy:     opts.GrantedBy,
			Reason:        opts.Reason,
			ExpiresAt:     opts.ExpiresAt,
		},
	}, opts.Timeout)
}

// DeleteACLRuleOptions configures DeleteACLRule.
type DeleteACLRuleOptions struct {
	AdminTimeoutOption

	// RuleID is the rule to delete. Required.
	RuleID string
}

// DeleteACLRule deletes an ACL rule by ID.
func (a *AdminClient) DeleteACLRule(ctx context.Context, opts DeleteACLRuleOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:     pb.ACLOperation_REVOKE,
		RuleId: opts.RuleID,
	}, opts.Timeout)
}

// ListACLRulesOptions configures ListACLRules.
type ListACLRulesOptions struct {
	AdminTimeoutOption

	PrincipalType string
	PrincipalID   string
	ResourceType  string
	ResourceID    string
}

// ListACLRules lists ACL rules with optional filters.
func (a *AdminClient) ListACLRules(ctx context.Context, opts ListACLRulesOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op: pb.ACLOperation_LIST_RULES,
		RuleFilter: &pb.ACLRuleFilter{
			PrincipalType: opts.PrincipalType,
			PrincipalId:   opts.PrincipalID,
			ResourceType:  opts.ResourceType,
			ResourceId:    opts.ResourceID,
		},
	}, opts.Timeout)
}

// GetFallbackPolicyOptions configures GetFallbackPolicy.
type GetFallbackPolicyOptions struct {
	AdminTimeoutOption

	// RuleCategory is the "{principal_type}_{resource_type}" slug.
	// Required.
	RuleCategory string
}

// GetFallbackPolicy reads the fallback policy for a rule category.
func (a *AdminClient) GetFallbackPolicy(ctx context.Context, opts GetFallbackPolicyOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:           pb.ACLOperation_GET_FALLBACK_POLICY,
		RuleCategory: opts.RuleCategory,
	}, opts.Timeout)
}

// SetFallbackPolicyOptions configures SetFallbackPolicy.
type SetFallbackPolicyOptions struct {
	AdminTimeoutOption

	// RuleCategory is the "{principal_type}_{resource_type}" slug.
	// Required.
	RuleCategory string

	// FallbackAccessLevel: 0=NONE, 10=READ, 20=READWRITE, 30=MANAGE,
	// 40=ADMIN, 50=SUPERADMIN.
	FallbackAccessLevel int32
}

// SetFallbackPolicy upserts a fallback policy for a rule category.
func (a *AdminClient) SetFallbackPolicy(ctx context.Context, opts SetFallbackPolicyOptions) (*ACLResponse, error) {
	return a.base.ACL().SendOpSync(ctx, &pb.ACLOperation{
		Op:           pb.ACLOperation_SET_FALLBACK_POLICY,
		RuleCategory: opts.RuleCategory,
		FallbackRequest: &pb.ACLSetFallbackRequest{
			RuleCategory:        opts.RuleCategory,
			FallbackAccessLevel: opts.FallbackAccessLevel,
		},
	}, opts.Timeout)
}

// =============================================================================
// Workspace Operations
// =============================================================================

// ListWorkspacesOptions configures ListWorkspaces.
type ListWorkspacesOptions struct {
	AdminTimeoutOption

	// Limit caps the number of results (0 = server default).
	Limit int32

	// Offset is the pagination offset.
	Offset int32
}

// ListWorkspaces lists workspaces.
func (a *AdminClient) ListWorkspaces(ctx context.Context, opts ListWorkspacesOptions) (*WorkspaceResponse, error) {
	return a.base.Workspace().SendOpSync(ctx, &pb.WorkspaceOperation{
		Op: pb.WorkspaceOperation_LIST,
		Filter: &pb.WorkspaceFilter{
			Limit:  opts.Limit,
			Offset: opts.Offset,
		},
	}, opts.Timeout)
}

// CreateWorkspaceOptions configures CreateWorkspace.
type CreateWorkspaceOptions struct {
	AdminTimeoutOption

	// WorkspaceID is the workspace's unique identifier. Required.
	WorkspaceID string

	// DisplayName is the human-readable display name.
	DisplayName string

	// Metadata is arbitrary metadata.
	Metadata map[string]string
}

// CreateWorkspace creates a new workspace.
func (a *AdminClient) CreateWorkspace(ctx context.Context, opts CreateWorkspaceOptions) (*WorkspaceResponse, error) {
	return a.base.Workspace().SendOpSync(ctx, &pb.WorkspaceOperation{
		Op: pb.WorkspaceOperation_CREATE,
		Workspace: &pb.WorkspaceInfo{
			WorkspaceId: opts.WorkspaceID,
			DisplayName: opts.DisplayName,
			Metadata:    opts.Metadata,
		},
	}, opts.Timeout)
}

// UpdateWorkspaceOptions configures UpdateWorkspace.
type UpdateWorkspaceOptions struct {
	AdminTimeoutOption

	// WorkspaceID identifies the workspace to update. Required.
	WorkspaceID string

	// DisplayName is the new display name.
	DisplayName string

	// Metadata is the updated metadata map.
	Metadata map[string]string
}

// UpdateWorkspace updates an existing workspace.
func (a *AdminClient) UpdateWorkspace(ctx context.Context, opts UpdateWorkspaceOptions) (*WorkspaceResponse, error) {
	return a.base.Workspace().SendOpSync(ctx, &pb.WorkspaceOperation{
		Op:          pb.WorkspaceOperation_UPDATE,
		WorkspaceId: opts.WorkspaceID,
		Workspace: &pb.WorkspaceInfo{
			WorkspaceId: opts.WorkspaceID,
			DisplayName: opts.DisplayName,
			Metadata:    opts.Metadata,
		},
	}, opts.Timeout)
}

// DeleteWorkspaceOptions configures DeleteWorkspace.
type DeleteWorkspaceOptions struct {
	AdminTimeoutOption

	// WorkspaceID identifies the workspace to delete. Required.
	WorkspaceID string
}

// DeleteWorkspace deletes a workspace by ID.
func (a *AdminClient) DeleteWorkspace(ctx context.Context, opts DeleteWorkspaceOptions) (*WorkspaceResponse, error) {
	return a.base.Workspace().SendOpSync(ctx, &pb.WorkspaceOperation{
		Op:          pb.WorkspaceOperation_DELETE,
		WorkspaceId: opts.WorkspaceID,
	}, opts.Timeout)
}

// =============================================================================
// Agent Operations
// =============================================================================

// ListAgentsOptions configures ListAgents.
//
// NOTE: The TypeScript SDK accepts a `workspace` filter, but the server's
// AgentFilter proto only supports filtering by `OrchestratorProfile`,
// limit, and offset (see api/proto/aether.proto::AgentFilter). The Workspace
// field is therefore omitted here.
type ListAgentsOptions struct {
	AdminTimeoutOption

	// OrchestratorProfile filters by orchestrator profile (empty = all).
	OrchestratorProfile string

	// Limit caps the number of results (0 = server default).
	Limit int32

	// Offset is the pagination offset.
	Offset int32
}

// ListAgents lists registered agent implementations.
func (a *AdminClient) ListAgents(ctx context.Context, opts ListAgentsOptions) (*AgentResponse, error) {
	return a.base.Agent().SendOpSync(ctx, &pb.AgentOperation{
		Op: pb.AgentOperation_LIST,
		Filter: &pb.AgentFilter{
			OrchestratorProfile: opts.OrchestratorProfile,
			Limit:               opts.Limit,
			Offset:              opts.Offset,
		},
	}, opts.Timeout)
}

// GetAgentOptions configures GetAgent.
type GetAgentOptions struct {
	AdminTimeoutOption

	// Implementation is the agent implementation name. Required.
	Implementation string
}

// GetAgent retrieves a specific agent registration.
func (a *AdminClient) GetAgent(ctx context.Context, opts GetAgentOptions) (*AgentResponse, error) {
	return a.base.Agent().SendOpSync(ctx, &pb.AgentOperation{
		Op:             pb.AgentOperation_GET,
		Implementation: opts.Implementation,
	}, opts.Timeout)
}

// =============================================================================
// Admin Queries (Health / Info / Stats / Connections)
// =============================================================================

// GetHealth queries gateway health, including component status for Redis,
// RabbitMQ, and PostgreSQL. The gateway must have admin-over-stream enabled.
func (a *AdminClient) GetHealth(ctx context.Context, timeout time.Duration) (*AdminResponse, error) {
	return a.base.Admin().SendOpSync(ctx, &pb.AdminQuery{
		Op: pb.AdminQuery_GET_HEALTH,
	}, timeout)
}

// GetInfo retrieves gateway runtime information.
func (a *AdminClient) GetInfo(ctx context.Context, timeout time.Duration) (*AdminResponse, error) {
	return a.base.Admin().SendOpSync(ctx, &pb.AdminQuery{
		Op: pb.AdminQuery_GET_INFO,
	}, timeout)
}

// GetStats retrieves gateway-wide statistics.
func (a *AdminClient) GetStats(ctx context.Context, timeout time.Duration) (*AdminResponse, error) {
	return a.base.Admin().SendOpSync(ctx, &pb.AdminQuery{
		Op: pb.AdminQuery_GET_STATS,
	}, timeout)
}

// ListConnectionsOptions configures GetConnections.
type ListConnectionsOptions struct {
	AdminTimeoutOption

	// Workspace filters by workspace (empty = all).
	Workspace string

	// PrincipalType filters by principal type (empty = all).
	PrincipalType pb.PrincipalType

	// Limit caps the number of results (0 = server default).
	Limit int32

	// Offset is the pagination offset.
	Offset int32
}

// GetConnections lists active gateway connections.
func (a *AdminClient) GetConnections(ctx context.Context, opts ListConnectionsOptions) (*AdminResponse, error) {
	return a.base.Admin().SendOpSync(ctx, &pb.AdminQuery{
		Op: pb.AdminQuery_LIST_CONNECTIONS,
		Filter: &pb.ConnectionFilter{
			Type:      opts.PrincipalType,
			Workspace: opts.Workspace,
			Limit:     opts.Limit,
			Offset:    opts.Offset,
		},
	}, opts.Timeout)
}

// GetConnectionOptions configures GetConnection.
type GetConnectionOptions struct {
	AdminTimeoutOption

	// SessionID identifies the connection to retrieve. Required.
	SessionID string
}

// GetConnection retrieves a specific connection by session ID.
func (a *AdminClient) GetConnection(ctx context.Context, opts GetConnectionOptions) (*AdminResponse, error) {
	return a.base.Admin().SendOpSync(ctx, &pb.AdminQuery{
		Op:        pb.AdminQuery_GET_CONNECTION,
		SessionId: opts.SessionID,
	}, opts.Timeout)
}

// DisconnectSessionOptions configures DisconnectSession.
type DisconnectSessionOptions struct {
	AdminTimeoutOption

	// SessionID identifies the connection to disconnect. Required.
	SessionID string

	// Reason is an optional message delivered to the disconnected client.
	Reason string
}

// DisconnectSession forcibly disconnects a session by session ID. This
// maps to the gRPC SessionOperation DISCONNECT op (DELETE
// /api/connections/{id}).
func (a *AdminClient) DisconnectSession(ctx context.Context, opts DisconnectSessionOptions) (*SessionOperationResponse, error) {
	return a.base.Session().SendOpSync(ctx, &pb.SessionOperation{
		Op:        pb.SessionOperation_DISCONNECT,
		SessionId: opts.SessionID,
		Reason:    opts.Reason,
	}, opts.Timeout)
}
