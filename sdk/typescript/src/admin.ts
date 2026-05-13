/**
 * Admin client for the Aether TypeScript SDK.
 *
 * AdminClient wraps a connected AetherClient and provides named helper methods
 * for all administrative operations exposed through the gRPC streaming protocol:
 * token management, ACL rules, workspace CRUD, agent registry, and gateway
 * health / connection queries.
 *
 * The underlying AetherClient must already be connected before calling any
 * AdminClient method. All methods return Promises that resolve when the
 * gateway sends the correlated response, or reject on timeout.
 *
 * @module admin
 */

import { AetherClient } from "./client.js";
import type {
  TokenResponse,
  ACLResponse,
  WorkspaceResponse,
  AgentResponse,
} from "./types.js";

// =============================================================================
// Shared option types
// =============================================================================

/** Timeout option shared by all admin methods. */
export interface AdminTimeoutOptions {
  /** Operation timeout in milliseconds. Default: 10000. */
  timeout?: number;
}

// =============================================================================
// Token operation types
// =============================================================================

/** Options for creating an API token. */
export interface CreateTokenOptions extends AdminTimeoutOptions {
  /** Human-readable name for the token. Required. */
  name: string;
  /** Principal type this token authenticates (e.g., "agent", "user"). */
  principalType?: string;
  /** Glob patterns for workspaces this token may access. */
  workspacePatterns?: string[];
  /** Permission scopes granted by this token. */
  scopes?: string[];
  /** Expiry duration in seconds from now (0 = no expiry). */
  expiresInSeconds?: number;
}

/** Options for revoking an API token. */
export interface RevokeTokenOptions extends AdminTimeoutOptions {
  /** Token ID to revoke. Required. */
  tokenId: string;
}

/** Options for listing API tokens. */
export interface ListTokensOptions extends AdminTimeoutOptions {
  /** Filter by principal type. */
  principalType?: string;
  /** If true, include revoked tokens in results. */
  includeRevoked?: boolean;
  /** Maximum number of results to return. */
  limit?: number;
}

// =============================================================================
// ACL operation types
// =============================================================================

/** Options for creating an ACL rule (granting access). */
export interface CreateACLRuleOptions extends AdminTimeoutOptions {
  /** Principal type being granted access (e.g., "agent", "user"). Required. */
  principalType: string;
  /** Principal ID being granted access. Required. */
  principalId: string;
  /** Resource type the access applies to (e.g., "workspace"). Required. */
  resourceType: string;
  /** Resource ID the access applies to. Required. */
  resourceId: string;
  /** Permission level (e.g., "read", "write", "admin"). Required. */
  permission: string;
  /** Optional expiry as Unix timestamp (seconds). 0 = no expiry. */
  expiresAt?: number;
  /** Arbitrary metadata to attach to the rule. */
  metadata?: Record<string, string>;
}

/** Options for deleting an ACL rule. */
export interface DeleteACLRuleOptions extends AdminTimeoutOptions {
  /** Rule ID to delete. Required. */
  ruleId: string;
}

/** Options for listing ACL rules. */
export interface ListACLRulesOptions extends AdminTimeoutOptions {
  /** Filter by principal type. */
  principalType?: string;
  /** Filter by principal ID. */
  principalId?: string;
  /** Filter by resource type. */
  resourceType?: string;
  /** Filter by resource ID. */
  resourceId?: string;
}

/** Options for reading a fallback policy by category. */
export interface GetFallbackPolicyOptions extends AdminTimeoutOptions {
  /**
   * Rule category, formatted as "{principal_type}_{resource_type}" (e.g.,
   * "user_workspace", "agent_kv_scope", "service_kv_scope").
   */
  ruleCategory: string;
}

/** Options for upserting a fallback policy. */
export interface SetFallbackPolicyOptions extends AdminTimeoutOptions {
  /** Rule category to upsert. */
  ruleCategory: string;
  /**
   * Default access level when no explicit acl_rules row matches. Use the
   * numeric tiers from internal/acl/types.go: 0=NONE, 10=READ, 20=READWRITE,
   * 30=MANAGE, 40=ADMIN, 50=SUPERADMIN.
   */
  fallbackAccessLevel: number;
}

/** Principal reference used by authority-grant admin operations. */
export interface AuthorityGrantPrincipalRef {
  principalType: string;
  principalId: string;
}

/** Resource-specific scope entry for authority grants. */
export interface AuthorityGrantResourceScope {
  resourceType: string;
  patterns: string[];
}

// NOTE: ListAuthorityGrantsOptions / GetAuthorityGrantOptions /
// CreateAuthorityGrantOptions / RenewAuthorityGrantOptions /
// RevokeAuthorityGrantOptions were removed alongside the corresponding
// streaming-admin methods. The streaming ACLOperation no longer carries
// authority-grant ops; use the runtime AuthorityGrantOperation surface
// (Phase 4 SDK cache helpers) or the REST admin endpoints for management.

// =============================================================================
// Workspace operation types
// =============================================================================

/** Options for listing workspaces. */
export interface ListWorkspacesOptions extends AdminTimeoutOptions {
  /** Maximum number of results to return. */
  limit?: number;
  /** Offset for pagination. */
  offset?: number;
}

/** Data for creating a workspace. */
export interface CreateWorkspaceOptions extends AdminTimeoutOptions {
  /** Workspace ID (the string identifier). Required. */
  workspaceId: string;
  /** Human-readable display name. */
  displayName?: string;
  /** Arbitrary metadata. */
  metadata?: Record<string, string>;
}

/** Data for updating a workspace. */
export interface UpdateWorkspaceOptions extends AdminTimeoutOptions {
  /** Workspace ID to update. Required. */
  workspaceId: string;
  /** New display name. */
  displayName?: string;
  /** Updated metadata. */
  metadata?: Record<string, string>;
}

/** Options for deleting a workspace. */
export interface DeleteWorkspaceOptions extends AdminTimeoutOptions {
  /** Workspace ID to delete. Required. */
  workspaceId: string;
}

// =============================================================================
// Agent operation types
// =============================================================================

/** Options for listing registered agent types. */
export interface ListAgentsOptions extends AdminTimeoutOptions {
  /** Filter by workspace. */
  workspace?: string;
  /** Maximum number of results to return. */
  limit?: number;
}

/** Options for getting a specific agent registration. */
export interface GetAgentOptions extends AdminTimeoutOptions {
  /** The agent implementation name. Required. */
  implementation: string;
}

// =============================================================================
// Admin query types
// =============================================================================

/** Response from admin queries. Loosely typed to accommodate the AdminResponse oneof. */
export interface AdminQueryResponse {
  readonly success: boolean;
  readonly error: string;
  readonly health?: Record<string, unknown>;
  readonly info?: Record<string, unknown>;
  readonly stats?: Record<string, unknown>;
  readonly connection?: Record<string, unknown>;
  readonly connections?: Record<string, unknown>[];
  readonly totalCount: number;
}

/** Handler type for admin query responses. */
export type AdminQueryResponseHandler = (response: AdminQueryResponse) => void | Promise<void>;

/** Options for listing connections. */
export interface ListConnectionsOptions extends AdminTimeoutOptions {
  /** Filter by workspace. */
  workspace?: string;
  /** Filter by principal type. */
  principalType?: string;
}

/** Options for disconnecting a session. */
export interface DisconnectSessionOptions extends AdminTimeoutOptions {
  /** Session ID to disconnect. Required. */
  sessionId: string;
  /** Optional reason message sent to the disconnected client. */
  reason?: string;
}

// =============================================================================
// AdminClient
// =============================================================================

/**
 * Administrative client for managing an Aether gateway.
 *
 * AdminClient wraps any connected {@link AetherClient} (typically an
 * AgentClient or UserClient) and exposes named helper methods for the full
 * set of administrative operations that are available through the gRPC
 * streaming protocol.
 *
 * @example
 * ```typescript
 * import { AgentClient, AdminClient } from "@scitrera/aether-client";
 *
 * const agent = new AgentClient({
 *   address: "localhost:50051",
 *   workspace: "default",
 *   implementation: "admin-agent",
 *   specifier: "ops-1",
 *   credentials: { "x-api-key": "my-admin-key" },
 * });
 * await agent.connect();
 *
 * const admin = new AdminClient(agent);
 *
 * // List workspaces
 * const wsr = await admin.listWorkspaces();
 * console.log(wsr);
 *
 * // Create a token
 * const tr = await admin.createToken({ name: "ci-token", principalType: "agent" });
 * console.log(tr.plaintextToken);
 * ```
 */
export class AdminClient {
  private readonly _client: AetherClient;

  /**
   * Creates an AdminClient backed by the given AetherClient.
   *
   * The AetherClient must be connected before calling any method.
   *
   * @param client - A connected AetherClient instance
   */
  constructor(client: AetherClient) {
    this._client = client;
  }

  // ===========================================================================
  // Token Operations
  // ===========================================================================

  /**
   * Creates a new API token.
   *
   * @param opts - Token creation parameters
   * @returns Promise resolving to the token response (includes plaintextToken)
   */
  createToken(opts: CreateTokenOptions): Promise<TokenResponse> {
    const { timeout, name, principalType, workspacePatterns, scopes, expiresInSeconds } = opts;
    return this._client.sendTokenOperation(
      {
        op: "CREATE",
        createRequest: {
          name,
          principalType: principalType ?? "",
          workspacePatterns: workspacePatterns ?? [],
          scopes: scopes ?? [],
          expiresInSeconds: expiresInSeconds ?? 0,
        },
      },
      timeout,
    );
  }

  /**
   * Revokes an API token by ID.
   *
   * @param opts - Revoke options containing the token ID
   * @returns Promise resolving to the token response
   */
  revokeToken(opts: RevokeTokenOptions): Promise<TokenResponse> {
    const { timeout, tokenId } = opts;
    return this._client.sendTokenOperation({ op: "REVOKE", tokenId }, timeout);
  }

  /**
   * Lists API tokens with optional filters.
   *
   * @param opts - List options
   * @returns Promise resolving to the token response (tokens array)
   */
  listTokens(opts: ListTokensOptions = {}): Promise<TokenResponse> {
    const { timeout, principalType, includeRevoked, limit } = opts;
    return this._client.sendTokenOperation(
      {
        op: "LIST",
        filter: {
          principalType: principalType ?? "",
          includeRevoked: includeRevoked ?? false,
          limit: limit ?? 0,
        },
      },
      timeout,
    );
  }

  // ===========================================================================
  // ACL Operations
  // ===========================================================================

  /**
   * Creates an ACL rule granting access.
   *
   * @param opts - ACL rule creation parameters
   * @returns Promise resolving to the ACL response
   */
  createACLRule(opts: CreateACLRuleOptions): Promise<ACLResponse> {
    const { timeout, principalType, principalId, resourceType, resourceId, permission, expiresAt, metadata } = opts;
    return this._client.sendACLOperation(
      {
        op: "GRANT",
        grantRequest: {
          principalType,
          principalId,
          resourceType,
          resourceId,
          permission,
          expiresAt: expiresAt ?? 0,
          metadata: metadata ?? {},
        },
      },
      timeout,
    );
  }

  /**
   * Deletes an ACL rule by ID.
   *
   * @param opts - Delete options containing the rule ID
   * @returns Promise resolving to the ACL response
   */
  deleteACLRule(opts: DeleteACLRuleOptions): Promise<ACLResponse> {
    const { timeout, ruleId } = opts;
    return this._client.sendACLOperation({ op: "REVOKE", ruleId }, timeout);
  }

  /**
   * Lists ACL rules with optional filters.
   *
   * @param opts - List options
   * @returns Promise resolving to the ACL response (rules array in response)
   */
  listACLRules(opts: ListACLRulesOptions = {}): Promise<ACLResponse> {
    const { timeout, principalType, principalId, resourceType, resourceId } = opts;
    return this._client.sendACLOperation(
      {
        op: "LIST_RULES",
        ruleFilter: {
          principalType: principalType ?? "",
          principalId: principalId ?? "",
          resourceType: resourceType ?? "",
          resourceId: resourceId ?? "",
        },
      },
      timeout,
    );
  }

  /**
   * Reads the fallback policy for a rule category.
   *
   * Fallback policies decide the default ACL outcome when no explicit
   * `acl_rules` row matches the (principal_type, resource_type) pair.
   * The `ruleCategory` is the catonical
   * `{principal_type}_{resource_type}` slug — e.g., "agent_kv_scope".
   */
  getFallbackPolicy(opts: GetFallbackPolicyOptions): Promise<ACLResponse> {
    const { timeout, ruleCategory } = opts;
    return this._client.sendACLOperation(
      { op: "GET_FALLBACK_POLICY", ruleCategory },
      timeout,
    );
  }

  /**
   * Upserts a fallback policy for a rule category.
   *
   * Use ``fallbackAccessLevel: 0`` to flip a category to default-deny.
   */
  setFallbackPolicy(opts: SetFallbackPolicyOptions): Promise<ACLResponse> {
    const { timeout, ruleCategory, fallbackAccessLevel } = opts;
    return this._client.sendACLOperation(
      {
        op: "SET_FALLBACK_POLICY",
        ruleCategory,
        fallbackRequest: {
          ruleCategory,
          fallbackAccessLevel,
        },
      },
      timeout,
    );
  }

  // NOTE: listAuthorityGrants / getAuthorityGrant / createAuthorityGrant /
  // renewAuthorityGrant / revokeAuthorityGrant were removed alongside the
  // corresponding streaming ACLOperation cases. Use the runtime
  // AuthorityGrantOperation surface (Phase 4 SDK cache helpers) or the
  // REST admin endpoints for management.

  // ===========================================================================
  // Workspace Operations
  // ===========================================================================

  /**
   * Lists workspaces.
   *
   * @param opts - List options
   * @returns Promise resolving to the workspace response
   */
  listWorkspaces(opts: ListWorkspacesOptions = {}): Promise<WorkspaceResponse> {
    const { timeout, limit, offset } = opts;
    return this._client.sendWorkspaceOperation(
      {
        op: "LIST",
        filter: { limit: limit ?? 0, offset: offset ?? 0 },
      },
      timeout,
    );
  }

  /**
   * Creates a new workspace.
   *
   * @param opts - Workspace creation parameters
   * @returns Promise resolving to the workspace response
   */
  createWorkspace(opts: CreateWorkspaceOptions): Promise<WorkspaceResponse> {
    const { timeout, workspaceId, displayName, metadata } = opts;
    return this._client.sendWorkspaceOperation(
      {
        op: "CREATE",
        workspace: {
          workspaceId,
          displayName: displayName ?? "",
          metadata: metadata ?? {},
        },
      },
      timeout,
    );
  }

  /**
   * Updates an existing workspace.
   *
   * @param opts - Workspace update parameters
   * @returns Promise resolving to the workspace response
   */
  updateWorkspace(opts: UpdateWorkspaceOptions): Promise<WorkspaceResponse> {
    const { timeout, workspaceId, displayName, metadata } = opts;
    return this._client.sendWorkspaceOperation(
      {
        op: "UPDATE",
        workspaceId,
        workspace: {
          workspaceId,
          displayName: displayName ?? "",
          metadata: metadata ?? {},
        },
      },
      timeout,
    );
  }

  /**
   * Deletes a workspace by ID.
   *
   * @param opts - Delete options containing the workspace ID
   * @returns Promise resolving to the workspace response
   */
  deleteWorkspace(opts: DeleteWorkspaceOptions): Promise<WorkspaceResponse> {
    const { timeout, workspaceId } = opts;
    return this._client.sendWorkspaceOperation({ op: "DELETE", workspaceId }, timeout);
  }

  // ===========================================================================
  // Agent Operations
  // ===========================================================================

  /**
   * Lists registered agent types.
   *
   * @param opts - List options
   * @returns Promise resolving to the agent response
   */
  listAgents(opts: ListAgentsOptions = {}): Promise<AgentResponse> {
    const { timeout, workspace, limit } = opts;
    return this._client.sendAgentOperation(
      {
        op: "LIST",
        filter: {
          workspace: workspace ?? "",
          limit: limit ?? 0,
        },
      },
      timeout,
    );
  }

  /**
   * Gets the registration details for a specific agent implementation.
   *
   * @param opts - Options including the implementation name
   * @returns Promise resolving to the agent response
   */
  getAgent(opts: GetAgentOptions): Promise<AgentResponse> {
    const { timeout, implementation } = opts;
    return this._client.sendAgentOperation({ op: "GET", implementation }, timeout);
  }

  // ===========================================================================
  // Admin Queries (health, connections)
  // ===========================================================================

  /**
   * Queries gateway health, including component status for Redis, RabbitMQ,
   * and PostgreSQL.
   *
   * Note: This sends a GET_HEALTH admin query through the gRPC stream.
   * The gateway must have admin-over-stream enabled.
   *
   * @param timeout - Timeout in milliseconds (default: 10000)
   * @returns Promise resolving to the admin query response
   */
  getHealth(timeout?: number): Promise<AdminQueryResponse> {
    return this._client
      .sendWorkspaceOperation({ _adminQuery: "GET_HEALTH" }, timeout)
      .then((r) => ({
        success: r.success,
        error: r.error,
        health: r["health"] as Record<string, unknown> | undefined,
        info: undefined,
        stats: undefined,
        connection: undefined,
        connections: undefined,
        totalCount: 0,
      }));
  }

  /**
   * Lists active connections on the gateway.
   *
   * @param opts - Optional filter and timeout options
   * @returns Promise resolving to the admin query response (connections array)
   */
  getConnections(opts: ListConnectionsOptions = {}): Promise<AdminQueryResponse> {
    const { timeout, workspace, principalType } = opts;
    return this._client
      .sendWorkspaceOperation(
        {
          _adminQuery: "LIST_CONNECTIONS",
          filter: {
            workspace: workspace ?? "",
            principalType: principalType ?? "",
          },
        },
        timeout,
      )
      .then((r) => ({
        success: r.success,
        error: r.error,
        health: undefined,
        info: undefined,
        stats: undefined,
        connection: undefined,
        connections: r["connections"] as Record<string, unknown>[] | undefined,
        totalCount: r.totalCount,
      }));
  }

  /**
   * Forcibly disconnects a session by session ID.
   *
   * @param opts - Options containing the session ID and optional reason
   * @returns Promise resolving to the workspace response
   */
  disconnectSession(opts: DisconnectSessionOptions): Promise<WorkspaceResponse> {
    const { timeout, sessionId, reason } = opts;
    return this._client.sendWorkspaceOperation(
      {
        _sessionOp: "DISCONNECT",
        sessionId,
        reason: reason ?? "",
      },
      timeout,
    );
  }
}
