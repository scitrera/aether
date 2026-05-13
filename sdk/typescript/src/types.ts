/**
 * Type definitions for the Aether TypeScript SDK.
 *
 * This module provides comprehensive type definitions for principal types,
 * message types, KV scopes, topic construction, and callback handlers.
 * These types mirror the protobuf definitions in api/proto/aether.proto
 * and match the patterns established by the Go and Python SDKs.
 *
 * @module types
 */

// =============================================================================
// Principal Types
// =============================================================================

/**
 * Principal types supported by the Aether gateway.
 *
 * Each principal type has different routing rules and permissions:
 * - Agent: Persistent entity with workspace/impl/spec identity
 * - UniqueTask: Named task with persistent identity
 * - NonUniqueTask: Ephemeral task with server-assigned ID
 * - User: Browser/session-based identity with userId/windowId
 * - WorkflowEngine: Event processor (singleton per gateway)
 * - MetricsBridge: Telemetry collector (singleton per gateway)
 * - Orchestrator: Compute provisioner for lazy-loading agents/tasks
 */
export enum PrincipalType {
  Agent = "agent",
  UniqueTask = "unique_task",
  NonUniqueTask = "non_unique_task",
	User = "user",
	WorkflowEngine = "workflow_engine",
	MetricsBridge = "metrics_bridge",
	Orchestrator = "orchestrator",
	Service = "service",
}

// =============================================================================
// Message Types
// =============================================================================

/**
 * Message types for the Aether messaging protocol.
 *
 * - Chat: Conversational messages between principals
 * - Control: Control/command messages for state changes
 * - ToolCall: Tool invocation messages
 * - Event: Broadcast events processed by WorkflowEngine
 * - Metric: Telemetry data collected by MetricsBridge
 */
export enum MessageType {
  Unspecified = 0,
  Chat = 1,
  Control = 2,
  ToolCall = 3,
  Event = 4,
  Metric = 5,
  Opaque = 6,
}

// =============================================================================
// KV Scope
// =============================================================================

/**
 * Scopes for KV store operations.
 *
 * - Global: Accessible to all entities across all workspaces
 * - Workspace: Accessible within a specific workspace
 * - User: Accessible to a specific user across all workspaces
 * - UserWorkspace: Accessible to a specific user within a specific workspace
 * - GlobalExclusive: Global scope with exclusive (single-owner) write semantics
 * - WorkspaceExclusive: Workspace scope with exclusive (single-owner) write semantics
 * - UserShared: User scope shared across all agent implementations
 * - UserWorkspaceShared: User-workspace scope shared across all agent implementations
 */
export enum KVScope {
  Unspecified = 0,
  Global = 1,
  Workspace = 2,
  User = 3,
  UserWorkspace = 4,
  GlobalExclusive = 5,
  WorkspaceExclusive = 6,
  UserShared = 7,
  UserWorkspaceShared = 8,
}

// =============================================================================
// Task Assignment Mode
// =============================================================================

/**
 * Modes for task assignment.
 *
 * - SelfAssign: Caller self-assigns the task (default)
 * - Targeted: Task is assigned to a specific agent (may trigger orchestration)
 * - Pool: Any matching worker can claim the task
 */
export enum TaskAssignmentMode {
  SelfAssign = 0,
  Targeted = 1,
  Pool = 2,
}

// =============================================================================
// Signal Types
// =============================================================================

/**
 * Signal types from the gateway.
 */
export enum SignalType {
  ForceDisconnect = 0,
  GracefulDisconnect = 1,
}

// =============================================================================
// Message Structures
// =============================================================================

/**
 * An incoming message received from the Aether gateway.
 */
export interface IncomingMessage {
  /** The topic from which the message originated (identifies the sender). */
  readonly sourceTopic: string;
  /** The raw message payload. */
  readonly payload: Uint8Array;
  /** The type of the message (Chat, Control, ToolCall, Event, Metric). */
  readonly messageType?: number;
  /** Local timestamp when the message was received. */
  readonly receivedAt: Date;
}

/**
 * An outgoing message to send through the Aether gateway.
 */
export interface OutgoingMessage {
  /** The destination topic string. */
  targetTopic: string;
  /** The message payload (bytes). */
  payload: Uint8Array;
  /** The type of message. Defaults to Chat. */
  messageType?: MessageType;
}

/**
 * A configuration snapshot received upon connection.
 * Contains KV store data for the client's workspace.
 */
export interface ConfigSnapshot {
  /** Workspace-scoped key-value pairs (opaque bytes; decode with msgpack/TextDecoder as needed). */
  readonly kv: Record<string, Uint8Array>;
  /** Global-scoped key-value pairs (opaque bytes; decode with msgpack/TextDecoder as needed). */
  readonly globalKv: Record<string, Uint8Array>;
  /** Workspace-exclusive-scoped key-value pairs (opaque bytes). */
  readonly workspaceExclusiveKv: Record<string, Uint8Array>;
  /** Global-exclusive-scoped key-value pairs (opaque bytes). */
  readonly globalExclusiveKv: Record<string, Uint8Array>;
}

/**
 * A signal from the gateway (e.g., force disconnect).
 */
export interface Signal {
  /** The signal type. */
  readonly type: SignalType;
  /** Additional context for the signal. */
  readonly reason: string;
}

/**
 * An error response from the gateway.
 */
export interface ErrorResponse {
  /** Error code string. */
  readonly code: string;
  /** Human-readable error message. */
  readonly message: string;
  /** Whether the client should retry this request. */
  readonly retryable: boolean;
  /** Suggested retry delay in milliseconds (0 = use default backoff). */
  readonly retryAfterMs: number;
}

/**
 * Connection acknowledgment received after successful connection.
 */
export interface ConnectionAck {
  /** Server-assigned session identifier. Store for session resumption. */
  readonly sessionId: string;
  /** Whether this was a resumed session. */
  readonly resumed: boolean;
  /** For non-unique tasks: the server-assigned task instance ID. Empty otherwise. */
  readonly assignedId: string;
}

/**
 * A task assignment received by orchestrators.
 */
export interface TaskAssignment {
  readonly taskId: string;
  readonly taskType: string;
  readonly assignedTo: string;
  readonly metadata: Record<string, string>;
  readonly assignedAt: number;
  readonly profile: string;
  readonly launchParams: Record<string, string>;
  readonly targetImplementation: string;
  readonly workspace: string;
  readonly specifier: string;
}

// =============================================================================
// KV Types
// =============================================================================

/**
 * Response from a KV store operation.
 */
export interface KVResponse {
  readonly success: boolean;
  /** Retrieved value (for GET operations). */
  readonly value: Uint8Array;
  /** List of keys (for LIST operations). */
  readonly keys: string[];
  /** Key-value pairs (for LIST with values). */
  readonly kvMap: Record<string, string>;
  /** Correlation ID echoed from the request. */
  readonly requestId: string;
  /** Result of INCREMENT/DECREMENT operations. */
  readonly counterValue?: number;
  /** Whether the guarded increment/decrement was applied (INCREMENT_IF/DECREMENT_IF). */
  readonly applied?: boolean;
}

/**
 * Parameters for a KV GET operation.
 */
export interface KVGetOptions {
  key: string;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Parameters for a KV PUT operation.
 */
export interface KVPutOptions {
  key: string;
  value: Uint8Array;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** TTL in seconds (0 = no expiration). */
  ttl?: number;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Parameters for a KV LIST operation.
 */
export interface KVListOptions {
  keyPrefix?: string;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Parameters for a KV DELETE operation.
 */
export interface KVDeleteOptions {
  key: string;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Parameters for a KV INCREMENT operation.
 */
export interface KVIncrementOptions {
  key: string;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Parameters for a KV DECREMENT operation.
 */
export interface KVDecrementOptions {
  key: string;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Parameters for a KV INCREMENT_IF operation (increment only if counter is below ceiling).
 */
export interface KVIncrementIfOptions {
  key: string;
  /** Amount to increment. Default: 1. */
  delta?: bigint | number;
  /** Ceiling guard: increment only if the current value is strictly below this. */
  ceiling: bigint | number;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

/**
 * Parameters for a KV DECREMENT_IF operation (decrement only if counter is above floor).
 */
export interface KVDecrementIfOptions {
  key: string;
  /** Amount to decrement. Default: 1. */
  delta?: bigint | number;
  /** Floor guard: decrement only if the current value is strictly above this. */
  floor: bigint | number;
  scope?: KVScope;
  userId?: string;
  workspace?: string;
  /** Timeout in milliseconds for sync operations. */
  timeout?: number;
}

// =============================================================================
// Progress Types
// =============================================================================

/**
 * A progress update received from an agent or task.
 * Delivered via the pg.{workspace} RabbitMQ stream with server-side filtering.
 */
export interface ProgressUpdate {
  /** The identity of the reporting agent/task (topic format). */
  readonly source: string;
  /** Task or correlation ID this progress relates to. */
  readonly taskId: string;
  /** Current state (e.g., "running", "finishing", "idle"). */
  readonly state: string;
  /** Completion fraction 0.0-1.0, or -1 for indeterminate. */
  readonly completion: number;
  /** Human-readable summary of current activity. */
  readonly summary: string;
  /** Structured step information for multi-step operations. */
  readonly step?: ProgressStep;
  /** Server timestamp when progress was received (Unix milliseconds). */
  readonly timestampMs: number;
  /** Workspace the progress originated from. */
  readonly workspace: string;
  /** Correlation ID from the originating request. */
  readonly requestId: string;
  /** Arbitrary metadata from the reporter. */
  readonly metadata: Record<string, string>;
  /** Target recipient identity topic (empty = broadcast). */
  readonly recipient: string;
}

/**
 * A discrete step within a multi-step progress operation.
 */
export interface ProgressStep {
  /** Step name/title. */
  readonly name: string;
  /** Detailed description of what this step is doing. */
  readonly detail: string;
  /** Step sequence number (1-based). */
  readonly sequence: number;
  /** Total number of steps (0 = unknown). */
  readonly totalSteps: number;
  /** Step type hint for UI rendering (e.g., "llm_call", "tool_use"). */
  readonly stepType: string;
}

/**
 * Options for reporting progress from an agent or task.
 */
export interface ReportProgressOptions {
  /** Task or correlation ID this progress relates to. Required. */
  taskId: string;
  /** Current state (e.g., "running", "finishing", "idle"). */
  state?: string;
  /** Completion fraction 0.0-1.0, or -1 for indeterminate. Default: -1. */
  completion?: number;
  /** Human-readable summary of current activity. */
  summary?: string;
  /** Step name for multi-step progress. */
  stepName?: string;
  /** Step detail description. */
  stepDetail?: string;
  /** Step sequence number (1-based). */
  stepSequence?: number;
  /** Total number of steps (0 = unknown). */
  stepTotal?: number;
  /** Step type hint for UI rendering. */
  stepType?: string;
  /** Target identity topic to receive updates (empty = broadcast). */
  recipient?: string;
  /** Correlation ID for the originating request. */
  requestId?: string;
  /** Arbitrary key-value metadata. */
  metadata?: Record<string, string>;
}

// =============================================================================
// Checkpoint Types
// =============================================================================

/**
 * Response from a checkpoint operation.
 */
export interface CheckpointResponse {
  readonly success: boolean;
  readonly data: Uint8Array;
  readonly keys: string[];
  readonly error: string;
  readonly savedAt: number;
  readonly requestId: string;
}

// =============================================================================
// Task Management Types
// =============================================================================

/**
 * Represents a task's information from the server.
 */
export interface TaskInfo {
  taskId: string;
  taskType: string;
  status: string;
  workspace: string;
  targetTopic: string;
  assignedTo: string;
  createdAt: number;
  startedAt: number;
  completedAt: number;
  attempt: number;
  maxAttempts: number;
  error: string;
  metadata: Record<string, string>;
}

/**
 * Response to a task query (list or get).
 */
export interface TaskQueryResponse {
  success: boolean;
  error: string;
  task?: TaskInfo;
  tasks: TaskInfo[];
  totalCount: number;
  /** Correlation ID echoed from the request. */
  requestId?: string;
}

/**
 * Response to a task operation (cancel or retry).
 */
export interface TaskOperationResponse {
  success: boolean;
  message: string;
  error: string;
  task?: TaskInfo;
  /** Correlation ID echoed from the request. */
  requestId?: string;
}

/**
 * Response to a CreateTaskRequest that carried a non-empty request_id.
 * Gives the creator the server-assigned task_id for later COMPLETE/FAIL/CANCEL.
 */
export interface CreateTaskResponse {
  /** Whether the task was created successfully. */
  success: boolean;
  /** Server-assigned task ID on success. */
  taskId: string;
  /** Initial task status, e.g., "pending", "assigned", "pending_pool". */
  status: string;
  /** Error code on failure, e.g., "ERR_PERMISSION_DENIED". */
  errorCode: string;
  /** Human-readable error description. */
  errorMessage: string;
  /** Echoed from the originating CreateTaskRequest for correlation. */
  requestId: string;
  /** For TARGETED tasks successfully delivered: the receiving identity string. */
  assignedTo: string;
}

// =============================================================================
// Workspace / Agent / ACL / Workflow Response Types
// =============================================================================

/**
 * Generic workspace operation response.
 */
export interface WorkspaceResponse {
  readonly success: boolean;
  readonly error: string;
  readonly message: string;
  readonly totalCount: number;
  readonly requestId?: string;
  readonly [key: string]: unknown;
}

/**
 * Generic agent operation response.
 */
export interface AgentResponse {
  readonly success: boolean;
  readonly error: string;
  readonly message: string;
  readonly totalCount: number;
  readonly requestId?: string;
  readonly [key: string]: unknown;
}

/**
 * Generic ACL operation response.
 */
export interface ACLResponse {
  readonly success: boolean;
  readonly error: string;
  readonly message: string;
  readonly requestId?: string;
  readonly [key: string]: unknown;
}

/**
 * Stable principal reference attached to an authority grant.
 */
export interface AuthorityGrantPrincipalRef {
  readonly principalType: string;
  readonly principalId: string;
}

/**
 * Resource-specific scope entry attached to an authority grant.
 */
export interface AuthorityGrantResourceScope {
  readonly resourceType: string;
  readonly patterns: string[];
}

/**
 * Authority grant details returned by runtime or admin grant APIs.
 */
export interface AuthorityGrantInfo {
  readonly grantId: string;
  readonly rootGrantId: string;
  readonly subject?: AuthorityGrantPrincipalRef;
  readonly delegate?: AuthorityGrantPrincipalRef;
  readonly issuedBy?: AuthorityGrantPrincipalRef;
  readonly rootSubject?: AuthorityGrantPrincipalRef;
  readonly parentGrantId: string;
  readonly mayDelegate: boolean;
  readonly remainingHops: number;
  readonly workspaceScope: string[];
  readonly resourceScope: AuthorityGrantResourceScope[];
  readonly operationScope: string[];
  readonly maxAccessLevel: number;
  readonly accessLevelName: string;
  readonly audienceType: string;
  readonly audienceId: string;
  readonly validWhileAudienceActive: boolean;
  readonly expiresAt: number;
  readonly renewableUntil: number;
  readonly renewedAt: number;
  readonly revoked: boolean;
  readonly revokedAt: number;
  readonly reason: string;
  readonly metadata: Record<string, string>;
  readonly createdAt: number;
}

/**
 * Runtime authority-grant operation response.
 */
export interface AuthorityGrantResponse {
  readonly success: boolean;
  readonly error: string;
  readonly message: string;
  readonly grant?: AuthorityGrantInfo;
  readonly requestId: string;
  /**
   * For LIST_MY_GRANTS / LIST_GRANTS_ON_ME / BATCH_EXCHANGE results.
   */
  readonly grants?: AuthorityGrantInfo[];
  /**
   * Total matching rows ignoring pagination (LIST_*) or count of returned
   * grants (BATCH_EXCHANGE).
   */
  readonly total?: number;
  /**
   * Server's hint to clients on how often to revalidate cached grants
   * (seconds). Zero means "no hint" — clients fall back to their own policy.
   */
  readonly cacheHintTtlSeconds?: number;
}

/**
 * Server-pushed AuthorityGrantRevocation event. Sent on the downstream
 * stream when a grant the connected client holds is revoked, directly or
 * via cascade from a parent revocation.
 */
export interface AuthorityGrantRevocation {
  readonly grantId: string;
  readonly rootGrantId: string;
  readonly reason: string;
  readonly revokedAt: number;
  readonly cascade: boolean;
}

/**
 * Workflow operation response.
 */
export interface WorkflowResponse {
  readonly success: boolean;
  readonly error: string;
  readonly message: string;
  readonly data?: Uint8Array;
  readonly totalCount: number;
  readonly requestId?: string;
}

/**
 * Options for querying tasks.
 */
export interface TaskQueryOptions {
  workspace?: string;
  status?: string;
  taskType?: string;
  limit?: number;
  offset?: number;
  timeout?: number;
}

// =============================================================================
// Token Management Types
// =============================================================================

/**
 * Information about an API token.
 */
export interface TokenInfo {
  readonly id: string;
  readonly name: string;
  readonly principalType: string;
  readonly workspacePatterns: string[];
  readonly scopes: string[];
  readonly createdBy: string;
  readonly expiresAt: number;
  readonly lastUsedAt: number;
  readonly revoked: boolean;
  readonly revokedAt: number;
  readonly createdAt: number;
  readonly updatedAt: number;
}

/**
 * Response to a token operation.
 */
export interface TokenResponse {
  readonly success: boolean;
  readonly error: string;
  readonly message: string;
  readonly token?: TokenInfo;
  readonly tokens: TokenInfo[];
  readonly totalCount: number;
  readonly plaintextToken: string;
  readonly createdToken?: TokenInfo;
  readonly requestId: string;
}

// =============================================================================
// Callback Types
// =============================================================================

/** Handler for incoming messages. */
export type MessageHandler = (message: IncomingMessage) => void | Promise<void>;

/** Handler for configuration snapshots. */
export type ConfigHandler = (config: ConfigSnapshot) => void | Promise<void>;

/** Handler for signals from the gateway. */
export type SignalHandler = (signal: Signal) => void | Promise<void>;

/** Handler for error responses from the gateway. */
export type ErrorHandler = (error: ErrorResponse) => void | Promise<void>;

/** Handler for KV operation responses. */
export type KVResponseHandler = (response: KVResponse) => void | Promise<void>;

/** Handler for task assignments (used by orchestrators). */
export type TaskAssignmentHandler = (assignment: TaskAssignment) => void | Promise<void>;

/** Handler for checkpoint responses. */
export type CheckpointResponseHandler = (response: CheckpointResponse) => void | Promise<void>;

/** Handler for create task responses. */
export type CreateTaskResponseHandler = (response: CreateTaskResponse) => void | Promise<void>;

/** Handler for task query responses. */
export type TaskQueryResponseHandler = (response: TaskQueryResponse) => void;

/** Handler for task operation responses. */
export type TaskOperationResponseHandler = (response: TaskOperationResponse) => void;

/** Handler for workspace operation responses. */
export type WorkspaceResponseHandler = (response: WorkspaceResponse) => void | Promise<void>;

/** Handler for agent operation responses. */
export type AgentResponseHandler = (response: AgentResponse) => void | Promise<void>;

/** Handler for ACL operation responses. */
export type ACLResponseHandler = (response: ACLResponse) => void | Promise<void>;

/** Handler for runtime authority-grant responses. */
export type AuthorityGrantResponseHandler = (response: AuthorityGrantResponse) => void | Promise<void>;

/** Handler for server-pushed authority-grant revocation events. */
export type AuthorityGrantRevocationHandler = (event: AuthorityGrantRevocation) => void | Promise<void>;

/** Handler for workflow operation responses. */
export type WorkflowResponseHandler = (response: WorkflowResponse) => void | Promise<void>;

/** Handler for token operation responses. */
export type TokenResponseHandler = (response: TokenResponse) => void;

// ---------------------------------------------------------------------------
// Audit submit types
// ---------------------------------------------------------------------------

/** Response type for submitAuditEvent requests. */
export type AuditSubmitResponse = import("./proto/aether/v1/SubmitAuditEventResponse.js").SubmitAuditEventResponse__Output;

/** Handler for audit-submit responses. */
export type AuditSubmitResponseHandler = (response: AuditSubmitResponse) => void;

/** Handler for progress updates from agents/tasks. */
export type ProgressHandler = (update: ProgressUpdate) => void | Promise<void>;

/** Handler for successful connection. */
export type ConnectHandler = (ack: ConnectionAck) => void | Promise<void>;

/** Handler for disconnection events. */
export type DisconnectHandler = (reason: string) => void | Promise<void>;

/** Handler for reconnection attempts. */
export type ReconnectingHandler = (attempt: number) => void | Promise<void>;

// =============================================================================
// Connection Options
// =============================================================================

/**
 * TLS configuration for secure connections.
 */
export interface TLSOptions {
  /** PEM-encoded root CA certificates for server verification. */
  rootCerts?: Buffer;
  /** PEM-encoded client private key (for mTLS). */
  privateKey?: Buffer;
  /** PEM-encoded client certificate chain (for mTLS). */
  certChain?: Buffer;
  /** Override server hostname for certificate validation. */
  serverNameOverride?: string;
}

/**
 * Connection behavior configuration.
 */
export interface ConnectionOptions {
  /** Maximum number of connection attempts (0 = infinite for reconnection). Default: 5. */
  maxRetries?: number;
  /** Initial backoff delay in milliseconds. Default: 1000. */
  initialBackoff?: number;
  /** Maximum backoff delay in milliseconds. Default: 30000. */
  maxBackoff?: number;
  /** Multiplier for exponential backoff. Default: 2.0. */
  backoffMultiplier?: number;
  /** Whether to automatically reconnect on connection loss. Default: true. */
  autoReconnect?: boolean;
  /** Timeout for establishing a connection in milliseconds. Default: 30000. */
  connectTimeout?: number;
  /**
   * When true, if the gateway returns a DuplicateIdentityError (ALREADY_EXISTS)
   * during connection, the client will wait briefly and retry. Useful when a
   * previous instance may still be releasing its distributed lock.
   * Default: false.
   */
  retryOnDuplicate?: boolean;
  /**
   * How long to wait (ms) before retrying after a DuplicateIdentityError.
   * Only used when retryOnDuplicate is true. Default: 5000.
   */
  retryOnDuplicateDelay?: number;
  /**
   * Maximum number of duplicate-identity retries before giving up.
   * Only used when retryOnDuplicate is true. Default: 5.
   */
  retryOnDuplicateMaxAttempts?: number;
}

/**
 * Authentication credentials passed to the gateway.
 */
export interface Credentials {
  [key: string]: string;
}

// =============================================================================
// Credential Helpers
// =============================================================================

/**
 * Creates a credentials object with an API key.
 *
 * @param key - The long-lived API key for authentication
 * @returns Credentials map with the API key header
 */
export function withAPIKey(key: string): Credentials {
  return { "x-api-key": key };
}

/**
 * Creates a credentials object with a bearer token.
 *
 * @param token - The OAuth/JWT bearer token
 * @returns Credentials map with the authorization header
 */
export function withToken(token: string): Credentials {
  return { authorization: `Bearer ${token}` };
}

/**
 * Creates a credentials object with a task token.
 *
 * @param token - The short-lived task authentication token
 * @returns Credentials map with the token header
 */
export function withTaskToken(token: string): Credentials {
  return { token };
}

/**
 * Creates a credentials object with a tenant ID.
 *
 * @param tenantId - The tenant identifier
 * @returns Credentials map with the tenant ID header
 */
export function withTenant(tenantId: string): Credentials {
  return { "x-tenant-id": tenantId };
}
