/**
 * @scitrera/aether-client - TypeScript SDK for the Aether distributed control plane.
 *
 * Aether is a distributed control plane for routing structured messages,
 * tracking tasks, and managing connection lifecycles. This SDK provides
 * TypeScript/JavaScript clients for agents, users, and other principal types.
 *
 * Key architectural principle: The connection itself IS the distributed lock
 * AND the heartbeat. When the gRPC stream closes, the identity lock is
 * immediately released. No separate heartbeat API exists.
 *
 * Principal Types:
 * - {@link AgentClient}: For persistent agents with workspace/impl/spec identity
 * - {@link TaskClient}: For task connections (unique or non-unique)
 * - {@link UserClient}: For user connections with userId/windowId identity
 * - {@link OrchestratorClient}: For orchestrating agent/task lifecycle
 * - {@link WorkflowEngineClient}: For event processing (singleton)
 * - {@link MetricsBridgeClient}: For telemetry collection (singleton, receive-only)
 * - {@link AetherClient}: Base client for custom principal types
 *
 * @example
 * ```typescript
 * import { AgentClient, MessageType } from "@scitrera/aether-client";
 *
 * const agent = new AgentClient({
 *   address: "localhost:50051",
 *   workspace: "production",
 *   implementation: "my-agent",
 *   specifier: "instance-1",
 * });
 *
 * agent.onMessage((msg) => {
 *   console.log(`Received from ${msg.sourceTopic}:`, msg.payload);
 * });
 *
 * agent.onConnect((ack) => {
 *   console.log(`Connected with session ${ack.sessionId}`);
 * });
 *
 * await agent.connect();
 * ```
 *
 * @packageDocumentation
 */

// Client classes
export { AetherClient } from "./client.js";
export type { AetherClientOptions } from "./client.js";

export { AdminClient } from "./admin.js";
export type {
  AdminQueryResponse,
  AdminQueryResponseHandler,
  AdminTimeoutOptions,
  CreateTokenOptions,
  RevokeTokenOptions,
  ListTokensOptions,
  CreateACLRuleOptions,
  DeleteACLRuleOptions,
  ListACLRulesOptions,
  ListWorkspacesOptions,
  CreateWorkspaceOptions,
  UpdateWorkspaceOptions,
  DeleteWorkspaceOptions,
  ListAgentsOptions,
  GetAgentOptions,
  ListConnectionsOptions,
  DisconnectSessionOptions,
} from "./admin.js";

export { AgentClient } from "./agents.js";
export type { AgentClientOptions, CreateTaskOptions } from "./agents.js";

export { TaskClient } from "./tasks.js";
export type { TaskClientOptions } from "./tasks.js";

export { UserClient } from "./users.js";
export type { UserClientOptions } from "./users.js";

export { OrchestratorClient, BaseOrchestrator } from "./orchestrator.js";
export type { OrchestratorClientOptions, BaseOrchestratorOptions } from "./orchestrator.js";

export { WorkflowEngineClient } from "./workflow.js";
export type { WorkflowEngineClientOptions } from "./workflow.js";

export { MetricsBridgeClient } from "./metrics.js";
export type { MetricsBridgeClientOptions } from "./metrics.js";

export { MetricBuilder, newMetric } from "./metrics-builder.js";
export type { Metric, MetricEntry } from "./metrics-builder.js";

export { BridgeClient } from "./bridge.js";
export type { BridgeClientOptions } from "./bridge.js";

export { KVClient } from "./kv.js";

export { CheckpointClient } from "./checkpoint.js";
export type {
  CheckpointSaveOptions,
  CheckpointLoadOptions,
  CheckpointDeleteOptions,
  CheckpointListOptions,
} from "./checkpoint.js";

// Types and enums
export {
  PrincipalType,
  MessageType,
  KVScope,
  TaskAssignmentMode,
  SignalType,
} from "./types.js";

export type {
  // Message structures
  IncomingMessage,
  OutgoingMessage,
  ConfigSnapshot,
  Signal,
  ErrorResponse,
  ConnectionAck,
  TaskAssignment,
  // KV types
  KVResponse,
  KVGetOptions,
  KVPutOptions,
  KVListOptions,
  KVDeleteOptions,
  KVIncrementOptions,
  KVDecrementOptions,
  KVIncrementIfOptions,
  KVDecrementIfOptions,
  // Checkpoint types
  CheckpointResponse,
  // Progress types
  ProgressUpdate,
  ProgressStep,
  ReportProgressOptions,
  // Task management types
  TaskInfo,
  TaskQueryResponse,
  TaskOperationResponse,
  TaskQueryOptions,
  TaskQueryResponseHandler,
  TaskOperationResponseHandler,
  // Workspace/Agent/ACL/Workflow response types
  WorkspaceResponse,
  AgentResponse,
  ACLResponse,
  AuthorityGrantPrincipalRef,
  AuthorityGrantResourceScope,
  AuthorityGrantInfo,
  AuthorityGrantResponse,
  AuthorityGrantRevocation,
  WorkflowResponse,
  // Token management types
  TokenInfo,
  TokenResponse,
  // Callback types
  MessageHandler,
  ConfigHandler,
  SignalHandler,
  ErrorHandler,
  KVResponseHandler,
  TaskAssignmentHandler,
  CheckpointResponseHandler,
  ProgressHandler,
  ConnectHandler,
  DisconnectHandler,
  ReconnectingHandler,
  WorkspaceResponseHandler,
  AgentResponseHandler,
  ACLResponseHandler,
  AuthorityGrantResponseHandler,
  AuthorityGrantRevocationHandler,
  WorkflowResponseHandler,
  TokenResponseHandler,
  AuditSubmitResponse,
  AuditSubmitResponseHandler,
  // Configuration types
  TLSOptions,
  ConnectionOptions,
  Credentials,
} from "./types.js";

// Credential helpers
export {
  withAPIKey,
  withToken,
  withTaskToken,
  withTenant,
} from "./types.js";

// Error classes
export {
  AetherError,
  ConnectionError,
  ConnectionClosedError,
  ReconnectionError,
  AuthenticationError,
  PermissionDeniedError,
  DuplicateIdentityError,
  TimeoutError,
  InvalidArgumentError,
  NotFoundError,
  UnimplementedError,
  MessageError,
  KVOperationError,
  CheckpointError,
  // Error classification utilities
  isRecoverable,
  isConnectionError,
  isTimeoutError,
} from "./errors.js";

// Proxy HTTP support
export { proxyHttp, AetherFetchTransport, ProxyErrorKind, ProxyHttpError } from "./proxy.js";
export type { ProxyHttpResponse, ProxyHttpOptions, ProxyError } from "./proxy.js";

// Tunnel support
export { tunnelDial, TunnelClosedError } from "./tunnel.js";
export type { TunnelProtocol, TunnelDialOptions, TunnelStream } from "./tunnel.js";

// Authority-grant cache (Phase 4 of the ACL/grant cleanup)
export { AuthorityGrantCache } from "./authority-cache.js";
export type { AuthorityGrantCacheOptions, AuthorityCacheClient } from "./authority-cache.js";

// Topic construction helpers
export {
  agentTopic,
  globalAgentsTopic,
  uniqueTaskTopic,
  taskTopic,
  taskBroadcastTopic,
  userTopic,
  userWorkspaceTopic,
  globalUsersTopic,
  eventTopic,
  eventWildcardTopic,
  metricTopic,
  metricWildcardTopic,
  progressTopic,
  bridgeTopic,
  // Topic prefix constants
  TOPIC_PREFIX_AGENT,
  TOPIC_PREFIX_UNIQUE_TASK,
  TOPIC_PREFIX_TASK,
  TOPIC_PREFIX_TASK_BROADCAST,
  TOPIC_PREFIX_USER,
  TOPIC_PREFIX_USER_WORKSPACE,
  TOPIC_PREFIX_GLOBAL_AGENTS,
  TOPIC_PREFIX_GLOBAL_USERS,
  TOPIC_PREFIX_EVENT,
  TOPIC_PREFIX_METRIC,
  TOPIC_PREFIX_PROGRESS,
  TOPIC_PREFIX_BRIDGE,
} from "./topics.js";
