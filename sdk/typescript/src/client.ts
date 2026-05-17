/**
 * Base client implementation for the Aether TypeScript SDK.
 *
 * This module provides the AetherClient class that handles gRPC connection
 * management, TLS configuration, message queue infrastructure, and automatic
 * reconnection with exponential backoff. Specific client types (AgentClient,
 * UserClient, etc.) extend this base client.
 *
 * Key architectural principle: The connection itself IS the distributed lock
 * AND the heartbeat. When the gRPC stream closes, the identity lock is
 * immediately released. No separate heartbeat API exists.
 *
 * @module client
 */

import type {
  ConnectionOptions,
  TLSOptions,
  Credentials,
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
  AuthorityGrantRevocation,
  AuthorityGrantRevocationHandler,
  WorkflowResponseHandler,
  TokenResponseHandler,
  OutgoingMessage,
  IncomingMessage,
  ConfigSnapshot,
  Signal,
  ErrorResponse,
  ConnectionAck,
  TaskAssignment,
  KVResponse,
  CheckpointResponse,
  ProgressUpdate,
  ReportProgressOptions,
  TaskQueryResponse,
  TaskOperationResponse,
  TaskInfo,
  TaskQueryOptions,
  TaskQueryResponseHandler,
  TaskOperationResponseHandler,
  CreateTaskResponse,
  CreateTaskResponseHandler,
  WorkspaceResponse,
  AgentResponse,
  ACLResponse,
  AuthorityGrantResponse,
  AuthorityGrantInfo,
  WorkflowResponse,
  TokenResponse,
  TokenInfo,
  AuditSubmitResponse,
  AuditSubmitResponseHandler,
} from "./types.js";
import { MessageType, KVScope, SignalType } from "./types.js";
import {
  ConnectionError,
  DuplicateIdentityError,
  ReconnectionError,
  InvalidArgumentError,
  isRecoverable,
} from "./errors.js";
import { KVClient } from "./kv.js";
import { CheckpointClient } from "./checkpoint.js";
import type { TunnelInflight } from "./tunnel.js";
import { TunnelClosedError } from "./tunnel.js";
import type { AuthorityGrantCacheOptions } from "./authority-cache.js";
import { AuthorityGrantCache } from "./authority-cache.js";
import type { ProxyHttpResponse } from "./proxy.js";
import { clientVersionMeta } from "./version-meta.js";

// =============================================================================
// Client Options
// =============================================================================

/**
 * Configuration options for the base Aether client.
 */
export interface AetherClientOptions {
  /** Gateway address in host:port format. Required. */
  address: string;
  /** Optional TLS configuration for secure connections. */
  tls?: TLSOptions;
  /** Connection behavior configuration. */
  connection?: ConnectionOptions;
  /** Authentication credentials. */
  credentials?: Credentials;
  /** Whether to automatically reconnect on connection loss. Default: true. */
  reconnect?: boolean;
  /** Initial delay in ms between reconnect attempts. Default: 1000. */
  reconnectDelay?: number;
  /** Maximum delay in ms between reconnect attempts. Default: 30000. */
  maxReconnectDelay?: number;
  /**
   * When true, if the gateway returns a DuplicateIdentityError (ALREADY_EXISTS)
   * during connection, the client will wait and retry automatically.
   * Shorthand for connection.retryOnDuplicate. Default: false.
   */
  retryOnDuplicate?: boolean;
  /**
   * How long to wait (ms) before retrying after a DuplicateIdentityError.
   * Shorthand for connection.retryOnDuplicateDelay. Default: 5000.
   */
  retryOnDuplicateDelay?: number;
}

// =============================================================================
// Internal KV Operation Type
// =============================================================================

/** @internal */
export interface KVOperationParams {
  op: "GET" | "PUT" | "LIST" | "DELETE" | "INCREMENT" | "DECREMENT" | "INCREMENT_IF" | "DECREMENT_IF";
  scope: KVScope;
  key?: string;
  value?: Uint8Array;
  userId?: string;
  workspace?: string;
  ttl?: number;
  requestId?: string;
  /** Guard value for INCREMENT_IF (ceiling) and DECREMENT_IF (floor) operations. */
  guardValue?: bigint;
  /** Step size for INCREMENT_IF / DECREMENT_IF. When omitted the server defaults to 1. */
  deltaValue?: bigint;
}

// =============================================================================
// Internal Checkpoint Operation Type
// =============================================================================

/** @internal */
export interface CheckpointOperationParams {
  op: "SAVE" | "LOAD" | "DELETE" | "LIST";
  key?: string;
  data?: Uint8Array;
  ttl?: number;
  requestId?: string;
}

// =============================================================================
// Default Connection Options
// =============================================================================

const DEFAULT_CONNECTION: Required<ConnectionOptions> = {
  maxRetries: 5,
  initialBackoff: 1000,
  maxBackoff: 30000,
  backoffMultiplier: 2.0,
  autoReconnect: true,
  connectTimeout: 30000,
  retryOnDuplicate: false,
  retryOnDuplicateDelay: 5000,
  retryOnDuplicateMaxAttempts: 5,
};

// =============================================================================
// AetherClient
// =============================================================================

/**
 * Base client for connecting to the Aether distributed control plane.
 *
 * AetherClient manages the gRPC bidirectional streaming connection,
 * handles automatic reconnection with exponential backoff, and provides
 * the message send/receive infrastructure.
 *
 * This class is not typically used directly. Instead, use one of the
 * specialized client types:
 * - {@link AgentClient} for agent connections
 * - {@link UserClient} for user/browser connections
 *
 * @example
 * ```typescript
 * import { AetherClient } from "@scitrera/aether-client";
 *
 * const client = new AetherClient({ address: "localhost:50051" });
 *
 * client.onMessage((msg) => {
 *   console.log(`Received from ${msg.sourceTopic}:`, msg.payload);
 * });
 *
 * await client.connect();
 * ```
 */
export class AetherClient {
  // Configuration
  protected readonly _address: string;
  protected readonly _tls: TLSOptions | undefined;
  protected readonly _connectionOpts: Required<ConnectionOptions>;
  protected readonly _credentials: Credentials;

  // Connection state
  protected _connected = false;
  protected _connecting = false;
  protected _sessionId = "";
  protected _resumeSessionId = "";
  private _requestCounter = 0;

  // Handler registry
  private _onMessage: MessageHandler = () => {};
  private _onConfig: ConfigHandler = () => {};
  private _onSignal: SignalHandler = () => {};
  private _onError: ErrorHandler = () => {};
  private _onKVResponse: KVResponseHandler = () => {};
  private _onTaskAssignment: TaskAssignmentHandler = () => {};
  private _onCheckpointResponse: CheckpointResponseHandler = () => {};
  private _onProgress: ProgressHandler = () => {};
  private _onConnect: ConnectHandler = () => {};
  private _onDisconnect: DisconnectHandler = () => {};
  private _onReconnecting: ReconnectingHandler = () => {};

  // Task management handlers
  private _onTaskQueryResponse: TaskQueryResponseHandler = () => {};
  private _onTaskOperationResponse: TaskOperationResponseHandler = () => {};
  private _onCreateTaskResponse: CreateTaskResponseHandler = () => {};

  // Workspace/Agent/ACL/Workflow handlers
  private _onWorkspaceResponse: WorkspaceResponseHandler = () => {};
  private _onAgentResponse: AgentResponseHandler = () => {};
  private _onACLResponse: ACLResponseHandler = () => {};
  private _onAuthorityGrantResponse: AuthorityGrantResponseHandler = () => {};
  private _onAuthorityGrantRevocation: AuthorityGrantRevocationHandler = () => {};
  private _onAuditSubmitResponse: AuditSubmitResponseHandler = () => {};

  // Registered authority-grant caches receive AuthorityGrantRevocation
  // push events before the user-supplied handler. @internal
  private _authorityGrantCaches: AuthorityGrantCache[] = [];
  private _onWorkflowResponse: WorkflowResponseHandler = () => {};
  private _onTokenResponse: TokenResponseHandler = () => {};

  // Typed message handlers
  private _onChatMessage: MessageHandler | null = null;
  private _onControlMessage: MessageHandler | null = null;
  private _onToolCallMessage: MessageHandler | null = null;
  private _onEventMessage: MessageHandler | null = null;
  private _onMetricMessage: MessageHandler | null = null;

  // Pending request correlation maps
  private _pendingKVRequests = new Map<string, (response: KVResponse) => void>();
  private _pendingCheckpointRequests = new Map<string, (response: CheckpointResponse) => void>();
  private _pendingTaskQueryRequests = new Map<string, (response: TaskQueryResponse) => void>();
  private _pendingTaskOpRequests = new Map<string, (response: TaskOperationResponse) => void>();
  private _pendingCreateTaskRequests = new Map<string, (response: CreateTaskResponse) => void>();
  private _pendingWorkspaceRequests = new Map<string, (response: WorkspaceResponse) => void>();
  private _pendingAgentRequests = new Map<string, (response: AgentResponse) => void>();
  private _pendingACLRequests = new Map<string, (response: ACLResponse) => void>();
  private _pendingAuthorityGrantRequests = new Map<string, (response: AuthorityGrantResponse) => void>();
  private _pendingWorkflowRequests = new Map<string, (response: WorkflowResponse) => void>();
  private _pendingAuditSubmitRequests = new Map<string, (response: AuditSubmitResponse) => void>();
  private _pendingTokenRequests = new Map<string, (response: TokenResponse) => void>();

  // Proxy HTTP pending requests: request_id → resolver
  // @internal
  _pendingProxyHttpRequests = new Map<string, (response: ProxyHttpResponse) => void>();
  // Accumulated body chunks keyed by request_id, for chunked responses
  // @internal
  _pendingProxyHttpChunks = new Map<string, Uint8Array[]>();
  // Active streaming proxy responses: request_id → controller. Set by
  // proxyHttp() when streamResponse=true; populated as ProxyHttpBodyChunk
  // frames arrive so the caller's ReadableStream<Uint8Array> can yield them
  // incrementally.
  // @internal
  _pendingProxyHttpStreams = new Map<
    string,
    {
      controller: ReadableStreamDefaultController<Uint8Array>;
      headerResolved: boolean;
    }
  >();

  // Tunnel inflight map: tunnel_id → TunnelInflight handler
  // @internal
  _pendingTunnels = new Map<string, TunnelInflight>();

  // KV client instance (lazy)
  private _kvClient: KVClient | undefined;

  // Checkpoint client instance (lazy)
  private _checkpointClient: CheckpointClient | undefined;

  // RetryOnDuplicate configuration
  private readonly _retryOnDuplicate: boolean;
  private readonly _retryOnDuplicateDelay: number;
  private readonly _retryOnDuplicateMaxAttempts: number;

  // gRPC connection objects (populated when @grpc/grpc-js is available)
  private _grpcClient: unknown = null;
  private _stream: unknown = null;
  private _disconnectRequested = false;
  private _reconnecting = false;

  // Loaded proto package definition — stored for sub-message encoding (e.g. Metric)
  private _packageDefinition: Record<string, unknown> | null = null;

  /**
   * Creates a new AetherClient.
   *
   * The client is created but not connected. Call {@link connect} to establish
   * the connection to the gateway.
   *
   * @param options - Client configuration options
   * @throws {@link InvalidArgumentError} if the address is not provided
   */
  constructor(options: AetherClientOptions) {
    if (!options.address) {
      throw new InvalidArgumentError("Gateway address is required", "address");
    }

    this._address = options.address;
    this._tls = options.tls;
    this._credentials = options.credentials ?? {};

    // Merge connection options with defaults
    const connOpts = options.connection ?? {};

    // RetryOnDuplicate: top-level shorthand takes precedence over connection sub-object
    this._retryOnDuplicate =
      options.retryOnDuplicate ?? connOpts.retryOnDuplicate ?? false;
    this._retryOnDuplicateDelay =
      options.retryOnDuplicateDelay ?? connOpts.retryOnDuplicateDelay ?? 5000;
    this._retryOnDuplicateMaxAttempts =
      connOpts.retryOnDuplicateMaxAttempts ?? 5;

    this._connectionOpts = {
      maxRetries: connOpts.maxRetries ?? DEFAULT_CONNECTION.maxRetries,
      initialBackoff: options.reconnectDelay ?? connOpts.initialBackoff ?? DEFAULT_CONNECTION.initialBackoff,
      maxBackoff: options.maxReconnectDelay ?? connOpts.maxBackoff ?? DEFAULT_CONNECTION.maxBackoff,
      backoffMultiplier: connOpts.backoffMultiplier ?? DEFAULT_CONNECTION.backoffMultiplier,
      autoReconnect: options.reconnect ?? connOpts.autoReconnect ?? DEFAULT_CONNECTION.autoReconnect,
      connectTimeout: connOpts.connectTimeout ?? DEFAULT_CONNECTION.connectTimeout,
      retryOnDuplicate: this._retryOnDuplicate,
      retryOnDuplicateDelay: this._retryOnDuplicateDelay,
      retryOnDuplicateMaxAttempts: this._retryOnDuplicateMaxAttempts,
    };
  }

  // ===========================================================================
  // Connection Lifecycle
  // ===========================================================================

  /**
   * Establishes a connection to the Aether gateway.
   *
   * Opens a gRPC bidirectional stream and sends the InitConnection message.
   * If auto-reconnect is enabled, the client will automatically attempt to
   * reconnect on connection loss.
   *
   * If `retryOnDuplicate` is enabled and the gateway responds with a
   * DuplicateIdentityError (ALREADY_EXISTS), the client will wait briefly
   * and retry up to `retryOnDuplicateMaxAttempts` times. This handles the
   * race condition where a previous instance's lock has not yet expired.
   *
   * @throws {@link ConnectionError} if the connection cannot be established
   * @throws {@link DuplicateIdentityError} if identity is already connected
   *   and retryOnDuplicate is disabled or exhausted
   */
  async connect(): Promise<void> {
    if (this._connected) {
      return;
    }
    if (this._connecting) {
      return;
    }

    this._connecting = true;
    this._disconnectRequested = false;

    try {
      if (this._retryOnDuplicate) {
        await this._connectWithDuplicateRetry();
      } else {
        await this._establishConnection();
      }
      this._connected = true;
    } finally {
      this._connecting = false;
    }
  }

  /**
   * Attempts connection, retrying on DuplicateIdentityError.
   * @internal
   */
  private async _connectWithDuplicateRetry(): Promise<void> {
    const maxAttempts = this._retryOnDuplicateMaxAttempts;
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      // Reset any partial connection state before each attempt
      await this._closeConnection();

      // Track whether a duplicate error was received during this attempt.
      // We listen for it via a one-shot error handler shim.
      let duplicateReceived = false;
      const prevOnError = this._onError;
      this._onError = (errResp) => {
        if (errResp.code === "ALREADY_EXISTS" || errResp.code === "DUPLICATE_IDENTITY") {
          duplicateReceived = true;
        }
        prevOnError(errResp);
      };

      try {
        await this._establishConnection();
        // Connection succeeded — restore handler and return
        this._onError = prevOnError;
        return;
      } catch (err) {
        this._onError = prevOnError;
        lastError = err instanceof Error ? err : new Error(String(err));

        // Check if the error is a DuplicateIdentityError or the flag was set
        const isDuplicate =
          duplicateReceived ||
          err instanceof DuplicateIdentityError ||
          (err instanceof Error && err.message.toLowerCase().includes("already_exists"));

        if (!isDuplicate || attempt >= maxAttempts) {
          throw err;
        }

        // Wait before retrying
        await new Promise<void>((resolve) => setTimeout(resolve, this._retryOnDuplicateDelay));
      }
    }

    throw lastError ?? new DuplicateIdentityError("", "exhausted retryOnDuplicate attempts");
  }

  /**
   * Gracefully disconnects from the Aether gateway.
   *
   * Closes the gRPC stream and releases the identity lock on the server.
   * Automatic reconnection is suppressed for this disconnection.
   */
  async disconnect(): Promise<void> {
    this._disconnectRequested = true;
    this._connected = false;
    await this._closeConnection();
  }

  /**
   * Returns whether the client is currently connected to the gateway.
   */
  get connected(): boolean {
    return this._connected;
  }

  /**
   * Returns the current session ID assigned by the gateway.
   * Empty string if not connected.
   */
  get sessionId(): string {
    return this._sessionId;
  }

  // ===========================================================================
  // Message Sending
  // ===========================================================================

  /**
   * Sends a message through the Aether gateway.
   *
   * @param message - The outgoing message to send
   * @throws {@link ConnectionError} if not connected
   */
  async send(message: OutgoingMessage): Promise<void> {
    if (!this._connected) {
      throw new ConnectionError("Not connected to gateway");
    }
    this._sendUpstream({
      send: {
        targetTopic: message.targetTopic,
        payload: message.payload,
        messageType: message.messageType ?? MessageType.Opaque,
      },
    });
  }

  /**
   * Sends a KV operation through the gateway.
   * @internal Used by KVClient.
   */
  sendKVOperation(params: KVOperationParams): void {
    this._sendUpstream({
      kvOp: {
        op: params.op,
        scope: params.scope,
        key: params.key ?? "",
        value: params.value,
        userId: params.userId ?? "",
        workspace: params.workspace ?? "",
        ttl: params.ttl ?? 0,
        requestId: params.requestId ?? "",
        guardValue: params.guardValue,
        deltaValue: params.deltaValue,
      },
    });
  }

  /**
   * Sends a checkpoint operation through the gateway.
   * @internal Used by CheckpointClient.
   */
  sendCheckpointOperation(params: CheckpointOperationParams): void {
    this._sendUpstream({
      checkpointOp: {
        op: params.op,
        key: params.key ?? "",
        data: params.data,
        ttl: params.ttl ?? -1,
        requestId: params.requestId ?? "",
      },
    });
  }

  // ===========================================================================
  // Handler Registration
  // ===========================================================================

  /**
   * Registers a handler for incoming messages.
   *
   * @param handler - Function called when a message is received
   *
   * @example
   * ```typescript
   * client.onMessage((msg) => {
   *   console.log(`From ${msg.sourceTopic}: ${new TextDecoder().decode(msg.payload)}`);
   * });
   * ```
   */
  onMessage(handler: MessageHandler): void {
    this._onMessage = handler;
  }

  /**
   * Registers a handler for configuration snapshots.
   *
   * Called once when the connection is established, providing the initial
   * KV store state for the workspace.
   *
   * @param handler - Function called when a config snapshot is received
   */
  onConfig(handler: ConfigHandler): void {
    this._onConfig = handler;
  }

  /**
   * Registers a handler for signals from the gateway.
   *
   * @param handler - Function called when a signal is received
   */
  onSignal(handler: SignalHandler): void {
    this._onSignal = handler;
  }

  /**
   * Registers a handler for error responses from the gateway.
   *
   * @param handler - Function called when an error response is received
   */
  onError(handler: ErrorHandler): void {
    this._onError = handler;
  }

  /**
   * Registers a handler for KV operation responses.
   *
   * @param handler - Function called when a KV response is received
   */
  onKVResponse(handler: KVResponseHandler): void {
    this._onKVResponse = handler;
  }

  /**
   * Registers a handler for task assignments.
   *
   * Used by orchestrators to receive task assignments that require
   * starting new agent or task instances.
   *
   * @param handler - Function called when a task assignment is received
   */
  onTaskAssignment(handler: TaskAssignmentHandler): void {
    this._onTaskAssignment = handler;
  }

  /**
   * Registers a handler for checkpoint operation responses.
   *
   * @param handler - Function called when a checkpoint response is received
   */
  onCheckpointResponse(handler: CheckpointResponseHandler): void {
    this._onCheckpointResponse = handler;
  }

  /**
   * Registers a handler for progress updates from agents/tasks.
   *
   * Progress updates are delivered via the pg::{workspace} stream with
   * server-side recipient filtering.
   *
   * @param handler - Function called when a progress update is received
   */
  onProgress(handler: ProgressHandler): void {
    this._onProgress = handler;
  }

  /**
   * Registers a handler for successful connections.
   *
   * Called both for initial connection and reconnections.
   *
   * @param handler - Function called with the connection acknowledgment
   */
  onConnect(handler: ConnectHandler): void {
    this._onConnect = handler;
  }

  /**
   * Registers a handler for disconnection events.
   *
   * Called before any automatic reconnection attempt.
   *
   * @param handler - Function called with the disconnection reason
   */
  onDisconnect(handler: DisconnectHandler): void {
    this._onDisconnect = handler;
  }

  /**
   * Registers a handler for reconnection attempts.
   *
   * @param handler - Function called with the attempt number
   */
  onReconnecting(handler: ReconnectingHandler): void {
    this._onReconnecting = handler;
  }

  /**
   * Registers a handler for task query responses.
   *
   * @param handler - Function called when a task query response is received
   */
  onTaskQueryResponse(handler: TaskQueryResponseHandler): void {
    this._onTaskQueryResponse = handler;
  }

  /**
   * Registers a handler for task operation responses.
   *
   * @param handler - Function called when a task operation response is received
   */
  onTaskOperationResponse(handler: TaskOperationResponseHandler): void {
    this._onTaskOperationResponse = handler;
  }

  /**
   * Registers a handler for create-task responses (fire-and-forget path,
   * i.e. when no request_id is supplied or a response arrives without a
   * matching pending entry).
   *
   * @param handler - Function called when a create-task response is received
   */
  onCreateTaskResponse(handler: CreateTaskResponseHandler): void {
    this._onCreateTaskResponse = handler;
  }

  /**
   * Registers a handler for Chat messages.
   *
   * @param handler - Function called when a Chat message is received
   */
  onChatMessage(handler: MessageHandler): void {
    this._onChatMessage = handler;
  }

  /**
   * Registers a handler for Control messages.
   *
   * @param handler - Function called when a Control message is received
   */
  onControlMessage(handler: MessageHandler): void {
    this._onControlMessage = handler;
  }

  /**
   * Registers a handler for ToolCall messages.
   *
   * @param handler - Function called when a ToolCall message is received
   */
  onToolCallMessage(handler: MessageHandler): void {
    this._onToolCallMessage = handler;
  }

  /**
   * Registers a handler for Event messages.
   *
   * @param handler - Function called when an Event message is received
   */
  onEventMessage(handler: MessageHandler): void {
    this._onEventMessage = handler;
  }

  /**
   * Registers a handler for Metric messages.
   *
   * @param handler - Function called when a Metric message is received
   */
  onMetricMessage(handler: MessageHandler): void {
    this._onMetricMessage = handler;
  }

  // ===========================================================================
  // KV Client Access
  // ===========================================================================

  /**
   * Returns the KV operations client.
   *
   * @returns The KVClient instance for performing KV store operations
   *
   * @example
   * ```typescript
   * const kv = client.kv();
   * const response = await kv.getSync({ key: "my-key", scope: KVScope.Global });
   * ```
   */
  kv(): KVClient {
    if (!this._kvClient) {
      this._kvClient = new KVClient(this);
    }
    return this._kvClient;
  }

  /**
   * Returns the checkpoint operations client.
   *
   * @returns The CheckpointClient instance for checkpoint save/load/delete/list
   *
   * @example
   * ```typescript
   * const cp = client.checkpoint();
   * const response = await cp.loadSync({ key: "my-state" });
   * ```
   */
  checkpoint(): CheckpointClient {
    if (!this._checkpointClient) {
      this._checkpointClient = new CheckpointClient(this);
    }
    return this._checkpointClient;
  }

  // ===========================================================================
  // Request Correlation (internal)
  // ===========================================================================

  /**
   * Generates a unique request ID for correlating responses.
   * @internal
   */
  nextRequestId(): string {
    this._requestCounter++;
    return `req-${Date.now()}-${this._requestCounter}`;
  }

  /**
   * Registers a pending KV request for response correlation.
   * @internal
   */
  registerPendingKVRequest(requestId: string, callback: (response: KVResponse) => void): void {
    this._pendingKVRequests.set(requestId, callback);
  }

  /**
   * Registers a pending checkpoint request for response correlation.
   * @internal
   */
  registerPendingCheckpointRequest(requestId: string, callback: (response: CheckpointResponse) => void): void {
    this._pendingCheckpointRequests.set(requestId, callback);
  }

  /**
   * Removes a pending request (used on timeout cleanup).
   * @internal
   */
  removePendingRequest(requestId: string): void {
    this._pendingKVRequests.delete(requestId);
    this._pendingCheckpointRequests.delete(requestId);
  }

  // ===========================================================================
  // Init Message (overridden by subclasses)
  // ===========================================================================

  /**
   * Builds the InitConnection message for the connection handshake.
   * Subclasses override this to provide their specific identity.
   * @internal
   */
  protected _buildInitMessage(): Record<string, unknown> {
    return {
      credentials: this._credentials,
      resumeSessionId: this._resumeSessionId,
    };
  }

  // ===========================================================================
  // gRPC Connection Management
  // ===========================================================================

  /**
   * Establishes the gRPC connection and starts the message loop.
   * @internal
   */
  private async _establishConnection(): Promise<void> {
    try {
      // Dynamic import of @grpc/grpc-js (Node.js only)
      const grpc = await import("@grpc/grpc-js");
      const protoLoader = await import("@grpc/proto-loader");

      // Resolve proto file path
      const path = await import("path");
      const url = await import("url");
      const dirname = path.dirname(url.fileURLToPath(import.meta.url));
      const protoPath = path.resolve(dirname, "../../api/proto/aether.proto");

      // Load proto definition
      const packageDefinition = await protoLoader.load(protoPath, {
        keepCase: false,
        longs: Number,
        enums: Number,
        defaults: true,
        oneofs: true,
      });

      // Store package definition for sub-message encoding (e.g. Metric)
      this._packageDefinition = packageDefinition as Record<string, unknown>;

      const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as Record<string, unknown>;
      const aetherV1 = (protoDescriptor["aether"] as Record<string, unknown>)?.["v1"] as Record<string, unknown>;

      if (!aetherV1) {
        throw new ConnectionError("Failed to load aether.v1 proto package");
      }

      // Create gRPC channel credentials
      let channelCredentials: ReturnType<typeof grpc.credentials.createInsecure>;
      if (this._tls) {
        channelCredentials = grpc.credentials.createSsl(
          this._tls.rootCerts ?? null,
          this._tls.privateKey ?? null,
          this._tls.certChain ?? null,
        );
      } else {
        channelCredentials = grpc.credentials.createInsecure();
      }

      // Create client
      const ServiceClient = aetherV1["AetherGateway"] as new (
        address: string,
        credentials: ReturnType<typeof grpc.credentials.createInsecure>,
      ) => Record<string, unknown>;
      this._grpcClient = new ServiceClient(this._address, channelCredentials);

      // Open bidirectional stream
      const client = this._grpcClient as Record<string, (...args: unknown[]) => unknown>;
      this._stream = client["connect"]() as Record<string, unknown>;

      const stream = this._stream as {
        write: (msg: unknown) => void;
        on: (event: string, handler: (...args: unknown[]) => void) => void;
        end: () => void;
      };

      // Set up message handler
      stream.on("data", (...args: unknown[]) => {
        this._handleDownstreamMessage(args[0] as Record<string, unknown>);
      });

      // Set up error handler
      stream.on("error", (...args: unknown[]) => {
        this._handleStreamError(args[0] as Error);
      });

      // Set up end handler
      stream.on("end", () => {
        this._handleStreamEnd();
      });

      // Send init message. Merge in SDK version metadata so the gateway
      // can attribute the connection to a specific TS build in audit
      // rows — additive, ignored by older gateways. Subclass-supplied
      // fields win on collision.
      const initMsg = { ...clientVersionMeta(), ...this._buildInitMessage() };
      stream.write({ init: initMsg });

    } catch (err) {
      if (err instanceof ConnectionError) {
        throw err;
      }
      throw new ConnectionError(
        `Failed to connect to gateway at ${this._address}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Closes the gRPC connection.
   * @internal
   */
  private async _closeConnection(): Promise<void> {
    if (this._stream) {
      const stream = this._stream as { end: () => void; removeAllListeners?: () => void };
      try {
        // Remove event listeners before ending to prevent stale handlers
        // from firing during reconnection and triggering duplicate attempts
        stream.removeAllListeners?.();
        stream.end();
      } catch {
        // Ignore errors during close
      }
      this._stream = null;
    }

    if (this._grpcClient) {
      const client = this._grpcClient as { close?: () => void };
      try {
        client.close?.();
      } catch {
        // Ignore errors during close
      }
      this._grpcClient = null;
    }
  }

  /**
   * Sends an upstream message on the gRPC stream.
   * @internal
   */
  protected _sendUpstream(message: Record<string, unknown>): void {
    if (!this._stream) {
      throw new ConnectionError("Not connected to gateway");
    }
    const stream = this._stream as { write: (msg: unknown) => void };
    stream.write(message);
  }

  // ===========================================================================
  // Downstream Message Handling
  // ===========================================================================

  /**
   * Routes a downstream message to the appropriate handler.
   * @internal
   */
  private _handleDownstreamMessage(data: Record<string, unknown>): void {
    // Detect which payload field is set (oneof)
    if (data["connectionAck"] || data["connection_ack"]) {
      const ack = (data["connectionAck"] ?? data["connection_ack"]) as Record<string, unknown>;
      this._sessionId = String(ack["sessionId"] ?? ack["session_id"] ?? "");
      this._resumeSessionId = this._sessionId;
      const connAck: ConnectionAck = {
        sessionId: this._sessionId,
        resumed: Boolean(ack["resumed"]),
        assignedId: String(ack["assignedId"] ?? ack["assigned_id"] ?? ""),
      };
      this._onConnect(connAck);
      return;
    }

    if (data["msg"]) {
      const msg = data["msg"] as Record<string, unknown>;
      const incoming: IncomingMessage = {
        sourceTopic: String(msg["sourceTopic"] ?? msg["source_topic"] ?? ""),
        payload: msg["payload"] instanceof Uint8Array ? msg["payload"] : new Uint8Array(),
        messageType: Number(msg["messageType"] ?? msg["message_type"] ?? 0),
        receivedAt: new Date(),
      };
      this._onMessage(incoming);

      // Dispatch to typed message handler
      switch (incoming.messageType) {
        case MessageType.Chat:
          this._onChatMessage?.(incoming);
          break;
        case MessageType.Control:
          this._onControlMessage?.(incoming);
          break;
        case MessageType.ToolCall:
          this._onToolCallMessage?.(incoming);
          break;
        case MessageType.Event:
          this._onEventMessage?.(incoming);
          break;
        case MessageType.Metric:
          this._onMetricMessage?.(incoming);
          break;
        case MessageType.Opaque:
          // No typed handler — falls through to onMessage catch-all above
          break;
      }
      return;
    }

    if (data["config"]) {
      const cfg = data["config"] as Record<string, unknown>;
      const rawKv = (cfg["kv"] ?? {}) as Record<string, Buffer | Uint8Array>;
      const rawGlobalKv = (cfg["globalKv"] ?? cfg["global_kv"] ?? {}) as Record<string, Buffer | Uint8Array>;
      const rawWsExclKv = (cfg["workspaceExclusiveKv"] ?? cfg["workspace_exclusive_kv"] ?? {}) as Record<string, Buffer | Uint8Array>;
      const rawGlobalExclKv = (cfg["globalExclusiveKv"] ?? cfg["global_exclusive_kv"] ?? {}) as Record<string, Buffer | Uint8Array>;
      const config: ConfigSnapshot = {
        kv: Object.fromEntries(Object.entries(rawKv).map(([k, v]) => [k, new Uint8Array(v)])),
        globalKv: Object.fromEntries(Object.entries(rawGlobalKv).map(([k, v]) => [k, new Uint8Array(v)])),
        workspaceExclusiveKv: Object.fromEntries(Object.entries(rawWsExclKv).map(([k, v]) => [k, new Uint8Array(v)])),
        globalExclusiveKv: Object.fromEntries(Object.entries(rawGlobalExclKv).map(([k, v]) => [k, new Uint8Array(v)])),
      };
      this._onConfig(config);
      return;
    }

    if (data["signal"]) {
      const sig = data["signal"] as Record<string, unknown>;
      const signal: Signal = {
        type: Number(sig["type"]) as SignalType,
        reason: String(sig["reason"] ?? ""),
      };
      this._onSignal(signal);

      if (signal.type === SignalType.ForceDisconnect) {
        this._connected = false;
        this._disconnectRequested = true; // Terminal — do not auto-reconnect
        this._onDisconnect(signal.reason || "Force disconnect signal received");
      } else if (signal.type === SignalType.GracefulDisconnect) {
        this._connected = false;
        // Do NOT set _disconnectRequested — allow auto-reconnect
        this._onDisconnect(signal.reason || "Graceful disconnect signal received");
      }
      return;
    }

    if (data["error"]) {
      const err = data["error"] as Record<string, unknown>;
      const errorResponse: ErrorResponse = {
        code: String(err["code"] ?? ""),
        message: String(err["message"] ?? ""),
        retryable: Boolean(err["retryable"]),
        retryAfterMs: Number(err["retryAfterMs"] ?? err["retry_after_ms"] ?? 0),
      };
      this._onError(errorResponse);
      return;
    }

    if (data["kv"]) {
      const kv = data["kv"] as Record<string, unknown>;
      const rawCounter = kv["counterValue"] ?? kv["counter_value"];
      const rawApplied = kv["applied"];
      const response: KVResponse = {
        success: Boolean(kv["success"]),
        value: kv["value"] instanceof Uint8Array ? kv["value"] : new Uint8Array(),
        keys: (kv["keys"] ?? []) as string[],
        kvMap: (kv["kvMap"] ?? kv["kv_map"] ?? {}) as Record<string, string>,
        requestId: String(kv["requestId"] ?? kv["request_id"] ?? ""),
        counterValue: rawCounter !== undefined ? Number(rawCounter) : undefined,
        applied: rawApplied !== undefined ? Boolean(rawApplied) : undefined,
      };

      // Route to correlated request if available
      const pending = this._pendingKVRequests.get(response.requestId);
      if (pending) {
        this._pendingKVRequests.delete(response.requestId);
        pending(response);
      } else {
        this._onKVResponse(response);
      }
      return;
    }

    if (data["taskAssignment"] || data["task_assignment"]) {
      const ta = (data["taskAssignment"] ?? data["task_assignment"]) as Record<string, unknown>;
      const assignment: TaskAssignment = {
        taskId: String(ta["taskId"] ?? ta["task_id"] ?? ""),
        taskType: String(ta["taskType"] ?? ta["task_type"] ?? ""),
        assignedTo: String(ta["assignedTo"] ?? ta["assigned_to"] ?? ""),
        metadata: (ta["metadata"] ?? {}) as Record<string, string>,
        assignedAt: Number(ta["assignedAt"] ?? ta["assigned_at"] ?? 0),
        profile: String(ta["profile"] ?? ""),
        launchParams: (ta["launchParams"] ?? ta["launch_params"] ?? {}) as Record<string, string>,
        targetImplementation: String(ta["targetImplementation"] ?? ta["target_implementation"] ?? ""),
        workspace: String(ta["workspace"] ?? ""),
        specifier: String(ta["specifier"] ?? ""),
      };
      this._onTaskAssignment(assignment);
      return;
    }

    if (data["progressUpdate"] || data["progress_update"]) {
      const pu = (data["progressUpdate"] ?? data["progress_update"]) as Record<string, unknown>;
      const step = pu["step"] as Record<string, unknown> | undefined;
      const update: ProgressUpdate = {
        source: String(pu["source"] ?? ""),
        taskId: String(pu["taskId"] ?? pu["task_id"] ?? ""),
        state: String(pu["state"] ?? ""),
        completion: Number(pu["completion"] ?? -1),
        summary: String(pu["summary"] ?? ""),
        step: step ? {
          name: String(step["name"] ?? ""),
          detail: String(step["detail"] ?? ""),
          sequence: Number(step["sequence"] ?? 0),
          totalSteps: Number(step["totalSteps"] ?? step["total_steps"] ?? 0),
          stepType: String(step["stepType"] ?? step["step_type"] ?? ""),
        } : undefined,
        timestampMs: Number(pu["timestampMs"] ?? pu["timestamp_ms"] ?? 0),
        workspace: String(pu["workspace"] ?? ""),
        requestId: String(pu["requestId"] ?? pu["request_id"] ?? ""),
        metadata: (pu["metadata"] ?? {}) as Record<string, string>,
        recipient: String(pu["recipient"] ?? ""),
      };
      this._onProgress(update);
      return;
    }

    if (data["checkpoint"]) {
      const cp = data["checkpoint"] as Record<string, unknown>;
      const response: CheckpointResponse = {
        success: Boolean(cp["success"]),
        data: cp["data"] instanceof Uint8Array ? cp["data"] : new Uint8Array(),
        keys: (cp["keys"] ?? []) as string[],
        error: String(cp["error"] ?? ""),
        savedAt: Number(cp["savedAt"] ?? cp["saved_at"] ?? 0),
        requestId: String(cp["requestId"] ?? cp["request_id"] ?? ""),
      };

      // Route to correlated request if available
      const pending = this._pendingCheckpointRequests.get(response.requestId);
      if (pending) {
        this._pendingCheckpointRequests.delete(response.requestId);
        pending(response);
      } else {
        this._onCheckpointResponse(response);
      }
      return;
    }

    if (data["taskQuery"] || data["task_query"]) {
      const tq = (data["taskQuery"] ?? data["task_query"]) as Record<string, unknown>;
      const requestId = String(tq["requestId"] ?? tq["request_id"] ?? "");
      const response: TaskQueryResponse = {
        success: Boolean(tq["success"]),
        error: String(tq["error"] ?? ""),
        task: tq["task"] ? this._parseTaskInfo(tq["task"] as Record<string, unknown>) : undefined,
        tasks: ((tq["tasks"] ?? []) as Record<string, unknown>[]).map(t => this._parseTaskInfo(t)),
        totalCount: Number(tq["totalCount"] ?? tq["total_count"] ?? 0),
        requestId,
      };

      const pending = this._pendingTaskQueryRequests.get(requestId);
      if (pending) {
        this._pendingTaskQueryRequests.delete(requestId);
        pending(response);
      } else {
        this._onTaskQueryResponse(response);
      }
      return;
    }

    if (data["taskOp"] || data["task_op"]) {
      const to = (data["taskOp"] ?? data["task_op"]) as Record<string, unknown>;
      const requestId = String(to["requestId"] ?? to["request_id"] ?? "");
      const response: TaskOperationResponse = {
        success: Boolean(to["success"]),
        message: String(to["message"] ?? ""),
        error: String(to["error"] ?? ""),
        task: to["task"] ? this._parseTaskInfo(to["task"] as Record<string, unknown>) : undefined,
        requestId,
      };

      const pending = this._pendingTaskOpRequests.get(requestId);
      if (pending) {
        this._pendingTaskOpRequests.delete(requestId);
        pending(response);
      } else {
        this._onTaskOperationResponse(response);
      }
      return;
    }

    if (data["workspace"]) {
      const ws = data["workspace"] as Record<string, unknown>;
      const requestId = String(ws["requestId"] ?? ws["request_id"] ?? "");
      const response: WorkspaceResponse = {
        success: Boolean(ws["success"]),
        error: String(ws["error"] ?? ""),
        message: String(ws["message"] ?? ""),
        totalCount: Number(ws["totalCount"] ?? ws["total_count"] ?? 0),
        requestId,
        ...ws,
      };

      const pending = this._pendingWorkspaceRequests.get(requestId);
      if (pending) {
        this._pendingWorkspaceRequests.delete(requestId);
        pending(response);
      } else {
        this._onWorkspaceResponse(response);
      }
      return;
    }

    if (data["agent"]) {
      const ag = data["agent"] as Record<string, unknown>;
      const requestId = String(ag["requestId"] ?? ag["request_id"] ?? "");
      const response: AgentResponse = {
        success: Boolean(ag["success"]),
        error: String(ag["error"] ?? ""),
        message: String(ag["message"] ?? ""),
        totalCount: Number(ag["totalCount"] ?? ag["total_count"] ?? 0),
        requestId,
        ...ag,
      };

      const pending = this._pendingAgentRequests.get(requestId);
      if (pending) {
        this._pendingAgentRequests.delete(requestId);
        pending(response);
      } else {
        this._onAgentResponse(response);
      }
      return;
    }

    if (data["acl"]) {
      const acl = data["acl"] as Record<string, unknown>;
      const requestId = String(acl["requestId"] ?? acl["request_id"] ?? "");
      const response: ACLResponse = {
        success: Boolean(acl["success"]),
        error: String(acl["error"] ?? ""),
        message: String(acl["message"] ?? ""),
        requestId,
        ...acl,
      };

      const pending = this._pendingACLRequests.get(requestId);
      if (pending) {
        this._pendingACLRequests.delete(requestId);
        pending(response);
      } else {
        this._onACLResponse(response);
      }
      return;
    }

    if (data["authorityGrant"] || data["authority_grant"]) {
      const ag = (data["authorityGrant"] ?? data["authority_grant"]) as Record<string, unknown>;
      const requestId = String(ag["requestId"] ?? ag["request_id"] ?? "");
      const rawGrant = ag["grant"] as Record<string, unknown> | null | undefined;
      const rawGrants = ag["grants"];

      const response: AuthorityGrantResponse = {
        success: Boolean(ag["success"]),
        error: String(ag["error"] ?? ""),
        message: String(ag["message"] ?? ""),
        grant: rawGrant ? this._parseAuthorityGrantInfo(rawGrant) : undefined,
        requestId,
        grants: Array.isArray(rawGrants)
          ? (rawGrants as Record<string, unknown>[]).map((g) => this._parseAuthorityGrantInfo(g))
          : undefined,
        total: Number(ag["total"] ?? 0),
        cacheHintTtlSeconds: Number(ag["cacheHintTtlSeconds"] ?? ag["cache_hint_ttl_seconds"] ?? 0),
      };

      const pending = this._pendingAuthorityGrantRequests.get(requestId);
      if (pending) {
        this._pendingAuthorityGrantRequests.delete(requestId);
        pending(response);
      } else {
        this._onAuthorityGrantResponse(response);
      }
      return;
    }

    if (data["authorityGrantRevocation"] || data["authority_grant_revocation"]) {
      const raw = (data["authorityGrantRevocation"] ?? data["authority_grant_revocation"]) as Record<string, unknown>;
      const evt: AuthorityGrantRevocation = {
        grantId: String(raw["grantId"] ?? raw["grant_id"] ?? ""),
        rootGrantId: String(raw["rootGrantId"] ?? raw["root_grant_id"] ?? ""),
        reason: String(raw["reason"] ?? ""),
        revokedAt: Number(raw["revokedAt"] ?? raw["revoked_at"] ?? 0),
        cascade: Boolean(raw["cascade"]),
      };

      // Dispatch to every registered cache before firing the user handler.
      for (const cache of this._authorityGrantCaches) {
        try {
          cache.handleRevocationEvent(evt);
        } catch {
          // Best-effort: cache errors are isolated from each other and
          // from the user handler.
        }
      }
      this._onAuthorityGrantRevocation(evt);
      return;
    }

    if (data["createTask"] || data["create_task"]) {
      const ct = (data["createTask"] ?? data["create_task"]) as Record<string, unknown>;
      const requestId = String(ct["requestId"] ?? ct["request_id"] ?? "");
      const response: CreateTaskResponse = {
        success: Boolean(ct["success"]),
        taskId: String(ct["taskId"] ?? ct["task_id"] ?? ""),
        status: String(ct["status"] ?? ""),
        errorCode: String(ct["errorCode"] ?? ct["error_code"] ?? ""),
        errorMessage: String(ct["errorMessage"] ?? ct["error_message"] ?? ""),
        requestId,
        assignedTo: String(ct["assignedTo"] ?? ct["assigned_to"] ?? ""),
      };

      const pending = this._pendingCreateTaskRequests.get(requestId);
      if (pending) {
        this._pendingCreateTaskRequests.delete(requestId);
        pending(response);
      } else {
        this._onCreateTaskResponse(response);
      }
      return;
    }

    if (data["workflowResponse"] || data["workflow_response"]) {
      const wf = (data["workflowResponse"] ?? data["workflow_response"]) as Record<string, unknown>;
      const requestId = String(wf["requestId"] ?? wf["request_id"] ?? "");
      const response: WorkflowResponse = {
        success: Boolean(wf["success"]),
        error: String(wf["error"] ?? ""),
        message: String(wf["message"] ?? ""),
        data: wf["data"] instanceof Uint8Array ? wf["data"] : undefined,
        totalCount: Number(wf["totalCount"] ?? wf["total_count"] ?? 0),
        requestId,
      };

      const pending = this._pendingWorkflowRequests.get(requestId);
      if (pending) {
        this._pendingWorkflowRequests.delete(requestId);
        pending(response);
      } else {
        this._onWorkflowResponse(response);
      }
      return;
    }

    if (data["submitAuditEventResponse"] || data["submit_audit_event_response"]) {
      const ar = (data["submitAuditEventResponse"] ?? data["submit_audit_event_response"]) as Record<string, unknown>;
      const clientRequestId = String(ar["clientRequestId"] ?? ar["client_request_id"] ?? "");
      const response: AuditSubmitResponse = {
        clientRequestId,
        success: Boolean(ar["success"]),
        errorCode: String(ar["errorCode"] ?? ar["error_code"] ?? ""),
        errorMessage: String(ar["errorMessage"] ?? ar["error_message"] ?? ""),
      };
      const pending = this._pendingAuditSubmitRequests.get(clientRequestId);
      if (pending) {
        this._pendingAuditSubmitRequests.delete(clientRequestId);
        pending(response);
      } else {
        this._onAuditSubmitResponse(response);
      }
      return;
    }

    if (data["token"] || data["tokenResponse"] || data["token_response"]) {
      const tr = (data["token"] ?? data["tokenResponse"] ?? data["token_response"]) as Record<string, unknown>;
      const requestId = String(tr["requestId"] ?? tr["request_id"] ?? "");

      const parseTokenInfo = (t: Record<string, unknown>): TokenInfo => ({
        id: String(t["id"] ?? ""),
        name: String(t["name"] ?? ""),
        principalType: String(t["principalType"] ?? t["principal_type"] ?? ""),
        workspacePatterns: Array.isArray(t["workspacePatterns"] ?? t["workspace_patterns"])
          ? ((t["workspacePatterns"] ?? t["workspace_patterns"]) as unknown[]).map(String)
          : [],
        scopes: Array.isArray(t["scopes"]) ? (t["scopes"] as unknown[]).map(String) : [],
        createdBy: String(t["createdBy"] ?? t["created_by"] ?? ""),
        expiresAt: Number(t["expiresAt"] ?? t["expires_at"] ?? 0),
        lastUsedAt: Number(t["lastUsedAt"] ?? t["last_used_at"] ?? 0),
        revoked: Boolean(t["revoked"]),
        revokedAt: Number(t["revokedAt"] ?? t["revoked_at"] ?? 0),
        createdAt: Number(t["createdAt"] ?? t["created_at"] ?? 0),
        updatedAt: Number(t["updatedAt"] ?? t["updated_at"] ?? 0),
      });

      const rawToken = tr["token"] as Record<string, unknown> | null | undefined;
      const rawCreated = tr["createdToken"] ?? tr["created_token"] as Record<string, unknown> | null | undefined;
      const rawTokens = tr["tokens"];

      const response: TokenResponse = {
        success: Boolean(tr["success"]),
        error: String(tr["error"] ?? ""),
        message: String(tr["message"] ?? ""),
        token: rawToken ? parseTokenInfo(rawToken) : undefined,
        tokens: Array.isArray(rawTokens) ? (rawTokens as Record<string, unknown>[]).map(parseTokenInfo) : [],
        totalCount: Number(tr["totalCount"] ?? tr["total_count"] ?? 0),
        plaintextToken: String(tr["plaintextToken"] ?? tr["plaintext_token"] ?? ""),
        createdToken: rawCreated ? parseTokenInfo(rawCreated as Record<string, unknown>) : undefined,
        requestId,
      };

      const pending = this._pendingTokenRequests.get(requestId);
      if (pending) {
        this._pendingTokenRequests.delete(requestId);
        pending(response);
      } else {
        this._onTokenResponse(response);
      }
      return;
    }

    if (data["proxyHttpResponse"] || data["proxy_http_response"]) {
      const raw = (data["proxyHttpResponse"] ?? data["proxy_http_response"]) as Record<string, unknown>;
      const requestId = String(raw["requestId"] ?? raw["request_id"] ?? "");
      const rawErr = raw["error"] as Record<string, unknown> | null | undefined;
      const response = {
        requestId,
        statusCode: Number(raw["statusCode"] ?? raw["status_code"] ?? 0),
        headers: (raw["headers"] ?? {}) as Record<string, string>,
        body: raw["body"] instanceof Uint8Array ? raw["body"] : new Uint8Array(),
        bodyChunked: Boolean(raw["bodyChunked"] ?? raw["body_chunked"]),
        error: rawErr ? { kind: Number(rawErr["kind"] ?? 0), message: String(rawErr["message"] ?? "") } : undefined,
      };

      // Streaming path: a header lands first (resolves the request), then a
      // post-header header frame is treated as a terminal mid-stream error.
      const streamSlot = this._pendingProxyHttpStreams.get(requestId);
      if (streamSlot) {
        if (streamSlot.headerResolved) {
          // Mid-stream error — close the stream with an error.
          if (response.error) {
            streamSlot.controller.error(new Error(`${response.error.message}`));
          } else {
            streamSlot.controller.close();
          }
          this._pendingProxyHttpStreams.delete(requestId);
          return;
        }
        streamSlot.headerResolved = true;
        const pending = this._pendingProxyHttpRequests.get(requestId);
        if (pending) {
          this._pendingProxyHttpRequests.delete(requestId);
          pending(response);
        }
        // If header already carries a terminal error (TTFB failure), close.
        if (response.error) {
          streamSlot.controller.error(new Error(`${response.error.message}`));
          this._pendingProxyHttpStreams.delete(requestId);
        }
        return;
      }

      if (response.bodyChunked) {
        // Store partial response — body will be assembled from chunks
        this._pendingProxyHttpChunks.set(requestId, []);
        // Stash the response shell to reuse when fin arrives
        // We reuse the pending map entry itself once fin arrives; store the shell alongside chunks
        (this._pendingProxyHttpChunks as Map<string, unknown>).set(requestId + ":shell", response);
      } else {
        const pending = this._pendingProxyHttpRequests.get(requestId);
        if (pending) {
          this._pendingProxyHttpRequests.delete(requestId);
          pending(response);
        }
      }
      return;
    }

    if (data["proxyHttpBodyChunk"] || data["proxy_http_body_chunk"]) {
      const raw = (data["proxyHttpBodyChunk"] ?? data["proxy_http_body_chunk"]) as Record<string, unknown>;
      const requestId = String(raw["requestId"] ?? raw["request_id"] ?? "");
      const isRequest = Boolean(raw["isRequest"] ?? raw["is_request"]);
      if (isRequest) return; // sent by us; ignore echoes

      const data_ = raw["data"] instanceof Uint8Array ? raw["data"] : new Uint8Array();
      const fin = Boolean(raw["fin"]);

      // Streaming-body path: enqueue directly into the caller's
      // ReadableStream<Uint8Array> controller.
      const streamSlot = this._pendingProxyHttpStreams.get(requestId);
      if (streamSlot) {
        if (data_.length > 0) {
          streamSlot.controller.enqueue(data_);
        }
        if (fin) {
          streamSlot.controller.close();
          this._pendingProxyHttpStreams.delete(requestId);
        }
        return;
      }

      const chunks = this._pendingProxyHttpChunks.get(requestId);
      if (chunks && Array.isArray(chunks)) {
        chunks.push(data_);
        if (fin) {
          // Assemble body
          const totalLen = chunks.reduce((s, c) => s + c.length, 0);
          const body = new Uint8Array(totalLen);
          let offset = 0;
          for (const c of chunks) { body.set(c, offset); offset += c.length; }

          this._pendingProxyHttpChunks.delete(requestId);
          const shell = (this._pendingProxyHttpChunks as Map<string, unknown>).get(requestId + ":shell") as Record<string, unknown> | undefined;
          (this._pendingProxyHttpChunks as Map<string, unknown>).delete(requestId + ":shell");

          const pending = this._pendingProxyHttpRequests.get(requestId);
          if (pending) {
            this._pendingProxyHttpRequests.delete(requestId);
            pending({ ...(shell ?? {}), body } as Parameters<typeof pending>[0]);
          }
        }
      }
      return;
    }

    if (data["tunnelData"] || data["tunnel_data"]) {
      const raw = (data["tunnelData"] ?? data["tunnel_data"]) as Record<string, unknown>;
      const tunnelId = String(raw["tunnelId"] ?? raw["tunnel_id"] ?? "");
      const rawData = raw["data"];
      const bytes = rawData instanceof Uint8Array ? rawData : (Buffer.isBuffer(rawData) ? new Uint8Array(rawData) : new Uint8Array());
      const fin = Boolean(raw["fin"]);
      const inflight = this._pendingTunnels.get(tunnelId);
      if (inflight) {
        inflight.pushData(bytes, fin);
      }
      return;
    }

    if (data["tunnelAck"] || data["tunnel_ack"]) {
      const raw = (data["tunnelAck"] ?? data["tunnel_ack"]) as Record<string, unknown>;
      const tunnelId = String(raw["tunnelId"] ?? raw["tunnel_id"] ?? "");
      const credits = Number(raw["credits"] ?? 0);
      const inflight = this._pendingTunnels.get(tunnelId);
      if (inflight) {
        inflight.addCredits(credits);
      }
      return;
    }

    if (data["tunnelClose"] || data["tunnel_close"]) {
      const raw = (data["tunnelClose"] ?? data["tunnel_close"]) as Record<string, unknown>;
      const tunnelId = String(raw["tunnelId"] ?? raw["tunnel_id"] ?? "");
      const reason = String(raw["reason"] ?? "NORMAL");
      const detail = String(raw["detail"] ?? "");
      const inflight = this._pendingTunnels.get(tunnelId);
      if (inflight) {
        inflight.closeWithError(new TunnelClosedError(reason, detail));
      }
      return;
    }
  }

  // ===========================================================================
  // Error and Stream End Handling
  // ===========================================================================

  /**
   * Handles gRPC stream errors.
   * @internal
   */
  private _handleStreamError(err: Error): void {
    const wasConnected = this._connected;
    this._connected = false;

    if (this._disconnectRequested) {
      return;
    }

    if (wasConnected) {
      this._onDisconnect(err.message);
    }

    // Attempt reconnection if the error is recoverable
    if (this._connectionOpts.autoReconnect && isRecoverable(err)) {
      this._attemptReconnection();
    }
  }

  /**
   * Handles gRPC stream end.
   * @internal
   */
  private _handleStreamEnd(): void {
    const wasConnected = this._connected;
    this._connected = false;

    if (this._disconnectRequested) {
      return;
    }

    if (wasConnected) {
      this._onDisconnect("Stream ended");
    }

    if (this._connectionOpts.autoReconnect) {
      this._attemptReconnection();
    }
  }

  /**
   * Attempts automatic reconnection with exponential backoff.
   * @internal
   */
  private async _attemptReconnection(): Promise<void> {
    // Prevent concurrent reconnection loops (e.g. both error and end handlers fire)
    if (this._reconnecting) {
      return;
    }
    this._reconnecting = true;

    try {
      let delay = this._connectionOpts.initialBackoff;
      const maxRetries = this._connectionOpts.maxRetries;

      for (let attempt = 1; maxRetries === 0 || attempt <= maxRetries; attempt++) {
        if (this._disconnectRequested) {
          return;
        }

        this._onReconnecting(attempt);

        // Wait with backoff
        await new Promise<void>((resolve) => setTimeout(resolve, delay));

        if (this._disconnectRequested) {
          return;
        }

        try {
          // Clean up old connection before establishing new one
          await this._closeConnection();
          await this._establishConnection();
          this._connected = true;

          // Re-send init message is handled inside _establishConnection
          return;
        } catch {
          // Calculate next backoff delay
          delay = Math.min(
            delay * this._connectionOpts.backoffMultiplier,
            this._connectionOpts.maxBackoff,
          );
          // Add jitter (10% random variation)
          delay = delay * (0.9 + Math.random() * 0.2);
        }
      }

      // Exhausted all retries
      const err = new ReconnectionError(maxRetries);
      this._onDisconnect(err.message);
    } finally {
      this._reconnecting = false;
    }
  }

  // ===========================================================================
  // Progress Reporting
  // ===========================================================================

  /**
   * Reports progress for a task through the Aether gateway.
   *
   * Progress updates are routed through RabbitMQ Streams via the pg::{workspace}
   * topic with server-side recipient filtering.
   *
   * @param opts - Progress report options
   * @throws {@link ConnectionError} if not connected
   */
  reportProgress(opts: ReportProgressOptions): void {
    if (!this._connected) {
      throw new ConnectionError("Not connected to gateway");
    }

    const report: Record<string, unknown> = {
      taskId: opts.taskId,
      state: opts.state ?? "",
      completion: opts.completion ?? -1,
      summary: opts.summary ?? "",
      recipient: opts.recipient ?? "",
      requestId: opts.requestId ?? "",
      metadata: opts.metadata ?? {},
    };

    if (opts.stepName) {
      report["step"] = {
        name: opts.stepName,
        detail: opts.stepDetail ?? "",
        sequence: opts.stepSequence ?? 0,
        totalSteps: opts.stepTotal ?? 0,
        stepType: opts.stepType ?? "",
      };
    }

    this._sendUpstream({ progressReport: report });
  }

  // ===========================================================================
  // Internal Send Helper
  // ===========================================================================

  /**
   * Encodes a Metric object to protobuf bytes using the loaded proto descriptor.
   *
   * Uses the `aether.v1.Metric` type from the package definition stored at
   * connection time. Throws if the descriptor is not yet available — sending
   * JSON-encoded bytes here would silently fail on the gateway with
   * ERR_METRIC_INVALID, so we eagerly surface the misuse.
   *
   * @param metric - The metric object to encode
   * @returns Encoded bytes
   * @internal
   */
  protected _encodeMetric(metric: Record<string, unknown>): Uint8Array {
    const pkgDef = this._packageDefinition;
    if (!pkgDef) {
      throw new Error("Cannot encode metric: proto descriptor not loaded — call connect() first");
    }
    const metricType = pkgDef["aether.v1.Metric"] as { encode: (obj: unknown) => { finish: () => Uint8Array } } | undefined;
    if (!metricType?.encode) {
      throw new Error("Cannot encode metric: aether.v1.Metric not present in loaded proto descriptor");
    }
    return metricType.encode(metric).finish();
  }

  /**
   * Low-level message send helper used by subclasses.
   * @internal
   */
  protected _sendMessage(targetTopic: string, payload: Uint8Array, messageType: MessageType): void {
    this._sendUpstream({
      send: {
        targetTopic,
        payload,
        messageType,
      },
    });
  }

  // ===========================================================================
  // Task Management
  // ===========================================================================

  /**
   * Parses a raw task info object from the server into a TaskInfo.
   * @internal
   */
  private _parseTaskInfo(t: Record<string, unknown>): TaskInfo {
    return {
      taskId: String(t["taskId"] ?? t["task_id"] ?? ""),
      taskType: String(t["taskType"] ?? t["task_type"] ?? ""),
      status: String(t["status"] ?? ""),
      workspace: String(t["workspace"] ?? ""),
      targetTopic: String(t["targetTopic"] ?? t["target_topic"] ?? ""),
      assignedTo: String(t["assignedTo"] ?? t["assigned_to"] ?? ""),
      createdAt: Number(t["createdAt"] ?? t["created_at"] ?? 0),
      startedAt: Number(t["startedAt"] ?? t["started_at"] ?? 0),
      completedAt: Number(t["completedAt"] ?? t["completed_at"] ?? 0),
      attempt: Number(t["attempt"] ?? 0),
      maxAttempts: Number(t["maxAttempts"] ?? t["max_attempts"] ?? 0),
      error: String(t["error"] ?? ""),
      metadata: (t["metadata"] ?? {}) as Record<string, string>,
    };
  }

  /**
   * Parses a raw authority grant object from the server into an AuthorityGrantInfo.
   * @internal
   */
  private _parseAuthorityGrantInfo(g: Record<string, unknown>): AuthorityGrantInfo {
    const parsePrincipalRef = (ref: unknown) => {
      if (!ref || typeof ref !== "object") {
        return undefined;
      }
      const value = ref as Record<string, unknown>;
      return {
        principalType: String(value["principalType"] ?? value["principal_type"] ?? ""),
        principalId: String(value["principalId"] ?? value["principal_id"] ?? ""),
      };
    };

    const parseResourceScope = (entry: unknown) => {
      const value = (entry ?? {}) as Record<string, unknown>;
      return {
        resourceType: String(value["resourceType"] ?? value["resource_type"] ?? ""),
        patterns: Array.isArray(value["patterns"]) ? (value["patterns"] as unknown[]).map(String) : [],
      };
    };

    return {
      grantId: String(g["grantId"] ?? g["grant_id"] ?? ""),
      rootGrantId: String(g["rootGrantId"] ?? g["root_grant_id"] ?? ""),
      subject: parsePrincipalRef(g["subject"]),
      delegate: parsePrincipalRef(g["delegate"]),
      issuedBy: parsePrincipalRef(g["issuedBy"] ?? g["issued_by"]),
      rootSubject: parsePrincipalRef(g["rootSubject"] ?? g["root_subject"]),
      parentGrantId: String(g["parentGrantId"] ?? g["parent_grant_id"] ?? ""),
      mayDelegate: Boolean(g["mayDelegate"] ?? g["may_delegate"]),
      remainingHops: Number(g["remainingHops"] ?? g["remaining_hops"] ?? 0),
      workspaceScope: Array.isArray(g["workspaceScope"] ?? g["workspace_scope"])
        ? ((g["workspaceScope"] ?? g["workspace_scope"]) as unknown[]).map(String)
        : [],
      resourceScope: Array.isArray(g["resourceScope"] ?? g["resource_scope"])
        ? ((g["resourceScope"] ?? g["resource_scope"]) as unknown[]).map(parseResourceScope)
        : [],
      operationScope: Array.isArray(g["operationScope"] ?? g["operation_scope"])
        ? ((g["operationScope"] ?? g["operation_scope"]) as unknown[]).map(String)
        : [],
      maxAccessLevel: Number(g["maxAccessLevel"] ?? g["max_access_level"] ?? 0),
      accessLevelName: String(g["accessLevelName"] ?? g["access_level_name"] ?? ""),
      audienceType: String(g["audienceType"] ?? g["audience_type"] ?? ""),
      audienceId: String(g["audienceId"] ?? g["audience_id"] ?? ""),
      validWhileAudienceActive: Boolean(g["validWhileAudienceActive"] ?? g["valid_while_audience_active"]),
      expiresAt: Number(g["expiresAt"] ?? g["expires_at"] ?? 0),
      renewableUntil: Number(g["renewableUntil"] ?? g["renewable_until"] ?? 0),
      renewedAt: Number(g["renewedAt"] ?? g["renewed_at"] ?? 0),
      revoked: Boolean(g["revoked"]),
      revokedAt: Number(g["revokedAt"] ?? g["revoked_at"] ?? 0),
      reason: String(g["reason"] ?? ""),
      metadata: (g["metadata"] ?? {}) as Record<string, string>,
      createdAt: Number(g["createdAt"] ?? g["created_at"] ?? 0),
    };
  }

  /**
   * Lists tasks with optional filters.
   */
  queryTasks(opts: TaskQueryOptions = {}): Promise<TaskQueryResponse> {
    const timeout = opts.timeout ?? 10000;
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingTaskQueryRequests.delete(requestId);
        reject(new Error("queryTasks timed out"));
      }, timeout);

      this._pendingTaskQueryRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        taskQuery: {
          op: 0, // LIST
          requestId,
          filter: {
            status: opts.status ?? "",
            workspace: opts.workspace ?? "",
            taskType: opts.taskType ?? "",
            limit: opts.limit ?? 0,
            offset: opts.offset ?? 0,
          },
        },
      });
    });
  }

  /**
   * Gets a specific task by ID.
   */
  getTask(taskId: string, timeout = 10000): Promise<TaskQueryResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingTaskQueryRequests.delete(requestId);
        reject(new Error("getTask timed out"));
      }, timeout);

      this._pendingTaskQueryRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        taskQuery: {
          op: 1, // GET
          taskId,
          requestId,
        },
      });
    });
  }

  /**
   * Cancels a running or queued task.
   */
  cancelTask(taskId: string, reason = "", timeout = 10000): Promise<TaskOperationResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingTaskOpRequests.delete(requestId);
        reject(new Error("cancelTask timed out"));
      }, timeout);

      this._pendingTaskOpRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        taskOp: {
          op: 1, // CANCEL
          taskId,
          reason,
          requestId,
        },
      });
    });
  }

  /**
   * Retries a failed task.
   */
  retryTask(taskId: string, timeout = 10000): Promise<TaskOperationResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingTaskOpRequests.delete(requestId);
        reject(new Error("retryTask timed out"));
      }, timeout);

      this._pendingTaskOpRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        taskOp: {
          op: 0, // RETRY
          taskId,
          requestId,
        },
      });
    });
  }

  /**
   * Completes a task (for POOL workers).
   */
  completeTask(taskId: string, timeout = 10000): Promise<TaskOperationResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingTaskOpRequests.delete(requestId);
        reject(new Error("completeTask timed out"));
      }, timeout);

      this._pendingTaskOpRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        taskOp: {
          op: 2, // COMPLETE
          taskId,
          requestId,
        },
      });
    });
  }

  /**
   * Marks a task as failed (for POOL workers).
   */
  failTask(taskId: string, reason = "", timeout = 10000): Promise<TaskOperationResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingTaskOpRequests.delete(requestId);
        reject(new Error("failTask timed out"));
      }, timeout);

      this._pendingTaskOpRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        taskOp: {
          op: 3, // FAIL
          taskId,
          reason,
          requestId,
        },
      });
    });
  }

  // ===========================================================================
  // Create Task (blocking)
  // ===========================================================================

  /**
   * Sends a CreateTaskRequest and waits for the server's CreateTaskResponse.
   *
   * A unique request_id is injected automatically so the response can be
   * correlated.  Unlike the fire-and-forget `createTask` override in subclasses,
   * this method blocks until the gateway echoes back the assigned task_id.
   *
   * @param op - CreateTaskRequest fields (task_type, workspace, assignment_mode, etc.)
   * @param timeout - Timeout in ms (default 10 000)
   * @returns Promise resolving to the CreateTaskResponse
   */
  createTaskBlocking(op: Record<string, unknown>, timeout = 10000): Promise<CreateTaskResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingCreateTaskRequests.delete(requestId);
        reject(new Error("createTaskBlocking timed out"));
      }, timeout);

      this._pendingCreateTaskRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        createTask: { ...op, requestId },
      });
    });
  }

  // ===========================================================================
  // Workspace / Agent / ACL / Workflow Operations
  // ===========================================================================

  /**
   * Sends a workspace operation upstream.
   *
   * @param op - The workspace operation payload
   * @param timeout - Timeout in ms
   * @returns Promise resolving to the workspace response
   */
  sendWorkspaceOperation(op: Record<string, unknown>, timeout = 10000): Promise<WorkspaceResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingWorkspaceRequests.delete(requestId);
        reject(new Error("workspace operation timed out"));
      }, timeout);

      this._pendingWorkspaceRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        workspaceOp: { ...op, requestId },
      });
    });
  }

  /**
   * Sends an agent operation upstream.
   *
   * @param op - The agent operation payload
   * @param timeout - Timeout in ms
   * @returns Promise resolving to the agent response
   */
  sendAgentOperation(op: Record<string, unknown>, timeout = 10000): Promise<AgentResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingAgentRequests.delete(requestId);
        reject(new Error("agent operation timed out"));
      }, timeout);

      this._pendingAgentRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        agentOp: { ...op, requestId },
      });
    });
  }

  /**
   * Sends an ACL operation upstream.
   *
   * @param op - The ACL operation payload
   * @param timeout - Timeout in ms
   * @returns Promise resolving to the ACL response
   */
  sendACLOperation(op: Record<string, unknown>, timeout = 10000): Promise<ACLResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingACLRequests.delete(requestId);
        reject(new Error("ACL operation timed out"));
      }, timeout);

      this._pendingACLRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        aclOp: { ...op, requestId },
      });
    });
  }

  /**
   * Sends a runtime authority-grant operation upstream.
   *
   * @param op - The authority grant operation payload
   * @param timeout - Timeout in ms
   * @returns Promise resolving to the authority-grant response
   */
  sendAuthorityGrantOperation(op: Record<string, unknown>, timeout = 10000): Promise<AuthorityGrantResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingAuthorityGrantRequests.delete(requestId);
        reject(new Error("authority grant operation timed out"));
      }, timeout);

      this._pendingAuthorityGrantRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        authorityGrantOp: { ...op, requestId },
      });
    });
  }

  /**
   * Submits a foreign audit event to the gateway's audit pipeline.
   *
   * @param opts - Audit event fields
   * @param timeout - Timeout in ms (default 10 000)
   * @returns Promise resolving to the audit-submit response
   */
  submitAuditEvent(
    opts: {
      eventType: string;
      operation?: string;
      resourceType?: string;
      resourceId?: string;
      workspace?: string;
      success?: boolean;
      errorMessage?: string;
      metadata?: Record<string, string>;
    },
    timeout = 10000,
  ): Promise<AuditSubmitResponse> {
    const clientRequestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingAuditSubmitRequests.delete(clientRequestId);
        reject(new Error("submit audit event timed out"));
      }, timeout);
      this._pendingAuditSubmitRequests.set(clientRequestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });
      this._sendUpstream({
        submitAuditEvent: {
          clientRequestId,
          eventType: opts.eventType,
          operation: opts.operation ?? "",
          resourceType: opts.resourceType ?? "",
          resourceId: opts.resourceId ?? "",
          workspace: opts.workspace ?? "",
          success: opts.success ?? true,
          errorMessage: opts.errorMessage ?? "",
          metadata: opts.metadata ?? {},
        },
      });
    });
  }

  /**
   * Sends a workflow operation upstream.
   *
   * @param op - The workflow operation payload
   * @param timeout - Timeout in ms
   * @returns Promise resolving to the workflow response
   */
  sendWorkflowOperation(op: Record<string, unknown>, timeout = 10000): Promise<WorkflowResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingWorkflowRequests.delete(requestId);
        reject(new Error("workflow operation timed out"));
      }, timeout);

      this._pendingWorkflowRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        workflowOp: { ...op, requestId },
      });
    });
  }

  // ===========================================================================
  // Workspace / Agent / ACL / Workflow Handler Registration
  // ===========================================================================

  /**
   * Registers a handler for workspace operation responses.
   *
   * @param handler - Function called when a workspace response is received
   */
  onWorkspaceResponse(handler: WorkspaceResponseHandler): void {
    this._onWorkspaceResponse = handler;
  }

  /**
   * Registers a handler for agent operation responses.
   *
   * @param handler - Function called when an agent response is received
   */
  onAgentResponse(handler: AgentResponseHandler): void {
    this._onAgentResponse = handler;
  }

  /**
   * Registers a handler for ACL operation responses.
   *
   * @param handler - Function called when an ACL response is received
   */
  onACLResponse(handler: ACLResponseHandler): void {
    this._onACLResponse = handler;
  }

  /**
   * Registers a handler for runtime authority-grant responses.
   *
   * @param handler - Function called when an authority-grant response is received
   */
  onAuthorityGrantResponse(handler: AuthorityGrantResponseHandler): void {
    this._onAuthorityGrantResponse = handler;
  }

  /**
   * Registers a handler for unsolicited audit-submit responses (i.e. responses
   * not correlated to a pending {@link submitAuditEvent} call).
   *
   * @param handler - Function called when an audit-submit response is received
   */
  onAuditSubmitResponse(handler: AuditSubmitResponseHandler): void {
    this._onAuditSubmitResponse = handler;
  }

  /**
   * Registers a handler for server-pushed AuthorityGrantRevocation events.
   * Caches registered via {@link makeAuthorityCache} are invoked first; the
   * user handler runs after every cache has been notified.
   */
  onAuthorityGrantRevocation(handler: AuthorityGrantRevocationHandler): void {
    this._onAuthorityGrantRevocation = handler;
  }

  // =========================================================================
  // Authority Grant High-Level Wrappers
  //
  // Low-level: one method per AuthorityGrantOperation op. For ergonomic
  // caching, soft-renew, and revocation invalidation use
  // {@link makeAuthorityCache} and its high-level helpers
  // (getOrExchange / deriveForTask / isValid / listActive /
  // revokeLocal / refresh).
  // =========================================================================

  /**
   * Bootstrap a runtime authority grant for the current actor.
   *
   * If `sourceSessionId` is empty, only a user may self-exchange. When it
   * is set, the caller must hold the `_perm:exchange_authority_grants`
   * permission and the referenced session must belong to an active user.
   */
  exchangeAuthorityGrant(
    opts: {
      sourceSessionId?: string;
      workspaceScope?: string[];
      resourceScope?: { resourceType: string; patterns: string[] }[];
      operationScope?: string[];
      maxAccessLevel?: number;
      audienceType?: string;
      audienceId?: string;
      validWhileAudienceActive?: boolean;
      expiresAt?: number;
      renewableUntil?: number;
      mayDelegate?: boolean;
      remainingHops?: number;
      reason?: string;
      metadata?: Record<string, string>;
      timeout?: number;
    } = {},
  ): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation(
      {
        op: "EXCHANGE",
        exchangeRequest: {
          sourceSessionId: opts.sourceSessionId ?? "",
          workspaceScope: opts.workspaceScope ?? [],
          resourceScope: opts.resourceScope ?? [],
          operationScope: opts.operationScope ?? [],
          maxAccessLevel: opts.maxAccessLevel ?? 0,
          audienceType: opts.audienceType ?? "",
          audienceId: opts.audienceId ?? "",
          validWhileAudienceActive: opts.validWhileAudienceActive ?? false,
          expiresAt: opts.expiresAt ?? 0,
          renewableUntil: opts.renewableUntil ?? 0,
          mayDelegate: opts.mayDelegate ?? false,
          remainingHops: opts.remainingHops ?? 0,
          reason: opts.reason ?? "",
          metadata: opts.metadata ?? {},
        },
      },
      opts.timeout,
    );
  }

  /**
   * Derive a child runtime authority grant for a downstream delegate.
   */
  deriveAuthorityGrant(opts: {
    parentGrantId: string;
    delegateType: string;
    delegateId: string;
    workspaceScope?: string[];
    resourceScope?: { resourceType: string; patterns: string[] }[];
    operationScope?: string[];
    maxAccessLevel?: number;
    audienceType?: string;
    audienceId?: string;
    validWhileAudienceActive?: boolean;
    expiresAt?: number;
    renewableUntil?: number;
    mayDelegate?: boolean;
    remainingHops?: number;
    reason?: string;
    metadata?: Record<string, string>;
    timeout?: number;
  }): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation(
      {
        op: "DERIVE",
        deriveRequest: {
          parentGrantId: opts.parentGrantId,
          delegate: { principalType: opts.delegateType, principalId: opts.delegateId },
          workspaceScope: opts.workspaceScope ?? [],
          resourceScope: opts.resourceScope ?? [],
          operationScope: opts.operationScope ?? [],
          maxAccessLevel: opts.maxAccessLevel ?? 0,
          audienceType: opts.audienceType ?? "",
          audienceId: opts.audienceId ?? "",
          validWhileAudienceActive: opts.validWhileAudienceActive ?? false,
          expiresAt: opts.expiresAt ?? 0,
          renewableUntil: opts.renewableUntil ?? 0,
          mayDelegate: opts.mayDelegate ?? false,
          remainingHops: opts.remainingHops ?? 0,
          reason: opts.reason ?? "",
          metadata: opts.metadata ?? {},
        },
      },
      opts.timeout,
    );
  }

  /** Get a runtime authority grant by ID. */
  getAuthorityGrant(grantId: string, timeout?: number): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation({ op: "GET", grantId }, timeout);
  }

  /**
   * Renew a runtime authority grant lease. `expiresAt` (proto units) takes
   * precedence; `extendSeconds` extends the current expiry by N seconds
   * server-side and is ignored when `expiresAt` is non-zero.
   */
  renewAuthorityGrant(opts: {
    grantId: string;
    expiresAt?: number;
    extendSeconds?: number;
    timeout?: number;
  }): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation(
      {
        op: "RENEW",
        grantId: opts.grantId,
        renewRequest: {
          grantId: opts.grantId,
          expiresAt: opts.expiresAt ?? 0,
          extendSeconds: opts.extendSeconds ?? 0,
        },
      },
      opts.timeout,
    );
  }

  /** Revoke a runtime authority grant by ID. */
  revokeAuthorityGrant(grantId: string, timeout?: number): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation({ op: "REVOKE", grantId }, timeout);
  }

  /** List grants where the actor is delegate or subject. */
  listMyAuthorityGrants(opts: {
    audienceType?: string;
    audienceId?: string;
    includeRevoked?: boolean;
    limit?: number;
    offset?: number;
    timeout?: number;
  } = {}): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation(
      {
        op: "LIST_MY_GRANTS",
        listRequest: {
          audienceType: opts.audienceType ?? "",
          audienceId: opts.audienceId ?? "",
          includeRevoked: opts.includeRevoked ?? false,
          limit: opts.limit ?? 0,
          offset: opts.offset ?? 0,
        },
      },
      opts.timeout,
    );
  }

  /** List grants where the actor is the subject (i.e., grants OTHERS hold on me). */
  listAuthorityGrantsOnMe(opts: {
    audienceType?: string;
    audienceId?: string;
    includeRevoked?: boolean;
    limit?: number;
    offset?: number;
    timeout?: number;
  } = {}): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation(
      {
        op: "LIST_GRANTS_ON_ME",
        listRequest: {
          audienceType: opts.audienceType ?? "",
          audienceId: opts.audienceId ?? "",
          includeRevoked: opts.includeRevoked ?? false,
          limit: opts.limit ?? 0,
          offset: opts.offset ?? 0,
        },
      },
      opts.timeout,
    );
  }

  /** Exchange multiple authority grants in a single round-trip. */
  batchExchangeAuthorityGrants(opts: {
    requests: Record<string, unknown>[];
    stopOnFirstError?: boolean;
    timeout?: number;
  }): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation(
      {
        op: "BATCH_EXCHANGE",
        batchExchangeRequest: {
          requests: opts.requests,
          stopOnFirstError: opts.stopOnFirstError ?? false,
        },
      },
      opts.timeout,
    );
  }

  /**
   * Idempotent derive: returns existing visible grant matching
   * (parentGrantId, target, audience) or mints a new one.
   *
   * Safe to call repeatedly without leaking grants.
   */
  deriveAuthorityGrantForTarget(opts: {
    parentGrantId: string;
    targetType: string;
    targetId: string;
    audienceType?: string;
    audienceId?: string;
    operationScope?: string[];
    maxAccessLevel?: number;
    expiresAt?: number;
    renewableUntil?: number;
    mayDelegate?: boolean;
    remainingHops?: number;
    reason?: string;
    timeout?: number;
  }): Promise<AuthorityGrantResponse> {
    return this.sendAuthorityGrantOperation(
      {
        op: "DERIVE_FOR_TARGET",
        deriveForTargetRequest: {
          parentGrantId: opts.parentGrantId,
          target: { principalType: opts.targetType, principalId: opts.targetId },
          audienceType: opts.audienceType ?? "",
          audienceId: opts.audienceId ?? "",
          operationScope: opts.operationScope ?? [],
          maxAccessLevel: opts.maxAccessLevel ?? 0,
          expiresAt: opts.expiresAt ?? 0,
          renewableUntil: opts.renewableUntil ?? 0,
          mayDelegate: opts.mayDelegate ?? false,
          remainingHops: opts.remainingHops ?? 0,
          reason: opts.reason ?? "",
        },
      },
      opts.timeout,
    );
  }

  /**
   * Construct a new {@link AuthorityGrantCache} wired into this client.
   * AuthorityGrantRevocation push events on the downstream stream are
   * dispatched to the cache automatically. Call {@link AuthorityGrantCache.close}
   * to deregister it.
   */
  makeAuthorityCache(opts: AuthorityGrantCacheOptions = {}): AuthorityGrantCache {
    const cache = new AuthorityGrantCache(this, opts);
    this._authorityGrantCaches.push(cache);
    return cache;
  }

  /** @internal */
  _removeAuthorityCache(cache: AuthorityGrantCache): void {
    this._authorityGrantCaches = this._authorityGrantCaches.filter((c) => c !== cache);
  }

  /**
   * Registers a handler for workflow operation responses.
   *
   * @param handler - Function called when a workflow response is received
   */
  onWorkflowResponse(handler: WorkflowResponseHandler): void {
    this._onWorkflowResponse = handler;
  }

  /**
   * Sends a token management operation to the gateway.
   *
   * @param op - The token operation payload
   * @param timeout - Timeout in ms
   * @returns Promise resolving to the token response
   */
  sendTokenOperation(op: Record<string, unknown>, timeout = 10000): Promise<TokenResponse> {
    const requestId = this.nextRequestId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingTokenRequests.delete(requestId);
        reject(new Error("token operation timed out"));
      }, timeout);

      this._pendingTokenRequests.set(requestId, (response) => {
        clearTimeout(timer);
        resolve(response);
      });

      this._sendUpstream({
        tokenOp: { ...op, requestId },
      });
    });
  }

  /**
   * Registers a handler for token operation responses.
   *
   * @param handler - Function called when a token response is received
   */
  onTokenResponse(handler: TokenResponseHandler): void {
    this._onTokenResponse = handler;
  }
}
