// Package aether base client implementation.
//
// This file provides the BaseClient struct that handles gRPC connection
// management, TLS configuration, and message queue infrastructure.
// Specific client types (Agent, Task, User, etc.) embed this base client.

package aether

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/scitrera/aether/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// =============================================================================
// Base Client
// =============================================================================

// BaseClient is the core client that handles gRPC connection management.
// It provides connection establishment, TLS configuration, and message queue
// infrastructure. Specific client types (AgentClient, TaskClient, etc.) embed
// this struct and add their specialized functionality.
//
// BaseClient is safe for concurrent use. All methods that modify state use
// appropriate synchronization.
type BaseClient struct {
	// Configuration
	serverAddr string
	options    ConnectionOptions
	tlsConfig  *TLSConfig
	creds      map[string]string

	// preDialedConn, when non-nil, takes precedence over serverAddr/tlsConfig.
	// Used by embedded callers (e.g. AetherLite's workflow engine) that
	// already have a *grpc.ClientConn pointing at an in-process bufconn-
	// backed gRPC server. ownsPreDialedConn controls whether Close() also
	// closes preDialedConn — false for caller-managed lifetime (the default
	// for the bufconn case, where the parent process owns the listener).
	preDialedConn     *grpc.ClientConn
	ownsPreDialedConn bool

	// gRPC connection state
	conn   *grpc.ClientConn
	client pb.AetherGatewayClient
	stream pb.AetherGateway_ConnectClient

	// Request queue for outgoing messages
	requestQueue chan *pb.UpstreamMessage
	queueSize    int

	// Handler registry for callbacks
	handlers *Handlers

	// State management
	mu                  sync.RWMutex
	ctx                 context.Context
	cancel              context.CancelFunc
	running             atomic.Bool
	connected           atomic.Bool
	reconnecting        atomic.Bool
	forceDisconnect     atomic.Bool
	connectionConfirmed atomic.Bool

	// Session state for reconnection
	sessionID      string
	sessionIDMu    sync.RWMutex
	reconnectCount atomic.Int32

	// Response queues for synchronous operations (legacy fallback for responses without request_id)
	kvResponseQueue         chan *KVResponse
	checkpointResponseQueue chan *CheckpointResponse

	// Pending request maps for request_id-based correlation
	pendingKVRequests             pendingRequests[*KVResponse]
	pendingCheckpointRequests     pendingRequests[*CheckpointResponse]
	pendingCreateTaskRequests     pendingRequests[*CreateTaskResponse]
	pendingTaskQueryRequests      pendingRequests[*TaskQueryResponse]
	pendingTaskOpRequests         pendingRequests[*TaskOperationResponse]
	pendingWorkflowRequests       pendingRequests[*WorkflowResponse]
	pendingWorkspaceRequests      pendingRequests[*WorkspaceResponse]
	pendingAgentRequests          pendingRequests[*AgentResponse]
	pendingACLRequests            pendingRequests[*ACLResponse]
	pendingTokenRequests          pendingRequests[*TokenResponse]
	pendingAuthorityGrantRequests pendingRequests[*pb.AuthorityGrantResponse]
	pendingAdminRequests          pendingRequests[*AdminResponse]
	pendingSessionRequests        pendingRequests[*SessionOperationResponse]
	pendingAuditSubmitRequests    pendingRequests[*pb.SubmitAuditEventResponse]
	requestIDCounter              atomic.Uint64

	// Registered authority-grant caches receive AuthorityGrantRevocation
	// push events. Slice (not single field) so multiple caches per client
	// stay supported; in practice a single cache is normal.
	authorityCacheMu sync.RWMutex
	authorityCaches  []*AuthorityGrantCache

	// Cached KV, Checkpoint, and Workflow helpers (for sync mutex to work across calls)
	kvOnce            sync.Once
	kvInstance        *KV
	cpOnce            sync.Once
	cpInstance        *Checkpoint
	workflowOnce      sync.Once
	workflowInstance  *WorkflowOps
	workspaceOnce     sync.Once
	workspaceInstance *WorkspaceOps
	agentOnce         sync.Once
	agentInstance     *AgentOps
	aclOnce           sync.Once
	aclInstance       *ACLOps
	tokenOnce         sync.Once
	tokenInstance     *TokenOps
	authorityOnce     sync.Once
	authorityInstance *AuthorityGrantOps
	adminOnce         sync.Once
	adminInstance     *AdminOps
	sessionOnce       sync.Once
	sessionInstance   *SessionOps

	// InitConnection message builder (set by specific client types)
	initMsgBuilder func() *pb.InitConnection

	// Stream management
	streamMu     sync.Mutex
	streamCtx    context.Context
	streamCancel context.CancelFunc
}

// BaseClientConfig contains configuration for creating a BaseClient.
//
// This is typically used internally by higher-level client constructors
// (NewAgentClient, NewTaskClient, etc.) rather than directly by users.
type BaseClientConfig struct {
	// ServerAddr is the gRPC server address (host:port).
	// Required unless PreDialedConn is set.
	ServerAddr string

	// Connection configures retry and backoff behavior.
	Connection ConnectionOptions

	// TLS configures TLS/mTLS for secure connections.
	// If nil, insecure connections are used. Ignored when PreDialedConn is set.
	TLS *TLSConfig

	// Credentials for authentication.
	// Keys and values are passed to the server as metadata.
	Credentials map[string]string

	// QueueSize is the size of the outgoing message queue.
	// Default: 100.
	QueueSize int

	// PreDialedConn, when non-nil, is used in place of dialing ServerAddr.
	// This is for embedded callers that already have a *grpc.ClientConn
	// pointing at an in-process server (e.g. bufconn-backed). When set,
	// ServerAddr is optional and TLS is ignored — the conn provides its own
	// transport. See NewClientWithConn / NewWorkflowEngineClientWithConn.
	PreDialedConn *grpc.ClientConn

	// OwnsPreDialedConn controls whether Close() closes PreDialedConn.
	// Default: false (caller-managed lifetime). Set to true to transfer
	// ownership to the client.
	OwnsPreDialedConn bool
}

// NewBaseClient creates a new BaseClient with the given configuration.
//
// The client is created but not connected. Call Connect() to establish
// the connection to the server.
func NewBaseClient(cfg BaseClientConfig) (*BaseClient, error) {
	if cfg.PreDialedConn == nil && cfg.ServerAddr == "" {
		return nil, NewInvalidArgumentError("server address is required", "ServerAddr")
	}

	// Apply defaults
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100
	}

	connOpts := cfg.Connection
	if connOpts.MaxRetries == 0 && connOpts.InitialBackoff == 0 {
		connOpts = DefaultConnectionOptions()
	}

	bc := &BaseClient{
		serverAddr:              cfg.ServerAddr,
		options:                 connOpts,
		tlsConfig:               cfg.TLS,
		creds:                   cfg.Credentials,
		queueSize:               cfg.QueueSize,
		requestQueue:            make(chan *pb.UpstreamMessage, cfg.QueueSize),
		handlers:                NewHandlers(),
		kvResponseQueue:         make(chan *KVResponse, 10),
		checkpointResponseQueue: make(chan *CheckpointResponse, 10),
		preDialedConn:           cfg.PreDialedConn,
		ownsPreDialedConn:       cfg.OwnsPreDialedConn,
	}

	return bc, nil
}

// =============================================================================
// TLS Configuration
// =============================================================================

// buildTLSConfig creates a *tls.Config from the TLSConfig settings.
func (c *BaseClient) buildTLSConfig() (*tls.Config, error) {
	if c.tlsConfig == nil || !c.tlsConfig.Enabled {
		return nil, nil
	}

	config := &tls.Config{
		ServerName:         c.tlsConfig.ServerName,
		InsecureSkipVerify: c.tlsConfig.InsecureSkipVerify,
	}

	// Load root CA certificates if provided
	if len(c.tlsConfig.RootCAs) > 0 {
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(c.tlsConfig.RootCAs) {
			return nil, NewConnectionError("failed to parse root CA certificates")
		}
		config.RootCAs = certPool
	}

	// Load client certificate for mTLS if provided
	if len(c.tlsConfig.ClientCert) > 0 && len(c.tlsConfig.ClientKey) > 0 {
		cert, err := tls.X509KeyPair(c.tlsConfig.ClientCert, c.tlsConfig.ClientKey)
		if err != nil {
			return nil, NewConnectionError("failed to load client certificate: " + err.Error())
		}
		config.Certificates = []tls.Certificate{cert}
	} else if len(c.tlsConfig.ClientCert) > 0 || len(c.tlsConfig.ClientKey) > 0 {
		// Only one of cert/key provided - error
		return nil, NewInvalidArgumentError(
			"both ClientCert and ClientKey must be provided for mTLS",
			"TLSConfig",
		)
	}

	return config, nil
}

// buildDialOptions creates gRPC dial options with TLS and keepalive settings.
func (c *BaseClient) buildDialOptions() ([]grpc.DialOption, error) {
	var opts []grpc.DialOption

	// TLS configuration
	if c.tlsConfig != nil && c.tlsConfig.Enabled {
		tlsConfig, err := c.buildTLSConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Keepalive configuration
	if c.options.KeepAliveInterval > 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.options.KeepAliveInterval,
			Timeout:             c.options.KeepAliveInterval / 2,
			PermitWithoutStream: true,
		}))
	}

	return opts, nil
}

// =============================================================================
// Connection Management
// =============================================================================

// Connect establishes a connection to the Aether gateway.
//
// This method blocks until the connection is established or an error occurs.
// If AutoReconnect is enabled, the client will automatically attempt to
// reconnect on connection loss.
//
// The provided context controls connection lifetime. Canceling the context
// will close the connection and stop any reconnection attempts.
func (c *BaseClient) Connect(ctx context.Context) error {
	if c.initMsgBuilder == nil {
		return NewInvalidArgumentError("init message builder not set", "initMsgBuilder")
	}

	c.mu.Lock()
	if c.running.Load() {
		// Client is already running. Check if we're reconnecting internally
		// and if so, wait for the reconnection to complete.
		if c.reconnecting.Load() {
			c.mu.Unlock()
			// Wait for reconnection to complete (or context to cancel)
			for {
				select {
				case <-ctx.Done():
					return NewConnectionError("context canceled while waiting for reconnection")
				case <-time.After(10 * time.Millisecond):
					if !c.reconnecting.Load() && c.running.Load() && c.connectionConfirmed.Load() {
						// Reconnection succeeded, client is now connected
						return nil
					}
					if !c.running.Load() {
						// Reconnection failed, we can try to connect fresh
						c.mu.Lock()
						goto tryConnect
					}
				}
			}
		}
		// Already running and not reconnecting - we're connected
		c.mu.Unlock()
		return nil
	}

tryConnect:
	// Create a cancellable context for the client lifecycle
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running.Store(true)
	c.forceDisconnect.Store(false)
	c.reconnecting.Store(false)
	c.connectionConfirmed.Store(false)
	c.reconnectCount.Store(0)
	c.mu.Unlock()

	// Attempt initial connection
	return c.doConnect(ctx)
}

// doConnect performs the actual connection attempt without retry logic.
func (c *BaseClient) doConnect(ctx context.Context) error {
	var conn *grpc.ClientConn

	// If the caller supplied a pre-dialed conn (e.g. an in-process
	// bufconn-backed connection), reuse it. The underlying conn is
	// long-lived; we just rebind the gateway client + open a fresh
	// stream below. This also makes reconnect cheap — no re-dial needed.
	if c.preDialedConn != nil {
		conn = c.preDialedConn
	} else {
		// Build dial options
		dialOpts, err := c.buildDialOptions()
		if err != nil {
			return err
		}

		// Create a lazy gRPC connection. grpc.NewClient does not block; the
		// underlying transport is established on the first RPC call. Any
		// connection-level errors will surface when establishStream opens the
		// bidirectional stream below. ConnectTimeout is enforced there via the
		// stream context rather than at dial time.
		conn, err = grpc.NewClient(c.serverAddr, dialOpts...)
		if err != nil {
			return &ConnectionError{
				AetherError: AetherError{
					Message: fmt.Sprintf("failed to connect to %s", c.serverAddr),
					Details: err.Error(),
					cause:   err,
				},
			}
		}
	}

	c.mu.Lock()
	c.conn = conn
	c.client = pb.NewAetherGatewayClient(conn)
	c.mu.Unlock()

	// Establish the bidirectional stream
	if err := c.establishStream(ctx); err != nil {
		// Only close the conn if we dialed it ourselves. For pre-dialed
		// conns the caller owns the lifetime.
		if c.preDialedConn == nil {
			conn.Close()
		}
		c.mu.Lock()
		c.conn = nil
		c.client = nil
		c.mu.Unlock()
		return err
	}

	c.connected.Store(true)
	return nil
}

// establishStream creates the bidirectional gRPC stream and sends the init message.
func (c *BaseClient) establishStream(ctx context.Context) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	// Create a context for the stream
	c.streamCtx, c.streamCancel = context.WithCancel(ctx)

	// Create the stream
	stream, err := c.client.Connect(c.streamCtx)
	if err != nil {
		return FromGRPCError(err)
	}
	c.stream = stream

	// Build and send the init message
	initMsg := c.initMsgBuilder()

	// Include session ID for resume if available
	c.sessionIDMu.RLock()
	if c.sessionID != "" {
		initMsg.ResumeSessionId = c.sessionID
	}
	c.sessionIDMu.RUnlock()

	// Auto-populate client SDK version metadata. Per the InitConnection
	// versioning spec these fields are additive — older gateways simply
	// ignore them, so wiring them unconditionally is safe.
	version, commit, builtAt, runtimeStr, osStr := clientVersionMeta()
	initMsg.ClientVersion = version
	initMsg.ClientSdk = clientSDKName
	initMsg.ClientBuildInfo = &pb.BuildInfo{
		Commit:  commit,
		BuiltAt: builtAt,
		Runtime: runtimeStr,
		Os:      osStr,
	}

	// Send the init message
	upstream := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: initMsg,
		},
	}

	if err := stream.Send(upstream); err != nil {
		return FromGRPCError(err)
	}

	return nil
}

// Close gracefully closes the client connection.
//
// This method blocks until the connection is fully closed or the context
// times out. It's safe to call Close multiple times.
func (c *BaseClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running.Load() {
		return nil
	}

	c.running.Store(false)
	c.forceDisconnect.Store(true)

	// Cancel the stream context
	c.streamMu.Lock()
	if c.streamCancel != nil {
		c.streamCancel()
	}
	c.streamMu.Unlock()

	// Cancel the main context
	if c.cancel != nil {
		c.cancel()
	}

	// Close the gRPC connection. For pre-dialed conns, only close when
	// the caller transferred ownership via OwnsPreDialedConn=true.
	// Otherwise the parent process (which dialed the conn) closes it.
	if c.conn != nil {
		closeConn := c.preDialedConn == nil || c.ownsPreDialedConn
		if closeConn {
			if err := c.conn.Close(); err != nil {
				return &ConnectionClosedError{
					AetherError: AetherError{
						Message: "error closing connection",
						cause:   err,
					},
				}
			}
		}
	}

	c.connected.Store(false)
	c.conn = nil
	c.client = nil
	c.stream = nil

	return nil
}

// =============================================================================
// State Accessors
// =============================================================================

// IsRunning returns true if the client is currently running.
func (c *BaseClient) IsRunning() bool {
	return c.running.Load() || c.reconnecting.Load()
}

// IsConnected returns true if the client has an active connection.
func (c *BaseClient) IsConnected() bool {
	return c.connected.Load() && c.connectionConfirmed.Load()
}

// SessionID returns the current session ID, if any.
func (c *BaseClient) SessionID() string {
	c.sessionIDMu.RLock()
	defer c.sessionIDMu.RUnlock()
	return c.sessionID
}

// setSessionID sets the session ID (called when ConnectionAck is received).
func (c *BaseClient) setSessionID(id string) {
	c.sessionIDMu.Lock()
	defer c.sessionIDMu.Unlock()
	c.sessionID = id
}

// =============================================================================
// Handler Registration
// =============================================================================
//
// All handlers in this section are invoked SYNCHRONOUSLY on the receive loop:
// each downstream frame triggers one handler call before the next frame is
// read. Handlers that make synchronous SDK calls back to the gateway will
// deadlock because the response they're waiting for can only arrive on the
// loop they are holding. Wrap such handlers with aether.Async / aether.AsyncMessageHandler
// / aether.AsyncTaskAssignmentHandler — see async_handler.go and README.md §
// "Handler dispatch model" for the recommended idiom and the planned
// built-in async dispatch follow-up.

// OnMessage registers a handler for incoming messages.
//
// Sync-on-receive-loop semantics apply (see section comment above): if your
// handler routes the message into another SDK call (e.g., CreateTaskSync),
// wrap it with aether.AsyncMessageHandler.
func (c *BaseClient) OnMessage(handler MessageHandler) {
	c.handlers.OnMessage = handler
}

// OnConfig registers a handler for configuration snapshots.
func (c *BaseClient) OnConfig(handler ConfigHandler) {
	c.handlers.OnConfig = handler
}

// OnSignal registers a handler for signals from the gateway.
func (c *BaseClient) OnSignal(handler SignalHandler) {
	c.handlers.OnSignal = handler
}

// OnError registers a handler for error responses.
func (c *BaseClient) OnError(handler ErrorHandler) {
	c.handlers.OnError = handler
}

// OnKVResponse registers a handler for KV operation responses.
func (c *BaseClient) OnKVResponse(handler KVResponseHandler) {
	c.handlers.OnKVResponse = handler
}

// OnCheckpointResponse registers a handler for checkpoint operation responses.
func (c *BaseClient) OnCheckpointResponse(handler CheckpointResponseHandler) {
	c.handlers.OnCheckpointResponse = handler
}

// OnProgress registers a handler for progress updates from agents/tasks.
func (c *BaseClient) OnProgress(handler ProgressHandler) {
	c.handlers.OnProgress = handler
}

// OnTaskAssignment registers a handler for task assignments.
//
// Sync-on-receive-loop semantics apply (see Handler Registration section
// comment): task workers that make nested SDK calls (which is most of them —
// updating progress, completing the task, deriving grants, etc.) should
// wrap with aether.AsyncTaskAssignmentHandler.
func (c *BaseClient) OnTaskAssignment(handler TaskAssignmentHandler) {
	c.handlers.OnTaskAssignment = handler
}

// OnConnect registers a handler for successful connections.
func (c *BaseClient) OnConnect(handler ConnectHandler) {
	c.handlers.OnConnect = handler
}

// OnDisconnect registers a handler for disconnections.
func (c *BaseClient) OnDisconnect(handler DisconnectHandler) {
	c.handlers.OnDisconnect = handler
}

// OnReconnecting registers a handler for reconnection attempts.
func (c *BaseClient) OnReconnecting(handler ReconnectingHandler) {
	c.handlers.OnReconnecting = handler
}

// OnTaskQueryResponse registers a handler for task query responses.
func (c *BaseClient) OnTaskQueryResponse(handler TaskQueryResponseHandler) {
	c.handlers.OnTaskQueryResponse = handler
}

// OnTaskOperationResponse registers a handler for task operation responses.
func (c *BaseClient) OnTaskOperationResponse(handler TaskOperationResponseHandler) {
	c.handlers.OnTaskOperationResponse = handler
}

// OnWorkspaceResponse registers a handler for workspace operation responses.
func (c *BaseClient) OnWorkspaceResponse(handler WorkspaceResponseHandler) {
	c.handlers.OnWorkspaceResponse = handler
}

// OnAgentResponse registers a handler for agent operation responses.
func (c *BaseClient) OnAgentResponse(handler AgentResponseHandler) {
	c.handlers.OnAgentResponse = handler
}

// OnACLResponse registers a handler for ACL operation responses.
func (c *BaseClient) OnACLResponse(handler ACLResponseHandler) {
	c.handlers.OnACLResponse = handler
}

// OnTokenResponse registers a handler for token operation responses.
func (c *BaseClient) OnTokenResponse(handler TokenResponseHandler) {
	c.handlers.OnTokenResponse = handler
}

// OnAuthorityGrantResponse registers a handler for runtime authority-grant
// operation responses. Fires whenever a response arrives that is not consumed
// by a synchronous SendOpSync caller.
func (c *BaseClient) OnAuthorityGrantResponse(handler AuthorityGrantResponseHandler) {
	c.handlers.OnAuthorityGrantResponse = handler
}

// OnAuthorityGrantRevocation registers a handler for server-pushed
// AuthorityGrantRevocation events. Fires after every cache (registered via
// MakeAuthorityCache) has been notified.
func (c *BaseClient) OnAuthorityGrantRevocation(handler AuthorityGrantRevocationHandler) {
	c.handlers.OnAuthorityGrantRevocation = handler
}

// OnChatMessage registers a handler for CHAT type messages.
func (c *BaseClient) OnChatMessage(handler MessageHandler) {
	c.handlers.OnChatMessage = handler
}

// OnControlMessage registers a handler for CONTROL type messages.
func (c *BaseClient) OnControlMessage(handler MessageHandler) {
	c.handlers.OnControlMessage = handler
}

// OnToolCallMessage registers a handler for TOOL_CALL type messages.
func (c *BaseClient) OnToolCallMessage(handler MessageHandler) {
	c.handlers.OnToolCall = handler
}

// OnEventMessage registers a handler for EVENT type messages.
func (c *BaseClient) OnEventMessage(handler MessageHandler) {
	c.handlers.OnEvent = handler
}

// OnMetricMessage registers a handler for METRIC type messages.
func (c *BaseClient) OnMetricMessage(handler MessageHandler) {
	c.handlers.OnMetric = handler
}

// Handlers returns the handler registry for direct access.
func (c *BaseClient) Handlers() *Handlers {
	return c.handlers
}

// OnProxyHttpRequest registers a service-side handler for ProxyHttpRequest
// envelopes routed to the client's service topic. Only used by service
// principals (e.g. proxy-sidecar terminators).
//
// DEADLOCK WARNING: the handler runs synchronously on the receive loop. If
// it makes a synchronous SDK call back to the gateway (CreateTaskSync, KV
// ops with Wait*, ProxyHTTP, derive_authority_grant, etc.) it will hang —
// the response can only arrive on the very loop the handler is holding.
// Wrap with aether.Async for any handler that needs nested SDK calls; see
// README.md § "Handler dispatch model" for the rationale and idiom.
func (c *BaseClient) OnProxyHttpRequest(handler ProxyHttpRequestHandler) {
	c.handlers.OnProxyHttpRequest = handler
}

// OnProxyHttpBodyChunk registers a service-side handler for inbound
// ProxyHttpBodyChunk frames. Set by sidecar terminators that accumulate
// chunked request bodies before dispatching to a backend; when nil the
// dispatch falls back to the default caller-side accumulator used by
// ProxyHTTP responses.
func (c *BaseClient) OnProxyHttpBodyChunk(handler ProxyHttpBodyChunkHandler) {
	c.handlers.OnProxyHttpBodyChunk = handler
}

// OnTunnelDataIn registers a service-side handler for inbound TunnelData
// frames. Used by service principals to override the default caller-side
// dispatch (which assumes the tunnel-dialer state map).
func (c *BaseClient) OnTunnelDataIn(handler TunnelDataInboundHandler) {
	c.handlers.OnTunnelDataIn = handler
}

// OnTunnelAckIn registers a service-side handler for inbound TunnelAck
// frames. Service principals use this to apply credit grants to their
// per-tunnel outbound flow-control state.
func (c *BaseClient) OnTunnelAckIn(handler TunnelAckInboundHandler) {
	c.handlers.OnTunnelAckIn = handler
}

// OnTunnelCloseIn registers a service-side handler for inbound TunnelClose
// frames. Service principals use this to tear down per-tunnel state.
func (c *BaseClient) OnTunnelCloseIn(handler TunnelCloseInboundHandler) {
	c.handlers.OnTunnelCloseIn = handler
}

// =============================================================================
// Message Sending
// =============================================================================

// Send queues an upstream message to be sent to the gateway.
// Returns an error if the client is not connected or the queue is full.
func (c *BaseClient) Send(msg *pb.UpstreamMessage) error {
	if !c.running.Load() {
		return NewConnectionClosedError("client is not running")
	}

	select {
	case c.requestQueue <- msg:
		return nil
	default:
		return NewMessageError("request queue is full")
	}
}

// messageTypeToProto maps a MessageType string constant to the protobuf enum value.
// Defaults to OPAQUE for unknown or empty values.
func messageTypeToProto(mt MessageType) pb.MessageType {
	switch mt {
	case MessageTypeChat:
		return pb.MessageType_CHAT
	case MessageTypeControl:
		return pb.MessageType_CONTROL
	case MessageTypeToolCall:
		return pb.MessageType_TOOL_CALL
	case MessageTypeEvent:
		return pb.MessageType_EVENT
	case MessageTypeMetric:
		return pb.MessageType_METRIC
	default:
		return pb.MessageType_OPAQUE
	}
}

// SendWithOptions sends a message using the provided SendMessageOptions.
// This is the options-based counterpart to the typed Send methods on each client type.
func (c *BaseClient) SendWithOptions(opts SendMessageOptions) error {
	msgType := messageTypeToProto(opts.MessageType)
	return c.sendMessage(opts.TargetTopic, opts.Payload, msgType)
}

// sendMessage sends a message to a target topic.
func (c *BaseClient) sendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error {
	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Send{
			Send: &pb.SendMessage{
				TargetTopic: targetTopic,
				Payload:     payload,
				MessageType: msgType,
			},
		},
	}
	return c.Send(msg)
}

// sendDirect sends an upstream message directly to the stream (bypasses queue).
// Used internally for critical messages that must be sent immediately.
func (c *BaseClient) sendDirect(msg *pb.UpstreamMessage) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	if c.stream == nil {
		return NewConnectionClosedError("stream is not established")
	}

	if err := c.stream.Send(msg); err != nil {
		return FromGRPCError(err)
	}
	return nil
}

// =============================================================================
// Backoff Calculation
// =============================================================================

// calculateBackoff calculates the backoff delay with jitter for a given attempt.
func (c *BaseClient) calculateBackoff(attempt int) time.Duration {
	// Calculate exponential backoff
	delay := c.options.InitialBackoff
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * c.options.BackoffMultiplier)
		if delay > c.options.MaxBackoff {
			delay = c.options.MaxBackoff
			break
		}
	}

	// Add jitter (±25%)
	jitter := float64(delay) * 0.25 * (rand.Float64()*2 - 1)
	return delay + time.Duration(jitter)
}

// =============================================================================
// Client-Level Error Classification
// =============================================================================

// isRecoverableForClient checks if an error is recoverable for this specific client,
// taking into account client-level options like RetryOnDuplicate.
// The global IsRecoverable() stays unchanged as the canonical classification.
func (c *BaseClient) isRecoverableForClient(err error) bool {
	if IsRecoverable(err) {
		return true
	}
	if c.options.RetryOnDuplicate {
		var dupErr *DuplicateIdentityError
		if errors.As(err, &dupErr) {
			return true
		}
	}
	return false
}

// =============================================================================
// Context Access
// =============================================================================

// Context returns the client's context. This context is canceled when the
// client is closed.
func (c *BaseClient) Context() context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ctx
}

// =============================================================================
// TLS File Loading Helpers
// =============================================================================

// LoadTLSConfigFromFiles creates a TLSConfig from file paths.
// This is a convenience function for loading TLS configuration from the filesystem.
func LoadTLSConfigFromFiles(rootCAPath, clientCertPath, clientKeyPath string) (*TLSConfig, error) {
	config := &TLSConfig{Enabled: true}

	// Load root CA if specified
	if rootCAPath != "" {
		data, err := os.ReadFile(rootCAPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read root CA file: %w", err)
		}
		config.RootCAs = data
	}

	// Load client certificate if specified
	if clientCertPath != "" {
		data, err := os.ReadFile(clientCertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read client cert file: %w", err)
		}
		config.ClientCert = data
	}

	// Load client key if specified
	if clientKeyPath != "" {
		data, err := os.ReadFile(clientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read client key file: %w", err)
		}
		config.ClientKey = data
	}

	return config, nil
}

// =============================================================================
// Request Queue Processing
// =============================================================================

// RequestQueue returns the request queue channel for the message loop.
//
// This is primarily used internally by the Run() method's send loop.
// Most users should use Send() instead of accessing the queue directly.
func (c *BaseClient) RequestQueue() <-chan *pb.UpstreamMessage {
	return c.requestQueue
}

// Stream returns the current gRPC stream for the message loop.
//
// This is primarily used internally. Returns nil if no stream is established.
// Most users should use the higher-level Send methods instead.
func (c *BaseClient) Stream() pb.AetherGateway_ConnectClient {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	return c.stream
}

// =============================================================================
// Internal State Accessors
// =============================================================================
//
// The following methods are primarily for internal use by the message loop
// and reconnection logic. They are exported to support advanced use cases
// but most users should not need to call them directly.

// SetConnected sets the connected state.
//
// This is primarily used internally by the connection management logic.
func (c *BaseClient) SetConnected(connected bool) {
	c.connected.Store(connected)
}

// SetConnectionConfirmed sets the connection confirmed state.
//
// Connection is confirmed when the first response is received from the server.
// This is primarily used internally by the message loop.
func (c *BaseClient) SetConnectionConfirmed(confirmed bool) {
	c.connectionConfirmed.Store(confirmed)
}

// SetReconnecting sets the reconnecting state.
//
// This is primarily used internally by the reconnection logic.
func (c *BaseClient) SetReconnecting(reconnecting bool) {
	c.reconnecting.Store(reconnecting)
}

// ForceDisconnect returns whether a force disconnect was requested.
//
// This is true after Close() is called or a FORCE_DISCONNECT signal is received.
func (c *BaseClient) ForceDisconnect() bool {
	return c.forceDisconnect.Load()
}

// SetForceDisconnect sets the force disconnect state.
//
// This is primarily used internally. Setting to true prevents reconnection.
func (c *BaseClient) SetForceDisconnect(force bool) {
	c.forceDisconnect.Store(force)
}

// IncrementReconnectCount increments and returns the reconnect counter.
//
// This is primarily used internally by the reconnection logic to track
// the number of consecutive reconnection attempts.
func (c *BaseClient) IncrementReconnectCount() int32 {
	return c.reconnectCount.Add(1)
}

// ResetReconnectCount resets the reconnect counter to zero.
//
// This is called internally when a connection is successfully confirmed.
func (c *BaseClient) ResetReconnectCount() {
	c.reconnectCount.Store(0)
}

// ReconnectCount returns the current reconnect attempt count.
//
// This can be used to monitor reconnection progress.
func (c *BaseClient) ReconnectCount() int32 {
	return c.reconnectCount.Load()
}

// Options returns the connection options.
//
// This returns a copy of the connection options used by this client.
func (c *BaseClient) Options() ConnectionOptions {
	return c.options
}

// KVResponseQueue returns the KV response queue for synchronous operations.
//
// This is primarily used internally by the KV synchronous methods.
// Most users should use client.KV().GetSync() etc. instead.
func (c *BaseClient) KVResponseQueue() chan *KVResponse {
	return c.kvResponseQueue
}

// CheckpointResponseQueue returns the checkpoint response queue.
//
// This is primarily used internally by the Checkpoint synchronous methods.
// Most users should use client.Checkpoint().LoadSync() etc. instead.
func (c *BaseClient) CheckpointResponseQueue() chan *CheckpointResponse {
	return c.checkpointResponseQueue
}

// NextRequestID generates a monotonically increasing request ID for correlation.
func (c *BaseClient) NextRequestID() string {
	id := c.requestIDCounter.Add(1)
	return fmt.Sprintf("req-%d", id)
}

// RegisterPendingKVRequest registers a pending KV request channel keyed by request ID.
// Returns a channel that will receive the response.
func (c *BaseClient) RegisterPendingKVRequest(requestID string) chan *KVResponse {
	return c.pendingKVRequests.Register(requestID)
}

// ResolvePendingKVRequest resolves a pending KV request by request ID.
// Returns true if the request was found and resolved.
func (c *BaseClient) ResolvePendingKVRequest(requestID string, resp *KVResponse) bool {
	return c.pendingKVRequests.Resolve(requestID, resp)
}

// RegisterPendingCheckpointRequest registers a pending checkpoint request channel keyed by request ID.
func (c *BaseClient) RegisterPendingCheckpointRequest(requestID string) chan *CheckpointResponse {
	return c.pendingCheckpointRequests.Register(requestID)
}

// ResolvePendingCheckpointRequest resolves a pending checkpoint request by request ID.
func (c *BaseClient) ResolvePendingCheckpointRequest(requestID string, resp *CheckpointResponse) bool {
	return c.pendingCheckpointRequests.Resolve(requestID, resp)
}

// RegisterPendingCreateTaskRequest registers a pending create-task request channel keyed by request ID.
func (c *BaseClient) RegisterPendingCreateTaskRequest(requestID string) chan *CreateTaskResponse {
	return c.pendingCreateTaskRequests.Register(requestID)
}

// ResolvePendingCreateTaskRequest resolves a pending create-task request by request ID.
func (c *BaseClient) ResolvePendingCreateTaskRequest(requestID string, resp *CreateTaskResponse) bool {
	return c.pendingCreateTaskRequests.Resolve(requestID, resp)
}

// RegisterPendingWorkflowRequest registers a pending workflow request channel keyed by request ID.
func (c *BaseClient) RegisterPendingWorkflowRequest(requestID string) chan *WorkflowResponse {
	return c.pendingWorkflowRequests.Register(requestID)
}

// ResolvePendingWorkflowRequest resolves a pending workflow request by request ID.
func (c *BaseClient) ResolvePendingWorkflowRequest(requestID string, resp *WorkflowResponse) bool {
	return c.pendingWorkflowRequests.Resolve(requestID, resp)
}

// RegisterPendingTaskQueryRequest registers a pending task query request channel keyed by request ID.
func (c *BaseClient) RegisterPendingTaskQueryRequest(requestID string) chan *TaskQueryResponse {
	return c.pendingTaskQueryRequests.Register(requestID)
}

// RegisterPendingTaskOpRequest registers a pending task operation request channel keyed by request ID.
func (c *BaseClient) RegisterPendingTaskOpRequest(requestID string) chan *TaskOperationResponse {
	return c.pendingTaskOpRequests.Register(requestID)
}

// RegisterPendingWorkspaceRequest registers a pending workspace operation request channel keyed by request ID.
func (c *BaseClient) RegisterPendingWorkspaceRequest(requestID string) chan *WorkspaceResponse {
	return c.pendingWorkspaceRequests.Register(requestID)
}

// ResolvePendingWorkspaceRequest resolves a pending workspace request by request ID.
func (c *BaseClient) ResolvePendingWorkspaceRequest(requestID string, resp *WorkspaceResponse) bool {
	return c.pendingWorkspaceRequests.Resolve(requestID, resp)
}

// RegisterPendingAgentRequest registers a pending agent operation request channel keyed by request ID.
func (c *BaseClient) RegisterPendingAgentRequest(requestID string) chan *AgentResponse {
	return c.pendingAgentRequests.Register(requestID)
}

// ResolvePendingAgentRequest resolves a pending agent request by request ID.
func (c *BaseClient) ResolvePendingAgentRequest(requestID string, resp *AgentResponse) bool {
	return c.pendingAgentRequests.Resolve(requestID, resp)
}

// RegisterPendingACLRequest registers a pending ACL operation request channel keyed by request ID.
func (c *BaseClient) RegisterPendingACLRequest(requestID string) chan *ACLResponse {
	return c.pendingACLRequests.Register(requestID)
}

// ResolvePendingACLRequest resolves a pending ACL request by request ID.
func (c *BaseClient) ResolvePendingACLRequest(requestID string, resp *ACLResponse) bool {
	return c.pendingACLRequests.Resolve(requestID, resp)
}

// RegisterPendingTokenRequest registers a pending token operation request channel keyed by request ID.
func (c *BaseClient) RegisterPendingTokenRequest(requestID string) chan *TokenResponse {
	return c.pendingTokenRequests.Register(requestID)
}

// ResolvePendingTokenRequest resolves a pending token request by request ID.
func (c *BaseClient) ResolvePendingTokenRequest(requestID string, resp *TokenResponse) bool {
	return c.pendingTokenRequests.Resolve(requestID, resp)
}

// RegisterPendingAuthorityGrantRequest registers a pending authority-grant
// operation request channel keyed by request ID.
func (c *BaseClient) RegisterPendingAuthorityGrantRequest(requestID string) chan *pb.AuthorityGrantResponse {
	return c.pendingAuthorityGrantRequests.Register(requestID)
}

// ResolvePendingAuthorityGrantRequest resolves a pending authority-grant
// request by request ID.
func (c *BaseClient) ResolvePendingAuthorityGrantRequest(requestID string, resp *pb.AuthorityGrantResponse) bool {
	return c.pendingAuthorityGrantRequests.Resolve(requestID, resp)
}

// RegisterPendingAuditSubmitRequest registers a pending foreign audit-event
// submission request channel keyed by client_request_id.
func (c *BaseClient) RegisterPendingAuditSubmitRequest(clientRequestID string) chan *pb.SubmitAuditEventResponse {
	return c.pendingAuditSubmitRequests.Register(clientRequestID)
}

// ResolvePendingAuditSubmitRequest resolves a pending audit-submit request by
// client_request_id.
func (c *BaseClient) ResolvePendingAuditSubmitRequest(clientRequestID string, resp *pb.SubmitAuditEventResponse) bool {
	return c.pendingAuditSubmitRequests.Resolve(clientRequestID, resp)
}

// ClearRequestQueue drains the request queue.
//
// This is called internally during reconnection to discard pending messages.
// Use with caution as this may result in message loss.
func (c *BaseClient) ClearRequestQueue() {
	for {
		select {
		case <-c.requestQueue:
			// Drain the queue
		default:
			return
		}
	}
}

// CloseStream closes the current stream and cancels its context.
//
// This is primarily used internally during reconnection or shutdown.
func (c *BaseClient) CloseStream() {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	if c.streamCancel != nil {
		c.streamCancel()
		c.streamCancel = nil
	}
	c.stream = nil
}

// CloseConnection closes the gRPC connection.
//
// This is primarily used internally during reconnection or shutdown.
// Most users should use Close() instead, which performs a graceful shutdown.
func (c *BaseClient) CloseConnection() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.client = nil
		return err
	}
	return nil
}

// Reconnect attempts to re-establish the connection.
//
// This is primarily used internally by the reconnection logic.
// Most users should rely on automatic reconnection (enabled by default)
// rather than calling this method directly.
func (c *BaseClient) Reconnect(ctx context.Context) error {
	return c.doConnect(ctx)
}

// ServerAddr returns the server address (host:port) that this client connects to.
func (c *BaseClient) ServerAddr() string {
	return c.serverAddr
}

// =============================================================================
// Exponential Backoff Reconnection
// =============================================================================

// attemptReconnect attempts to reconnect with exponential backoff.
//
// This method runs a reconnection loop until either:
// - A connection is successfully re-established
// - The maximum number of retries (if set) is exceeded
// - A force disconnect is requested
// - The context is canceled
//
// When max retries is exceeded, the client stops attempting to reconnect
// and returns a ReconnectionError.
func (c *BaseClient) attemptReconnect(ctx context.Context) error {
	// Prevent nested reconnect calls - if already reconnecting, just return
	if c.reconnecting.Load() {
		return nil
	}

	c.reconnecting.Store(true)
	defer c.reconnecting.Store(false)

	// Get the current attempt count (it persists across reconnect calls)
	// It gets reset only when we successfully receive a response from server

	for {
		// Check if we should stop trying
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if c.forceDisconnect.Load() {
			return NewConnectionClosedError("force disconnect requested")
		}

		// Check if we've exceeded max retries (0 = infinite)
		attempt := int(c.reconnectCount.Load())
		if c.options.MaxRetries > 0 && attempt >= c.options.MaxRetries {
			c.running.Store(false)
			return NewReconnectionError(attempt)
		}

		// Calculate backoff delay
		backoff := c.calculateBackoff(attempt)

		// Notify reconnecting handler
		if c.handlers.OnReconnecting != nil {
			if err := c.handlers.OnReconnecting(ctx, attempt+1); err != nil {
				// Handler requested abort
				c.running.Store(false)
				return err
			}
		}

		// Increment attempt counter BEFORE trying
		c.reconnectCount.Add(1)

		// Wait for backoff period (interruptible)
		if !c.sleepWithContext(ctx, backoff) {
			// Context canceled or force disconnect during sleep
			if c.forceDisconnect.Load() {
				return NewConnectionClosedError("force disconnect requested")
			}
			return ctx.Err()
		}

		// Clean up old connection before reconnecting
		c.cleanupForReconnect()

		// Reset state flags before reconnection attempt
		c.connectionConfirmed.Store(false)

		// Attempt reconnection
		err := c.doConnect(ctx)
		if err == nil {
			// Successfully connected - the message loop will confirm when server responds
			return nil
		}

		// Check if error is non-recoverable (including client-level overrides)
		if !c.isRecoverableForClient(err) {
			c.running.Store(false)
			return err
		}

		// Recoverable error - continue retry loop
		// Attempt already incremented above
	}
}

// sleepWithContext sleeps for the given duration but returns early if:
// - The context is canceled
// - A force disconnect is requested
//
// Returns true if the full sleep duration completed, false otherwise.
func (c *BaseClient) sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	// Check periodically (every 100ms) for force disconnect
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-ticker.C:
			if c.forceDisconnect.Load() {
				return false
			}
		}
	}
}

// cleanupForReconnect cleans up the current connection state in preparation
// for a reconnection attempt.
func (c *BaseClient) cleanupForReconnect() {
	// Cancel the stream context first
	c.streamMu.Lock()
	if c.streamCancel != nil {
		c.streamCancel()
		c.streamCancel = nil
	}
	c.stream = nil
	c.streamMu.Unlock()

	// Close the gRPC connection. For pre-dialed conns we keep the
	// underlying ClientConn alive across reconnect — the in-process
	// listener doesn't need rebuilding, and a fresh stream is reopened
	// in doConnect. Avoids tearing down a perfectly good bufconn.
	c.mu.Lock()
	if c.conn != nil {
		if c.preDialedConn == nil {
			c.conn.Close()
			c.conn = nil
			c.client = nil
		}
	}
	c.mu.Unlock()

	// Clear the request queue
	c.ClearRequestQueue()

	// Mark as disconnected
	c.connected.Store(false)
}

// StartReconnect triggers a reconnection attempt.
//
// This is typically called by the message loop when a recoverable error is detected.
// The method is non-blocking and starts reconnection in the background.
//
// Returns a channel that will receive the result of the reconnection attempt.
func (c *BaseClient) StartReconnect(ctx context.Context) <-chan error {
	result := make(chan error, 1)

	go func() {
		defer close(result)
		err := c.attemptReconnect(ctx)
		result <- err
	}()

	return result
}

// AttemptReconnect performs synchronous reconnection.
//
// This is the synchronous version of StartReconnect. It blocks until
// the reconnection completes or fails.
func (c *BaseClient) AttemptReconnect(ctx context.Context) error {
	return c.attemptReconnect(ctx)
}

// ReconnectionEnabled returns true if automatic reconnection is enabled.
func (c *BaseClient) ReconnectionEnabled() bool {
	return c.options.AutoReconnect
}

// ConnectionConfirmed returns whether the connection has been confirmed by the server.
func (c *BaseClient) ConnectionConfirmed() bool {
	return c.connectionConfirmed.Load()
}

// ConfirmConnection is called when a successful response is received from the server.
// This resets the reconnection counter and marks the connection as confirmed.
func (c *BaseClient) ConfirmConnection() {
	c.connectionConfirmed.Store(true)
	c.reconnectCount.Store(0)
}

// Reconnecting returns true if the client is currently attempting to reconnect.
func (c *BaseClient) Reconnecting() bool {
	return c.reconnecting.Load()
}

// =============================================================================
// Bidirectional Streaming Message Loop
// =============================================================================

// Run starts the bidirectional message loop.
//
// This method starts two goroutines:
//   - sendLoop: reads messages from the request queue and sends them to the server
//   - receiveLoop: receives messages from the server and dispatches to handlers
//
// Run blocks until the connection is closed or the context is canceled.
// If auto-reconnect is enabled, it will attempt to reconnect on recoverable errors.
//
// The returned error indicates why the loop exited. A nil error means the
// connection was closed gracefully.
func (c *BaseClient) Run(ctx context.Context) error {
	if !c.running.Load() {
		return NewConnectionClosedError("client is not running")
	}

	// Create a done channel to signal when loops exit
	done := make(chan struct{})
	var loopErr error
	var errMu sync.Mutex

	// Start the send loop
	go func() {
		c.sendLoop(ctx)
	}()

	// Start the receive loop (this is the main loop that handles messages)
	go func() {
		defer close(done)
		errMu.Lock()
		loopErr = c.receiveLoop(ctx)
		errMu.Unlock()
	}()

	// Wait for the receive loop to finish
	<-done

	errMu.Lock()
	defer errMu.Unlock()
	return loopErr
}

// sendLoop continuously reads from the request queue and sends to the stream.
//
// The loop exits when:
//   - The context is canceled
//   - The client is no longer running
//   - The stream is closed
func (c *BaseClient) sendLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if !c.running.Load() {
				return
			}
		}

		// Try to get a message from the queue with a timeout
		// This allows us to periodically check the context and running state
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.requestQueue:
			if !ok {
				return // Channel closed
			}

			// Send the message to the stream
			if err := c.sendDirect(msg); err != nil {
				// Log error but don't exit - let receive loop handle disconnection
				if c.handlers.OnError != nil {
					errInfo := &ErrorInfo{
						Code:    "SEND_ERROR",
						Message: err.Error(),
					}
					_ = c.handlers.OnError(ctx, errInfo)
				}
			}
		}
	}
}

// receiveLoop continuously receives messages from the stream and dispatches to handlers.
//
// The loop handles:
//   - IncomingMessage: routed to OnMessage handler
//   - ConfigSnapshot: routed to OnConfig handler
//   - Signal: handles FORCE_DISCONNECT, routed to OnSignal handler
//   - ErrorResponse: routed to OnError handler
//   - KVResponse: routed to KV response queue and OnKVResponse handler
//   - TaskAssignment: routed to OnTaskAssignment handler
//   - ConnectionAck: stores session ID, routed to OnConnect handler
//   - CheckpointResponse: routed to checkpoint queue and OnCheckpointResponse handler
//
// On recoverable errors (if auto-reconnect is enabled), the loop attempts to reconnect.
// On non-recoverable errors or context cancellation, the loop exits.
func (c *BaseClient) receiveLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			c.handleDisconnect(ctx, "context canceled")
			return ctx.Err()
		default:
			if !c.running.Load() && !c.reconnecting.Load() {
				c.handleDisconnect(ctx, "client stopped")
				return nil
			}
		}

		// Get the current stream
		stream := c.Stream()
		if stream == nil {
			if c.forceDisconnect.Load() {
				return nil
			}

			// No stream available - attempt reconnect if enabled
			if c.options.AutoReconnect && !c.forceDisconnect.Load() {
				if err := c.attemptReconnect(ctx); err != nil {
					return err
				}
				continue
			}
			return NewConnectionClosedError("stream is not established")
		}

		// Receive the next message from the stream
		response, err := stream.Recv()
		if err != nil {
			return c.handleReceiveError(ctx, err)
		}

		// Dispatch the response to the appropriate handler
		if err := c.dispatchResponse(ctx, response); err != nil {
			// FORCE_DISCONNECT signal sets forceDisconnect — exit cleanly.
			if c.forceDisconnect.Load() {
				return nil
			}
			// For recoverable dispatch errors (e.g., graceful disconnect),
			// use the same reconnection path as receive errors.
			return c.handleReceiveError(ctx, err)
		}
	}
}

// handleReceiveError handles errors from stream.Recv().
//
// For recoverable errors (if auto-reconnect is enabled), it attempts to reconnect.
// For non-recoverable errors, it returns the error to exit the loop.
func (c *BaseClient) handleReceiveError(ctx context.Context, err error) error {
	// Convert to AetherError
	aetherErr := FromGRPCError(err)

	// Don't log or handle if we're intentionally disconnecting
	if c.forceDisconnect.Load() {
		return nil
	}

	// Check if error is recoverable (including client-level overrides like RetryOnDuplicate)
	if c.isRecoverableForClient(aetherErr) && c.options.AutoReconnect {
		// Notify disconnect handler before reconnecting
		c.handleDisconnect(ctx, "connection lost")

		// Mark as disconnected
		c.connected.Store(false)
		c.connectionConfirmed.Store(false)

		// Attempt reconnection
		if err := c.attemptReconnect(ctx); err != nil {
			return err
		}

		// Successfully reconnected - continue receive loop
		return nil
	}

	// Non-recoverable error
	c.running.Store(false)
	c.handleDisconnect(ctx, aetherErr.Error())
	return aetherErr
}

// handleDisconnect calls the disconnect handler if registered.
func (c *BaseClient) handleDisconnect(ctx context.Context, reason string) {
	if c.handlers.OnDisconnect != nil {
		_ = c.handlers.OnDisconnect(ctx, reason)
	}
}

// dispatchResponse routes a downstream message to the appropriate handler.
//
// Returns an error if the response indicates the connection should be terminated
// (e.g., FORCE_DISCONNECT signal).
func (c *BaseClient) dispatchResponse(ctx context.Context, response *pb.DownstreamMessage) error {
	// Connection confirmed when we receive first response from server
	if !c.connectionConfirmed.Load() {
		c.ConfirmConnection()
	}

	// Dispatch based on payload type
	switch payload := response.GetPayload().(type) {
	case *pb.DownstreamMessage_Msg:
		return c.handleIncomingMessage(ctx, payload.Msg)

	case *pb.DownstreamMessage_Config:
		return c.handleConfigSnapshot(ctx, payload.Config)

	case *pb.DownstreamMessage_Signal:
		return c.handleSignal(ctx, payload.Signal)

	case *pb.DownstreamMessage_Error:
		return c.handleErrorResponse(ctx, payload.Error)

	case *pb.DownstreamMessage_Kv:
		return c.handleKVResponse(ctx, payload.Kv)

	case *pb.DownstreamMessage_TaskAssignment:
		return c.handleTaskAssignment(ctx, payload.TaskAssignment)

	case *pb.DownstreamMessage_ConnectionAck:
		return c.handleConnectionAck(ctx, payload.ConnectionAck)

	case *pb.DownstreamMessage_Checkpoint:
		return c.handleCheckpointResponse(ctx, payload.Checkpoint)

	case *pb.DownstreamMessage_ProgressUpdate:
		return c.handleProgressUpdate(ctx, payload.ProgressUpdate)

	case *pb.DownstreamMessage_TaskQuery:
		return c.handleTaskQueryResponse(ctx, payload.TaskQuery)

	case *pb.DownstreamMessage_TaskOp:
		return c.handleTaskOperationResponse(ctx, payload.TaskOp)

	case *pb.DownstreamMessage_WorkflowResponse:
		return c.handleWorkflowResponse(ctx, payload.WorkflowResponse)

	case *pb.DownstreamMessage_WorkflowOp:
		return c.handleWorkflowOperation(ctx, payload.WorkflowOp)

	case *pb.DownstreamMessage_Workspace:
		return c.handleWorkspaceResponse(ctx, payload.Workspace)

	case *pb.DownstreamMessage_Agent:
		return c.handleAgentResponse(ctx, payload.Agent)

	case *pb.DownstreamMessage_Acl:
		return c.handleACLResponse(ctx, payload.Acl)

	case *pb.DownstreamMessage_Token:
		return c.handleTokenResponse(ctx, payload.Token)

	case *pb.DownstreamMessage_Admin:
		return c.handleAdminResponse(ctx, payload.Admin)

	case *pb.DownstreamMessage_SessionResponse:
		return c.handleSessionResponse(ctx, payload.SessionResponse)

	case *pb.DownstreamMessage_AuthorityGrant:
		return c.handleAuthorityGrantResponse(ctx, payload.AuthorityGrant)

	case *pb.DownstreamMessage_AuthorityGrantRevocation:
		return c.handleAuthorityGrantRevocation(ctx, payload.AuthorityGrantRevocation)

	case *pb.DownstreamMessage_SubmitAuditEventResponse:
		return c.handleSubmitAuditEventResponse(ctx, payload.SubmitAuditEventResponse)

	case *pb.DownstreamMessage_CreateTask:
		return c.handleCreateTaskResponse(ctx, payload.CreateTask)

	case *pb.DownstreamMessage_ProxyHttpResponse:
		c.handleProxyHttpResponse(payload.ProxyHttpResponse)
		return nil

	case *pb.DownstreamMessage_ProxyHttpBodyChunk:
		// Service-side principals (e.g. proxy-sidecar terminators) register
		// OnProxyHttpBodyChunk to accumulate chunked request bodies before
		// dispatching to a backend. Caller-side principals leave it nil and
		// rely on the default response-chunk accumulator handleProxyHttpBodyChunk.
		if c.handlers.OnProxyHttpBodyChunk != nil {
			return c.handlers.OnProxyHttpBodyChunk(ctx, payload.ProxyHttpBodyChunk)
		}
		c.handleProxyHttpBodyChunk(payload.ProxyHttpBodyChunk)
		return nil

	case *pb.DownstreamMessage_ProxyHttpRequest:
		// Service-side delivery: gateway forwards a ProxyHttpRequest
		// envelope to the sv:: topic. Only fire when a handler is set —
		// caller-side principals don't expect to receive these.
		if c.handlers.OnProxyHttpRequest != nil {
			return c.handlers.OnProxyHttpRequest(ctx, payload.ProxyHttpRequest)
		}
		return nil

	case *pb.DownstreamMessage_TunnelData:
		// Service-side hook takes precedence when registered (e.g.
		// proxy-sidecar terminators). Default caller-side dispatch handles
		// the tunnel-dialer state map.
		if c.handlers.OnTunnelDataIn != nil {
			return c.handlers.OnTunnelDataIn(ctx, payload.TunnelData)
		}
		c.handleTunnelData(payload.TunnelData)
		return nil

	case *pb.DownstreamMessage_TunnelAck:
		if c.handlers.OnTunnelAckIn != nil {
			return c.handlers.OnTunnelAckIn(ctx, payload.TunnelAck)
		}
		c.handleTunnelAck(payload.TunnelAck)
		return nil

	case *pb.DownstreamMessage_TunnelClose:
		if c.handlers.OnTunnelCloseIn != nil {
			return c.handlers.OnTunnelCloseIn(ctx, payload.TunnelClose)
		}
		c.handleTunnelClose(payload.TunnelClose)
		return nil

	default:
		// Unknown payload type - ignore
		return nil
	}
}

// handleIncomingMessage processes an incoming message from the server.
func (c *BaseClient) handleIncomingMessage(ctx context.Context, msg *pb.IncomingMessage) error {
	// Convert to high-level Message type
	message := &Message{
		SourceTopic: msg.GetSourceTopic(),
		Payload:     msg.GetPayload(),
		MessageType: msg.GetMessageType(),
		ReceivedAt:  time.Now(),
	}

	// Dispatch to generic message handler
	if c.handlers.OnMessage != nil {
		if err := c.handlers.OnMessage(ctx, message); err != nil {
			return err
		}
	}

	// Dispatch to typed message handler if registered
	var typedHandler MessageHandler
	switch msg.GetMessageType() {
	case pb.MessageType_CHAT:
		typedHandler = c.handlers.OnChatMessage
	case pb.MessageType_CONTROL:
		typedHandler = c.handlers.OnControlMessage
	case pb.MessageType_TOOL_CALL:
		typedHandler = c.handlers.OnToolCall
	case pb.MessageType_EVENT:
		typedHandler = c.handlers.OnEvent
	case pb.MessageType_METRIC:
		typedHandler = c.handlers.OnMetric
	case pb.MessageType_OPAQUE:
		// No typed handler — falls through to OnMessage catch-all above
	}
	if typedHandler != nil {
		if err := typedHandler(ctx, message); err != nil {
			return err
		}
	}

	return nil
}

// handleConfigSnapshot processes a configuration snapshot from the server.
func (c *BaseClient) handleConfigSnapshot(ctx context.Context, config *pb.ConfigSnapshot) error {
	if c.handlers.OnConfig == nil {
		return nil
	}

	// Convert to high-level ConfigSnapshot type
	snapshot := &ConfigSnapshot{
		KV:       config.GetKv(),
		GlobalKV: config.GetGlobalKv(),
	}

	return c.handlers.OnConfig(ctx, snapshot)
}

// handleSignal processes a signal from the server.
//
// FORCE_DISCONNECT signals cause the connection to be closed immediately (terminal).
// GRACEFUL_DISCONNECT signals cause the connection to close and auto-reconnect.
func (c *BaseClient) handleSignal(ctx context.Context, signal *pb.Signal) error {
	// GRACEFUL_DISCONNECT: server is shutting down or cycling connections.
	// Disconnect cleanly and allow auto-reconnect to re-establish.
	if signal.GetType() == pb.Signal_GRACEFUL_DISCONNECT {
		c.connected.Store(false)
		c.connectionConfirmed.Store(false)

		// Notify signal handler before triggering reconnect
		if c.handlers.OnSignal != nil {
			sig := &Signal{
				Type:   SignalGracefulDisconnect,
				Reason: signal.GetReason(),
			}
			_ = c.handlers.OnSignal(ctx, sig)
		}

		// Don't set forceDisconnect — allow auto-reconnect.
		// Return a recoverable error so receiveLoop triggers reconnection.
		reason := signal.GetReason()
		if reason == "" {
			reason = "graceful disconnect"
		}
		return NewConnectionError("graceful disconnect: " + reason)
	}

	// Check for FORCE_DISCONNECT signal — terminal, no reconnect
	if signal.GetType() == pb.Signal_FORCE_DISCONNECT {
		c.forceDisconnect.Store(true)
		c.running.Store(false)

		// Call disconnect handler with the reason
		reason := signal.GetReason()
		if reason == "" {
			reason = "force disconnect"
		}
		c.handleDisconnect(ctx, reason)

		// Return nil to indicate graceful exit (not an error)
		return nil
	}

	// Route to signal handler
	if c.handlers.OnSignal == nil {
		return nil
	}

	// Convert to high-level Signal type
	sig := &Signal{
		Type:   convertSignalType(signal.GetType()),
		Reason: signal.GetReason(),
	}

	return c.handlers.OnSignal(ctx, sig)
}

// convertSignalType converts a protobuf signal type to the high-level SignalType.
func convertSignalType(pbType pb.Signal_SignalType) SignalType {
	switch pbType {
	case pb.Signal_FORCE_DISCONNECT:
		return SignalForceDisconnect
	case pb.Signal_GRACEFUL_DISCONNECT:
		return SignalGracefulDisconnect
	default:
		return SignalType(-1) // Unknown
	}
}

// handleErrorResponse processes an error response from the server.
func (c *BaseClient) handleErrorResponse(ctx context.Context, errResp *pb.ErrorResponse) error {
	if c.handlers.OnError == nil {
		return nil
	}

	// Convert to high-level ErrorInfo type
	errInfo := &ErrorInfo{
		Code:         errResp.GetCode(),
		Message:      errResp.GetMessage(),
		Retryable:    errResp.GetRetryable(),
		RetryAfterMs: errResp.GetRetryAfterMs(),
	}

	return c.handlers.OnError(ctx, errInfo)
}

// handleKVResponse processes a KV operation response from the server.
func (c *BaseClient) handleKVResponse(ctx context.Context, kv *pb.KVResponse) error {
	// Convert to high-level KVResponse type
	kvResp := &KVResponse{
		Success:      kv.GetSuccess(),
		Value:        kv.GetValue(),
		Keys:         kv.GetKeys(),
		KVMap:        kv.GetKvMap(),
		RequestId:    kv.GetRequestId(),
		CounterValue: kv.GetCounterValue(),
		Applied:      kv.GetApplied(),
	}

	// If response has a request_id, try to resolve a pending correlated request first
	if reqID := kv.GetRequestId(); reqID != "" {
		if c.ResolvePendingKVRequest(reqID, kvResp) {
			// Also route to async handler if registered
			if c.handlers.OnKVResponse != nil {
				return c.handlers.OnKVResponse(ctx, kvResp)
			}
			return nil
		}
	}

	// Fallback: put in legacy queue for synchronous operations (non-blocking)
	select {
	case c.kvResponseQueue <- kvResp:
	default:
		// Queue full - response will be handled by async handler only
	}

	// Route to async handler
	if c.handlers.OnKVResponse != nil {
		return c.handlers.OnKVResponse(ctx, kvResp)
	}

	return nil
}

// handleTaskAssignment processes a task assignment from the server.
func (c *BaseClient) handleTaskAssignment(ctx context.Context, ta *pb.TaskAssignment) error {
	if c.handlers.OnTaskAssignment == nil {
		return nil
	}

	// Convert to high-level TaskAssignment type
	assignment := &TaskAssignment{
		TaskID:               ta.GetTaskId(),
		TaskType:             ta.GetTaskType(),
		AssignedTo:           ta.GetAssignedTo(),
		Metadata:             ta.GetMetadata(),
		Profile:              ta.GetProfile(),
		LaunchParams:         ta.GetLaunchParams(),
		TargetImplementation: ta.GetTargetImplementation(),
		Workspace:            ta.GetWorkspace(),
		Specifier:            ta.GetSpecifier(),
		Payload:              ta.GetPayload(),
	}

	// Convert Unix timestamp if present
	if assignedAt := ta.GetAssignedAt(); assignedAt > 0 {
		assignment.AssignedAt = time.Unix(assignedAt, 0)
	} else {
		assignment.AssignedAt = time.Now()
	}

	return c.handlers.OnTaskAssignment(ctx, assignment)
}

// handleConnectionAck processes the connection acknowledgment from the server.
func (c *BaseClient) handleConnectionAck(ctx context.Context, ack *pb.ConnectionAck) error {
	// Store the session ID for reconnection
	c.setSessionID(ack.GetSessionId())

	// Call connect handler
	if c.handlers.OnConnect != nil {
		connAck := &ConnectionAck{
			SessionID: ack.GetSessionId(),
			Resumed:   ack.GetResumed(),
		}
		return c.handlers.OnConnect(ctx, connAck)
	}

	return nil
}

// handleCheckpointResponse processes a checkpoint operation response from the server.
func (c *BaseClient) handleCheckpointResponse(ctx context.Context, cp *pb.CheckpointResponse) error {
	// Convert to high-level CheckpointResponse type
	cpResp := &CheckpointResponse{
		Success:   cp.GetSuccess(),
		Data:      cp.GetData(),
		Keys:      cp.GetKeys(),
		Error:     cp.GetError(),
		RequestId: cp.GetRequestId(),
	}

	// Convert Unix timestamp if present
	if savedAt := cp.GetSavedAt(); savedAt > 0 {
		cpResp.SavedAt = time.Unix(savedAt, 0)
	}

	// If response has a request_id, try to resolve a pending correlated request first
	if reqID := cp.GetRequestId(); reqID != "" {
		if c.ResolvePendingCheckpointRequest(reqID, cpResp) {
			// Also route to async handler if registered
			if c.handlers.OnCheckpointResponse != nil {
				return c.handlers.OnCheckpointResponse(ctx, cpResp)
			}
			return nil
		}
	}

	// Fallback: put in legacy queue for synchronous operations (non-blocking)
	select {
	case c.checkpointResponseQueue <- cpResp:
	default:
		// Queue full - response will be handled by async handler only
	}

	// Route to async handler
	if c.handlers.OnCheckpointResponse != nil {
		return c.handlers.OnCheckpointResponse(ctx, cpResp)
	}

	return nil
}

// handleProgressUpdate processes a progress update from the server.
func (c *BaseClient) handleProgressUpdate(ctx context.Context, pu *pb.ProgressUpdate) error {
	if c.handlers.OnProgress == nil {
		return nil
	}

	update := &ProgressUpdate{
		Source:      pu.GetSource(),
		TaskID:      pu.GetTaskId(),
		State:       pu.GetState(),
		Completion:  pu.GetCompletion(),
		Summary:     pu.GetSummary(),
		TimestampMs: pu.GetTimestampMs(),
		Workspace:   pu.GetWorkspace(),
		RequestID:   pu.GetRequestId(),
		Metadata:    pu.GetMetadata(),
		Recipient:   pu.GetRecipient(),
	}

	if step := pu.GetStep(); step != nil {
		update.Step = &ProgressStep{
			Name:       step.GetName(),
			Detail:     step.GetDetail(),
			Sequence:   step.GetSequence(),
			TotalSteps: step.GetTotalSteps(),
			StepType:   step.GetStepType(),
		}
	}

	return c.handlers.OnProgress(ctx, update)
}

// handleTaskQueryResponse processes a task query response from the server.
func (c *BaseClient) handleTaskQueryResponse(ctx context.Context, resp *pb.TaskQueryResponse) error {
	tqr := &TaskQueryResponse{
		Success:    resp.GetSuccess(),
		Error:      resp.GetError(),
		TotalCount: resp.GetTotalCount(),
	}
	if t := resp.GetTask(); t != nil {
		tqr.Task = protoTaskInfoToSDK(t)
	}
	for _, t := range resp.GetTasks() {
		tqr.Tasks = append(tqr.Tasks, protoTaskInfoToSDK(t))
	}

	// If response has a request_id, do a targeted lookup first.
	// Fall back to resolving the first pending request for backward compat with old servers.
	if reqID := resp.GetRequestId(); reqID != "" {
		c.pendingTaskQueryRequests.Resolve(reqID, tqr)
	} else {
		c.pendingTaskQueryRequests.ResolveFirst(tqr)
	}

	if c.handlers.OnTaskQueryResponse != nil {
		return c.handlers.OnTaskQueryResponse(ctx, tqr)
	}
	return nil
}

// handleTaskOperationResponse processes a task operation response from the server.
func (c *BaseClient) handleTaskOperationResponse(ctx context.Context, resp *pb.TaskOperationResponse) error {
	tor := &TaskOperationResponse{
		Success: resp.GetSuccess(),
		Message: resp.GetMessage(),
		Error:   resp.GetError(),
	}
	if t := resp.GetTask(); t != nil {
		tor.Task = protoTaskInfoToSDK(t)
	}

	// If response has a request_id, do a targeted lookup first.
	// Fall back to resolving the first pending request for backward compat with old servers.
	if reqID := resp.GetRequestId(); reqID != "" {
		c.pendingTaskOpRequests.Resolve(reqID, tor)
	} else {
		c.pendingTaskOpRequests.ResolveFirst(tor)
	}

	if c.handlers.OnTaskOperationResponse != nil {
		return c.handlers.OnTaskOperationResponse(ctx, tor)
	}
	return nil
}

// handleCreateTaskResponse processes a CreateTaskResponse from the server.
func (c *BaseClient) handleCreateTaskResponse(ctx context.Context, resp *pb.CreateTaskResponse) error {
	ctr := &CreateTaskResponse{
		Success:      resp.GetSuccess(),
		TaskID:       resp.GetTaskId(),
		Status:       resp.GetStatus(),
		ErrorCode:    resp.GetErrorCode(),
		ErrorMessage: resp.GetErrorMessage(),
		RequestId:    resp.GetRequestId(),
		AssignedTo:   resp.GetAssignedTo(),
		TaskToken:    resp.GetTaskToken(),
	}

	// Route to correlated pending request if available.
	if reqID := resp.GetRequestId(); reqID != "" {
		c.pendingCreateTaskRequests.Resolve(reqID, ctr)
	}

	if c.handlers.OnCreateTaskResponse != nil {
		return c.handlers.OnCreateTaskResponse(ctx, ctr)
	}
	return nil
}

// CreateTask sends a fire-and-forget CreateTaskRequest with no request_id.
// The server will not send a response; use CreateTaskSync when you need the task_id.
func (c *BaseClient) CreateTask(taskType, workspace string, opts CreateTaskOptions) error {
	req := &pb.CreateTaskRequest{
		TaskType:             taskType,
		Workspace:            workspace,
		AssignmentMode:       pb.TaskAssignmentMode(pb.TaskAssignmentMode_value[string(opts.AssignmentMode)]),
		TargetAgentId:        opts.TargetAgentID,
		TargetIdentity:       opts.TargetIdentity,
		TargetImplementation: opts.TargetImplementation,
		LaunchParamOverrides: opts.LaunchParamOverrides,
		Metadata:             opts.Metadata,
		Payload:              opts.Payload,
	}
	return c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CreateTask{CreateTask: req},
	})
}

// CreateTaskSync sends a CreateTaskRequest with a correlated request_id and waits
// for the server's CreateTaskResponse. Returns the response or an error on timeout.
//
// Use this variant when you need the server-assigned task_id (e.g., for SELF_ASSIGN
// tasks that the caller will later COMPLETE/FAIL via TaskOperation).
func (c *BaseClient) CreateTaskSync(ctx context.Context, taskType, workspace string, opts CreateTaskOptions, timeout time.Duration) (*CreateTaskResponse, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	requestID := c.NextRequestID()
	ch := c.RegisterPendingCreateTaskRequest(requestID)
	defer c.pendingCreateTaskRequests.Delete(requestID)

	req := &pb.CreateTaskRequest{
		TaskType:             taskType,
		Workspace:            workspace,
		AssignmentMode:       pb.TaskAssignmentMode(pb.TaskAssignmentMode_value[string(opts.AssignmentMode)]),
		TargetAgentId:        opts.TargetAgentID,
		TargetIdentity:       opts.TargetIdentity,
		TargetImplementation: opts.TargetImplementation,
		LaunchParamOverrides: opts.LaunchParamOverrides,
		Metadata:             opts.Metadata,
		Payload:              opts.Payload,
		RequestId:            requestID,
	}
	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CreateTask{CreateTask: req},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("create task timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// protoTaskInfoToSDK converts a protobuf TaskInfo to the SDK TaskInfo type.
func protoTaskInfoToSDK(t *pb.TaskInfo) *TaskInfo {
	return &TaskInfo{
		TaskID:      t.GetTaskId(),
		TaskType:    t.GetTaskType(),
		Status:      t.GetStatus().String(),
		Workspace:   t.GetWorkspace(),
		TargetTopic: t.GetTargetTopic(),
		AssignedTo:  t.GetAssignedTo(),
		CreatedAt:   t.GetCreatedAt(),
		StartedAt:   t.GetStartedAt(),
		CompletedAt: t.GetCompletedAt(),
		Attempt:     t.GetAttempt(),
		MaxAttempts: t.GetMaxAttempts(),
		Error:       t.GetError(),
		Metadata:    t.GetMetadata(),
	}
}

// ReportProgressOptions contains options for reporting progress.
type ReportProgressOptions struct {
	// TaskID is the task or correlation ID this progress relates to. Required.
	TaskID string

	// State is the current state (e.g., "running", "finishing", "idle").
	State string

	// Completion is the completion fraction 0.0-1.0, or -1 for indeterminate.
	Completion float64

	// Summary is a human-readable description of current activity.
	Summary string

	// StepName is the name of the current step (for multi-step operations).
	StepName string

	// StepDetail is a description of what the current step is doing.
	StepDetail string

	// StepSequence is the step number (1-based).
	StepSequence int32

	// StepTotal is the total number of steps (0 = unknown).
	StepTotal int32

	// StepType is a UI rendering hint (e.g., "llm_call", "tool_use").
	StepType string

	// Recipient is the target identity topic for the update.
	// Empty = broadcast to all subscribers in the workspace.
	Recipient string

	// RequestID is an optional correlation ID for the originating request.
	RequestID string

	// Metadata contains arbitrary key-value pairs.
	Metadata map[string]string

	// Kind classifies the report by its intended consumer or UI surface.
	// Use the pb.ProgressKind_* constants (e.g., pb.ProgressKind_PROGRESS_KIND_CHAT).
	// Zero value (PROGRESS_KIND_UNSPECIFIED) applies legacy heuristics.
	Kind pb.ProgressKind
}

// ReportProgress sends a progress report upstream through the gateway.
// Only agents and tasks can report progress.
func (c *BaseClient) ReportProgress(opts ReportProgressOptions) error {
	var step *pb.ProgressStep
	if opts.StepName != "" {
		step = &pb.ProgressStep{
			Name:       opts.StepName,
			Detail:     opts.StepDetail,
			Sequence:   opts.StepSequence,
			TotalSteps: opts.StepTotal,
			StepType:   opts.StepType,
		}
	}

	metadata := opts.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Progress{
			Progress: &pb.ProgressReport{
				TaskId:     opts.TaskID,
				State:      opts.State,
				Completion: opts.Completion,
				Summary:    opts.Summary,
				Step:       step,
				Recipient:  opts.Recipient,
				RequestId:  opts.RequestID,
				Metadata:   metadata,
				Kind:       opts.Kind,
			},
		},
	}

	return c.Send(msg)
}

// handleWorkflowResponse processes a workflow operation response from the server.
func (c *BaseClient) handleWorkflowResponse(ctx context.Context, resp *pb.WorkflowResponse) error {
	wfResp := &WorkflowResponse{
		Success:    resp.GetSuccess(),
		Error:      resp.GetError(),
		Message:    resp.GetMessage(),
		Data:       resp.GetData(),
		TotalCount: resp.GetTotalCount(),
		RequestId:  resp.GetRequestId(),
	}

	// Try to resolve a pending correlated request first
	if reqID := resp.GetRequestId(); reqID != "" {
		if c.ResolvePendingWorkflowRequest(reqID, wfResp) {
			if c.handlers.OnWorkflowResponse != nil {
				return c.handlers.OnWorkflowResponse(ctx, wfResp)
			}
			return nil
		}
	}

	// Fallback to async handler
	if c.handlers.OnWorkflowResponse != nil {
		return c.handlers.OnWorkflowResponse(ctx, wfResp)
	}
	return nil
}

// handleWorkflowOperation processes a forwarded workflow operation from the gateway.
func (c *BaseClient) handleWorkflowOperation(ctx context.Context, op *pb.WorkflowOperation) error {
	if c.handlers.OnWorkflowOperation == nil {
		return nil
	}
	resp, err := c.handlers.OnWorkflowOperation(ctx, op)
	if err != nil {
		resp = &pb.WorkflowResponse{
			Success:   false,
			Error:     err.Error(),
			RequestId: op.GetRequestId(),
		}
	}
	if resp != nil {
		return c.Send(&pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_WorkflowResponse{
				WorkflowResponse: resp,
			},
		})
	}
	return nil
}

// Workflow returns the WorkflowOps helper for this client.
func (c *BaseClient) Workflow() *WorkflowOps {
	c.workflowOnce.Do(func() {
		c.workflowInstance = newWorkflowOps(c)
	})
	return c.workflowInstance
}

// handleWorkspaceResponse processes a workspace operation response from the server.
func (c *BaseClient) handleWorkspaceResponse(ctx context.Context, resp *pb.WorkspaceResponse) error {
	wsResp := protoWorkspaceResponseToSDK(resp)

	// Try to resolve a pending correlated request (fall back to first pending for old servers).
	c.pendingWorkspaceRequests.ResolveFirst(wsResp)

	if c.handlers.OnWorkspaceResponse != nil {
		return c.handlers.OnWorkspaceResponse(ctx, wsResp)
	}
	return nil
}

// handleAgentResponse processes an agent operation response from the server.
func (c *BaseClient) handleAgentResponse(ctx context.Context, resp *pb.AgentResponse) error {
	agResp := protoAgentResponseToSDK(resp)

	// Try to resolve a pending correlated request (fall back to first pending for old servers).
	c.pendingAgentRequests.ResolveFirst(agResp)

	if c.handlers.OnAgentResponse != nil {
		return c.handlers.OnAgentResponse(ctx, agResp)
	}
	return nil
}

// handleACLResponse processes an ACL operation response from the server.
func (c *BaseClient) handleACLResponse(ctx context.Context, resp *pb.ACLResponse) error {
	aclResp := protoACLResponseToSDK(resp)

	// Try to resolve a pending correlated request (fall back to first pending for old servers).
	c.pendingACLRequests.ResolveFirst(aclResp)

	if c.handlers.OnACLResponse != nil {
		return c.handlers.OnACLResponse(ctx, aclResp)
	}
	return nil
}

// handleTokenResponse processes a token operation response from the server.
func (c *BaseClient) handleTokenResponse(ctx context.Context, resp *pb.TokenResponse) error {
	tokenResp := protoTokenResponseToSDK(resp)

	// Try to resolve a pending correlated request first
	if reqID := resp.GetRequestId(); reqID != "" {
		if c.ResolvePendingTokenRequest(reqID, tokenResp) {
			if c.handlers.OnTokenResponse != nil {
				return c.handlers.OnTokenResponse(ctx, tokenResp)
			}
			return nil
		}
	}

	// Fallback to async handler
	if c.handlers.OnTokenResponse != nil {
		return c.handlers.OnTokenResponse(ctx, tokenResp)
	}
	return nil
}

// Tokens returns the TokenOps helper for this client.
func (c *BaseClient) Tokens() *TokenOps {
	c.tokenOnce.Do(func() {
		c.tokenInstance = newTokenOps(c)
	})
	return c.tokenInstance
}

// handleAuthorityGrantResponse processes a runtime authority-grant response
// from the server.
func (c *BaseClient) handleAuthorityGrantResponse(ctx context.Context, resp *pb.AuthorityGrantResponse) error {
	// Try to resolve a pending correlated request first.
	if reqID := resp.GetRequestId(); reqID != "" {
		if c.ResolvePendingAuthorityGrantRequest(reqID, resp) {
			if c.handlers.OnAuthorityGrantResponse != nil {
				return c.handlers.OnAuthorityGrantResponse(ctx, resp)
			}
			return nil
		}
	}

	// Fallback to async handler.
	if c.handlers.OnAuthorityGrantResponse != nil {
		return c.handlers.OnAuthorityGrantResponse(ctx, resp)
	}
	return nil
}

// handleAuthorityGrantRevocation dispatches a server-pushed
// AuthorityGrantRevocation event to every registered cache (best-effort)
// and then to the user-supplied OnAuthorityGrantRevocation handler.
func (c *BaseClient) handleAuthorityGrantRevocation(ctx context.Context, evt *pb.AuthorityGrantRevocation) error {
	c.authorityCacheMu.RLock()
	caches := make([]*AuthorityGrantCache, len(c.authorityCaches))
	copy(caches, c.authorityCaches)
	c.authorityCacheMu.RUnlock()

	for _, cache := range caches {
		cache.HandleRevocationEvent(evt)
	}

	if c.handlers.OnAuthorityGrantRevocation != nil {
		return c.handlers.OnAuthorityGrantRevocation(ctx, evt)
	}
	return nil
}

// handleSubmitAuditEventResponse routes a foreign audit-submit response to the
// pending caller registered by client_request_id. There is no async fallback
// handler — every SubmitAuditEvent call is a sync round-trip.
func (c *BaseClient) handleSubmitAuditEventResponse(ctx context.Context, resp *pb.SubmitAuditEventResponse) error {
	_ = ctx
	if reqID := resp.GetClientRequestId(); reqID != "" {
		c.ResolvePendingAuditSubmitRequest(reqID, resp)
	}
	return nil
}

// AuthorityGrants returns the AuthorityGrantOps helper for this client.
func (c *BaseClient) AuthorityGrants() *AuthorityGrantOps {
	c.authorityOnce.Do(func() {
		c.authorityInstance = newAuthorityGrantOps(c)
	})
	return c.authorityInstance
}

// MakeAuthorityCache constructs a new AuthorityGrantCache wired into this
// client. AuthorityGrantRevocation push events on the downstream stream are
// dispatched to this cache automatically. Callers should pin the cache for
// the lifetime of the client; multiple caches are supported but rare. Use
// AuthorityGrantCache.Close() to deregister the cache.
func (c *BaseClient) MakeAuthorityCache(opts ...AuthorityGrantCacheOption) *AuthorityGrantCache {
	cache := NewAuthorityGrantCache(c.AuthorityGrants(), opts...)
	cache.parent = c
	c.authorityCacheMu.Lock()
	c.authorityCaches = append(c.authorityCaches, cache)
	c.authorityCacheMu.Unlock()
	return cache
}

// removeAuthorityCache de-registers a cache so it no longer receives
// revocation events. Used by AuthorityGrantCache.Close().
func (c *BaseClient) removeAuthorityCache(target *AuthorityGrantCache) {
	c.authorityCacheMu.Lock()
	defer c.authorityCacheMu.Unlock()
	out := c.authorityCaches[:0]
	for _, cache := range c.authorityCaches {
		if cache != target {
			out = append(out, cache)
		}
	}
	c.authorityCaches = out
}

// =============================================================================
// Task Lifecycle Methods
// =============================================================================

// QueryTasks sends a task LIST query and returns the response synchronously.
func (c *BaseClient) QueryTasks(ctx context.Context, filter *pb.TaskFilter, timeout time.Duration) (*TaskQueryResponse, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	requestID := c.NextRequestID()
	ch := c.RegisterPendingTaskQueryRequest(requestID)
	defer c.pendingTaskQueryRequests.Delete(requestID)

	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TaskQuery{
			TaskQuery: &pb.TaskQuery{
				Op:     pb.TaskQuery_LIST,
				Filter: filter,
			},
		},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("task query timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// GetTask sends a task GET query and returns the response synchronously.
func (c *BaseClient) GetTask(ctx context.Context, taskID string, timeout time.Duration) (*TaskQueryResponse, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	requestID := c.NextRequestID()
	ch := c.RegisterPendingTaskQueryRequest(requestID)
	defer c.pendingTaskQueryRequests.Delete(requestID)

	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TaskQuery{
			TaskQuery: &pb.TaskQuery{
				Op:     pb.TaskQuery_GET,
				TaskId: taskID,
			},
		},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("task get timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// RetryTask sends a task RETRY operation and returns the response synchronously.
func (c *BaseClient) RetryTask(ctx context.Context, taskID string, timeout time.Duration) (*TaskOperationResponse, error) {
	return c.doTaskOperation(ctx, pb.TaskOperation_RETRY, taskID, "", timeout)
}

// CancelTask sends a task CANCEL operation and returns the response synchronously.
func (c *BaseClient) CancelTask(ctx context.Context, taskID, reason string, timeout time.Duration) (*TaskOperationResponse, error) {
	return c.doTaskOperation(ctx, pb.TaskOperation_CANCEL, taskID, reason, timeout)
}

// CompleteTask sends a task COMPLETE operation and returns the response synchronously.
func (c *BaseClient) CompleteTask(ctx context.Context, taskID string, timeout time.Duration) (*TaskOperationResponse, error) {
	return c.doTaskOperation(ctx, pb.TaskOperation_COMPLETE, taskID, "", timeout)
}

// FailTask sends a task FAIL operation and returns the response synchronously.
func (c *BaseClient) FailTask(ctx context.Context, taskID, reason string, timeout time.Duration) (*TaskOperationResponse, error) {
	return c.doTaskOperation(ctx, pb.TaskOperation_FAIL, taskID, reason, timeout)
}

// =============================================================================
// Proto Conversion Helpers
// =============================================================================

// protoWorkspaceResponseToSDK converts a protobuf WorkspaceResponse to the SDK type.
func protoWorkspaceResponseToSDK(resp *pb.WorkspaceResponse) *WorkspaceResponse {
	r := &WorkspaceResponse{
		Success:    resp.GetSuccess(),
		Error:      resp.GetError(),
		Message:    resp.GetMessage(),
		TotalCount: resp.GetTotalCount(),
	}
	if ws := resp.GetWorkspace(); ws != nil {
		r.Workspace = protoWorkspaceInfoToSDK(ws)
	}
	for _, ws := range resp.GetWorkspaces() {
		r.Workspaces = append(r.Workspaces, protoWorkspaceInfoToSDK(ws))
	}
	return r
}

// protoWorkspaceInfoToSDK converts a protobuf WorkspaceInfo to the SDK type.
func protoWorkspaceInfoToSDK(ws *pb.WorkspaceInfo) *WorkspaceInfo {
	return &WorkspaceInfo{
		WorkspaceID:   ws.GetWorkspaceId(),
		DisplayName:   ws.GetDisplayName(),
		Description:   ws.GetDescription(),
		TenantID:      ws.GetTenantId(),
		CreatedAt:     ws.GetCreatedAt(),
		UpdatedAt:     ws.GetUpdatedAt(),
		Metadata:      ws.GetMetadata(),
		ActiveAgents:  ws.GetActiveAgents(),
		ActiveTasks:   ws.GetActiveTasks(),
		ActiveUsers:   ws.GetActiveUsers(),
		TotalMessages: ws.GetTotalMessages(),
	}
}

// protoAgentResponseToSDK converts a protobuf AgentResponse to the SDK type.
func protoAgentResponseToSDK(resp *pb.AgentResponse) *AgentResponse {
	r := &AgentResponse{
		Success:    resp.GetSuccess(),
		Error:      resp.GetError(),
		Message:    resp.GetMessage(),
		TotalCount: resp.GetTotalCount(),
	}
	if a := resp.GetAgent(); a != nil {
		r.Agent = protoAgentRegInfoToSDK(a)
	}
	for _, a := range resp.GetAgents() {
		r.Agents = append(r.Agents, protoAgentRegInfoToSDK(a))
	}
	for _, o := range resp.GetOrchestrators() {
		r.Orchestrators = append(r.Orchestrators, &OrchestratorInfo{
			OrchestratorID: o.GetOrchestratorId(),
			Profiles:       o.GetProfiles(),
			ConnectedAt:    o.GetConnectedAt(),
		})
	}
	if lr := resp.GetLaunchResult(); lr != nil {
		r.LaunchResult = &AgentLaunchResult{
			TaskID:  lr.GetTaskId(),
			Message: lr.GetMessage(),
		}
	}
	return r
}

// protoAgentRegInfoToSDK converts a protobuf AgentRegistrationInfo to the SDK type.
func protoAgentRegInfoToSDK(a *pb.AgentRegistrationInfo) *AgentRegistrationInfo {
	return &AgentRegistrationInfo{
		Implementation:      a.GetImplementation(),
		OrchestratorProfile: a.GetOrchestratorProfile(),
		Description:         a.GetDescription(),
		LaunchParams:        a.GetLaunchParams(),
		RegisteredAt:        a.GetRegisteredAt(),
		UpdatedAt:           a.GetUpdatedAt(),
	}
}

// protoACLResponseToSDK converts a protobuf ACLResponse to the SDK type.
func protoACLResponseToSDK(resp *pb.ACLResponse) *ACLResponse {
	r := &ACLResponse{
		Success:           resp.GetSuccess(),
		Error:             resp.GetError(),
		Message:           resp.GetMessage(),
		TotalRules:        resp.GetTotalRules(),
		TotalAuditEntries: resp.GetTotalAuditEntries(),
	}
	if rule := resp.GetRule(); rule != nil {
		r.Rule = protoACLRuleInfoToSDK(rule)
	}
	for _, rule := range resp.GetRules() {
		r.Rules = append(r.Rules, protoACLRuleInfoToSDK(rule))
	}
	if fp := resp.GetFallbackPolicy(); fp != nil {
		r.FallbackPolicy = &ACLFallbackPolicyInfo{
			PolicyID:                fp.GetPolicyId(),
			RuleCategory:            fp.GetRuleCategory(),
			FallbackAccessLevel:     fp.GetFallbackAccessLevel(),
			FallbackAccessLevelName: fp.GetFallbackAccessLevelName(),
			UpdatedBy:               fp.GetUpdatedBy(),
			UpdatedAt:               fp.GetUpdatedAt(),
		}
	}
	for _, ae := range resp.GetAuditEntries() {
		r.AuditEntries = append(r.AuditEntries, &ACLAuditEntryInfo{
			AuditID:         ae.GetAuditId(),
			Timestamp:       ae.GetTimestamp(),
			Decision:        ae.GetDecision(),
			AccessLevel:     ae.GetAccessLevel(),
			AccessLevelName: ae.GetAccessLevelName(),
			PrincipalType:   ae.GetPrincipalType(),
			PrincipalID:     ae.GetPrincipalId(),
			ResourceType:    ae.GetResourceType(),
			ResourceID:      ae.GetResourceId(),
			Operation:       ae.GetOperation(),
			Workspace:       ae.GetWorkspace(),
			RuleID:          ae.GetRuleId(),
			FallbackApplied: ae.GetFallbackApplied(),
			GatewayID:       ae.GetGatewayId(),
			SessionID:       ae.GetSessionId(),
			Metadata:        ae.GetMetadata(),
		})
	}
	if cr := resp.GetCleanupResult(); cr != nil {
		r.CleanupResult = &ACLCleanupResult{
			DeletedCount: cr.GetDeletedCount(),
			Message:      cr.GetMessage(),
		}
	}
	return r
}

// protoACLRuleInfoToSDK converts a protobuf ACLRuleInfo to the SDK type.
func protoACLRuleInfoToSDK(rule *pb.ACLRuleInfo) *ACLRuleInfo {
	return &ACLRuleInfo{
		RuleID:          rule.GetRuleId(),
		PrincipalType:   rule.GetPrincipalType(),
		PrincipalID:     rule.GetPrincipalId(),
		ResourceType:    rule.GetResourceType(),
		ResourceID:      rule.GetResourceId(),
		AccessLevel:     rule.GetAccessLevel(),
		AccessLevelName: rule.GetAccessLevelName(),
		GrantedBy:       rule.GetGrantedBy(),
		GrantedAt:       rule.GetGrantedAt(),
		ExpiresAt:       rule.GetExpiresAt(),
		Reason:          rule.GetReason(),
	}
}

// protoTokenInfoToSDK converts a protobuf TokenInfo to the SDK type.
func protoTokenInfoToSDK(t *pb.TokenInfo) *TokenInfo {
	if t == nil {
		return nil
	}
	return &TokenInfo{
		ID:                t.GetId(),
		Name:              t.GetName(),
		PrincipalType:     t.GetPrincipalType(),
		WorkspacePatterns: t.GetWorkspacePatterns(),
		Scopes:            t.GetScopes(),
		CreatedBy:         t.GetCreatedBy(),
		ExpiresAt:         t.GetExpiresAt(),
		LastUsedAt:        t.GetLastUsedAt(),
		Revoked:           t.GetRevoked(),
		RevokedAt:         t.GetRevokedAt(),
		CreatedAt:         t.GetCreatedAt(),
		UpdatedAt:         t.GetUpdatedAt(),
	}
}

// protoTokenResponseToSDK converts a protobuf TokenResponse to the SDK type.
func protoTokenResponseToSDK(resp *pb.TokenResponse) *TokenResponse {
	r := &TokenResponse{
		Success:        resp.GetSuccess(),
		Error:          resp.GetError(),
		Message:        resp.GetMessage(),
		TotalCount:     resp.GetTotalCount(),
		PlaintextToken: resp.GetPlaintextToken(),
		RequestId:      resp.GetRequestId(),
	}
	if t := resp.GetToken(); t != nil {
		r.Token = protoTokenInfoToSDK(t)
	}
	for _, t := range resp.GetTokens() {
		r.Tokens = append(r.Tokens, protoTokenInfoToSDK(t))
	}
	if ct := resp.GetCreatedToken(); ct != nil {
		r.CreatedToken = protoTokenInfoToSDK(ct)
	}
	return r
}

// doTaskOperation is a helper for synchronous task lifecycle operations.
func (c *BaseClient) doTaskOperation(ctx context.Context, op pb.TaskOperation_OpType, taskID, reason string, timeout time.Duration) (*TaskOperationResponse, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	requestID := c.NextRequestID()
	ch := c.RegisterPendingTaskOpRequest(requestID)
	defer c.pendingTaskOpRequests.Delete(requestID)

	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TaskOp{
			TaskOp: &pb.TaskOperation{
				Op:     op,
				TaskId: taskID,
				Reason: reason,
			},
		},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("task operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}
