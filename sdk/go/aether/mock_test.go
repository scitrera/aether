package aether

import (
	"context"
	"io"
	"sync"

	pb "github.com/scitrera/aether/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// =============================================================================
// Test Constants
// =============================================================================

const (
	TestServerAddr     = "localhost:50051"
	TestWorkspace      = "test-workspace"
	TestImplementation = "test-impl"
	TestSpecifier      = "test-spec"
	TestUserID         = "test-user-123"
	TestWindowID       = "test-window-456"
	TestSessionID      = "test-session-abc123"
)

// TestProfiles is a list of test profiles for orchestrators.
var TestProfiles = []string{"profile-1", "profile-2"}

// =============================================================================
// Mock gRPC Stream
// =============================================================================

// mockStream implements pb.AetherGateway_ConnectClient for testing.
type mockStream struct {
	grpc.ClientStream

	mu         sync.Mutex
	sendMsgs   []*pb.UpstreamMessage
	recvMsgs   []*pb.DownstreamMessage
	recvIndex  int
	recvErr    error
	sendErr    error
	closed     bool
	closedSend bool
	sendCalled chan struct{}
	recvCalled chan struct{}
	headerMD   metadata.MD
	trailerMD  metadata.MD
	headerErr  error
}

// newMockStream creates a new mock stream with optional messages to receive.
func newMockStream(recvMsgs ...*pb.DownstreamMessage) *mockStream {
	return &mockStream{
		recvMsgs:   recvMsgs,
		sendCalled: make(chan struct{}, 100),
		recvCalled: make(chan struct{}, 100),
	}
}

// Send stores the sent message for later inspection.
func (m *mockStream) Send(msg *pb.UpstreamMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sendErr != nil {
		return m.sendErr
	}
	if m.closedSend {
		return io.EOF
	}

	m.sendMsgs = append(m.sendMsgs, msg)
	select {
	case m.sendCalled <- struct{}{}:
	default:
	}
	return nil
}

// Recv returns the next message from the queue or an error.
func (m *mockStream) Recv() (*pb.DownstreamMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case m.recvCalled <- struct{}{}:
	default:
	}

	if m.recvErr != nil {
		return nil, m.recvErr
	}
	if m.closed {
		return nil, io.EOF
	}
	if m.recvIndex >= len(m.recvMsgs) {
		return nil, io.EOF
	}

	msg := m.recvMsgs[m.recvIndex]
	m.recvIndex++
	return msg, nil
}

// Header returns the header metadata.
func (m *mockStream) Header() (metadata.MD, error) {
	return m.headerMD, m.headerErr
}

// Trailer returns the trailer metadata.
func (m *mockStream) Trailer() metadata.MD {
	return m.trailerMD
}

// CloseSend marks the send side as closed.
func (m *mockStream) CloseSend() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closedSend = true
	return nil
}

// Context returns a background context.
func (m *mockStream) Context() context.Context {
	return context.Background()
}

// SendMsg implements the grpc.ClientStream interface.
func (m *mockStream) SendMsg(msg interface{}) error {
	return m.Send(msg.(*pb.UpstreamMessage))
}

// RecvMsg implements the grpc.ClientStream interface.
func (m *mockStream) RecvMsg(msg interface{}) error {
	resp, err := m.Recv()
	if err != nil {
		return err
	}
	// Copy the response to the message using proto.Merge to avoid copying mutex
	proto.Merge(msg.(proto.Message), resp)
	return nil
}

// SetRecvError sets an error to be returned on next Recv.
func (m *mockStream) SetRecvError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvErr = err
}

// SetSendError sets an error to be returned on next Send.
func (m *mockStream) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendErr = err
}

// Close marks the stream as closed.
func (m *mockStream) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

// GetSentMessages returns all messages sent to the stream.
func (m *mockStream) GetSentMessages() []*pb.UpstreamMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*pb.UpstreamMessage, len(m.sendMsgs))
	copy(result, m.sendMsgs)
	return result
}

// AddRecvMessage adds a message to be received.
func (m *mockStream) AddRecvMessage(msg *pb.DownstreamMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvMsgs = append(m.recvMsgs, msg)
}

// =============================================================================
// Mock gRPC Client
// =============================================================================

// mockGatewayClient implements pb.AetherGatewayClient for testing.
type mockGatewayClient struct {
	mu          sync.Mutex
	connectErr  error
	stream      *mockStream
	connectHook func(ctx context.Context, opts ...grpc.CallOption) (pb.AetherGateway_ConnectClient, error)
}

// newMockGatewayClient creates a new mock gateway client.
func newMockGatewayClient() *mockGatewayClient {
	return &mockGatewayClient{
		stream: newMockStream(),
	}
}

// Connect returns the mock stream or an error.
func (m *mockGatewayClient) Connect(ctx context.Context, opts ...grpc.CallOption) (pb.AetherGateway_ConnectClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.connectHook != nil {
		return m.connectHook(ctx, opts...)
	}
	if m.connectErr != nil {
		return nil, m.connectErr
	}
	return m.stream, nil
}

// SetConnectError sets an error to be returned on Connect.
func (m *mockGatewayClient) SetConnectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectErr = err
}

// SetStream sets the stream to be returned on Connect.
func (m *mockGatewayClient) SetStream(stream *mockStream) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stream = stream
}

// GetStream returns the current mock stream.
func (m *mockGatewayClient) GetStream() *mockStream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stream
}

// SetConnectHook sets a custom function to be called on Connect.
func (m *mockGatewayClient) SetConnectHook(hook func(ctx context.Context, opts ...grpc.CallOption) (pb.AetherGateway_ConnectClient, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectHook = hook
}

// =============================================================================
// Mock Protobuf Message Helpers
// =============================================================================

// newMockConnectionAck creates a mock ConnectionAck message.
func newMockConnectionAck(sessionID string, resumed bool) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: &pb.ConnectionAck{
				SessionId: sessionID,
				Resumed:   resumed,
			},
		},
	}
}

// newMockIncomingMessage creates a mock IncomingMessage.
func newMockIncomingMessage(sourceTopic string, payload []byte) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Msg{
			Msg: &pb.IncomingMessage{
				SourceTopic: sourceTopic,
				Payload:     payload,
			},
		},
	}
}

// newMockConfigSnapshot creates a mock ConfigSnapshot message.
func newMockConfigSnapshot(kv, globalKV map[string][]byte) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Config{
			Config: &pb.ConfigSnapshot{
				Kv:       kv,
				GlobalKv: globalKV,
			},
		},
	}
}

// newMockSignal creates a mock Signal message.
func newMockSignal(signalType pb.Signal_SignalType, reason string) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Signal{
			Signal: &pb.Signal{
				Type:   signalType,
				Reason: reason,
			},
		},
	}
}

// newMockErrorResponse creates a mock ErrorResponse message.
func newMockErrorResponse(code, message string) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Error{
			Error: &pb.ErrorResponse{
				Code:    code,
				Message: message,
			},
		},
	}
}

// newMockKVResponse creates a mock KVResponse message.
func newMockKVResponse(success bool, value string, keys []string) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success: success,
				Value:   []byte(value),
				Keys:    keys,
			},
		},
	}
}

// newMockTaskAssignment creates a mock TaskAssignment message.
func newMockTaskAssignment(taskID, taskType, assignedTo string) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskAssignment{
			TaskAssignment: &pb.TaskAssignment{
				TaskId:     taskID,
				TaskType:   taskType,
				AssignedTo: assignedTo,
			},
		},
	}
}

// newMockCheckpointResponse creates a mock CheckpointResponse message.
func newMockCheckpointResponse(success bool, data []byte, savedAt int64) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Checkpoint{
			Checkpoint: &pb.CheckpointResponse{
				Success: success,
				Data:    data,
				SavedAt: savedAt,
			},
		},
	}
}

// =============================================================================
// Mock gRPC Error Helpers
// =============================================================================

// newGRPCError creates a gRPC error with the given code and message.
func newGRPCError(code codes.Code, message string) error {
	return status.Error(code, message)
}

// =============================================================================
// Test TLS Certificates (dummy values for testing)
// =============================================================================

// TestRootCA is a dummy PEM-encoded root CA certificate for testing.
var TestRootCA = []byte(`-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHBfpD5xhZBMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl
c3RjYTAeFw0yMzAxMDEwMDAwMDBaFw0zMzAxMDEwMDAwMDBaMBExDzANBgNVBAMM
BnRlc3RjYTBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC7o96W+PxLb0TPWiGhGVJk
Kp7VhKWL4nH7H8A+EVvT7uSl3K5J0vu4PZTqDJdEJ0E1H+7zVT7HxRJd1N8M7L3P
AgMBAAGjUzBRMB0GA1UdDgQWBBQJD0L2jHG3+PqE4PGpzYLMhJnzJTAfBgNVHSME
GDAWgBQJD0L2jHG3+PqE4PGpzYLMhJnzJTAPBgNVHRMBAf8EBTADAQH/MA0GCSqG
SIb3DQEBCwUAA0EAl8aGLX7iH7yb7YLsP7pPmTh0h6tP7m9qGF4oI3k9aZX9J8I
-----END CERTIFICATE-----`)

// TestClientCert is a dummy PEM-encoded client certificate for testing.
var TestClientCert = []byte(`-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHBfpD5xhZCMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl
c3RjYTAeFw0yMzAxMDEwMDAwMDBaFw0zMzAxMDEwMDAwMDBaMBMxETAPBgNVBAMM
CGNsaWVudGNhMFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALuj3pb4/EtvRM9aIaEZ
UmQqntWEpYvicfsfwD4RW9Pu5KXcrknS+7g9lOoMl0QnQTUf7vNVPsfFEl3U3wzs
vc8CAwEAAaNTMFEwHQYDVR0OBBYEFAkPQvaMcbf4+oTg8anNgsyEmfMlMB8GA1Ud
IwQYMBaAFAkPQvaMcbf4+oTg8anNgsyEmfMlMA8GA1UdEwEB/wQFMAMBAf8wDQYJ
KoZIhvcNAQELBQADQQCXxoYtfuIfvJvtguw/uk+ZOHSHq0/ub2oYXigjeT1plf0n
-----END CERTIFICATE-----`)

// TestClientKey is a dummy PEM-encoded client key for testing.
var TestClientKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIBOgIBAAJBALuj3pb4/EtvRM9aIaEZUmQqntWEpYvicfsfwD4RW9Pu5KXcrknS
+7g9lOoMl0QnQTUf7vNVPsfFEl3U3wzsvc8CAwEAAQJAZV9K5G4/xZaC8d5PJ0xL
nXCL9FMy7T7J0h3R0v1M0e8mWP7qJ0f3nL3+3X7e8K7p9q6kQhHH7S2F6E5d4F5L
AQIhAOkj8B7d9M0Y6j3q6I8E0s1p6L5g8K9x7V5O6e3X7Q5tAiEAzK8j9h6Y8X7
-----END RSA PRIVATE KEY-----`)

// =============================================================================
// Test Data Helpers
// =============================================================================

// testPayload returns a test message payload.
func testPayload() []byte {
	return []byte(`{"message": "Hello, Aether!"}`)
}

// testKVValue returns a test KV store value.
func testKVValue() []byte {
	return []byte(`{"key": "value", "count": 42}`)
}

// testCheckpointData returns a test checkpoint data.
func testCheckpointData() []byte {
	return []byte(`{"state": "checkpoint_state", "version": 1}`)
}

// =============================================================================
// Handler Tracking Helpers
// =============================================================================

// handlerTracker tracks handler invocations for testing.
type handlerTracker struct {
	mu          sync.Mutex
	messages    []*Message
	configs     []*ConfigSnapshot
	signals     []*Signal
	errors      []*ErrorInfo
	kvResponses []*KVResponse
	checkpoints []*CheckpointResponse
	tasks       []*TaskAssignment
	connects    []*ConnectionAck
	disconnects []string
	reconnects  []int
}

// newHandlerTracker creates a new handler tracker.
func newHandlerTracker() *handlerTracker {
	return &handlerTracker{}
}

// OnMessage tracks message handler calls.
func (h *handlerTracker) OnMessage(ctx context.Context, msg *Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, msg)
	return nil
}

// OnConfig tracks config handler calls.
func (h *handlerTracker) OnConfig(ctx context.Context, config *ConfigSnapshot) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.configs = append(h.configs, config)
	return nil
}

// OnSignal tracks signal handler calls.
func (h *handlerTracker) OnSignal(ctx context.Context, signal *Signal) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.signals = append(h.signals, signal)
	return nil
}

// OnError tracks error handler calls.
func (h *handlerTracker) OnError(ctx context.Context, err *ErrorInfo) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errors = append(h.errors, err)
	return nil
}

// OnKVResponse tracks KV response handler calls.
func (h *handlerTracker) OnKVResponse(ctx context.Context, resp *KVResponse) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.kvResponses = append(h.kvResponses, resp)
	return nil
}

// OnCheckpointResponse tracks checkpoint response handler calls.
func (h *handlerTracker) OnCheckpointResponse(ctx context.Context, resp *CheckpointResponse) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkpoints = append(h.checkpoints, resp)
	return nil
}

// OnTaskAssignment tracks task assignment handler calls.
func (h *handlerTracker) OnTaskAssignment(ctx context.Context, task *TaskAssignment) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tasks = append(h.tasks, task)
	return nil
}

// OnConnect tracks connect handler calls.
func (h *handlerTracker) OnConnect(ctx context.Context, ack *ConnectionAck) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connects = append(h.connects, ack)
	return nil
}

// OnDisconnect tracks disconnect handler calls.
func (h *handlerTracker) OnDisconnect(ctx context.Context, reason string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnects = append(h.disconnects, reason)
	return nil
}

// OnReconnecting tracks reconnecting handler calls.
func (h *handlerTracker) OnReconnecting(ctx context.Context, attempt int) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reconnects = append(h.reconnects, attempt)
	return nil
}

// MessageCount returns the number of messages received.
func (h *handlerTracker) MessageCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.messages)
}

// ConfigCount returns the number of configs received.
func (h *handlerTracker) ConfigCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.configs)
}

// SignalCount returns the number of signals received.
func (h *handlerTracker) SignalCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.signals)
}

// ErrorCount returns the number of errors received.
func (h *handlerTracker) ErrorCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.errors)
}

// ConnectCount returns the number of connect calls.
func (h *handlerTracker) ConnectCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.connects)
}

// DisconnectCount returns the number of disconnect calls.
func (h *handlerTracker) DisconnectCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.disconnects)
}

// ReconnectCount returns the number of reconnect calls.
func (h *handlerTracker) ReconnectCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.reconnects)
}

// GetLastConnect returns the last connection ack.
func (h *handlerTracker) GetLastConnect() *ConnectionAck {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.connects) == 0 {
		return nil
	}
	return h.connects[len(h.connects)-1]
}

// GetLastDisconnectReason returns the last disconnect reason.
func (h *handlerTracker) GetLastDisconnectReason() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.disconnects) == 0 {
		return ""
	}
	return h.disconnects[len(h.disconnects)-1]
}
