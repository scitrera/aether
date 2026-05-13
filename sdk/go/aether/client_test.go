package aether

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"

	"google.golang.org/grpc/codes"
)

// =============================================================================
// BaseClient Initialization Tests
// =============================================================================

func TestNewBaseClient_DefaultValues(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
	}

	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Check default queue size
	if client.queueSize != 100 {
		t.Errorf("queueSize = %d, want 100", client.queueSize)
	}

	// Check default connection options are applied
	opts := client.Options()
	defaultOpts := DefaultConnectionOptions()
	if opts.MaxRetries != defaultOpts.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", opts.MaxRetries, defaultOpts.MaxRetries)
	}
	if opts.InitialBackoff != defaultOpts.InitialBackoff {
		t.Errorf("InitialBackoff = %v, want %v", opts.InitialBackoff, defaultOpts.InitialBackoff)
	}
	if opts.MaxBackoff != defaultOpts.MaxBackoff {
		t.Errorf("MaxBackoff = %v, want %v", opts.MaxBackoff, defaultOpts.MaxBackoff)
	}
	if opts.BackoffMultiplier != defaultOpts.BackoffMultiplier {
		t.Errorf("BackoffMultiplier = %v, want %v", opts.BackoffMultiplier, defaultOpts.BackoffMultiplier)
	}
	if opts.AutoReconnect != defaultOpts.AutoReconnect {
		t.Errorf("AutoReconnect = %v, want %v", opts.AutoReconnect, defaultOpts.AutoReconnect)
	}
}

func TestNewBaseClient_CustomValues(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		Connection: ConnectionOptions{
			MaxRetries:        10,
			InitialBackoff:    500 * time.Millisecond,
			MaxBackoff:        60 * time.Second,
			BackoffMultiplier: 1.5,
			AutoReconnect:     false,
		},
		QueueSize: 50,
		Credentials: map[string]string{
			"token": "test-token",
		},
	}

	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	if client.queueSize != 50 {
		t.Errorf("queueSize = %d, want 50", client.queueSize)
	}

	opts := client.Options()
	if opts.MaxRetries != 10 {
		t.Errorf("MaxRetries = %d, want 10", opts.MaxRetries)
	}
	if opts.InitialBackoff != 500*time.Millisecond {
		t.Errorf("InitialBackoff = %v, want 500ms", opts.InitialBackoff)
	}
	if opts.MaxBackoff != 60*time.Second {
		t.Errorf("MaxBackoff = %v, want 60s", opts.MaxBackoff)
	}
	if opts.BackoffMultiplier != 1.5 {
		t.Errorf("BackoffMultiplier = %v, want 1.5", opts.BackoffMultiplier)
	}
	if opts.AutoReconnect != false {
		t.Errorf("AutoReconnect = %v, want false", opts.AutoReconnect)
	}
}

func TestNewBaseClient_MissingServerAddr(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: "",
	}

	_, err := NewBaseClient(cfg)
	if err == nil {
		t.Fatal("NewBaseClient() should fail with empty ServerAddr")
	}

	var argErr *InvalidArgumentError
	if !errors.As(err, &argErr) {
		t.Errorf("NewBaseClient() error type = %T, want *InvalidArgumentError", err)
	}
}

func TestNewBaseClient_TLSConfig(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		TLS: &TLSConfig{
			Enabled:            true,
			ServerName:         "test.example.com",
			InsecureSkipVerify: true,
		},
	}

	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	if client.tlsConfig == nil {
		t.Fatal("tlsConfig should not be nil")
	}
	if !client.tlsConfig.Enabled {
		t.Error("TLS should be enabled")
	}
	if client.tlsConfig.ServerName != "test.example.com" {
		t.Errorf("ServerName = %q, want %q", client.tlsConfig.ServerName, "test.example.com")
	}
}

// =============================================================================
// State Accessor Tests
// =============================================================================

func TestBaseClient_IsRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Initially not running
	if client.IsRunning() {
		t.Error("IsRunning() should be false initially")
	}

	// Simulate running
	client.running.Store(true)
	if !client.IsRunning() {
		t.Error("IsRunning() should be true when running")
	}

	// Reconnecting also counts as running
	client.running.Store(false)
	client.reconnecting.Store(true)
	if !client.IsRunning() {
		t.Error("IsRunning() should be true during reconnection")
	}
}

func TestBaseClient_IsConnected(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Initially not connected
	if client.IsConnected() {
		t.Error("IsConnected() should be false initially")
	}

	// Need both connected and connectionConfirmed
	client.connected.Store(true)
	if client.IsConnected() {
		t.Error("IsConnected() should be false without connectionConfirmed")
	}

	client.connectionConfirmed.Store(true)
	if !client.IsConnected() {
		t.Error("IsConnected() should be true when connected and confirmed")
	}
}

func TestBaseClient_SessionID(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Initially empty
	if client.SessionID() != "" {
		t.Errorf("SessionID() = %q, want empty", client.SessionID())
	}

	// Set and get
	client.setSessionID(TestSessionID)
	if client.SessionID() != TestSessionID {
		t.Errorf("SessionID() = %q, want %q", client.SessionID(), TestSessionID)
	}
}

// =============================================================================
// Backoff Calculation Tests
// =============================================================================

func TestBaseClient_CalculateBackoff(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		Connection: ConnectionOptions{
			InitialBackoff:    1 * time.Second,
			MaxBackoff:        30 * time.Second,
			BackoffMultiplier: 2.0,
		},
	}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tests := []struct {
		name    string
		attempt int
		minMs   int64
		maxMs   int64
	}{
		{
			name:    "first attempt",
			attempt: 0,
			minMs:   750,  // 1s - 25%
			maxMs:   1250, // 1s + 25%
		},
		{
			name:    "second attempt",
			attempt: 1,
			minMs:   1500, // 2s - 25%
			maxMs:   2500, // 2s + 25%
		},
		{
			name:    "third attempt",
			attempt: 2,
			minMs:   3000, // 4s - 25%
			maxMs:   5000, // 4s + 25%
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backoff := client.calculateBackoff(tt.attempt)
			ms := backoff.Milliseconds()
			if ms < tt.minMs || ms > tt.maxMs {
				t.Errorf("calculateBackoff(%d) = %dms, want between %dms and %dms",
					tt.attempt, ms, tt.minMs, tt.maxMs)
			}
		})
	}
}

func TestBaseClient_CalculateBackoff_RespectsMax(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		Connection: ConnectionOptions{
			InitialBackoff:    1 * time.Second,
			MaxBackoff:        10 * time.Second,
			BackoffMultiplier: 2.0,
		},
	}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Attempt 10 would be 1024s without cap, but should be capped
	backoff := client.calculateBackoff(10)
	maxWithJitter := 10*time.Second + 10*time.Second/4 // 10s + 25%
	if backoff > maxWithJitter {
		t.Errorf("calculateBackoff(10) = %v, should not exceed %v", backoff, maxWithJitter)
	}
}

// =============================================================================
// Handler Registration Tests
// =============================================================================

func TestBaseClient_HandlerRegistration(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()

	// Register all handlers
	client.OnMessage(tracker.OnMessage)
	client.OnConfig(tracker.OnConfig)
	client.OnSignal(tracker.OnSignal)
	client.OnError(tracker.OnError)
	client.OnKVResponse(tracker.OnKVResponse)
	client.OnCheckpointResponse(tracker.OnCheckpointResponse)
	client.OnTaskAssignment(tracker.OnTaskAssignment)
	client.OnConnect(tracker.OnConnect)
	client.OnDisconnect(tracker.OnDisconnect)
	client.OnReconnecting(tracker.OnReconnecting)

	// Verify handlers are set
	handlers := client.Handlers()
	if handlers.OnMessage == nil {
		t.Error("OnMessage handler should be set")
	}
	if handlers.OnConfig == nil {
		t.Error("OnConfig handler should be set")
	}
	if handlers.OnSignal == nil {
		t.Error("OnSignal handler should be set")
	}
	if handlers.OnError == nil {
		t.Error("OnError handler should be set")
	}
	if handlers.OnKVResponse == nil {
		t.Error("OnKVResponse handler should be set")
	}
	if handlers.OnCheckpointResponse == nil {
		t.Error("OnCheckpointResponse handler should be set")
	}
	if handlers.OnTaskAssignment == nil {
		t.Error("OnTaskAssignment handler should be set")
	}
	if handlers.OnConnect == nil {
		t.Error("OnConnect handler should be set")
	}
	if handlers.OnDisconnect == nil {
		t.Error("OnDisconnect handler should be set")
	}
	if handlers.OnReconnecting == nil {
		t.Error("OnReconnecting handler should be set")
	}
}

// =============================================================================
// Message Sending Tests
// =============================================================================

func TestBaseClient_Send_NotRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Send{
			Send: &pb.SendMessage{
				TargetTopic: "test.topic",
				Payload:     []byte("test"),
			},
		},
	}

	err = client.Send(msg)
	if err == nil {
		t.Error("Send() should fail when client is not running")
	}

	var closedErr *ConnectionClosedError
	if !errors.As(err, &closedErr) {
		t.Errorf("Send() error type = %T, want *ConnectionClosedError", err)
	}
}

func TestBaseClient_Send_QueueFull(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		QueueSize:  1,
	}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	client.running.Store(true)

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Send{
			Send: &pb.SendMessage{
				TargetTopic: "test.topic",
				Payload:     []byte("test"),
			},
		},
	}

	// First send should succeed
	err = client.Send(msg)
	if err != nil {
		t.Errorf("First Send() error = %v, want nil", err)
	}

	// Second send should fail (queue full)
	err = client.Send(msg)
	if err == nil {
		t.Error("Second Send() should fail when queue is full")
	}

	var msgErr *MessageError
	if !errors.As(err, &msgErr) {
		t.Errorf("Send() error type = %T, want *MessageError", err)
	}
}

func TestBaseClient_Send_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	client.running.Store(true)

	msg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Send{
			Send: &pb.SendMessage{
				TargetTopic: "test.topic",
				Payload:     []byte("test"),
			},
		},
	}

	err = client.Send(msg)
	if err != nil {
		t.Errorf("Send() error = %v, want nil", err)
	}

	// Verify message is in the queue
	select {
	case received := <-client.RequestQueue():
		if received != msg {
			t.Error("Received message should be the same as sent")
		}
	default:
		t.Error("Message should be in the queue")
	}
}

// =============================================================================
// Connection Lifecycle Tests
// =============================================================================

func TestBaseClient_Connect_MissingInitBuilder(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err == nil {
		t.Fatal("Connect() should fail without initMsgBuilder")
	}

	var argErr *InvalidArgumentError
	if !errors.As(err, &argErr) {
		t.Errorf("Connect() error type = %T, want *InvalidArgumentError", err)
	}
}

func TestBaseClient_Connect_AlreadyRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	client.initMsgBuilder = func() *pb.InitConnection {
		return &pb.InitConnection{
			ClientType: &pb.InitConnection_Agent{
				Agent: &pb.AgentIdentity{
					Workspace:      TestWorkspace,
					Implementation: TestImplementation,
					Specifier:      TestSpecifier,
				},
			},
		}
	}

	// Simulate already running but not reconnecting
	client.running.Store(true)
	client.reconnecting.Store(false)
	client.connectionConfirmed.Store(true)

	ctx := context.Background()
	err = client.Connect(ctx)
	// When already running and not reconnecting, Connect() should succeed (no-op)
	if err != nil {
		t.Fatalf("Connect() should succeed when already running and not reconnecting, got: %v", err)
	}
}

func TestBaseClient_Close_NotRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Close without connecting should be safe
	err = client.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestBaseClient_Close_MultipleCallsSafe(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Multiple Close calls should be safe
	for i := 0; i < 3; i++ {
		err = client.Close()
		if err != nil {
			t.Errorf("Close() call %d error = %v, want nil", i+1, err)
		}
	}
}

// =============================================================================
// Reconnection State Tests
// =============================================================================

func TestBaseClient_ReconnectionState(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		Connection: ConnectionOptions{
			AutoReconnect: true,
			MaxRetries:    3,
		},
	}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Test reconnection enabled
	if !client.ReconnectionEnabled() {
		t.Error("ReconnectionEnabled() should be true")
	}

	// Test reconnecting state
	if client.Reconnecting() {
		t.Error("Reconnecting() should be false initially")
	}

	client.SetReconnecting(true)
	if !client.Reconnecting() {
		t.Error("Reconnecting() should be true after SetReconnecting(true)")
	}

	// Test reconnect counter
	if client.ReconnectCount() != 0 {
		t.Errorf("ReconnectCount() = %d, want 0", client.ReconnectCount())
	}

	count := client.IncrementReconnectCount()
	if count != 1 {
		t.Errorf("IncrementReconnectCount() = %d, want 1", count)
	}

	if client.ReconnectCount() != 1 {
		t.Errorf("ReconnectCount() = %d, want 1", client.ReconnectCount())
	}

	client.ResetReconnectCount()
	if client.ReconnectCount() != 0 {
		t.Errorf("ReconnectCount() = %d after reset, want 0", client.ReconnectCount())
	}
}

func TestBaseClient_ConnectionConfirmed(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Initially not confirmed
	if client.ConnectionConfirmed() {
		t.Error("ConnectionConfirmed() should be false initially")
	}

	// Test ConfirmConnection
	client.reconnectCount.Store(5)
	client.ConfirmConnection()

	if !client.ConnectionConfirmed() {
		t.Error("ConnectionConfirmed() should be true after ConfirmConnection()")
	}
	if client.ReconnectCount() != 0 {
		t.Errorf("ReconnectCount() should be reset to 0, got %d", client.ReconnectCount())
	}
}

func TestBaseClient_ForceDisconnect(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Initially not force disconnected
	if client.ForceDisconnect() {
		t.Error("ForceDisconnect() should be false initially")
	}

	client.SetForceDisconnect(true)
	if !client.ForceDisconnect() {
		t.Error("ForceDisconnect() should be true after SetForceDisconnect(true)")
	}
}

// =============================================================================
// Request Queue Tests
// =============================================================================

func TestBaseClient_ClearRequestQueue(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		QueueSize:  10,
	}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	client.running.Store(true)

	// Add some messages to the queue
	for i := 0; i < 5; i++ {
		msg := &pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_Send{
				Send: &pb.SendMessage{
					TargetTopic: "test.topic",
				},
			},
		}
		_ = client.Send(msg)
	}

	// Verify queue has messages
	if len(client.RequestQueue()) == 0 {
		t.Error("Queue should have messages before clearing")
	}

	// Clear the queue
	client.ClearRequestQueue()

	// Verify queue is empty
	select {
	case <-client.RequestQueue():
		t.Error("Queue should be empty after clearing")
	default:
		// Expected
	}
}

// =============================================================================
// Response Dispatch Tests
// =============================================================================

func TestBaseClient_DispatchResponse_ConnectionAck(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnConnect(tracker.OnConnect)

	ctx := context.Background()
	response := newMockConnectionAck(TestSessionID, false)

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	// Check session ID was set
	if client.SessionID() != TestSessionID {
		t.Errorf("SessionID = %q, want %q", client.SessionID(), TestSessionID)
	}

	// Check connection was confirmed
	if !client.ConnectionConfirmed() {
		t.Error("Connection should be confirmed after ConnectionAck")
	}

	// Check handler was called
	if tracker.ConnectCount() != 1 {
		t.Errorf("Connect handler called %d times, want 1", tracker.ConnectCount())
	}
}

func TestBaseClient_DispatchResponse_IncomingMessage(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnMessage(tracker.OnMessage)

	ctx := context.Background()
	response := newMockIncomingMessage("ag.test.impl.spec", testPayload())

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	if tracker.MessageCount() != 1 {
		t.Errorf("Message handler called %d times, want 1", tracker.MessageCount())
	}
}

func TestBaseClient_DispatchResponse_ConfigSnapshot(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnConfig(tracker.OnConfig)

	ctx := context.Background()
	kv := map[string][]byte{"key1": []byte("value1")}
	globalKV := map[string][]byte{"global_key": []byte("global_value")}
	response := newMockConfigSnapshot(kv, globalKV)

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	if tracker.ConfigCount() != 1 {
		t.Errorf("Config handler called %d times, want 1", tracker.ConfigCount())
	}
}

func TestBaseClient_DispatchResponse_Signal(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnSignal(tracker.OnSignal)

	ctx := context.Background()
	// Use a non-FORCE_DISCONNECT signal type for this test
	response := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Signal{
			Signal: &pb.Signal{
				Type:   pb.Signal_SignalType(99), // Unknown type
				Reason: "test signal",
			},
		},
	}

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	if tracker.SignalCount() != 1 {
		t.Errorf("Signal handler called %d times, want 1", tracker.SignalCount())
	}
}

func TestBaseClient_DispatchResponse_ForceDisconnect(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnDisconnect(tracker.OnDisconnect)

	client.running.Store(true)

	ctx := context.Background()
	response := newMockSignal(pb.Signal_FORCE_DISCONNECT, "server requested disconnect")

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	if !client.ForceDisconnect() {
		t.Error("ForceDisconnect should be true after FORCE_DISCONNECT signal")
	}

	if client.running.Load() {
		t.Error("Client should not be running after FORCE_DISCONNECT signal")
	}

	if tracker.DisconnectCount() != 1 {
		t.Errorf("Disconnect handler called %d times, want 1", tracker.DisconnectCount())
	}
}

func TestBaseClient_DispatchResponse_ErrorResponse(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnError(tracker.OnError)

	ctx := context.Background()
	response := newMockErrorResponse("TEST_ERROR", "Test error message")

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	if tracker.ErrorCount() != 1 {
		t.Errorf("Error handler called %d times, want 1", tracker.ErrorCount())
	}
}

func TestBaseClient_DispatchResponse_KVResponse(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnKVResponse(tracker.OnKVResponse)

	ctx := context.Background()
	response := newMockKVResponse(true, "test-value", nil)

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	// Check handler was called
	if len(tracker.kvResponses) != 1 {
		t.Errorf("KV response handler called %d times, want 1", len(tracker.kvResponses))
	}

	// Check response queue
	select {
	case resp := <-client.KVResponseQueue():
		if !resp.Success {
			t.Error("KV response should be successful")
		}
		if string(resp.Value) != "test-value" {
			t.Errorf("KV response value = %q, want %q", resp.Value, "test-value")
		}
	default:
		t.Error("KV response should be in queue")
	}
}

func TestBaseClient_DispatchResponse_CheckpointResponse(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnCheckpointResponse(tracker.OnCheckpointResponse)

	ctx := context.Background()
	response := newMockCheckpointResponse(true, testCheckpointData(), time.Now().Unix())

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	// Check handler was called
	if len(tracker.checkpoints) != 1 {
		t.Errorf("Checkpoint response handler called %d times, want 1", len(tracker.checkpoints))
	}

	// Check response queue
	select {
	case resp := <-client.CheckpointResponseQueue():
		if !resp.Success {
			t.Error("Checkpoint response should be successful")
		}
	default:
		t.Error("Checkpoint response should be in queue")
	}
}

func TestBaseClient_DispatchResponse_TaskAssignment(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	tracker := newHandlerTracker()
	client.OnTaskAssignment(tracker.OnTaskAssignment)

	ctx := context.Background()
	response := newMockTaskAssignment("task-123", "process", "ag.test.worker.inst")

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	if len(tracker.tasks) != 1 {
		t.Errorf("Task assignment handler called %d times, want 1", len(tracker.tasks))
	}
}

// =============================================================================
// Task Lifecycle Tests
// =============================================================================

func TestBaseClient_QueryTasks(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		tq := msg.GetTaskQuery()
		if tq == nil || tq.Op != pb.TaskQuery_LIST {
			return
		}
		// resolve via first pending
		client.pendingTaskQueryRequests.Range(func(key, val any) bool {
			ch := val.(chan *TaskQueryResponse)
			client.pendingTaskQueryRequests.Delete(key)
			ch <- &TaskQueryResponse{Success: true, TotalCount: 2}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.QueryTasks(ctx, &pb.TaskFilter{Workspace: TestWorkspace}, 1*time.Second)
	if err != nil {
		t.Errorf("QueryTasks() error = %v", err)
	}
	if resp == nil {
		t.Fatal("QueryTasks() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if resp.TotalCount != 2 {
		t.Errorf("TotalCount = %d, want 2", resp.TotalCount)
	}
}

func TestBaseClient_GetTask(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		tq := msg.GetTaskQuery()
		if tq == nil || tq.Op != pb.TaskQuery_GET {
			return
		}
		client.pendingTaskQueryRequests.Range(func(key, val any) bool {
			ch := val.(chan *TaskQueryResponse)
			client.pendingTaskQueryRequests.Delete(key)
			ch <- &TaskQueryResponse{Success: true, Task: &TaskInfo{TaskID: "task-abc"}}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.GetTask(ctx, "task-abc", 1*time.Second)
	if err != nil {
		t.Errorf("GetTask() error = %v", err)
	}
	if resp == nil {
		t.Fatal("GetTask() response should not be nil")
	}
	if resp.Task == nil || resp.Task.TaskID != "task-abc" {
		t.Errorf("Task.TaskID = %q, want %q", resp.Task.TaskID, "task-abc")
	}

	// Verify that a TaskQuery_GET was enqueued
	// (already drained by goroutine above — just check success)
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestBaseClient_CancelTask(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		op := msg.GetTaskOp()
		if op == nil || op.Op != pb.TaskOperation_CANCEL {
			return
		}
		client.pendingTaskOpRequests.Range(func(key, val any) bool {
			ch := val.(chan *TaskOperationResponse)
			client.pendingTaskOpRequests.Delete(key)
			ch <- &TaskOperationResponse{Success: true, Message: "cancelled"}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.CancelTask(ctx, "task-xyz", "user requested", 1*time.Second)
	if err != nil {
		t.Errorf("CancelTask() error = %v", err)
	}
	if resp == nil {
		t.Fatal("CancelTask() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestBaseClient_RetryTask(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		op := msg.GetTaskOp()
		if op == nil || op.Op != pb.TaskOperation_RETRY {
			return
		}
		client.pendingTaskOpRequests.Range(func(key, val any) bool {
			ch := val.(chan *TaskOperationResponse)
			client.pendingTaskOpRequests.Delete(key)
			ch <- &TaskOperationResponse{Success: true}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.RetryTask(ctx, "task-xyz", 1*time.Second)
	if err != nil {
		t.Errorf("RetryTask() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("RetryTask() response should be successful")
	}
}

func TestBaseClient_CompleteTask(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		op := msg.GetTaskOp()
		if op == nil || op.Op != pb.TaskOperation_COMPLETE {
			return
		}
		client.pendingTaskOpRequests.Range(func(key, val any) bool {
			ch := val.(chan *TaskOperationResponse)
			client.pendingTaskOpRequests.Delete(key)
			ch <- &TaskOperationResponse{Success: true}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.CompleteTask(ctx, "task-xyz", 1*time.Second)
	if err != nil {
		t.Errorf("CompleteTask() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("CompleteTask() response should be successful")
	}
}

func TestBaseClient_FailTask(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		op := msg.GetTaskOp()
		if op == nil || op.Op != pb.TaskOperation_FAIL {
			return
		}
		client.pendingTaskOpRequests.Range(func(key, val any) bool {
			ch := val.(chan *TaskOperationResponse)
			client.pendingTaskOpRequests.Delete(key)
			ch <- &TaskOperationResponse{Success: true}
			return false
		})
	}()

	ctx := context.Background()
	resp, err := client.FailTask(ctx, "task-xyz", "fatal error", 1*time.Second)
	if err != nil {
		t.Errorf("FailTask() error = %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("FailTask() response should be successful")
	}
}

// =============================================================================
// Additional Dispatch Tests
// =============================================================================

func TestBaseClient_DispatchResponse_WorkspaceResponse(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	called := false
	client.OnWorkspaceResponse(func(ctx context.Context, resp *WorkspaceResponse) error {
		called = true
		return nil
	})

	ctx := context.Background()
	response := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Workspace{
			Workspace: &pb.WorkspaceResponse{Success: true},
		},
	}

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}
	if !called {
		t.Error("Workspace response handler should have been called")
	}
}

func TestBaseClient_DispatchResponse_AgentResponse(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	called := false
	client.OnAgentResponse(func(ctx context.Context, resp *AgentResponse) error {
		called = true
		return nil
	})

	ctx := context.Background()
	response := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Agent{
			Agent: &pb.AgentResponse{Success: true},
		},
	}

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}
	if !called {
		t.Error("Agent response handler should have been called")
	}
}

func TestBaseClient_DispatchResponse_ACLResponse(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	called := false
	client.OnACLResponse(func(ctx context.Context, resp *ACLResponse) error {
		called = true
		return nil
	})

	ctx := context.Background()
	response := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Acl{
			Acl: &pb.ACLResponse{Success: true},
		},
	}

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}
	if !called {
		t.Error("ACL response handler should have been called")
	}
}

func TestBaseClient_DispatchResponse_TaskQueryResponse_RequestID(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Register a pending request to verify correlation works (not empty string)
	requestID := client.NextRequestID()
	if requestID == "" {
		t.Fatal("NextRequestID() should not return empty string")
	}
	ch := client.RegisterPendingTaskQueryRequest(requestID)

	ctx := context.Background()
	response := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskQuery{
			TaskQuery: &pb.TaskQueryResponse{Success: true, TotalCount: 3},
		},
	}

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	select {
	case resp := <-ch:
		if !resp.Success {
			t.Error("Response should be successful")
		}
		if resp.TotalCount != 3 {
			t.Errorf("TotalCount = %d, want 3", resp.TotalCount)
		}
	default:
		t.Error("Pending task query request should have been resolved")
	}
}

func TestBaseClient_DispatchResponse_TaskOpResponse_RequestID(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Register a pending request to verify correlation works (not empty string)
	requestID := client.NextRequestID()
	if requestID == "" {
		t.Fatal("NextRequestID() should not return empty string")
	}
	ch := client.RegisterPendingTaskOpRequest(requestID)

	ctx := context.Background()
	response := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TaskOp{
			TaskOp: &pb.TaskOperationResponse{Success: true, Message: "done"},
		},
	}

	err = client.dispatchResponse(ctx, response)
	if err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	select {
	case resp := <-ch:
		if !resp.Success {
			t.Error("Response should be successful")
		}
		if resp.Message != "done" {
			t.Errorf("Message = %q, want %q", resp.Message, "done")
		}
	default:
		t.Error("Pending task op request should have been resolved")
	}
}

// =============================================================================
// Error Recovery Tests
// =============================================================================

func TestBaseClient_RecoverableErrors(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		recoverable bool
	}{
		{
			name:        "UNAVAILABLE is recoverable",
			err:         newGRPCError(codes.Unavailable, "service unavailable"),
			recoverable: true,
		},
		{
			name:        "UNAUTHENTICATED is not recoverable",
			err:         newGRPCError(codes.Unauthenticated, "invalid credentials"),
			recoverable: false,
		},
		{
			name:        "PERMISSION_DENIED is not recoverable",
			err:         newGRPCError(codes.PermissionDenied, "access denied"),
			recoverable: false,
		},
		{
			name:        "ALREADY_EXISTS is not recoverable",
			err:         newGRPCError(codes.AlreadyExists, "identity in use"),
			recoverable: false,
		},
		{
			name:        "DEADLINE_EXCEEDED is recoverable",
			err:         newGRPCError(codes.DeadlineExceeded, "timeout"),
			recoverable: true,
		},
		{
			name:        "INTERNAL is recoverable",
			err:         newGRPCError(codes.Internal, "internal error"),
			recoverable: true,
		},
		{
			name:        "ConnectionError is recoverable",
			err:         NewConnectionError("network down"),
			recoverable: true,
		},
		{
			name:        "AuthenticationError is not recoverable",
			err:         NewAuthenticationError("bad token"),
			recoverable: false,
		},
		{
			name:        "DuplicateIdentityError is not recoverable",
			err:         NewDuplicateIdentityError("ag.test.agent"),
			recoverable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRecoverable(tt.err)
			if result != tt.recoverable {
				t.Errorf("IsRecoverable() = %v, want %v", result, tt.recoverable)
			}
		})
	}
}

// =============================================================================
// Signal Type Conversion Tests
// =============================================================================

func TestConvertSignalType(t *testing.T) {
	tests := []struct {
		name     string
		pbType   pb.Signal_SignalType
		expected SignalType
	}{
		{
			name:     "FORCE_DISCONNECT",
			pbType:   pb.Signal_FORCE_DISCONNECT,
			expected: SignalForceDisconnect,
		},
		{
			name:     "Unknown type",
			pbType:   pb.Signal_SignalType(999),
			expected: SignalType(-1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertSignalType(tt.pbType)
			if result != tt.expected {
				t.Errorf("convertSignalType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Sleep With Context Tests
// =============================================================================

func TestBaseClient_SleepWithContext_Completes(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	ctx := context.Background()
	start := time.Now()

	completed := client.sleepWithContext(ctx, 100*time.Millisecond)

	elapsed := time.Since(start)
	if !completed {
		t.Error("sleepWithContext should return true when completed")
	}
	if elapsed < 90*time.Millisecond {
		t.Error("sleepWithContext should sleep for the specified duration")
	}
}

func TestBaseClient_SleepWithContext_ContextCanceled(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	completed := client.sleepWithContext(ctx, 1*time.Second)
	elapsed := time.Since(start)

	if completed {
		t.Error("sleepWithContext should return false when context is canceled")
	}
	if elapsed > 200*time.Millisecond {
		t.Error("sleepWithContext should return quickly when context is canceled")
	}
}

func TestBaseClient_SleepWithContext_ForceDisconnect(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	ctx := context.Background()

	// Set force disconnect after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		client.SetForceDisconnect(true)
	}()

	start := time.Now()
	completed := client.sleepWithContext(ctx, 1*time.Second)
	elapsed := time.Since(start)

	if completed {
		t.Error("sleepWithContext should return false when force disconnect is set")
	}
	// Should return within ~150ms (50ms delay + up to 100ms polling interval)
	if elapsed > 300*time.Millisecond {
		t.Errorf("sleepWithContext should return quickly when force disconnect is set, took %v", elapsed)
	}
}

// =============================================================================
// Context Tests
// =============================================================================

func TestBaseClient_Context(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Context should be nil before Connect
	if client.Context() != nil {
		t.Error("Context should be nil before Connect")
	}

	// Set up a mock context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.ctx = ctx

	if client.Context() != ctx {
		t.Error("Context should return the set context")
	}
}

// =============================================================================
// Handlers Default Behavior Tests
// =============================================================================

func TestNewHandlers_DefaultBehavior(t *testing.T) {
	handlers := NewHandlers()

	ctx := context.Background()

	// All handlers should return nil (no-op)
	if err := handlers.OnMessage(ctx, &Message{}); err != nil {
		t.Errorf("OnMessage default handler error = %v", err)
	}
	if err := handlers.OnConfig(ctx, &ConfigSnapshot{}); err != nil {
		t.Errorf("OnConfig default handler error = %v", err)
	}
	if err := handlers.OnSignal(ctx, &Signal{}); err != nil {
		t.Errorf("OnSignal default handler error = %v", err)
	}
	if err := handlers.OnError(ctx, &ErrorInfo{}); err != nil {
		t.Errorf("OnError default handler error = %v", err)
	}
	if err := handlers.OnKVResponse(ctx, &KVResponse{}); err != nil {
		t.Errorf("OnKVResponse default handler error = %v", err)
	}
	if err := handlers.OnCheckpointResponse(ctx, &CheckpointResponse{}); err != nil {
		t.Errorf("OnCheckpointResponse default handler error = %v", err)
	}
	if err := handlers.OnTaskAssignment(ctx, &TaskAssignment{}); err != nil {
		t.Errorf("OnTaskAssignment default handler error = %v", err)
	}
	if err := handlers.OnConnect(ctx, &ConnectionAck{}); err != nil {
		t.Errorf("OnConnect default handler error = %v", err)
	}
	if err := handlers.OnDisconnect(ctx, "test"); err != nil {
		t.Errorf("OnDisconnect default handler error = %v", err)
	}
	if err := handlers.OnReconnecting(ctx, 1); err != nil {
		t.Errorf("OnReconnecting default handler error = %v", err)
	}
}

// =============================================================================
// Server Address Tests
// =============================================================================

func TestBaseClient_ServerAddr(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: "custom-server:8080"}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	if client.ServerAddr() != "custom-server:8080" {
		t.Errorf("ServerAddr() = %q, want %q", client.ServerAddr(), "custom-server:8080")
	}
}

// =============================================================================
// Signal Type String Tests
// =============================================================================

func TestSignalType_String(t *testing.T) {
	tests := []struct {
		signal   SignalType
		expected string
	}{
		{SignalForceDisconnect, "FORCE_DISCONNECT"},
		{SignalType(-1), "UNKNOWN"},
		{SignalType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.signal.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// =============================================================================
// Clean Up For Reconnect Tests
// =============================================================================

func TestBaseClient_CleanupForReconnect(t *testing.T) {
	cfg := BaseClientConfig{
		ServerAddr: TestServerAddr,
		QueueSize:  10,
	}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Simulate a connected state
	client.running.Store(true)
	client.connected.Store(true)
	client.connectionConfirmed.Store(true)

	// Add some messages to the queue
	for i := 0; i < 3; i++ {
		msg := &pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_Send{
				Send: &pb.SendMessage{TargetTopic: "test"},
			},
		}
		_ = client.Send(msg)
	}

	// Cleanup
	client.cleanupForReconnect()

	// Verify state
	if client.connected.Load() {
		t.Error("connected should be false after cleanup")
	}

	// Queue should be empty
	select {
	case <-client.RequestQueue():
		t.Error("Request queue should be empty after cleanup")
	default:
		// Expected
	}
}
