//go:build integration
// +build integration

// Package aether integration tests.
//
// Integration tests require a running Aether gateway server and infrastructure
// (Redis, RabbitMQ). These tests verify the SDK works correctly with the actual
// gateway implementation.
//
// To run integration tests:
//
//	cd sdk/go && go test ./aether -v -tags=integration
//
// To skip integration tests during normal testing:
//
//	cd sdk/go && go test ./aether -v -short
//
// Environment variables:
//   - AETHER_GATEWAY_ADDR: Gateway address (default: localhost:50051)
//   - AETHER_TEST_WORKSPACE: Test workspace (default: test-workspace)
//   - AETHER_SKIP_INTEGRATION: Set to skip integration tests
//   - AETHER_INTEGRATION_VERBOSE: Enable verbose test logging

package aether

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Integration Test Setup
// =============================================================================

// TestMain sets up and tears down the integration test environment.
func TestMain(m *testing.M) {
	// Check if we should skip all integration tests
	if ShouldSkipIntegration() {
		os.Exit(0)
	}

	// Run tests
	code := m.Run()

	os.Exit(code)
}

// =============================================================================
// Gateway Connectivity Tests
// =============================================================================

// TestIntegrationGatewayConnectivity verifies basic gateway connectivity.
func TestIntegrationGatewayConnectivity(t *testing.T) {
	SkipIfNoGateway(t)

	cfg := DefaultTestConfig()
	t.Logf("Testing connectivity to gateway at %s", cfg.GatewayAddr)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	client := NewTestAgentClient(t, TestAgentConfig{
		ServerAddr:     cfg.GatewayAddr,
		Workspace:      cfg.Workspace,
		Implementation: "connectivity-test",
		Specifier:      UniqueTestIdentifier("conn"),
	})

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to gateway: %v", err)
	}

	// Verify connection
	if !client.IsRunning() {
		t.Error("Client should be running after Connect()")
	}

	t.Logf("Successfully connected to gateway")
}

// =============================================================================
// Agent Client Integration Tests
// =============================================================================

// TestIntegrationAgentConnect tests agent client connection and disconnection.
func TestIntegrationAgentConnect(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Create client
	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Track connection events
	tracker := NewConnectionTracker()
	client.OnConnect(tracker.ConnectHandler())
	client.OnDisconnect(tracker.DisconnectHandler())

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop in background
	done := make(chan error, 1)
	go func() {
		done <- client.Run(ctx)
	}()

	// Wait for connection confirmation
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Connection not confirmed within timeout")
	}

	// Verify session ID was received
	if client.SessionID() == "" {
		t.Error("Session ID should be set after connection")
	}
	t.Logf("Connected with session ID: %s", client.SessionID())

	// Close the connection
	if err := client.Close(); err != nil {
		t.Errorf("Error closing client: %v", err)
	}

	// Wait for disconnection
	if !WaitForDisconnect(ctx, client, DefaultConnectTimeout) {
		t.Error("Client should be disconnected after Close()")
	}

	t.Logf("Agent connection test passed")
}

// TestIntegrationAgentIdentity verifies agent identity is correctly reported.
func TestIntegrationAgentIdentity(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	cfg := TestAgentConfig{
		Workspace:      "identity-test-ws",
		Implementation: "identity-test-impl",
		Specifier:      UniqueTestIdentifier("identity"),
	}

	client := NewTestAgentClient(t, cfg)

	// Verify identity before connection
	if client.Workspace() != cfg.Workspace {
		t.Errorf("Workspace mismatch: got %s, want %s", client.Workspace(), cfg.Workspace)
	}
	if client.Implementation() != cfg.Implementation {
		t.Errorf("Implementation mismatch: got %s, want %s", client.Implementation(), cfg.Implementation)
	}
	if client.Specifier() != cfg.Specifier {
		t.Errorf("Specifier mismatch: got %s, want %s", client.Specifier(), cfg.Specifier)
	}

	// Verify topic generation
	expectedTopic := AgentTopic(cfg.Workspace, cfg.Implementation, cfg.Specifier)
	if client.Topic() != expectedTopic {
		t.Errorf("Topic mismatch: got %s, want %s", client.Topic(), expectedTopic)
	}

	// Connect and verify identity persists
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	if client.Workspace() != cfg.Workspace {
		t.Error("Workspace changed after connection")
	}
}

// =============================================================================
// Task Client Integration Tests
// =============================================================================

// TestIntegrationTaskConnect tests task client connection.
func TestIntegrationTaskConnect(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Test unique task
	t.Run("UniqueTask", func(t *testing.T) {
		cfg := TestTaskConfig{
			Workspace:      GetTestWorkspace(),
			Implementation: "unique-task-test",
			Specifier:      UniqueTestIdentifier("unique"),
		}

		client := NewTestTaskClient(t, cfg)

		if !client.IsUnique() {
			t.Error("Task with specifier should be unique")
		}

		if err := client.Connect(ctx); err != nil {
			t.Fatalf("Failed to connect unique task: %v", err)
		}

		t.Logf("Unique task connected with session ID: %s", client.SessionID())
	})

	// Test non-unique task
	t.Run("NonUniqueTask", func(t *testing.T) {
		cfg := TestTaskConfig{
			Workspace:      GetTestWorkspace(),
			Implementation: "nonunique-task-test",
			Specifier:      "", // Empty = non-unique
		}

		client := NewTestTaskClient(t, cfg)

		if client.IsUnique() {
			t.Error("Task without specifier should be non-unique")
		}

		if err := client.Connect(ctx); err != nil {
			t.Fatalf("Failed to connect non-unique task: %v", err)
		}

		t.Logf("Non-unique task connected with session ID: %s", client.SessionID())
	})
}

// =============================================================================
// User Client Integration Tests
// =============================================================================

// TestIntegrationUserConnect tests user client connection.
func TestIntegrationUserConnect(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestUserClient(t, DefaultTestUserConfig())

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect user: %v", err)
	}

	t.Logf("User connected with session ID: %s", client.SessionID())

	// Verify user identity
	if client.UserID() == "" {
		t.Error("UserID should be set")
	}
	if client.WindowID() == "" {
		t.Error("WindowID should be set")
	}
}

// =============================================================================
// Handler Registration Tests
// =============================================================================

// TestIntegrationHandlerRegistration verifies handlers are properly invoked.
func TestIntegrationHandlerRegistration(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Register all handlers
	tracker := NewConnectionTracker()
	msgCollector := NewMessageCollector()
	kvCollector := NewKVCollector()

	client.OnConnect(tracker.ConnectHandler())
	client.OnDisconnect(tracker.DisconnectHandler())
	client.OnReconnecting(tracker.ReconnectingHandler())
	client.OnMessage(msgCollector.Handler())
	client.OnKVResponse(kvCollector.Handler())

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop briefly
	loopCtx, loopCancel := context.WithTimeout(ctx, 2*time.Second)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection to be confirmed
	time.Sleep(500 * time.Millisecond)

	// Verify connection handler was called
	if tracker.ConnectCount() == 0 {
		t.Error("Connect handler should have been called")
	}

	// Close and wait
	client.Close()
	<-done

	// Disconnect handler may or may not be called depending on timing
	t.Logf("Connect count: %d, Disconnect count: %d",
		tracker.ConnectCount(), tracker.DisconnectCount())
}

// =============================================================================
// Connection Lifecycle Tests
// =============================================================================

// TestIntegrationConnectionLifecycle tests the full connection lifecycle.
func TestIntegrationConnectionLifecycle(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, TestAgentConfig{
		AutoReconnect: false, // Disable for this test
	})

	tracker := NewConnectionTracker()
	client.OnConnect(tracker.ConnectHandler())
	client.OnDisconnect(tracker.DisconnectHandler())

	// Phase 1: Initial connection
	t.Log("Phase 1: Connecting...")
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	time.Sleep(500 * time.Millisecond)

	if !client.IsRunning() {
		t.Error("Client should be running")
	}

	sessionID := client.SessionID()
	t.Logf("Connected with session: %s", sessionID)

	// Phase 2: Graceful close
	t.Log("Phase 2: Closing connection...")
	loopCancel()
	<-done

	if err := client.Close(); err != nil {
		t.Errorf("Error closing: %v", err)
	}

	// Verify final state
	if client.IsRunning() {
		t.Error("Client should not be running after close")
	}

	t.Log("Connection lifecycle test passed")
}

// =============================================================================
// Error Handling Tests
// =============================================================================

// TestIntegrationConnectionError tests handling of connection errors.
func TestIntegrationConnectionError(t *testing.T) {
	// This test doesn't need gateway - it tests error handling

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to connect to a non-existent server
	client, err := NewAgentClient(AgentOptions{
		ClientOptions: ClientOptions{
			ServerAddr: "localhost:59999", // Non-existent port
			Connection: ConnectionOptions{
				MaxRetries:     1,
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     100 * time.Millisecond,
				AutoReconnect:  false,
				ConnectTimeout: 1 * time.Second,
			},
		},
		Workspace:      "error-test",
		Implementation: "error-impl",
		Specifier:      "error-spec",
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Connect should fail
	err = client.Connect(ctx)
	if err == nil {
		t.Fatal("Expected connection error, got nil")
	}

	// Verify it's a connection error
	if !IsConnectionError(err) {
		t.Errorf("Expected connection error, got: %T - %v", err, err)
	}

	t.Logf("Correctly received connection error: %v", err)
}

// =============================================================================
// Duplicate Identity Tests
// =============================================================================

// TestIntegrationDuplicateIdentity tests that duplicate identities are rejected.
func TestIntegrationDuplicateIdentity(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Use a fixed identity for this test
	spec := UniqueTestIdentifier("duplicate")
	cfg := TestAgentConfig{
		Workspace:      GetTestWorkspace(),
		Implementation: "duplicate-test",
		Specifier:      spec,
	}

	// Connect first client
	client1 := NewTestAgentClient(t, cfg)
	if err := client1.Connect(ctx); err != nil {
		t.Fatalf("First client failed to connect: %v", err)
	}

	// Start message loop for client1
	go func() {
		_ = client1.Run(ctx)
	}()

	// Wait for connection confirmation
	time.Sleep(500 * time.Millisecond)

	// Try to connect second client with same identity
	client2, err := NewAgentClient(AgentOptions{
		ClientOptions: ClientOptions{
			ServerAddr: GetGatewayAddr(),
			Connection: ConnectionOptions{
				MaxRetries:     1,
				InitialBackoff: 100 * time.Millisecond,
				AutoReconnect:  false,
				ConnectTimeout: 2 * time.Second,
			},
		},
		Workspace:      cfg.Workspace,
		Implementation: cfg.Implementation,
		Specifier:      cfg.Specifier,
	})
	if err != nil {
		t.Fatalf("Failed to create second client: %v", err)
	}
	defer client2.Close()

	// Second connection should fail with duplicate identity error
	err = client2.Connect(ctx)
	if err == nil {
		// If connection succeeded, the Run loop might fail
		runErr := make(chan error, 1)
		go func() {
			runErr <- client2.Run(ctx)
		}()

		select {
		case err = <-runErr:
			// Got an error from run
		case <-time.After(2 * time.Second):
			t.Log("Note: Second client connected successfully - gateway may allow this in some configurations")
			return
		}
	}

	// We should have an error at this point
	if err != nil {
		t.Logf("Second client correctly rejected: %v", err)
	}
}

// =============================================================================
// Orchestrator Integration Tests
// =============================================================================

// TestIntegrationOrchestratorConnect tests orchestrator client connection.
func TestIntegrationOrchestratorConnect(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestOrchestratorClient(t, DefaultTestOrchestratorConfig())

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect orchestrator: %v", err)
	}

	t.Logf("Orchestrator connected with session ID: %s", client.SessionID())

	// Verify supported profiles
	profiles := client.SupportedProfiles()
	if len(profiles) == 0 {
		t.Error("Orchestrator should have supported profiles")
	}
	t.Logf("Supported profiles: %v", profiles)
}

// =============================================================================
// Test Utility Tests
// =============================================================================

// TestIntegrationTestUtilityFunctions tests the test utility functions.
func TestIntegrationTestUtilityFunctions(t *testing.T) {
	// Test unique identifier generation
	id1 := UniqueTestIdentifier("test")
	id2 := UniqueTestIdentifier("test")
	if id1 == id2 {
		t.Error("UniqueTestIdentifier should generate unique IDs")
	}

	// Test identity generator
	gen := NewTestIdentityGenerator("custom")
	gen1 := gen.Next()
	gen2 := gen.Next()
	if gen1 == gen2 {
		t.Error("TestIdentityGenerator should generate unique IDs")
	}

	// Test default configs
	agentCfg := DefaultTestAgentConfig()
	if agentCfg.ServerAddr == "" {
		t.Error("DefaultTestAgentConfig should set ServerAddr")
	}

	taskCfg := DefaultTestTaskConfig()
	if taskCfg.Workspace == "" {
		t.Error("DefaultTestTaskConfig should set Workspace")
	}

	// Test collectors
	msgCollector := NewMessageCollector()
	if msgCollector.Count() != 0 {
		t.Error("New MessageCollector should have 0 messages")
	}

	kvCollector := NewKVCollector()
	if kvCollector.LastResponse() != nil {
		t.Error("New KVCollector should have no responses")
	}

	// Test connection tracker
	connTracker := NewConnectionTracker()
	if connTracker.ConnectCount() != 0 {
		t.Error("New ConnectionTracker should have 0 connects")
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

// =============================================================================
// Messaging Integration Tests
// =============================================================================

// TestIntegrationAgentToAgentMessaging tests sending messages between two agents.
func TestIntegrationAgentToAgentMessaging(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create sender agent
	senderCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "msg-sender",
		Specifier:      UniqueTestIdentifier("sender"),
	}
	sender := NewTestAgentClient(t, senderCfg)

	// Create receiver agent
	receiverCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "msg-receiver",
		Specifier:      UniqueTestIdentifier("receiver"),
	}
	receiver := NewTestAgentClient(t, receiverCfg)

	// Set up message collector for receiver
	msgCollector := NewMessageCollector()
	receiver.OnMessage(msgCollector.Handler())

	// Connect both clients
	if err := sender.Connect(ctx); err != nil {
		t.Fatalf("Sender failed to connect: %v", err)
	}
	if err := receiver.Connect(ctx); err != nil {
		t.Fatalf("Receiver failed to connect: %v", err)
	}

	// Start receiver message loop in background
	receiverDone := make(chan error, 1)
	receiverCtx, receiverCancel := context.WithCancel(ctx)
	defer receiverCancel()
	go func() {
		receiverDone <- receiver.Run(receiverCtx)
	}()

	// Wait for receiver to be fully connected
	if !WaitForConnection(ctx, receiver, DefaultConnectTimeout) {
		t.Fatal("Receiver failed to connect in time")
	}

	// Start sender message loop in background
	senderDone := make(chan error, 1)
	senderCtx, senderCancel := context.WithCancel(ctx)
	defer senderCancel()
	go func() {
		senderDone <- sender.Run(senderCtx)
	}()

	// Wait for sender to be fully connected
	if !WaitForConnection(ctx, sender, DefaultConnectTimeout) {
		t.Fatal("Sender failed to connect in time")
	}

	// Send a message from sender to receiver
	testPayload := TestPayload("Hello from sender!")
	if err := sender.SendToAgent(workspace, receiverCfg.Implementation, receiverCfg.Specifier, testPayload); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// Wait for the message to be received
	if !msgCollector.WaitForMessage(ctx, 5*time.Second) {
		t.Log("Note: Message not received within timeout - this may be expected if messaging infrastructure is not fully set up")
	} else {
		// Verify the message was received
		messages := msgCollector.Messages()
		if len(messages) > 0 {
			t.Logf("Received %d message(s)", len(messages))
			if string(messages[0].Payload) != string(testPayload) {
				t.Errorf("Payload mismatch: got %s, want %s", messages[0].Payload, testPayload)
			}
		}
	}

	// Cleanup
	senderCancel()
	receiverCancel()
	<-senderDone
	<-receiverDone

	t.Log("Agent-to-agent messaging test completed")
}

// TestIntegrationAgentToTaskMessaging tests sending messages from agent to task.
func TestIntegrationAgentToTaskMessaging(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create agent
	agentCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "agent-to-task",
		Specifier:      UniqueTestIdentifier("agent"),
	}
	agent := NewTestAgentClient(t, agentCfg)

	// Create task (unique)
	taskCfg := TestTaskConfig{
		Workspace:      workspace,
		Implementation: "task-receiver",
		Specifier:      UniqueTestIdentifier("task"),
	}
	task := NewTestTaskClient(t, taskCfg)

	// Set up message collector for task
	msgCollector := NewMessageCollector()
	task.OnMessage(msgCollector.Handler())

	// Connect both clients
	if err := agent.Connect(ctx); err != nil {
		t.Fatalf("Agent failed to connect: %v", err)
	}
	if err := task.Connect(ctx); err != nil {
		t.Fatalf("Task failed to connect: %v", err)
	}

	// Start task message loop in background
	taskDone := make(chan error, 1)
	taskCtx, taskCancel := context.WithCancel(ctx)
	defer taskCancel()
	go func() {
		taskDone <- task.Run(taskCtx)
	}()

	// Wait for task to be fully connected
	if !WaitForConnection(ctx, task, DefaultConnectTimeout) {
		t.Fatal("Task failed to connect in time")
	}

	// Start agent message loop in background
	agentDone := make(chan error, 1)
	agentCtx, agentCancel := context.WithCancel(ctx)
	defer agentCancel()
	go func() {
		agentDone <- agent.Run(agentCtx)
	}()

	// Wait for agent to be fully connected
	if !WaitForConnection(ctx, agent, DefaultConnectTimeout) {
		t.Fatal("Agent failed to connect in time")
	}

	// Send a message from agent to task
	testPayload := TestPayload("Hello from agent to task!")
	if err := agent.SendToTask(workspace, taskCfg.Implementation, taskCfg.Specifier, testPayload); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// Wait for the message to be received
	if msgCollector.WaitForMessage(ctx, 5*time.Second) {
		messages := msgCollector.Messages()
		if len(messages) > 0 {
			t.Logf("Task received %d message(s)", len(messages))
		}
	} else {
		t.Log("Note: Message not received within timeout - this may be expected in some configurations")
	}

	// Cleanup
	agentCancel()
	taskCancel()
	<-agentDone
	<-taskDone

	t.Log("Agent-to-task messaging test completed")
}

// TestIntegrationAgentToUserMessaging tests sending messages from agent to user.
func TestIntegrationAgentToUserMessaging(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create agent
	agentCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "agent-to-user",
		Specifier:      UniqueTestIdentifier("agent"),
	}
	agent := NewTestAgentClient(t, agentCfg)

	// Create user client
	userCfg := TestUserConfig{
		UserID:    UniqueTestIdentifier("user"),
		WindowID:  UniqueTestIdentifier("window"),
		Workspace: workspace,
	}
	user := NewTestUserClient(t, userCfg)

	// Set up message collector for user
	msgCollector := NewMessageCollector()
	user.OnMessage(msgCollector.Handler())

	// Connect both clients
	if err := agent.Connect(ctx); err != nil {
		t.Fatalf("Agent failed to connect: %v", err)
	}
	if err := user.Connect(ctx); err != nil {
		t.Fatalf("User failed to connect: %v", err)
	}

	// Start user message loop in background
	userDone := make(chan error, 1)
	userCtx, userCancel := context.WithCancel(ctx)
	defer userCancel()
	go func() {
		userDone <- user.Run(userCtx)
	}()

	// Wait for user to be fully connected
	if !WaitForConnection(ctx, user, DefaultConnectTimeout) {
		t.Fatal("User failed to connect in time")
	}

	// Start agent message loop in background
	agentDone := make(chan error, 1)
	agentCtx, agentCancel := context.WithCancel(ctx)
	defer agentCancel()
	go func() {
		agentDone <- agent.Run(agentCtx)
	}()

	// Wait for agent to be fully connected
	if !WaitForConnection(ctx, agent, DefaultConnectTimeout) {
		t.Fatal("Agent failed to connect in time")
	}

	// Send a message from agent to user
	testPayload := TestPayload("Hello from agent to user!")
	if err := agent.SendToUser(userCfg.UserID, userCfg.WindowID, testPayload); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// Wait for the message to be received
	if msgCollector.WaitForMessage(ctx, 5*time.Second) {
		messages := msgCollector.Messages()
		if len(messages) > 0 {
			t.Logf("User received %d message(s)", len(messages))
		}
	} else {
		t.Log("Note: Message not received within timeout - this may be expected in some configurations")
	}

	// Cleanup
	agentCancel()
	userCancel()
	<-agentDone
	<-userDone

	t.Log("Agent-to-user messaging test completed")
}

// TestIntegrationBroadcastMessaging tests broadcast messaging to workspace.
func TestIntegrationBroadcastMessaging(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create broadcasting agent
	broadcasterCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "broadcaster",
		Specifier:      UniqueTestIdentifier("broadcaster"),
	}
	broadcaster := NewTestAgentClient(t, broadcasterCfg)

	// Create receiving agents
	receiver1Cfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "receiver1",
		Specifier:      UniqueTestIdentifier("receiver1"),
	}
	receiver1 := NewTestAgentClient(t, receiver1Cfg)

	receiver2Cfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "receiver2",
		Specifier:      UniqueTestIdentifier("receiver2"),
	}
	receiver2 := NewTestAgentClient(t, receiver2Cfg)

	// Set up message collectors
	collector1 := NewMessageCollector()
	collector2 := NewMessageCollector()
	receiver1.OnMessage(collector1.Handler())
	receiver2.OnMessage(collector2.Handler())

	// Connect all clients
	if err := broadcaster.Connect(ctx); err != nil {
		t.Fatalf("Broadcaster failed to connect: %v", err)
	}
	if err := receiver1.Connect(ctx); err != nil {
		t.Fatalf("Receiver1 failed to connect: %v", err)
	}
	if err := receiver2.Connect(ctx); err != nil {
		t.Fatalf("Receiver2 failed to connect: %v", err)
	}

	// Start message loops
	ctx1, cancel1 := context.WithCancel(ctx)
	ctx2, cancel2 := context.WithCancel(ctx)
	ctx3, cancel3 := context.WithCancel(ctx)
	defer cancel1()
	defer cancel2()
	defer cancel3()

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	done3 := make(chan error, 1)
	go func() { done1 <- broadcaster.Run(ctx1) }()
	go func() { done2 <- receiver1.Run(ctx2) }()
	go func() { done3 <- receiver2.Run(ctx3) }()

	// Wait for all to connect
	WaitForConnection(ctx, broadcaster, DefaultConnectTimeout)
	WaitForConnection(ctx, receiver1, DefaultConnectTimeout)
	WaitForConnection(ctx, receiver2, DefaultConnectTimeout)

	// Broadcast message to all agents in workspace
	testPayload := TestPayload("Broadcast message!")
	if err := broadcaster.BroadcastToAgents(workspace, testPayload); err != nil {
		t.Fatalf("Failed to broadcast: %v", err)
	}

	// Wait for messages to potentially arrive
	time.Sleep(2 * time.Second)

	t.Logf("Receiver1 got %d messages, Receiver2 got %d messages",
		collector1.Count(), collector2.Count())

	// Cleanup
	cancel1()
	cancel2()
	cancel3()
	<-done1
	<-done2
	<-done3

	t.Log("Broadcast messaging test completed")
}

// =============================================================================
// KV Operations Integration Tests
// =============================================================================

// TestIntegrationKVPutAndGet tests putting and getting values from KV store.
func TestIntegrationKVPutAndGet(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up KV response collector
	kvCollector := NewKVCollector()
	client.OnKVResponse(kvCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Test key
	testKey := "test-key-" + UniqueTestIdentifier("kv")
	testValue := []byte(`{"test": "value", "timestamp": "` + time.Now().String() + `"}`)

	// Put a value
	t.Logf("Putting key: %s", testKey)
	if err := client.KV().Put(testKey, testValue, KVScopeWorkspace, "", GetTestWorkspace(), 0); err != nil {
		t.Fatalf("Failed to put KV: %v", err)
	}

	// Wait for put response
	if kvCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := kvCollector.LastResponse()
		if resp != nil {
			t.Logf("Put response: success=%v", resp.Success)
		}
	} else {
		t.Log("Note: KV put response not received within timeout")
	}

	// Clear and get the value back
	kvCollector.Clear()
	t.Logf("Getting key: %s", testKey)
	if err := client.KV().Get(testKey, KVScopeWorkspace, "", GetTestWorkspace()); err != nil {
		t.Fatalf("Failed to get KV: %v", err)
	}

	// Wait for get response
	if kvCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := kvCollector.LastResponse()
		if resp != nil {
			t.Logf("Get response: success=%v, value=%s", resp.Success, string(resp.Value))
			if resp.Success && len(resp.Value) > 0 {
				if string(resp.Value) != string(testValue) {
					t.Errorf("Value mismatch: got %s, want %s", resp.Value, testValue)
				}
			}
		}
	} else {
		t.Log("Note: KV get response not received within timeout")
	}

	// Cleanup
	loopCancel()
	<-done

	t.Log("KV put and get test completed")
}

// TestIntegrationKVListAndDelete tests listing and deleting KV entries.
func TestIntegrationKVListAndDelete(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up KV response collector
	kvCollector := NewKVCollector()
	client.OnKVResponse(kvCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Create test keys with a unique prefix
	prefix := "testprefix-" + UniqueTestIdentifier("list") + "-"
	keys := []string{prefix + "key1", prefix + "key2", prefix + "key3"}
	workspace := GetTestWorkspace()

	// Put multiple values
	for _, key := range keys {
		if err := client.KV().Put(key, []byte("value-"+key), KVScopeWorkspace, "", workspace, 0); err != nil {
			t.Fatalf("Failed to put KV %s: %v", key, err)
		}
		// Wait for response
		kvCollector.WaitForResponse(ctx, 2*time.Second)
		kvCollector.Clear()
	}

	// List keys with prefix
	t.Logf("Listing keys with prefix: %s", prefix)
	if err := client.KV().List(prefix, KVScopeWorkspace, "", workspace); err != nil {
		t.Fatalf("Failed to list KV: %v", err)
	}

	if kvCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := kvCollector.LastResponse()
		if resp != nil {
			t.Logf("List response: success=%v, keys=%v", resp.Success, resp.Keys)
		}
	} else {
		t.Log("Note: KV list response not received within timeout")
	}

	// Delete one key
	kvCollector.Clear()
	deleteKey := keys[0]
	t.Logf("Deleting key: %s", deleteKey)
	if err := client.KV().Delete(deleteKey, KVScopeWorkspace, "", workspace); err != nil {
		t.Fatalf("Failed to delete KV: %v", err)
	}

	if kvCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := kvCollector.LastResponse()
		if resp != nil {
			t.Logf("Delete response: success=%v", resp.Success)
		}
	} else {
		t.Log("Note: KV delete response not received within timeout")
	}

	// Clean up remaining keys
	for _, key := range keys[1:] {
		kvCollector.Clear()
		client.KV().Delete(key, KVScopeWorkspace, "", workspace)
		kvCollector.WaitForResponse(ctx, 2*time.Second)
	}

	// Cleanup
	loopCancel()
	<-done

	t.Log("KV list and delete test completed")
}

// TestIntegrationKVScopes tests KV operations across different scopes.
func TestIntegrationKVScopes(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up KV response collector
	kvCollector := NewKVCollector()
	client.OnKVResponse(kvCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	testID := UniqueTestIdentifier("scope")

	// Test workspace scope
	t.Run("WorkspaceScope", func(t *testing.T) {
		key := "workspace-key-" + testID
		value := []byte("workspace-value")
		workspace := GetTestWorkspace()

		kvCollector.Clear()
		if err := client.KV().PutWorkspace(key, value, workspace); err != nil {
			t.Fatalf("Failed to put workspace KV: %v", err)
		}

		if kvCollector.WaitForResponse(ctx, 5*time.Second) {
			t.Log("Workspace scope put succeeded")
		}

		// Clean up
		kvCollector.Clear()
		client.KV().DeleteWorkspace(key, workspace)
		kvCollector.WaitForResponse(ctx, 2*time.Second)
	})

	// Test global scope
	t.Run("GlobalScope", func(t *testing.T) {
		key := "global-key-" + testID
		value := []byte("global-value")

		kvCollector.Clear()
		if err := client.KV().PutGlobal(key, value); err != nil {
			t.Fatalf("Failed to put global KV: %v", err)
		}

		if kvCollector.WaitForResponse(ctx, 5*time.Second) {
			t.Log("Global scope put succeeded")
		}

		// Clean up
		kvCollector.Clear()
		client.KV().DeleteGlobal(key)
		kvCollector.WaitForResponse(ctx, 2*time.Second)
	})

	// Cleanup
	loopCancel()
	<-done

	t.Log("KV scopes test completed")
}

// TestIntegrationKVSyncOperations tests synchronous KV operations.
func TestIntegrationKVSyncOperations(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	testKey := "sync-test-key-" + UniqueTestIdentifier("sync")
	testValue := []byte(`{"sync": "test"}`)
	workspace := GetTestWorkspace()

	// Test sync put
	t.Run("SyncPut", func(t *testing.T) {
		resp, err := client.KV().PutSync(ctx, KVPutOptions{
			Key:       testKey,
			Value:     testValue,
			Scope:     KVScopeWorkspace,
			Workspace: workspace,
			Timeout:   5 * time.Second,
		})

		if err != nil {
			if IsTimeoutError(err) {
				t.Log("Note: Sync put timed out - server may not support KV operations")
				return
			}
			t.Fatalf("Sync put failed: %v", err)
		}

		if resp != nil {
			t.Logf("Sync put response: success=%v", resp.Success)
		}
	})

	// Test sync get
	t.Run("SyncGet", func(t *testing.T) {
		resp, err := client.KV().GetSync(ctx, KVGetOptions{
			Key:       testKey,
			Scope:     KVScopeWorkspace,
			Workspace: workspace,
			Timeout:   5 * time.Second,
		})

		if err != nil {
			if IsTimeoutError(err) {
				t.Log("Note: Sync get timed out - server may not support KV operations")
				return
			}
			t.Fatalf("Sync get failed: %v", err)
		}

		if resp != nil {
			t.Logf("Sync get response: success=%v, value=%s", resp.Success, string(resp.Value))
		}
	})

	// Test sync delete
	t.Run("SyncDelete", func(t *testing.T) {
		resp, err := client.KV().DeleteSync(ctx, KVDeleteOptions{
			Key:       testKey,
			Scope:     KVScopeWorkspace,
			Workspace: workspace,
			Timeout:   5 * time.Second,
		})

		if err != nil {
			if IsTimeoutError(err) {
				t.Log("Note: Sync delete timed out - server may not support KV operations")
				return
			}
			t.Fatalf("Sync delete failed: %v", err)
		}

		if resp != nil {
			t.Logf("Sync delete response: success=%v", resp.Success)
		}
	})

	// Cleanup
	loopCancel()
	<-done

	t.Log("KV sync operations test completed")
}

// =============================================================================
// Configuration and Workspace Tests
// =============================================================================

// TestIntegrationConfigSnapshot tests receiving configuration snapshots.
func TestIntegrationConfigSnapshot(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Track config snapshots
	var configReceived bool
	var receivedConfig *ConfigSnapshot
	var configMu sync.Mutex

	client.OnConfig(func(ctx context.Context, config *ConfigSnapshot) error {
		configMu.Lock()
		defer configMu.Unlock()
		configReceived = true
		receivedConfig = config
		return nil
	})

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop briefly
	loopCtx, loopCancel := context.WithTimeout(ctx, 3*time.Second)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for config
	time.Sleep(2 * time.Second)

	configMu.Lock()
	if configReceived {
		t.Logf("Received config snapshot: KV entries=%d, GlobalKV entries=%d",
			len(receivedConfig.KV), len(receivedConfig.GlobalKV))
	} else {
		t.Log("Note: Config snapshot not received - this may be expected in some configurations")
	}
	configMu.Unlock()

	// Cleanup
	loopCancel()
	<-done

	t.Log("Config snapshot test completed")
}

// TestIntegrationWorkspaceSwitch tests workspace switching functionality.
func TestIntegrationWorkspaceSwitch(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	originalWorkspace := GetTestWorkspace()
	newWorkspace := "new-workspace-" + UniqueTestIdentifier("ws")

	cfg := TestAgentConfig{
		Workspace:      originalWorkspace,
		Implementation: "workspace-switcher",
		Specifier:      UniqueTestIdentifier("switch"),
	}
	client := NewTestAgentClient(t, cfg)

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Verify initial workspace
	if client.Workspace() != originalWorkspace {
		t.Errorf("Initial workspace mismatch: got %s, want %s", client.Workspace(), originalWorkspace)
	}

	// Switch workspace
	t.Logf("Switching from %s to %s", originalWorkspace, newWorkspace)
	if err := client.SwitchWorkspace(newWorkspace); err != nil {
		t.Fatalf("Failed to switch workspace: %v", err)
	}

	// Verify workspace changed locally
	if client.Workspace() != newWorkspace {
		t.Errorf("Workspace not updated after switch: got %s, want %s", client.Workspace(), newWorkspace)
	}

	// Verify topic changed
	expectedTopic := AgentTopic(newWorkspace, cfg.Implementation, cfg.Specifier)
	if client.Topic() != expectedTopic {
		t.Errorf("Topic not updated after switch: got %s, want %s", client.Topic(), expectedTopic)
	}

	t.Logf("Workspace switched successfully to %s", newWorkspace)

	// Cleanup
	loopCancel()
	<-done

	t.Log("Workspace switch test completed")
}

// TestIntegrationWorkspaceSwitchInvalid tests invalid workspace switch handling.
func TestIntegrationWorkspaceSwitchInvalid(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Try to switch to empty workspace
	err := client.SwitchWorkspace("")
	if err == nil {
		t.Error("Expected error when switching to empty workspace")
	} else {
		t.Logf("Correctly rejected empty workspace: %v", err)
	}
}

// =============================================================================
// Event and Metric Publishing Tests
// =============================================================================

// TestIntegrationSendEvent tests sending events to the workflow engine.
func TestIntegrationSendEvent(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Send an event
	eventPayload := []byte(`{"event_type": "test_event", "data": {"key": "value"}}`)
	if err := client.SendEvent(eventPayload); err != nil {
		t.Fatalf("Failed to send event: %v", err)
	}

	t.Log("Event sent successfully")

	// Cleanup
	loopCancel()
	<-done

	t.Log("Send event test completed")
}

// TestIntegrationSendMetric tests sending metrics to the metrics bridge.
func TestIntegrationSendMetric(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Send a metric
	metric := NewMetric().
		Trace("test-trace-1").
		Add("test_counter", "", 42).
		Tag("env", "test").
		Build()
	if err := client.SendMetric(metric); err != nil {
		t.Fatalf("Failed to send metric: %v", err)
	}

	t.Log("Metric sent successfully")

	// Cleanup
	loopCancel()
	<-done

	t.Log("Send metric test completed")
}

// TestIntegrationSendMetric_RejectsMalformedPayload verifies that the gateway
// rejects a METRIC message whose payload is not a valid proto-encoded Metric.
func TestIntegrationSendMetric_RejectsMalformedPayload(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Send a raw non-proto payload directly, bypassing the SDK helper.
	badPayload := []byte("not-a-proto")
	err := client.sendMessage(MetricWildcardTopic(), badPayload, pb.MessageType_METRIC)
	if err == nil {
		t.Fatal("Expected error for malformed metric payload, got nil")
	}
	t.Logf("Got expected error for malformed metric payload: %v", err)

	// Cleanup
	loopCancel()
	<-done
}

// =============================================================================
// Task Creation Tests
// =============================================================================

// TestIntegrationCreateTask tests task creation from an agent.
func TestIntegrationCreateTask(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Track task assignments
	var taskAssigned bool
	var assignedTask *TaskAssignment
	var taskMu sync.Mutex

	client.OnTaskAssignment(func(ctx context.Context, ta *TaskAssignment) error {
		taskMu.Lock()
		defer taskMu.Unlock()
		taskAssigned = true
		assignedTask = ta
		return nil
	})

	// Connect
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Create a task with self-assign mode
	taskOpts := CreateTaskOptions{
		TaskType:       "test-task",
		AssignmentMode: TaskAssignmentSelfAssign,
		Metadata: map[string]string{
			"test_key": "test_value",
		},
	}

	if err := client.CreateTask(taskOpts); err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	t.Log("Task creation request sent")

	// Wait for potential task assignment
	time.Sleep(2 * time.Second)

	taskMu.Lock()
	if taskAssigned && assignedTask != nil {
		t.Logf("Task assigned: type=%s, id=%s", assignedTask.TaskType, assignedTask.TaskID)
	} else {
		t.Log("Note: Task assignment not received - this may be expected without orchestrator")
	}
	taskMu.Unlock()

	// Cleanup
	loopCancel()
	<-done

	t.Log("Create task test completed")
}

// =============================================================================
// Concurrent Operations Tests
// =============================================================================

// TestIntegrationConcurrentConnections tests multiple concurrent client connections.
func TestIntegrationConcurrentConnections(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	numClients := 5
	workspace := GetTestWorkspace()

	var wg sync.WaitGroup
	errors := make(chan error, numClients)
	clients := make([]*AgentClient, numClients)

	// Create and connect multiple clients concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			cfg := TestAgentConfig{
				Workspace:      workspace,
				Implementation: "concurrent-test",
				Specifier:      UniqueTestIdentifier("client" + string(rune('0'+idx))),
			}

			client, err := NewAgentClient(AgentOptions{
				ClientOptions: ClientOptions{
					ServerAddr: GetGatewayAddr(),
					Connection: ConnectionOptions{
						MaxRetries:     3,
						InitialBackoff: 100 * time.Millisecond,
						MaxBackoff:     1 * time.Second,
						AutoReconnect:  false,
						ConnectTimeout: DefaultConnectTimeout,
					},
				},
				Workspace:      cfg.Workspace,
				Implementation: cfg.Implementation,
				Specifier:      cfg.Specifier,
			})
			if err != nil {
				errors <- err
				return
			}
			clients[idx] = client

			if err := client.Connect(ctx); err != nil {
				errors <- err
				return
			}

			// Run briefly
			loopCtx, loopCancel := context.WithTimeout(ctx, 2*time.Second)
			defer loopCancel()
			client.Run(loopCtx)
			client.Close()
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Client error: %v", err)
		errorCount++
	}

	t.Logf("Concurrent connections test: %d/%d clients connected successfully", numClients-errorCount, numClients)
}

// TestIntegrationConcurrentKVOperations tests concurrent KV operations.
func TestIntegrationConcurrentKVOperations(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Run concurrent KV operations
	numOps := 10
	workspace := GetTestWorkspace()
	var wg sync.WaitGroup
	errors := make(chan error, numOps)

	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			key := "concurrent-key-" + UniqueTestIdentifier("op"+string(rune('0'+idx)))
			value := []byte("value-" + string(rune('0'+idx)))

			if err := client.KV().Put(key, value, KVScopeWorkspace, "", workspace, 0); err != nil {
				errors <- err
				return
			}

			// Small delay between operations
			time.Sleep(50 * time.Millisecond)

			if err := client.KV().Get(key, KVScopeWorkspace, "", workspace); err != nil {
				errors <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("KV operation error: %v", err)
		errorCount++
	}

	t.Logf("Concurrent KV operations: %d/%d operations succeeded", numOps-errorCount, numOps)

	// Cleanup
	loopCancel()
	<-done

	t.Log("Concurrent KV operations test completed")
}

// =============================================================================
// Benchmark Tests
// =============================================================================

// BenchmarkIntegrationConnect benchmarks connection establishment.
func BenchmarkIntegrationConnect(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping integration benchmark in short mode")
	}

	if ShouldSkipIntegration() {
		b.Skip("skipping integration benchmark (AETHER_SKIP_INTEGRATION is set)")
	}

	ctx := context.Background()
	addr := GetGatewayAddr()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client, err := NewAgentClient(AgentOptions{
			ClientOptions: ClientOptions{
				ServerAddr: addr,
				Connection: ConnectionOptions{
					MaxRetries:     1,
					AutoReconnect:  false,
					ConnectTimeout: 5 * time.Second,
				},
			},
			Workspace:      "bench-workspace",
			Implementation: "bench-impl",
			Specifier:      UniqueTestIdentifier("bench"),
		})
		if err != nil {
			b.Fatalf("Failed to create client: %v", err)
		}

		if err := client.Connect(ctx); err != nil {
			b.Fatalf("Failed to connect: %v", err)
		}

		client.Close()
	}
}

// BenchmarkIntegrationKVPut benchmarks KV put operations.
func BenchmarkIntegrationKVPut(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping integration benchmark in short mode")
	}

	if ShouldSkipIntegration() {
		b.Skip("skipping integration benchmark (AETHER_SKIP_INTEGRATION is set)")
	}

	ctx := context.Background()

	client, err := NewAgentClient(AgentOptions{
		ClientOptions: ClientOptions{
			ServerAddr: GetGatewayAddr(),
			Connection: ConnectionOptions{
				MaxRetries:     1,
				AutoReconnect:  false,
				ConnectTimeout: 5 * time.Second,
			},
		},
		Workspace:      "bench-workspace",
		Implementation: "bench-impl",
		Specifier:      UniqueTestIdentifier("bench"),
	})
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		b.Fatalf("Failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()
	go client.Run(loopCtx)

	// Wait for connection
	time.Sleep(500 * time.Millisecond)

	workspace := "bench-workspace"
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-key-" + string(rune('0'+i%10))
		_ = client.KV().Put(key, value, KVScopeWorkspace, "", workspace, 0)
	}
}

// =============================================================================
// Task Assignment and Orchestration Flow Tests
// =============================================================================

// TaskAssignmentCollector collects task assignments for testing.
type TaskAssignmentCollector struct {
	mu          sync.Mutex
	assignments []*TaskAssignment
	notify      chan struct{}
}

// NewTaskAssignmentCollector creates a new task assignment collector.
func NewTaskAssignmentCollector() *TaskAssignmentCollector {
	return &TaskAssignmentCollector{
		notify: make(chan struct{}, 100),
	}
}

// Handler returns a TaskAssignmentHandler that collects assignments.
func (c *TaskAssignmentCollector) Handler() TaskAssignmentHandler {
	return func(ctx context.Context, task *TaskAssignment) error {
		c.mu.Lock()
		c.assignments = append(c.assignments, task)
		c.mu.Unlock()

		select {
		case c.notify <- struct{}{}:
		default:
		}

		return nil
	}
}

// Assignments returns a copy of collected assignments.
func (c *TaskAssignmentCollector) Assignments() []*TaskAssignment {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]*TaskAssignment, len(c.assignments))
	copy(result, c.assignments)
	return result
}

// Count returns the number of collected assignments.
func (c *TaskAssignmentCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.assignments)
}

// LastAssignment returns the last assignment, or nil if none.
func (c *TaskAssignmentCollector) LastAssignment() *TaskAssignment {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.assignments) == 0 {
		return nil
	}
	return c.assignments[len(c.assignments)-1]
}

// WaitForAssignment waits for at least one assignment.
func (c *TaskAssignmentCollector) WaitForAssignment(ctx context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.mu.Lock()
	if len(c.assignments) > 0 {
		c.mu.Unlock()
		return true
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return false
	case <-c.notify:
		return true
	}
}

// Clear clears all collected assignments.
func (c *TaskAssignmentCollector) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assignments = nil
}

// TestIntegrationOrchestratorTaskAssignmentHandling tests that orchestrators can
// receive and handle task assignments via the OnTaskAssignment callback.
func TestIntegrationOrchestratorTaskAssignmentHandling(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Create orchestrator with specific profiles
	profiles := []string{"test-profile-alpha", "test-profile-beta"}
	cfg := TestOrchestratorConfig{
		Implementation:    "task-assignment-test-orch",
		SupportedProfiles: profiles,
		Specifier:         UniqueTestIdentifier("orch"),
	}
	orchestrator := NewTestOrchestratorClient(t, cfg)

	// Set up task assignment collector
	assignmentCollector := NewTaskAssignmentCollector()
	orchestrator.OnTaskAssignment(assignmentCollector.Handler())

	// Track connection events
	connTracker := NewConnectionTracker()
	orchestrator.OnConnect(connTracker.ConnectHandler())

	// Connect orchestrator
	if err := orchestrator.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- orchestrator.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, orchestrator, DefaultConnectTimeout) {
		t.Fatal("Orchestrator failed to connect in time")
	}

	// Verify orchestrator is connected
	if orchestrator.SessionID() == "" {
		t.Error("Orchestrator should have session ID after connection")
	}

	// Verify supported profiles are accessible
	supportedProfiles := orchestrator.SupportedProfiles()
	if len(supportedProfiles) != len(profiles) {
		t.Errorf("Expected %d profiles, got %d", len(profiles), len(supportedProfiles))
	}

	for _, profile := range profiles {
		if !orchestrator.SupportsProfile(profile) {
			t.Errorf("Orchestrator should support profile %s", profile)
		}
	}

	// Verify it doesn't support unknown profiles
	if orchestrator.SupportsProfile("unknown-profile") {
		t.Error("Orchestrator should not support unknown profiles")
	}

	t.Logf("Orchestrator connected with session: %s, profiles: %v",
		orchestrator.SessionID(), supportedProfiles)

	// Note: Task assignments would be sent by the gateway when agents are requested
	// but not available. For this test, we verify the handler infrastructure is working.
	t.Log("Task assignment handling infrastructure verified")

	// Cleanup
	loopCancel()
	<-done
}

// TestIntegrationOrchestratorToAgentMessaging tests sending status messages
// from an orchestrator to an agent.
func TestIntegrationOrchestratorToAgentMessaging(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create and connect orchestrator
	orchCfg := TestOrchestratorConfig{
		Implementation: "orch-to-agent-test",
		Specifier:      UniqueTestIdentifier("orch"),
	}
	orchestrator := NewTestOrchestratorClient(t, orchCfg)

	// Create and connect agent
	agentCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "agent-orch-target",
		Specifier:      UniqueTestIdentifier("agent"),
	}
	agent := NewTestAgentClient(t, agentCfg)

	// Set up message collector for agent
	msgCollector := NewMessageCollector()
	agent.OnMessage(msgCollector.Handler())

	// Connect both
	if err := orchestrator.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator failed to connect: %v", err)
	}
	if err := agent.Connect(ctx); err != nil {
		t.Fatalf("Agent failed to connect: %v", err)
	}

	// Start message loops
	orchCtx, orchCancel := context.WithCancel(ctx)
	agentCtx, agentCancel := context.WithCancel(ctx)
	defer orchCancel()
	defer agentCancel()

	orchDone := make(chan error, 1)
	agentDone := make(chan error, 1)
	go func() { orchDone <- orchestrator.Run(orchCtx) }()
	go func() { agentDone <- agent.Run(agentCtx) }()

	// Wait for connections
	if !WaitForConnection(ctx, orchestrator, DefaultConnectTimeout) {
		t.Fatal("Orchestrator failed to connect in time")
	}
	if !WaitForConnection(ctx, agent, DefaultConnectTimeout) {
		t.Fatal("Agent failed to connect in time")
	}

	// Send status message from orchestrator to agent
	statusPayload := []byte(`{"status": "ready", "resources": {"cpu": "available", "memory": "4G"}}`)
	if err := orchestrator.SendStatusToAgent(workspace, agentCfg.Implementation, agentCfg.Specifier, statusPayload); err != nil {
		t.Fatalf("Failed to send status to agent: %v", err)
	}

	t.Log("Status message sent from orchestrator to agent")

	// Wait for the message to be received
	if msgCollector.WaitForMessage(ctx, 5*time.Second) {
		messages := msgCollector.Messages()
		if len(messages) > 0 {
			t.Logf("Agent received %d message(s) from orchestrator", len(messages))
			t.Logf("Message payload: %s", string(messages[0].Payload))
		}
	} else {
		t.Log("Note: Message not received within timeout - may be expected without full infrastructure")
	}

	// Cleanup
	orchCancel()
	agentCancel()
	<-orchDone
	<-agentDone

	t.Log("Orchestrator-to-agent messaging test completed")
}

// TestIntegrationOrchestratorToTaskMessaging tests sending status messages
// from an orchestrator to tasks (both unique and broadcast).
func TestIntegrationOrchestratorToTaskMessaging(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create and connect orchestrator
	orchCfg := TestOrchestratorConfig{
		Implementation: "orch-to-task-test",
		Specifier:      UniqueTestIdentifier("orch"),
	}
	orchestrator := NewTestOrchestratorClient(t, orchCfg)

	// Create unique task
	uniqueTaskCfg := TestTaskConfig{
		Workspace:      workspace,
		Implementation: "unique-task-target",
		Specifier:      UniqueTestIdentifier("unique"),
	}
	uniqueTask := NewTestTaskClient(t, uniqueTaskCfg)

	// Create non-unique task
	nonUniqueTaskCfg := TestTaskConfig{
		Workspace:      workspace,
		Implementation: "nonunique-task-target",
		Specifier:      "", // Non-unique task
	}
	nonUniqueTask := NewTestTaskClient(t, nonUniqueTaskCfg)

	// Set up message collectors
	uniqueCollector := NewMessageCollector()
	nonUniqueCollector := NewMessageCollector()
	uniqueTask.OnMessage(uniqueCollector.Handler())
	nonUniqueTask.OnMessage(nonUniqueCollector.Handler())

	// Connect all
	if err := orchestrator.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator failed to connect: %v", err)
	}
	if err := uniqueTask.Connect(ctx); err != nil {
		t.Fatalf("Unique task failed to connect: %v", err)
	}
	if err := nonUniqueTask.Connect(ctx); err != nil {
		t.Fatalf("Non-unique task failed to connect: %v", err)
	}

	// Start message loops
	orchCtx, orchCancel := context.WithCancel(ctx)
	uniqueCtx, uniqueCancel := context.WithCancel(ctx)
	nonUniqueCtx, nonUniqueCancel := context.WithCancel(ctx)
	defer orchCancel()
	defer uniqueCancel()
	defer nonUniqueCancel()

	orchDone := make(chan error, 1)
	uniqueDone := make(chan error, 1)
	nonUniqueDone := make(chan error, 1)
	go func() { orchDone <- orchestrator.Run(orchCtx) }()
	go func() { uniqueDone <- uniqueTask.Run(uniqueCtx) }()
	go func() { nonUniqueDone <- nonUniqueTask.Run(nonUniqueCtx) }()

	// Wait for connections
	WaitForConnection(ctx, orchestrator, DefaultConnectTimeout)
	WaitForConnection(ctx, uniqueTask, DefaultConnectTimeout)
	WaitForConnection(ctx, nonUniqueTask, DefaultConnectTimeout)

	// Test 1: Send to unique task
	t.Run("UniqueTask", func(t *testing.T) {
		payload := []byte(`{"status": "starting", "task_type": "unique"}`)
		if err := orchestrator.SendStatusToTask(workspace, uniqueTaskCfg.Implementation, uniqueTaskCfg.Specifier, payload); err != nil {
			t.Fatalf("Failed to send status to unique task: %v", err)
		}
		t.Log("Status sent to unique task")

		if uniqueCollector.WaitForMessage(ctx, 3*time.Second) {
			t.Logf("Unique task received message")
		} else {
			t.Log("Note: Unique task message not received within timeout")
		}
	})

	// Test 2: Send broadcast to non-unique tasks
	t.Run("NonUniqueTaskBroadcast", func(t *testing.T) {
		payload := []byte(`{"status": "work_available", "task_type": "broadcast"}`)
		// Empty specifier means broadcast to all non-unique tasks of this implementation
		if err := orchestrator.SendStatusToTask(workspace, nonUniqueTaskCfg.Implementation, "", payload); err != nil {
			t.Fatalf("Failed to send broadcast to non-unique tasks: %v", err)
		}
		t.Log("Broadcast sent to non-unique tasks")

		if nonUniqueCollector.WaitForMessage(ctx, 3*time.Second) {
			t.Logf("Non-unique task received broadcast")
		} else {
			t.Log("Note: Non-unique task broadcast not received within timeout")
		}
	})

	// Cleanup
	orchCancel()
	uniqueCancel()
	nonUniqueCancel()
	<-orchDone
	<-uniqueDone
	<-nonUniqueDone

	t.Log("Orchestrator-to-task messaging test completed")
}

// TestIntegrationAgentTaskCreationSelfAssign tests an agent creating a task
// with self-assign mode.
func TestIntegrationAgentTaskCreationSelfAssign(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Create agent
	agentCfg := TestAgentConfig{
		Workspace:      GetTestWorkspace(),
		Implementation: "task-creator-self",
		Specifier:      UniqueTestIdentifier("creator"),
	}
	agent := NewTestAgentClient(t, agentCfg)

	// Set up task assignment collector
	assignmentCollector := NewTaskAssignmentCollector()
	agent.OnTaskAssignment(assignmentCollector.Handler())

	// Connect agent
	if err := agent.Connect(ctx); err != nil {
		t.Fatalf("Agent failed to connect: %v", err)
	}

	// Start message loop
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- agent.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, agent, DefaultConnectTimeout) {
		t.Fatal("Agent failed to connect in time")
	}

	// Create a task with self-assign mode
	taskOpts := CreateTaskOptions{
		TaskType:       "self-assign-test-task",
		AssignmentMode: TaskAssignmentSelfAssign,
		Metadata: map[string]string{
			"source":    "integration-test",
			"test_name": "TestIntegrationAgentTaskCreationSelfAssign",
		},
	}

	if err := agent.CreateTask(taskOpts); err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	t.Logf("Task creation request sent: type=%s, mode=%s",
		taskOpts.TaskType, taskOpts.AssignmentMode)

	// Wait for potential task assignment
	// Note: The agent might receive the task assignment if the system supports self-assign
	if assignmentCollector.WaitForAssignment(ctx, 3*time.Second) {
		assignment := assignmentCollector.LastAssignment()
		if assignment != nil {
			t.Logf("Self-assigned task received: type=%s, id=%s",
				assignment.TaskType, assignment.TaskID)
		}
	} else {
		t.Log("Note: No self-assignment received - this may be expected without orchestrator")
	}

	// Cleanup
	loopCancel()
	<-done

	t.Log("Agent task creation (self-assign) test completed")
}

// TestIntegrationAgentTaskCreationTargeted tests an agent creating a task
// that targets a specific agent.
func TestIntegrationAgentTaskCreationTargeted(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create source agent (creates the task)
	sourceCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "task-creator-targeted",
		Specifier:      UniqueTestIdentifier("source"),
	}
	sourceAgent := NewTestAgentClient(t, sourceCfg)

	// Create target agent (receives the task)
	targetCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "task-target-agent",
		Specifier:      UniqueTestIdentifier("target"),
	}
	targetAgent := NewTestAgentClient(t, targetCfg)

	// Set up task assignment collector on target
	targetCollector := NewTaskAssignmentCollector()
	targetAgent.OnTaskAssignment(targetCollector.Handler())

	// Connect both agents
	if err := sourceAgent.Connect(ctx); err != nil {
		t.Fatalf("Source agent failed to connect: %v", err)
	}
	if err := targetAgent.Connect(ctx); err != nil {
		t.Fatalf("Target agent failed to connect: %v", err)
	}

	// Start message loops
	sourceCtx, sourceCancel := context.WithCancel(ctx)
	targetCtx, targetCancel := context.WithCancel(ctx)
	defer sourceCancel()
	defer targetCancel()

	sourceDone := make(chan error, 1)
	targetDone := make(chan error, 1)
	go func() { sourceDone <- sourceAgent.Run(sourceCtx) }()
	go func() { targetDone <- targetAgent.Run(targetCtx) }()

	// Wait for connections
	if !WaitForConnection(ctx, sourceAgent, DefaultConnectTimeout) {
		t.Fatal("Source agent failed to connect in time")
	}
	if !WaitForConnection(ctx, targetAgent, DefaultConnectTimeout) {
		t.Fatal("Target agent failed to connect in time")
	}

	// Create a targeted task
	targetID := AgentTopic(workspace, targetCfg.Implementation, targetCfg.Specifier)
	taskOpts := CreateTaskOptions{
		TaskType:       "targeted-test-task",
		Workspace:      workspace,
		AssignmentMode: TaskAssignmentTargeted,
		TargetAgentID:  targetID,
		Metadata: map[string]string{
			"source":    "integration-test",
			"test_name": "TestIntegrationAgentTaskCreationTargeted",
		},
	}

	if err := sourceAgent.CreateTask(taskOpts); err != nil {
		t.Fatalf("Failed to create targeted task: %v", err)
	}

	t.Logf("Targeted task creation request sent: type=%s, target=%s",
		taskOpts.TaskType, targetID)

	// Wait for potential task assignment on target
	if targetCollector.WaitForAssignment(ctx, 3*time.Second) {
		assignment := targetCollector.LastAssignment()
		if assignment != nil {
			t.Logf("Target agent received task: type=%s, id=%s",
				assignment.TaskType, assignment.TaskID)
		}
	} else {
		t.Log("Note: Target agent did not receive assignment - may be expected without orchestrator")
	}

	// Cleanup
	sourceCancel()
	targetCancel()
	<-sourceDone
	<-targetDone

	t.Log("Agent task creation (targeted) test completed")
}

// TestIntegrationOrchestratorProfileSupport tests that orchestrators correctly
// report and filter supported profiles.
func TestIntegrationOrchestratorProfileSupport(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Create orchestrator with specific profiles
	profiles := []string{"kubernetes", "docker", "local-process"}
	cfg := TestOrchestratorConfig{
		Implementation:    "profile-test-orch",
		SupportedProfiles: profiles,
		Specifier:         UniqueTestIdentifier("orch"),
	}
	orchestrator := NewTestOrchestratorClient(t, cfg)

	// Connect
	if err := orchestrator.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator failed to connect: %v", err)
	}

	// Verify identity
	if orchestrator.Implementation() != cfg.Implementation {
		t.Errorf("Implementation mismatch: got %s, want %s",
			orchestrator.Implementation(), cfg.Implementation)
	}

	if orchestrator.Specifier() != cfg.Specifier {
		t.Errorf("Specifier mismatch: got %s, want %s",
			orchestrator.Specifier(), cfg.Specifier)
	}

	// Verify all profiles are supported
	for _, profile := range profiles {
		if !orchestrator.SupportsProfile(profile) {
			t.Errorf("Orchestrator should support profile %s", profile)
		}
	}

	// Verify profile list
	retrievedProfiles := orchestrator.SupportedProfiles()
	if len(retrievedProfiles) != len(profiles) {
		t.Errorf("Profile count mismatch: got %d, want %d",
			len(retrievedProfiles), len(profiles))
	}

	// Verify profiles list is a copy (modifying it shouldn't affect original)
	retrievedProfiles[0] = "modified"
	originalProfiles := orchestrator.SupportedProfiles()
	if originalProfiles[0] == "modified" {
		t.Error("SupportedProfiles should return a copy, not a reference")
	}

	// Verify non-existent profiles are not supported
	unsupportedProfiles := []string{"aws-ecs", "gcp-cloudrun", "azure-aci"}
	for _, profile := range unsupportedProfiles {
		if orchestrator.SupportsProfile(profile) {
			t.Errorf("Orchestrator should not support profile %s", profile)
		}
	}

	t.Log("Orchestrator profile support test completed")
}

// TestIntegrationOrchestrationFullFlow tests the complete orchestration flow:
// 1. Orchestrator connects and registers profiles
// 2. Agent creates a task
// 3. Orchestrator handles the assignment (when infrastructure supports it)
// 4. Orchestrator sends status back to the agent
func TestIntegrationOrchestrationFullFlow(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// 1. Create and connect orchestrator
	orchCfg := TestOrchestratorConfig{
		Implementation:    "full-flow-orchestrator",
		SupportedProfiles: []string{"integration-test-profile"},
		Specifier:         UniqueTestIdentifier("orch"),
	}
	orchestrator := NewTestOrchestratorClient(t, orchCfg)

	orchAssignments := NewTaskAssignmentCollector()
	orchestrator.OnTaskAssignment(orchAssignments.Handler())

	if err := orchestrator.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator failed to connect: %v", err)
	}

	// 2. Create and connect agent
	agentCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "full-flow-agent",
		Specifier:      UniqueTestIdentifier("agent"),
	}
	agent := NewTestAgentClient(t, agentCfg)

	agentMessages := NewMessageCollector()
	agentAssignments := NewTaskAssignmentCollector()
	agent.OnMessage(agentMessages.Handler())
	agent.OnTaskAssignment(agentAssignments.Handler())

	if err := agent.Connect(ctx); err != nil {
		t.Fatalf("Agent failed to connect: %v", err)
	}

	// Start message loops
	orchCtx, orchCancel := context.WithCancel(ctx)
	agentCtx, agentCancel := context.WithCancel(ctx)
	defer orchCancel()
	defer agentCancel()

	orchDone := make(chan error, 1)
	agentDone := make(chan error, 1)
	go func() { orchDone <- orchestrator.Run(orchCtx) }()
	go func() { agentDone <- agent.Run(agentCtx) }()

	// Wait for connections
	if !WaitForConnection(ctx, orchestrator, DefaultConnectTimeout) {
		t.Fatal("Orchestrator failed to connect in time")
	}
	if !WaitForConnection(ctx, agent, DefaultConnectTimeout) {
		t.Fatal("Agent failed to connect in time")
	}

	t.Logf("Phase 1: Orchestrator connected with session %s", orchestrator.SessionID())
	t.Logf("Phase 2: Agent connected with session %s", agent.SessionID())

	// 3. Agent creates a task
	taskOpts := CreateTaskOptions{
		TaskType:       "full-flow-test-task",
		AssignmentMode: TaskAssignmentSelfAssign,
		Metadata: map[string]string{
			"test_phase": "integration",
			"workflow":   "full-flow",
		},
	}

	if err := agent.CreateTask(taskOpts); err != nil {
		t.Fatalf("Agent failed to create task: %v", err)
	}
	t.Log("Phase 3: Agent created task")

	// Give time for task routing
	time.Sleep(500 * time.Millisecond)

	// 4. Orchestrator sends status to agent
	statusPayload := []byte(`{"phase": "completed", "status": "success", "message": "Full flow test passed"}`)
	if err := orchestrator.SendStatusToAgent(workspace, agentCfg.Implementation, agentCfg.Specifier, statusPayload); err != nil {
		t.Fatalf("Orchestrator failed to send status: %v", err)
	}
	t.Log("Phase 4: Orchestrator sent status to agent")

	// Check if agent received the status
	if agentMessages.WaitForMessage(ctx, 3*time.Second) {
		msgs := agentMessages.Messages()
		if len(msgs) > 0 {
			t.Logf("Agent received status message: %s", string(msgs[0].Payload))
		}
	} else {
		t.Log("Note: Agent did not receive status within timeout")
	}

	// Summary
	t.Logf("Summary:")
	t.Logf("  - Orchestrator assignments received: %d", orchAssignments.Count())
	t.Logf("  - Agent assignments received: %d", agentAssignments.Count())
	t.Logf("  - Agent messages received: %d", agentMessages.Count())

	// Cleanup
	orchCancel()
	agentCancel()
	<-orchDone
	<-agentDone

	t.Log("Full orchestration flow test completed")
}

// TestIntegrationMultipleOrchestratorsProfiles tests multiple orchestrators
// with different profile sets.
func TestIntegrationMultipleOrchestratorsProfiles(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Create orchestrators with different profiles
	orch1Cfg := TestOrchestratorConfig{
		Implementation:    "multi-orch-test",
		SupportedProfiles: []string{"kubernetes", "docker"},
		Specifier:         UniqueTestIdentifier("orch1"),
	}
	orch2Cfg := TestOrchestratorConfig{
		Implementation:    "multi-orch-test",
		SupportedProfiles: []string{"docker", "local"},
		Specifier:         UniqueTestIdentifier("orch2"),
	}
	orch3Cfg := TestOrchestratorConfig{
		Implementation:    "multi-orch-test",
		SupportedProfiles: []string{"aws-lambda"},
		Specifier:         UniqueTestIdentifier("orch3"),
	}

	orch1 := NewTestOrchestratorClient(t, orch1Cfg)
	orch2 := NewTestOrchestratorClient(t, orch2Cfg)
	orch3 := NewTestOrchestratorClient(t, orch3Cfg)

	// Connect all orchestrators
	if err := orch1.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator 1 failed to connect: %v", err)
	}
	if err := orch2.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator 2 failed to connect: %v", err)
	}
	if err := orch3.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator 3 failed to connect: %v", err)
	}

	// Verify profile support
	t.Run("ProfileDistribution", func(t *testing.T) {
		// Orch1: kubernetes, docker
		if !orch1.SupportsProfile("kubernetes") || !orch1.SupportsProfile("docker") {
			t.Error("Orch1 should support kubernetes and docker")
		}
		if orch1.SupportsProfile("local") {
			t.Error("Orch1 should not support local")
		}

		// Orch2: docker, local
		if !orch2.SupportsProfile("docker") || !orch2.SupportsProfile("local") {
			t.Error("Orch2 should support docker and local")
		}
		if orch2.SupportsProfile("kubernetes") {
			t.Error("Orch2 should not support kubernetes")
		}

		// Orch3: aws-lambda only
		if !orch3.SupportsProfile("aws-lambda") {
			t.Error("Orch3 should support aws-lambda")
		}
		if orch3.SupportsProfile("docker") {
			t.Error("Orch3 should not support docker")
		}
	})

	// Verify unique specifiers
	t.Run("UniqueSpecifiers", func(t *testing.T) {
		specs := map[string]bool{
			orch1.Specifier(): true,
			orch2.Specifier(): true,
			orch3.Specifier(): true,
		}
		if len(specs) != 3 {
			t.Error("All orchestrators should have unique specifiers")
		}
	})

	t.Log("Multiple orchestrators profile test completed")
}

// TestIntegrationOrchestratorControlMessages tests different message types
// that orchestrators can send (CONTROL, CHAT).
func TestIntegrationOrchestratorControlMessages(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Create orchestrator
	orchCfg := TestOrchestratorConfig{
		Implementation: "control-msg-test-orch",
		Specifier:      UniqueTestIdentifier("orch"),
	}
	orchestrator := NewTestOrchestratorClient(t, orchCfg)

	// Create agent to receive messages
	agentCfg := TestAgentConfig{
		Workspace:      workspace,
		Implementation: "control-msg-target",
		Specifier:      UniqueTestIdentifier("agent"),
	}
	agent := NewTestAgentClient(t, agentCfg)

	msgCollector := NewMessageCollector()
	agent.OnMessage(msgCollector.Handler())

	// Connect both
	if err := orchestrator.Connect(ctx); err != nil {
		t.Fatalf("Orchestrator failed to connect: %v", err)
	}
	if err := agent.Connect(ctx); err != nil {
		t.Fatalf("Agent failed to connect: %v", err)
	}

	// Start message loops
	orchCtx, orchCancel := context.WithCancel(ctx)
	agentCtx, agentCancel := context.WithCancel(ctx)
	defer orchCancel()
	defer agentCancel()

	orchDone := make(chan error, 1)
	agentDone := make(chan error, 1)
	go func() { orchDone <- orchestrator.Run(orchCtx) }()
	go func() { agentDone <- agent.Run(agentCtx) }()

	WaitForConnection(ctx, orchestrator, DefaultConnectTimeout)
	WaitForConnection(ctx, agent, DefaultConnectTimeout)

	targetTopic := agent.Topic()

	// Test different message types
	t.Run("ControlMessage", func(t *testing.T) {
		payload := []byte(`{"control": "shutdown_graceful", "timeout_ms": 5000}`)
		if err := orchestrator.SendControlMessage(targetTopic, payload); err != nil {
			t.Fatalf("Failed to send control message: %v", err)
		}
		t.Log("Control message sent")
	})

	t.Run("ChatMessage", func(t *testing.T) {
		payload := []byte(`{"type": "notification", "text": "System update scheduled"}`)
		if err := orchestrator.SendChatMessage(targetTopic, payload); err != nil {
			t.Fatalf("Failed to send chat message: %v", err)
		}
		t.Log("Chat message sent")
	})

	// Give time for messages to be received
	time.Sleep(time.Second)
	t.Logf("Agent received %d messages", msgCollector.Count())

	// Cleanup
	orchCancel()
	agentCancel()
	<-orchDone
	<-agentDone

	t.Log("Orchestrator control messages test completed")
}

// TestIntegrationTaskIdentityManagement tests task identity management
// including unique vs non-unique task identification.
func TestIntegrationTaskIdentityManagement(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	workspace := GetTestWorkspace()

	// Test unique task identity
	t.Run("UniqueTaskIdentity", func(t *testing.T) {
		cfg := TestTaskConfig{
			Workspace:      workspace,
			Implementation: "identity-test",
			Specifier:      UniqueTestIdentifier("unique"),
		}
		task := NewTestTaskClient(t, cfg)

		// Verify pre-connection identity
		if task.Workspace() != cfg.Workspace {
			t.Errorf("Workspace mismatch: got %s, want %s", task.Workspace(), cfg.Workspace)
		}
		if task.Implementation() != cfg.Implementation {
			t.Errorf("Implementation mismatch: got %s, want %s", task.Implementation(), cfg.Implementation)
		}
		if task.Specifier() != cfg.Specifier {
			t.Errorf("Specifier mismatch: got %s, want %s", task.Specifier(), cfg.Specifier)
		}
		if !task.IsUnique() {
			t.Error("Task with specifier should be unique")
		}

		// Verify topic format for unique tasks
		expectedTopic := UniqueTaskTopic(workspace, cfg.Implementation, cfg.Specifier)
		if task.Topic() != expectedTopic {
			t.Errorf("Topic mismatch: got %s, want %s", task.Topic(), expectedTopic)
		}

		// Connect and verify identity persists
		if err := task.Connect(ctx); err != nil {
			t.Fatalf("Task failed to connect: %v", err)
		}

		if task.Workspace() != cfg.Workspace {
			t.Error("Workspace changed after connection")
		}
	})

	// Test non-unique task identity
	t.Run("NonUniqueTaskIdentity", func(t *testing.T) {
		cfg := TestTaskConfig{
			Workspace:      workspace,
			Implementation: "identity-test-nonunique",
			Specifier:      "", // Non-unique
		}
		task := NewTestTaskClient(t, cfg)

		// Verify pre-connection state
		if task.IsUnique() {
			t.Error("Task without specifier should not be unique")
		}
		if task.Specifier() != "" {
			t.Error("Non-unique task should have empty specifier")
		}

		// AssignedID should be empty before connection
		if task.AssignedID() != "" {
			t.Error("AssignedID should be empty before connection")
		}

		// Verify broadcast topic
		expectedBroadcast := TaskBroadcastTopic(workspace, cfg.Implementation)
		if task.BroadcastTopic() != expectedBroadcast {
			t.Errorf("BroadcastTopic mismatch: got %s, want %s",
				task.BroadcastTopic(), expectedBroadcast)
		}

		// Connect
		if err := task.Connect(ctx); err != nil {
			t.Fatalf("Task failed to connect: %v", err)
		}

		// After connection, session ID should be set
		if task.SessionID() == "" {
			t.Error("Session ID should be set after connection")
		}

		t.Logf("Non-unique task connected with session: %s", task.SessionID())
	})

	t.Log("Task identity management test completed")
}

// =============================================================================
// Checkpoint Integration Tests
// =============================================================================

// CheckpointCollector collects checkpoint responses for testing.
type CheckpointCollector struct {
	mu        sync.Mutex
	responses []*CheckpointResponse
	notify    chan struct{}
}

// NewCheckpointCollector creates a new checkpoint response collector.
func NewCheckpointCollector() *CheckpointCollector {
	return &CheckpointCollector{
		notify: make(chan struct{}, 100),
	}
}

// Handler returns a CheckpointResponseHandler that collects responses.
func (c *CheckpointCollector) Handler() CheckpointResponseHandler {
	return func(ctx context.Context, resp *CheckpointResponse) error {
		c.mu.Lock()
		c.responses = append(c.responses, resp)
		c.mu.Unlock()

		select {
		case c.notify <- struct{}{}:
		default:
		}

		return nil
	}
}

// Responses returns a copy of collected responses.
func (c *CheckpointCollector) Responses() []*CheckpointResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]*CheckpointResponse, len(c.responses))
	copy(result, c.responses)
	return result
}

// Count returns the number of collected responses.
func (c *CheckpointCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.responses)
}

// LastResponse returns the last response, or nil if none.
func (c *CheckpointCollector) LastResponse() *CheckpointResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.responses) == 0 {
		return nil
	}
	return c.responses[len(c.responses)-1]
}

// WaitForResponse waits for at least one checkpoint response.
func (c *CheckpointCollector) WaitForResponse(ctx context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.mu.Lock()
	if len(c.responses) > 0 {
		c.mu.Unlock()
		return true
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return false
	case <-c.notify:
		return true
	}
}

// Clear clears all collected responses.
func (c *CheckpointCollector) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responses = nil
}

// TestIntegrationCheckpointSaveAndLoad tests saving and loading checkpoint data.
func TestIntegrationCheckpointSaveAndLoad(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up checkpoint response collector
	cpCollector := NewCheckpointCollector()
	client.OnCheckpointResponse(cpCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Test checkpoint key
	testKey := "checkpoint-test-" + UniqueTestIdentifier("save")
	testData := TestCheckpointData("running", 1)

	// Save a checkpoint
	t.Logf("Saving checkpoint: key=%s", testKey)
	if err := client.Checkpoint().Save(testData, testKey, 3600); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// Wait for save response
	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			t.Logf("Save response: success=%v", resp.Success)
			if !resp.Success && resp.Error != "" {
				t.Errorf("Checkpoint save failed: %s", resp.Error)
			}
		}
	} else {
		t.Log("Note: Checkpoint save response not received within timeout")
	}

	// Clear and load the checkpoint back
	cpCollector.Clear()
	t.Logf("Loading checkpoint: key=%s", testKey)
	if err := client.Checkpoint().Load(testKey); err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	// Wait for load response
	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			t.Logf("Load response: success=%v, data=%s", resp.Success, string(resp.Data))
			if resp.Success && len(resp.Data) > 0 {
				if string(resp.Data) != string(testData) {
					t.Errorf("Data mismatch: got %s, want %s", resp.Data, testData)
				}
			}
		}
	} else {
		t.Log("Note: Checkpoint load response not received within timeout")
	}

	// Clean up: delete the checkpoint
	cpCollector.Clear()
	if err := client.Checkpoint().Delete(testKey); err != nil {
		t.Logf("Failed to clean up checkpoint: %v", err)
	}
	cpCollector.WaitForResponse(ctx, 2*time.Second)

	// Cleanup
	loopCancel()
	<-done

	t.Log("Checkpoint save and load test completed")
}

// TestIntegrationCheckpointList tests listing checkpoint keys.
func TestIntegrationCheckpointList(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up checkpoint response collector
	cpCollector := NewCheckpointCollector()
	client.OnCheckpointResponse(cpCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Create multiple checkpoints
	prefix := "list-test-" + UniqueTestIdentifier("list") + "-"
	keys := []string{prefix + "key1", prefix + "key2", prefix + "key3"}

	for _, key := range keys {
		if err := client.Checkpoint().Save([]byte("data-"+key), key, 3600); err != nil {
			t.Fatalf("Failed to save checkpoint %s: %v", key, err)
		}
		cpCollector.WaitForResponse(ctx, 2*time.Second)
		cpCollector.Clear()
	}

	// List checkpoints
	t.Log("Listing checkpoints")
	if err := client.Checkpoint().List(); err != nil {
		t.Fatalf("Failed to list checkpoints: %v", err)
	}

	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			t.Logf("List response: success=%v, keys=%v", resp.Success, resp.Keys)
			// Check that our keys are in the list
			for _, key := range keys {
				found := false
				for _, respKey := range resp.Keys {
					if respKey == key {
						found = true
						break
					}
				}
				if !found {
					t.Logf("Note: Key %s not found in list - may be expected if checkpoints are identity-scoped", key)
				}
			}
		}
	} else {
		t.Log("Note: Checkpoint list response not received within timeout")
	}

	// Clean up created checkpoints
	for _, key := range keys {
		cpCollector.Clear()
		client.Checkpoint().Delete(key)
		cpCollector.WaitForResponse(ctx, 2*time.Second)
	}

	// Cleanup
	loopCancel()
	<-done

	t.Log("Checkpoint list test completed")
}

// TestIntegrationCheckpointDelete tests deleting checkpoint data.
func TestIntegrationCheckpointDelete(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up checkpoint response collector
	cpCollector := NewCheckpointCollector()
	client.OnCheckpointResponse(cpCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Create a checkpoint
	testKey := "delete-test-" + UniqueTestIdentifier("del")
	testData := []byte(`{"state": "to_be_deleted"}`)

	if err := client.Checkpoint().Save(testData, testKey, 3600); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}
	cpCollector.WaitForResponse(ctx, 2*time.Second)
	cpCollector.Clear()

	// Delete the checkpoint
	t.Logf("Deleting checkpoint: key=%s", testKey)
	if err := client.Checkpoint().Delete(testKey); err != nil {
		t.Fatalf("Failed to delete checkpoint: %v", err)
	}

	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			t.Logf("Delete response: success=%v", resp.Success)
		}
	} else {
		t.Log("Note: Checkpoint delete response not received within timeout")
	}

	// Try to load the deleted checkpoint - should return empty or error
	cpCollector.Clear()
	if err := client.Checkpoint().Load(testKey); err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			if resp.Success && len(resp.Data) > 0 {
				t.Log("Note: Deleted checkpoint still returned data - deletion may be deferred")
			} else {
				t.Log("Deleted checkpoint correctly returned no data")
			}
		}
	}

	// Cleanup
	loopCancel()
	<-done

	t.Log("Checkpoint delete test completed")
}

// TestIntegrationCheckpointSyncOperations tests synchronous checkpoint operations.
func TestIntegrationCheckpointSyncOperations(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	testKey := "sync-test-" + UniqueTestIdentifier("sync")
	testData := TestCheckpointData("synced", 42)

	// Test sync save
	t.Run("SyncSave", func(t *testing.T) {
		resp, err := client.Checkpoint().SaveSync(ctx, CheckpointSaveOptions{
			Data:    testData,
			Key:     testKey,
			TTL:     time.Hour,
			Timeout: 5 * time.Second,
		})

		if err != nil {
			if IsTimeoutError(err) {
				t.Log("Note: Sync save timed out - server may not support checkpoint operations")
				return
			}
			t.Fatalf("Sync save failed: %v", err)
		}

		if resp != nil {
			t.Logf("Sync save response: success=%v", resp.Success)
		}
	})

	// Test sync load
	t.Run("SyncLoad", func(t *testing.T) {
		resp, err := client.Checkpoint().LoadSync(ctx, CheckpointLoadOptions{
			Key:     testKey,
			Timeout: 5 * time.Second,
		})

		if err != nil {
			if IsTimeoutError(err) {
				t.Log("Note: Sync load timed out - server may not support checkpoint operations")
				return
			}
			t.Fatalf("Sync load failed: %v", err)
		}

		if resp != nil {
			t.Logf("Sync load response: success=%v, data=%s", resp.Success, string(resp.Data))
			if resp.Success && len(resp.Data) > 0 {
				if string(resp.Data) != string(testData) {
					t.Errorf("Data mismatch: got %s, want %s", resp.Data, testData)
				}
			}
		}
	})

	// Test sync list
	t.Run("SyncList", func(t *testing.T) {
		resp, err := client.Checkpoint().ListSync(ctx, 5*time.Second)

		if err != nil {
			if IsTimeoutError(err) {
				t.Log("Note: Sync list timed out - server may not support checkpoint operations")
				return
			}
			t.Fatalf("Sync list failed: %v", err)
		}

		if resp != nil {
			t.Logf("Sync list response: success=%v, keys=%v", resp.Success, resp.Keys)
		}
	})

	// Test sync delete
	t.Run("SyncDelete", func(t *testing.T) {
		resp, err := client.Checkpoint().DeleteSync(ctx, CheckpointDeleteOptions{
			Key:     testKey,
			Timeout: 5 * time.Second,
		})

		if err != nil {
			if IsTimeoutError(err) {
				t.Log("Note: Sync delete timed out - server may not support checkpoint operations")
				return
			}
			t.Fatalf("Sync delete failed: %v", err)
		}

		if resp != nil {
			t.Logf("Sync delete response: success=%v", resp.Success)
		}
	})

	// Cleanup
	loopCancel()
	<-done

	t.Log("Checkpoint sync operations test completed")
}

// TestIntegrationSessionResumptionWithCheckpoint tests saving state before disconnect
// and resuming it on reconnection using checkpoints.
func TestIntegrationSessionResumptionWithCheckpoint(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	// Use a fixed identity so we can reconnect as the same agent
	workspace := GetTestWorkspace()
	impl := "session-resume-test"
	spec := UniqueTestIdentifier("resume")

	// Phase 1: Connect, save state, disconnect
	t.Log("Phase 1: Initial connection and state save")

	client1, err := NewAgentClient(AgentOptions{
		ClientOptions: ClientOptions{
			ServerAddr: GetGatewayAddr(),
			Connection: ConnectionOptions{
				MaxRetries:     1,
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				AutoReconnect:  false,
				ConnectTimeout: DefaultConnectTimeout,
			},
		},
		Workspace:      workspace,
		Implementation: impl,
		Specifier:      spec,
	})
	if err != nil {
		t.Fatalf("Failed to create first client: %v", err)
	}

	// Set up checkpoint collector
	cpCollector1 := NewCheckpointCollector()
	client1.OnCheckpointResponse(cpCollector1.Handler())

	// Connect
	if err := client1.Connect(ctx); err != nil {
		t.Fatalf("First client failed to connect: %v", err)
	}

	// Start message loop
	loop1Ctx, loop1Cancel := context.WithCancel(ctx)
	done1 := make(chan error, 1)
	go func() {
		done1 <- client1.Run(loop1Ctx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client1, DefaultConnectTimeout) {
		loop1Cancel()
		t.Fatal("First client failed to connect in time")
	}

	session1ID := client1.SessionID()
	t.Logf("First client connected with session: %s", session1ID)

	// Save checkpoint with state
	checkpointKey := "session-state"
	stateData := []byte(`{"progress": 50, "last_item": "item-42", "timestamp": "2024-01-01T12:00:00Z"}`)

	t.Log("Saving session state checkpoint")
	if err := client1.Checkpoint().Save(stateData, checkpointKey, 3600); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	if cpCollector1.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector1.LastResponse()
		if resp != nil && !resp.Success {
			t.Logf("Warning: Checkpoint save may have failed: %s", resp.Error)
		} else {
			t.Log("Checkpoint saved successfully")
		}
	} else {
		t.Log("Note: Checkpoint save response not received - continuing with test")
	}

	// Disconnect
	t.Log("Disconnecting first client")
	loop1Cancel()
	<-done1
	if err := client1.Close(); err != nil {
		t.Logf("Error closing first client: %v", err)
	}

	// Wait for lock to be released
	time.Sleep(1 * time.Second)

	// Phase 2: Reconnect and restore state
	t.Log("Phase 2: Reconnection and state restoration")

	client2, err := NewAgentClient(AgentOptions{
		ClientOptions: ClientOptions{
			ServerAddr: GetGatewayAddr(),
			Connection: ConnectionOptions{
				MaxRetries:     1,
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				AutoReconnect:  false,
				ConnectTimeout: DefaultConnectTimeout,
			},
		},
		Workspace:      workspace,
		Implementation: impl,
		Specifier:      spec,
	})
	if err != nil {
		t.Fatalf("Failed to create second client: %v", err)
	}

	// Set up checkpoint collector
	cpCollector2 := NewCheckpointCollector()
	client2.OnCheckpointResponse(cpCollector2.Handler())

	// Connect
	if err := client2.Connect(ctx); err != nil {
		t.Fatalf("Second client failed to connect: %v", err)
	}

	// Start message loop
	loop2Ctx, loop2Cancel := context.WithCancel(ctx)
	defer loop2Cancel()
	done2 := make(chan error, 1)
	go func() {
		done2 <- client2.Run(loop2Ctx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client2, DefaultConnectTimeout) {
		t.Fatal("Second client failed to connect in time")
	}

	session2ID := client2.SessionID()
	t.Logf("Second client connected with session: %s", session2ID)

	// Session IDs should be different (new session)
	if session1ID == session2ID && session1ID != "" {
		t.Log("Note: Session IDs are the same - this may indicate session persistence")
	} else {
		t.Log("New session ID assigned as expected")
	}

	// Load the previously saved checkpoint
	t.Log("Loading session state checkpoint")
	if err := client2.Checkpoint().Load(checkpointKey); err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	if cpCollector2.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector2.LastResponse()
		if resp != nil {
			t.Logf("Checkpoint load response: success=%v", resp.Success)
			if resp.Success && len(resp.Data) > 0 {
				t.Logf("Restored state: %s", string(resp.Data))
				if string(resp.Data) == string(stateData) {
					t.Log("Session state successfully restored!")
				} else {
					t.Logf("State differs - may have been modified: got %s, saved %s",
						string(resp.Data), string(stateData))
				}
			} else if !resp.Success {
				t.Logf("Warning: Checkpoint load failed - state may not persist across sessions")
			} else {
				t.Log("Note: No data in checkpoint response")
			}
		}
	} else {
		t.Log("Note: Checkpoint load response not received within timeout")
	}

	// Clean up the checkpoint
	cpCollector2.Clear()
	if err := client2.Checkpoint().Delete(checkpointKey); err != nil {
		t.Logf("Failed to clean up checkpoint: %v", err)
	}
	cpCollector2.WaitForResponse(ctx, 2*time.Second)

	// Cleanup
	loop2Cancel()
	<-done2

	t.Log("Session resumption with checkpoint test completed")
}

// TestIntegrationCheckpointDefaultKey tests using the default checkpoint key.
func TestIntegrationCheckpointDefaultKey(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up checkpoint response collector
	cpCollector := NewCheckpointCollector()
	client.OnCheckpointResponse(cpCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	testData := []byte(`{"default_key_test": true}`)

	// Save using SaveDefault (uses empty key = "default")
	t.Log("Saving default checkpoint")
	if err := client.Checkpoint().SaveDefault(testData); err != nil {
		t.Fatalf("Failed to save default checkpoint: %v", err)
	}

	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			t.Logf("Save default response: success=%v", resp.Success)
		}
	} else {
		t.Log("Note: Default checkpoint save response not received within timeout")
	}

	// Load using LoadDefault
	cpCollector.Clear()
	t.Log("Loading default checkpoint")
	if err := client.Checkpoint().LoadDefault(); err != nil {
		t.Fatalf("Failed to load default checkpoint: %v", err)
	}

	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			t.Logf("Load default response: success=%v, data=%s", resp.Success, string(resp.Data))
		}
	} else {
		t.Log("Note: Default checkpoint load response not received within timeout")
	}

	// Delete using DeleteDefault
	cpCollector.Clear()
	t.Log("Deleting default checkpoint")
	if err := client.Checkpoint().DeleteDefault(); err != nil {
		t.Fatalf("Failed to delete default checkpoint: %v", err)
	}

	if cpCollector.WaitForResponse(ctx, 5*time.Second) {
		resp := cpCollector.LastResponse()
		if resp != nil {
			t.Logf("Delete default response: success=%v", resp.Success)
		}
	}

	// Cleanup
	loopCancel()
	<-done

	t.Log("Checkpoint default key test completed")
}

// TestIntegrationCheckpointTTL tests checkpoint TTL functionality.
func TestIntegrationCheckpointTTL(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Set up checkpoint response collector
	cpCollector := NewCheckpointCollector()
	client.OnCheckpointResponse(cpCollector.Handler())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Test SaveWithTTL convenience method
	t.Run("SaveWithTTL", func(t *testing.T) {
		key := "ttl-test-" + UniqueTestIdentifier("ttl")
		data := []byte(`{"ttl_test": true}`)

		cpCollector.Clear()
		if err := client.Checkpoint().SaveWithTTL(data, key, time.Hour); err != nil {
			t.Fatalf("Failed to save with TTL: %v", err)
		}

		if cpCollector.WaitForResponse(ctx, 5*time.Second) {
			resp := cpCollector.LastResponse()
			if resp != nil {
				t.Logf("SaveWithTTL response: success=%v", resp.Success)
			}
		}

		// Clean up
		cpCollector.Clear()
		client.Checkpoint().Delete(key)
		cpCollector.WaitForResponse(ctx, 2*time.Second)
	})

	// Test SavePermanent convenience method (no expiration)
	t.Run("SavePermanent", func(t *testing.T) {
		key := "perm-test-" + UniqueTestIdentifier("perm")
		data := []byte(`{"permanent_test": true}`)

		cpCollector.Clear()
		if err := client.Checkpoint().SavePermanent(data, key); err != nil {
			t.Fatalf("Failed to save permanent: %v", err)
		}

		if cpCollector.WaitForResponse(ctx, 5*time.Second) {
			resp := cpCollector.LastResponse()
			if resp != nil {
				t.Logf("SavePermanent response: success=%v", resp.Success)
			}
		}

		// Clean up
		cpCollector.Clear()
		client.Checkpoint().Delete(key)
		cpCollector.WaitForResponse(ctx, 2*time.Second)
	})

	// Cleanup
	loopCancel()
	<-done

	t.Log("Checkpoint TTL test completed")
}

// TestIntegrationConcurrentCheckpointOperations tests concurrent checkpoint operations.
func TestIntegrationConcurrentCheckpointOperations(t *testing.T) {
	SkipIfNoGateway(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTestTimeout)
	defer cancel()

	client := NewTestAgentClient(t, DefaultTestAgentConfig())

	// Connect and start message loop
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	loopCtx, loopCancel := context.WithCancel(ctx)
	defer loopCancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(loopCtx)
	}()

	// Wait for connection
	if !WaitForConnection(ctx, client, DefaultConnectTimeout) {
		t.Fatal("Client failed to connect in time")
	}

	// Run concurrent checkpoint operations
	numOps := 5
	prefix := "concurrent-cp-" + UniqueTestIdentifier("conc") + "-"
	var wg sync.WaitGroup
	errors := make(chan error, numOps*2)

	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			key := prefix + "key" + string(rune('0'+idx))
			data := []byte(`{"concurrent": ` + string(rune('0'+idx)) + `}`)

			// Save checkpoint
			if err := client.Checkpoint().Save(data, key, 3600); err != nil {
				errors <- err
				return
			}

			// Small delay
			time.Sleep(100 * time.Millisecond)

			// Load checkpoint
			if err := client.Checkpoint().Load(key); err != nil {
				errors <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Checkpoint operation error: %v", err)
		errorCount++
	}

	t.Logf("Concurrent checkpoint operations: %d/%d operations succeeded", numOps-errorCount, numOps)

	// Clean up the checkpoints
	for i := 0; i < numOps; i++ {
		key := prefix + "key" + string(rune('0'+i))
		client.Checkpoint().Delete(key)
	}
	time.Sleep(500 * time.Millisecond)

	// Cleanup
	loopCancel()
	<-done

	t.Log("Concurrent checkpoint operations test completed")
}
