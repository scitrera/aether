// Package aether test utilities for integration tests.
//
// This file provides test configuration, fixtures, and helper functions for
// integration testing the Aether SDK against a running gateway server.
//
// Configuration is read from environment variables with defaults matching
// the development infrastructure setup.

package aether

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Test Configuration Constants
// =============================================================================

const (
	// DefaultGatewayAddr is the default gateway address for testing.
	DefaultGatewayAddr = "localhost:50051"

	// Test identity defaults
	DefaultTestWorkspace      = "test-workspace"
	DefaultTestImplementation = "test-impl"
	DefaultTestSpecifier      = "test-spec"
	DefaultTestUserID         = "test-user-123"
	DefaultTestWindowID       = "test-window-456"

	// Test timeouts
	DefaultTestTimeout     = 30 * time.Second
	DefaultConnectTimeout  = 5 * time.Second
	DefaultOperationTimout = 5 * time.Second
	DefaultPollInterval    = 50 * time.Millisecond

	// Environment variable names
	EnvGatewayAddr       = "AETHER_GATEWAY_ADDR"
	EnvTestWorkspace     = "AETHER_TEST_WORKSPACE"
	EnvSkipIntegration   = "AETHER_SKIP_INTEGRATION"
	EnvIntegrationVerbse = "AETHER_INTEGRATION_VERBOSE"
)

// TestProfiles is the list of test profiles for orchestrators.
var DefaultTestProfiles = []string{"test-profile-1", "test-profile-2"}

// =============================================================================
// Test Configuration
// =============================================================================

// TestConfig holds configuration for integration tests.
type TestConfig struct {
	// GatewayAddr is the gateway server address.
	GatewayAddr string

	// Workspace is the test workspace.
	Workspace string

	// ConnectTimeout is the timeout for connection attempts.
	ConnectTimeout time.Duration

	// OperationTimeout is the default timeout for operations.
	OperationTimeout time.Duration

	// Verbose enables verbose logging.
	Verbose bool
}

// DefaultTestConfig returns the default test configuration.
func DefaultTestConfig() TestConfig {
	return TestConfig{
		GatewayAddr:      GetGatewayAddr(),
		Workspace:        GetTestWorkspace(),
		ConnectTimeout:   DefaultConnectTimeout,
		OperationTimeout: DefaultOperationTimout,
		Verbose:          os.Getenv(EnvIntegrationVerbse) != "",
	}
}

// =============================================================================
// Environment Configuration
// =============================================================================

// GetGatewayAddr returns the gateway address from environment or default.
func GetGatewayAddr() string {
	if addr := os.Getenv(EnvGatewayAddr); addr != "" {
		return addr
	}
	return DefaultGatewayAddr
}

// GetTestWorkspace returns the test workspace from environment or default.
func GetTestWorkspace() string {
	if ws := os.Getenv(EnvTestWorkspace); ws != "" {
		return ws
	}
	return DefaultTestWorkspace
}

// ShouldSkipIntegration returns true if integration tests should be skipped.
func ShouldSkipIntegration() bool {
	return os.Getenv(EnvSkipIntegration) != ""
}

// =============================================================================
// Test Skip Helpers
// =============================================================================

// SkipIfShort skips the test if running with -short flag.
func SkipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
}

// SkipIfNoGateway skips the test if the gateway is not available.
// This performs a quick connection check.
func SkipIfNoGateway(t *testing.T) {
	t.Helper()
	SkipIfShort(t)

	if ShouldSkipIntegration() {
		t.Skip("skipping integration test (AETHER_SKIP_INTEGRATION is set)")
	}

	// Try a quick connection to check if gateway is running
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	addr := GetGatewayAddr()
	if !isGatewayAvailable(ctx, addr) {
		t.Skipf("skipping integration test: gateway not available at %s", addr)
	}
}

// isGatewayAvailable checks if the gateway is accepting connections.
func isGatewayAvailable(ctx context.Context, addr string) bool {
	// Create a minimal agent client to test connectivity
	client, err := NewAgentClient(AgentOptions{
		ClientOptions: ClientOptions{
			ServerAddr: addr,
			Connection: ConnectionOptions{
				MaxRetries:     1,
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     100 * time.Millisecond,
				AutoReconnect:  false,
				ConnectTimeout: 2 * time.Second,
			},
		},
		Workspace:      "connectivity-check",
		Implementation: "test",
		Specifier:      "check",
	})
	if err != nil {
		return false
	}
	defer client.Close()

	// Try to connect
	if err := client.Connect(ctx); err != nil {
		return false
	}

	return true
}

// =============================================================================
// Test Identity Generator
// =============================================================================

// TestIdentityGenerator generates unique test identities to avoid conflicts.
type TestIdentityGenerator struct {
	mu      sync.Mutex
	counter uint64
	prefix  string
}

// NewTestIdentityGenerator creates a new identity generator with the given prefix.
func NewTestIdentityGenerator(prefix string) *TestIdentityGenerator {
	if prefix == "" {
		prefix = "test"
	}
	return &TestIdentityGenerator{
		prefix: prefix,
	}
}

// Next generates the next unique identifier.
func (g *TestIdentityGenerator) Next() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.counter++
	return g.prefix + "-" + strconv.FormatUint(g.counter, 10)
}

// DefaultIdentityGenerator is the default identity generator for tests.
var DefaultIdentityGenerator = NewTestIdentityGenerator("test")

// UniqueTestIdentifier generates a unique test identifier.
func UniqueTestIdentifier(prefix string) string {
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// =============================================================================
// Test Client Factories
// =============================================================================

// TestAgentConfig holds configuration for creating test agent clients.
type TestAgentConfig struct {
	ServerAddr     string
	Workspace      string
	Implementation string
	Specifier      string
	AutoReconnect  bool
	Credentials    map[string]string
}

// DefaultTestAgentConfig returns a default test agent configuration.
func DefaultTestAgentConfig() TestAgentConfig {
	return TestAgentConfig{
		ServerAddr:     GetGatewayAddr(),
		Workspace:      GetTestWorkspace(),
		Implementation: DefaultTestImplementation,
		Specifier:      UniqueTestIdentifier("agent"),
		AutoReconnect:  false,
	}
}

// NewTestAgentClient creates a new agent client for testing.
// The client is created but not connected.
func NewTestAgentClient(t *testing.T, cfg TestAgentConfig) *AgentClient {
	t.Helper()

	if cfg.ServerAddr == "" {
		cfg.ServerAddr = GetGatewayAddr()
	}
	if cfg.Workspace == "" {
		cfg.Workspace = GetTestWorkspace()
	}
	if cfg.Implementation == "" {
		cfg.Implementation = DefaultTestImplementation
	}
	if cfg.Specifier == "" {
		cfg.Specifier = UniqueTestIdentifier("agent")
	}

	client, err := NewAgentClient(AgentOptions{
		ClientOptions: ClientOptions{
			ServerAddr: cfg.ServerAddr,
			Connection: ConnectionOptions{
				MaxRetries:        3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        1 * time.Second,
				BackoffMultiplier: 2.0,
				AutoReconnect:     cfg.AutoReconnect,
				ConnectTimeout:    DefaultConnectTimeout,
			},
			Credentials: cfg.Credentials,
		},
		Workspace:      cfg.Workspace,
		Implementation: cfg.Implementation,
		Specifier:      cfg.Specifier,
	})
	if err != nil {
		t.Fatalf("Failed to create test agent client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

// TestTaskConfig holds configuration for creating test task clients.
type TestTaskConfig struct {
	ServerAddr     string
	Workspace      string
	Implementation string
	Specifier      string // Empty for non-unique tasks
	AutoReconnect  bool
	Credentials    map[string]string
}

// DefaultTestTaskConfig returns a default test task configuration.
func DefaultTestTaskConfig() TestTaskConfig {
	return TestTaskConfig{
		ServerAddr:     GetGatewayAddr(),
		Workspace:      GetTestWorkspace(),
		Implementation: DefaultTestImplementation,
		Specifier:      UniqueTestIdentifier("task"),
		AutoReconnect:  false,
	}
}

// NewTestTaskClient creates a new task client for testing.
// The client is created but not connected.
func NewTestTaskClient(t *testing.T, cfg TestTaskConfig) *TaskClient {
	t.Helper()

	if cfg.ServerAddr == "" {
		cfg.ServerAddr = GetGatewayAddr()
	}
	if cfg.Workspace == "" {
		cfg.Workspace = GetTestWorkspace()
	}
	if cfg.Implementation == "" {
		cfg.Implementation = DefaultTestImplementation
	}

	client, err := NewTaskClient(TaskOptions{
		ClientOptions: ClientOptions{
			ServerAddr: cfg.ServerAddr,
			Connection: ConnectionOptions{
				MaxRetries:        3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        1 * time.Second,
				BackoffMultiplier: 2.0,
				AutoReconnect:     cfg.AutoReconnect,
				ConnectTimeout:    DefaultConnectTimeout,
			},
			Credentials: cfg.Credentials,
		},
		Workspace:      cfg.Workspace,
		Implementation: cfg.Implementation,
		Specifier:      cfg.Specifier,
	})
	if err != nil {
		t.Fatalf("Failed to create test task client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

// TestUserConfig holds configuration for creating test user clients.
type TestUserConfig struct {
	ServerAddr    string
	UserID        string
	WindowID      string
	Workspace     string
	AutoReconnect bool
	Credentials   map[string]string
}

// DefaultTestUserConfig returns a default test user configuration.
func DefaultTestUserConfig() TestUserConfig {
	return TestUserConfig{
		ServerAddr:    GetGatewayAddr(),
		UserID:        UniqueTestIdentifier("user"),
		WindowID:      UniqueTestIdentifier("window"),
		Workspace:     GetTestWorkspace(),
		AutoReconnect: false,
	}
}

// NewTestUserClient creates a new user client for testing.
// The client is created but not connected.
func NewTestUserClient(t *testing.T, cfg TestUserConfig) *UserClient {
	t.Helper()

	if cfg.ServerAddr == "" {
		cfg.ServerAddr = GetGatewayAddr()
	}
	if cfg.UserID == "" {
		cfg.UserID = UniqueTestIdentifier("user")
	}
	if cfg.WindowID == "" {
		cfg.WindowID = UniqueTestIdentifier("window")
	}

	client, err := NewUserClient(UserOptions{
		ClientOptions: ClientOptions{
			ServerAddr: cfg.ServerAddr,
			Connection: ConnectionOptions{
				MaxRetries:        3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        1 * time.Second,
				BackoffMultiplier: 2.0,
				AutoReconnect:     cfg.AutoReconnect,
				ConnectTimeout:    DefaultConnectTimeout,
			},
			Credentials: cfg.Credentials,
		},
		UserID:    cfg.UserID,
		WindowID:  cfg.WindowID,
		Workspace: cfg.Workspace,
	})
	if err != nil {
		t.Fatalf("Failed to create test user client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

// TestOrchestratorConfig holds configuration for creating test orchestrator clients.
type TestOrchestratorConfig struct {
	ServerAddr        string
	Implementation    string
	SupportedProfiles []string
	Specifier         string
	AutoReconnect     bool
	Credentials       map[string]string
}

// DefaultTestOrchestratorConfig returns a default test orchestrator configuration.
func DefaultTestOrchestratorConfig() TestOrchestratorConfig {
	return TestOrchestratorConfig{
		ServerAddr:        GetGatewayAddr(),
		Implementation:    DefaultTestImplementation,
		SupportedProfiles: DefaultTestProfiles,
		Specifier:         UniqueTestIdentifier("orch"),
		AutoReconnect:     false,
	}
}

// NewTestOrchestratorClient creates a new orchestrator client for testing.
// The client is created but not connected.
func NewTestOrchestratorClient(t *testing.T, cfg TestOrchestratorConfig) *OrchestratorClient {
	t.Helper()

	if cfg.ServerAddr == "" {
		cfg.ServerAddr = GetGatewayAddr()
	}
	if cfg.Implementation == "" {
		cfg.Implementation = DefaultTestImplementation
	}
	if len(cfg.SupportedProfiles) == 0 {
		cfg.SupportedProfiles = DefaultTestProfiles
	}
	if cfg.Specifier == "" {
		cfg.Specifier = UniqueTestIdentifier("orch")
	}

	client, err := NewOrchestratorClient(OrchestratorOptions{
		ClientOptions: ClientOptions{
			ServerAddr: cfg.ServerAddr,
			Connection: ConnectionOptions{
				MaxRetries:        3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        1 * time.Second,
				BackoffMultiplier: 2.0,
				AutoReconnect:     cfg.AutoReconnect,
				ConnectTimeout:    DefaultConnectTimeout,
			},
			Credentials: cfg.Credentials,
		},
		Implementation:    cfg.Implementation,
		SupportedProfiles: cfg.SupportedProfiles,
		Specifier:         cfg.Specifier,
	})
	if err != nil {
		t.Fatalf("Failed to create test orchestrator client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

// =============================================================================
// Wait Helpers
// =============================================================================

// WaitForCondition waits until the condition function returns true or timeout.
// Returns true if the condition was met, false if it timed out.
func WaitForCondition(ctx context.Context, timeout time.Duration, interval time.Duration, condition func() bool) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Check immediately
	if condition() {
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if condition() {
				return true
			}
		}
	}
}

// WaitForConnection waits for a client to be connected.
func WaitForConnection(ctx context.Context, client interface{ IsConnected() bool }, timeout time.Duration) bool {
	return WaitForCondition(ctx, timeout, DefaultPollInterval, client.IsConnected)
}

// WaitForDisconnect waits for a client to be disconnected.
func WaitForDisconnect(ctx context.Context, client interface{ IsConnected() bool }, timeout time.Duration) bool {
	return WaitForCondition(ctx, timeout, DefaultPollInterval, func() bool {
		return !client.IsConnected()
	})
}

// =============================================================================
// Message Collector
// =============================================================================

// MessageCollector collects messages received by a client for testing.
type MessageCollector struct {
	mu       sync.Mutex
	messages []*Message
	notify   chan struct{}
}

// NewMessageCollector creates a new message collector.
func NewMessageCollector() *MessageCollector {
	return &MessageCollector{
		notify: make(chan struct{}, 100),
	}
}

// Handler returns a MessageHandler that collects messages.
func (c *MessageCollector) Handler() MessageHandler {
	return func(ctx context.Context, msg *Message) error {
		c.mu.Lock()
		c.messages = append(c.messages, msg)
		c.mu.Unlock()

		select {
		case c.notify <- struct{}{}:
		default:
		}

		return nil
	}
}

// Messages returns a copy of collected messages.
func (c *MessageCollector) Messages() []*Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]*Message, len(c.messages))
	copy(result, c.messages)
	return result
}

// Count returns the number of collected messages.
func (c *MessageCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}

// Clear clears all collected messages.
func (c *MessageCollector) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = nil
}

// WaitForMessage waits for at least one message to be collected.
func (c *MessageCollector) WaitForMessage(ctx context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Check if we already have messages
	c.mu.Lock()
	if len(c.messages) > 0 {
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

// WaitForCount waits for at least count messages.
func (c *MessageCollector) WaitForCount(ctx context.Context, count int, timeout time.Duration) bool {
	return WaitForCondition(ctx, timeout, DefaultPollInterval, func() bool {
		return c.Count() >= count
	})
}

// =============================================================================
// Connection Event Tracker
// =============================================================================

// ConnectionTracker tracks connection lifecycle events.
type ConnectionTracker struct {
	mu           sync.Mutex
	connects     []*ConnectionAck
	disconnects  []string
	reconnects   []int
	connectCount int32
}

// NewConnectionTracker creates a new connection tracker.
func NewConnectionTracker() *ConnectionTracker {
	return &ConnectionTracker{}
}

// ConnectHandler returns a ConnectHandler that tracks connections.
func (t *ConnectionTracker) ConnectHandler() ConnectHandler {
	return func(ctx context.Context, ack *ConnectionAck) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.connects = append(t.connects, ack)
		atomic.AddInt32(&t.connectCount, 1)
		return nil
	}
}

// DisconnectHandler returns a DisconnectHandler that tracks disconnections.
func (t *ConnectionTracker) DisconnectHandler() DisconnectHandler {
	return func(ctx context.Context, reason string) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.disconnects = append(t.disconnects, reason)
		return nil
	}
}

// ReconnectingHandler returns a ReconnectingHandler that tracks reconnection attempts.
func (t *ConnectionTracker) ReconnectingHandler() ReconnectingHandler {
	return func(ctx context.Context, attempt int) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.reconnects = append(t.reconnects, attempt)
		return nil
	}
}

// ConnectCount returns the number of successful connections.
func (t *ConnectionTracker) ConnectCount() int {
	return int(atomic.LoadInt32(&t.connectCount))
}

// DisconnectCount returns the number of disconnections.
func (t *ConnectionTracker) DisconnectCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.disconnects)
}

// ReconnectAttempts returns the reconnection attempts.
func (t *ConnectionTracker) ReconnectAttempts() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]int, len(t.reconnects))
	copy(result, t.reconnects)
	return result
}

// LastConnect returns the last connection ack, or nil if none.
func (t *ConnectionTracker) LastConnect() *ConnectionAck {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.connects) == 0 {
		return nil
	}
	return t.connects[len(t.connects)-1]
}

// LastDisconnectReason returns the last disconnect reason, or empty string.
func (t *ConnectionTracker) LastDisconnectReason() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.disconnects) == 0 {
		return ""
	}
	return t.disconnects[len(t.disconnects)-1]
}

// =============================================================================
// KV Response Collector
// =============================================================================

// KVCollector collects KV operation responses.
type KVCollector struct {
	mu        sync.Mutex
	responses []*KVResponse
	notify    chan struct{}
}

// NewKVCollector creates a new KV response collector.
func NewKVCollector() *KVCollector {
	return &KVCollector{
		notify: make(chan struct{}, 100),
	}
}

// Handler returns a KVResponseHandler that collects responses.
func (c *KVCollector) Handler() KVResponseHandler {
	return func(ctx context.Context, resp *KVResponse) error {
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
func (c *KVCollector) Responses() []*KVResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]*KVResponse, len(c.responses))
	copy(result, c.responses)
	return result
}

// LastResponse returns the last response, or nil if none.
func (c *KVCollector) LastResponse() *KVResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.responses) == 0 {
		return nil
	}
	return c.responses[len(c.responses)-1]
}

// WaitForResponse waits for at least one response.
func (c *KVCollector) WaitForResponse(ctx context.Context, timeout time.Duration) bool {
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
func (c *KVCollector) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responses = nil
}

// =============================================================================
// Test Data Generators
// =============================================================================

// TestPayload returns a test message payload.
func TestPayload(message string) []byte {
	if message == "" {
		message = "Hello, Aether!"
	}
	return []byte(`{"message": "` + message + `"}`)
}

// TestKVData returns test KV data.
func TestKVData(key, value string) []byte {
	return []byte(`{"` + key + `": "` + value + `"}`)
}

// TestCheckpointData returns test checkpoint data.
func TestCheckpointData(state string, version int) []byte {
	return []byte(`{"state": "` + state + `", "version": ` + strconv.Itoa(version) + `}`)
}

// =============================================================================
// Assertion Helpers
// =============================================================================

// AssertConnected asserts that a client is connected.
func AssertConnected(t *testing.T, client interface{ IsConnected() bool }) {
	t.Helper()
	if !client.IsConnected() {
		t.Fatal("client is not connected")
	}
}

// AssertDisconnected asserts that a client is disconnected.
func AssertDisconnected(t *testing.T, client interface{ IsConnected() bool }) {
	t.Helper()
	if client.IsConnected() {
		t.Fatal("client is still connected")
	}
}

// AssertMessageReceived asserts that at least one message was received.
func AssertMessageReceived(t *testing.T, collector *MessageCollector) {
	t.Helper()
	if collector.Count() == 0 {
		t.Fatal("no messages received")
	}
}

// AssertMessageCount asserts the exact number of messages received.
func AssertMessageCount(t *testing.T, collector *MessageCollector, expected int) {
	t.Helper()
	actual := collector.Count()
	if actual != expected {
		t.Fatalf("expected %d messages, got %d", expected, actual)
	}
}
