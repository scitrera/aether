// Package main provides an example agent client demonstrating the Aether Go SDK.
//
// This example demonstrates the full feature set of the Aether Go SDK, including:
//   - Agent client creation with configuration
//   - Handler callbacks for messages, config, errors, etc.
//   - Message sending to agents, tasks, and users
//   - KV operations with different scopes (global, workspace, user)
//   - Task creation with different assignment modes
//   - Event and metric publishing
//   - Error handling and graceful shutdown
//   - TLS/mTLS configuration examples
//
// Usage:
//
//	go run main.go [flags]
//
// Flags:
//
//	-server    Gateway server address (default: localhost:50051)
//	-workspace Workspace to connect to (default: default)
//	-impl      Agent implementation name (default: go-demo)
//	-spec      Agent specifier/instance ID (default: agent-01)
//
// Example:
//
//	go run main.go -server=localhost:50051 -workspace=prod -impl=processor -spec=instance-1
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Configuration
// =============================================================================

// Config holds the example configuration.
type Config struct {
	ServerAddr     string
	Workspace      string
	Implementation string
	Specifier      string
	TLSEnabled     bool
	TLSCertPath    string
	TLSKeyPath     string
	TLSCAPath      string
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.ServerAddr, "server", "localhost:50051", "Gateway server address")
	flag.StringVar(&cfg.Workspace, "workspace", "default", "Workspace to connect to")
	flag.StringVar(&cfg.Implementation, "impl", "go-demo", "Agent implementation name")
	flag.StringVar(&cfg.Specifier, "spec", "agent-01", "Agent specifier/instance ID")
	flag.BoolVar(&cfg.TLSEnabled, "tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS client certificate path (for mTLS)")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS client key path (for mTLS)")
	flag.StringVar(&cfg.TLSCAPath, "tls-ca", "", "TLS CA certificate path")

	flag.Parse()

	return cfg
}

// =============================================================================
// Main
// =============================================================================

func main() {
	cfg := parseFlags()

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Aether Go SDK - Agent Client Example")
	fmt.Println("============================================================")
	fmt.Println()
	fmt.Printf("Server:         %s\n", cfg.ServerAddr)
	fmt.Printf("Workspace:      %s\n", cfg.Workspace)
	fmt.Printf("Implementation: %s\n", cfg.Implementation)
	fmt.Printf("Specifier:      %s\n", cfg.Specifier)
	fmt.Println()

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		fmt.Printf("\n[Agent] Received signal %v, shutting down...\n", sig)
		cancel()
	}()

	// Run the agent demo
	if err := runAgentDemo(ctx, cfg); err != nil {
		fmt.Printf("[Agent] Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("[Agent] Disconnected.")
}

// =============================================================================
// Agent Demo
// =============================================================================

func runAgentDemo(ctx context.Context, cfg Config) error {
	// Build TLS configuration if enabled
	var tlsConfig *aether.TLSConfig
	if cfg.TLSEnabled {
		var err error
		tlsConfig, err = buildTLSConfig(cfg)
		if err != nil {
			return fmt.Errorf("failed to build TLS config: %w", err)
		}
	}

	// Create the agent client
	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: cfg.ServerAddr,
			Connection: aether.ConnectionOptions{
				MaxRetries:        10,
				InitialBackoff:    1 * time.Second,
				MaxBackoff:        30 * time.Second,
				BackoffMultiplier: 2.0,
				AutoReconnect:     true,
				ConnectTimeout:    30 * time.Second,
				KeepAliveInterval: 30 * time.Second,
			},
			TLS:         tlsConfig,
			Credentials: buildCredentials(),
		},
		Workspace:      cfg.Workspace,
		Implementation: cfg.Implementation,
		Specifier:      cfg.Specifier,
	})
	if err != nil {
		return fmt.Errorf("failed to create agent client: %w", err)
	}

	// Set up handler callbacks
	setupHandlers(client)

	// Connect to the gateway
	fmt.Println("[Agent] Connecting to gateway...")
	if err := client.Connect(ctx); err != nil {
		return handleConnectionError(err)
	}
	fmt.Println("[Agent] Connected!")

	// Run demo operations in a goroutine
	go func() {
		// Wait for connection to stabilize
		time.Sleep(1 * time.Second)

		// Run demo operations
		runDemoOperations(ctx, client, cfg)
	}()

	// Start the message loop (blocks until disconnection)
	err = client.Run(ctx)
	if err != nil {
		// Check if this was a graceful shutdown
		if ctx.Err() != nil {
			return nil // Graceful shutdown
		}
		return handleConnectionError(err)
	}

	return nil
}

// =============================================================================
// Handler Setup
// =============================================================================

func setupHandlers(client *aether.AgentClient) {
	// Message handler - receives incoming messages
	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
		fmt.Printf("[Agent] Message from %s: %s\n", msg.SourceTopic, string(msg.Payload))
		return nil
	})

	// Config handler - receives workspace configuration on connect
	client.OnConfig(func(ctx context.Context, config *aether.ConfigSnapshot) error {
		fmt.Println("[Agent] Config snapshot received:")
		fmt.Printf("  Workspace KV: %v\n", config.KV)
		fmt.Printf("  Global KV:    %v\n", config.GlobalKV)
		return nil
	})

	// Task assignment handler - receives task assignments
	client.OnTaskAssignment(func(ctx context.Context, task *aether.TaskAssignment) error {
		fmt.Println("[Agent] Task assigned:")
		fmt.Printf("  Task ID:    %s\n", task.TaskID)
		fmt.Printf("  Task Type:  %s\n", task.TaskType)
		fmt.Printf("  Assigned To: %s\n", task.AssignedTo)
		fmt.Printf("  Metadata:   %v\n", task.Metadata)
		return nil
	})

	// KV response handler - receives responses to KV operations
	client.OnKVResponse(func(ctx context.Context, resp *aether.KVResponse) error {
		fmt.Printf("[Agent] KV Response: success=%v\n", resp.Success)
		if len(resp.Value) > 0 {
			fmt.Printf("  Value: %s\n", resp.Value)
		}
		if len(resp.Keys) > 0 {
			fmt.Printf("  Keys: %v\n", resp.Keys)
		}
		return nil
	})

	// Error handler - receives protocol-level errors
	client.OnError(func(ctx context.Context, err *aether.ErrorInfo) error {
		fmt.Printf("[Agent] Error: %s - %s\n", err.Code, err.Message)
		return nil
	})

	// Signal handler - receives signals like FORCE_DISCONNECT or GRACEFUL_DISCONNECT
	client.OnSignal(func(ctx context.Context, sig *aether.Signal) error {
		fmt.Printf("[Agent] Signal: %s - %s\n", sig.Type.String(), sig.Reason)
		return nil
	})

	// Connection lifecycle handlers
	client.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
		fmt.Printf("[Agent] Connected with session %s (resumed: %v)\n", ack.SessionID, ack.Resumed)
		return nil
	})

	client.OnDisconnect(func(ctx context.Context, reason string) error {
		fmt.Printf("[Agent] Disconnected: %s\n", reason)
		return nil
	})

	client.OnReconnecting(func(ctx context.Context, attempt int) error {
		fmt.Printf("[Agent] Reconnection attempt %d...\n", attempt)
		return nil
	})
}

// =============================================================================
// Demo Operations
// =============================================================================

func runDemoOperations(ctx context.Context, client *aether.AgentClient, cfg Config) {
	// Demo: Send a message to ourselves
	fmt.Println()
	fmt.Println("--- Sending message to self ---")
	if err := client.SendToAgent(cfg.Workspace, cfg.Implementation, cfg.Specifier, []byte("Hello from Go agent!")); err != nil {
		fmt.Printf("[Agent] Error sending message: %v\n", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: KV operations with different scopes
	fmt.Println()
	fmt.Println("--- KV Operations ---")

	// Global scope
	fmt.Println("Storing value in global scope...")
	if err := client.KV().PutGlobal("demo/setting", []byte("global-value")); err != nil {
		fmt.Printf("[Agent] Error putting KV: %v\n", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Workspace scope
	fmt.Println("Storing value in workspace scope...")
	if err := client.KV().PutWorkspace("demo/workspace-setting", []byte("workspace-value"), cfg.Workspace); err != nil {
		fmt.Printf("[Agent] Error putting KV: %v\n", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Retrieve a value (async - response comes via OnKVResponse handler)
	fmt.Println("Getting value from global scope...")
	if err := client.KV().GetGlobal("demo/setting"); err != nil {
		fmt.Printf("[Agent] Error getting KV: %v\n", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Synchronous KV operation
	fmt.Println()
	fmt.Println("--- Synchronous KV Operation ---")
	resp, err := client.KV().GetSync(ctx, aether.KVGetOptions{
		Key:   "demo/setting",
		Scope: aether.KVScopeGlobal,
	})
	if err != nil {
		fmt.Printf("[Agent] Error in sync KV get: %v\n", err)
	} else {
		fmt.Printf("Sync KV Get result: success=%v, value=%s\n", resp.Success, resp.Value)
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Create a self-assigned task
	fmt.Println()
	fmt.Println("--- Task Creation (Self-Assign) ---")
	if err := client.CreateTask(aether.CreateTaskOptions{
		TaskType:       "data-processing",
		Workspace:      cfg.Workspace,
		AssignmentMode: aether.TaskAssignmentSelfAssign,
		Metadata: map[string]string{
			"source":   "go-demo",
			"priority": "normal",
		},
	}); err != nil {
		fmt.Printf("[Agent] Error creating task: %v\n", err)
	} else {
		fmt.Println("Self-assigned task created!")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Send events and metrics
	fmt.Println()
	fmt.Println("--- Events and Metrics ---")

	// Send an event to the workflow engine
	eventPayload, _ := json.Marshal(map[string]interface{}{
		"event_type": "agent_started",
		"agent_id":   fmt.Sprintf("%s.%s", cfg.Implementation, cfg.Specifier),
		"timestamp":  time.Now().Unix(),
	})
	if err := client.SendEvent(eventPayload); err != nil {
		fmt.Printf("[Agent] Error sending event: %v\n", err)
	} else {
		fmt.Println("Sent event to workflow engine")
	}

	// Send a metric to the metrics bridge
	metric := aether.NewMetric().
		Add("messages_processed", "", 42).
		Tag("agent", cfg.Implementation).
		Build()
	if err := client.SendMetric(metric); err != nil {
		fmt.Printf("[Agent] Error sending metric: %v\n", err)
	} else {
		fmt.Println("Sent metric to metrics bridge")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Broadcast to all agents in workspace
	fmt.Println()
	fmt.Println("--- Broadcast ---")
	if err := client.BroadcastToAgents(cfg.Workspace, []byte("Hello all agents!")); err != nil {
		fmt.Printf("[Agent] Error broadcasting: %v\n", err)
	} else {
		fmt.Println("Broadcast sent to all agents in workspace")
	}

	time.Sleep(1 * time.Second)

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Demo operations complete!")
	fmt.Println("Waiting for messages (Ctrl+C to exit)...")
	fmt.Println("============================================================")
}

// =============================================================================
// Credentials
// =============================================================================

// buildCredentials constructs client credentials from environment variables.
// AETHER_API_KEY sets the API key; AETHER_TENANT sets the tenant ID.
func buildCredentials() aether.Credentials {
	creds := aether.NewCredentials()
	if apiKey := os.Getenv("AETHER_API_KEY"); apiKey != "" {
		creds = creds.WithAPIKey(apiKey)
	}
	if tenant := os.Getenv("AETHER_TENANT"); tenant != "" {
		creds = creds.WithTenant(tenant)
	}
	return creds
}

// =============================================================================
// TLS Configuration
// =============================================================================

func buildTLSConfig(cfg Config) (*aether.TLSConfig, error) {
	tlsConfig := &aether.TLSConfig{
		Enabled: true,
	}

	// Load CA certificate if provided
	if cfg.TLSCAPath != "" {
		caData, err := os.ReadFile(cfg.TLSCAPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}
		tlsConfig.RootCAs = caData
	}

	// Load client certificate and key for mTLS if provided
	if cfg.TLSCertPath != "" && cfg.TLSKeyPath != "" {
		certData, err := os.ReadFile(cfg.TLSCertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read client cert: %w", err)
		}
		tlsConfig.ClientCert = certData

		keyData, err := os.ReadFile(cfg.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read client key: %w", err)
		}
		tlsConfig.ClientKey = keyData
	}

	return tlsConfig, nil
}

// =============================================================================
// Error Handling
// =============================================================================

func handleConnectionError(err error) error {
	// Use the error type hierarchy for specific handling
	switch e := err.(type) {
	case *aether.AuthenticationError:
		fmt.Printf("[Agent] Authentication failed: %v\n", e)
		return err

	case *aether.DuplicateIdentityError:
		fmt.Printf("[Agent] Identity already in use: %v\n", e.Identity)
		return err

	case *aether.ReconnectionError:
		fmt.Printf("[Agent] Could not reconnect after %d attempts\n", e.Attempts)
		return err

	case *aether.ConnectionError:
		fmt.Printf("[Agent] Connection failed: %v\n", e)
		return err

	case *aether.TimeoutError:
		fmt.Printf("[Agent] Connection timed out: %v\n", e)
		return err

	default:
		// Check if it's an AetherError
		if aetherErr, ok := err.(*aether.AetherError); ok {
			fmt.Printf("[Agent] Aether error: %v\n", aetherErr)
			return err
		}

		// Unknown error
		return fmt.Errorf("unexpected error: %w", err)
	}
}
