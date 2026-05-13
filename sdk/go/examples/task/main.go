// Package main provides an example task client demonstrating the Aether Go SDK.
//
// This example demonstrates task-specific features of the Aether Go SDK, including:
//   - Unique task client (with specifier) - has exclusive identity
//   - Non-unique task client (server-assigned ID) - can have multiple workers
//   - Message handling with callback handlers
//   - Sending messages to agents, users, and other tasks
//   - Event and metric publishing
//   - Workspace switching
//   - Error handling and graceful shutdown
//
// Usage:
//
//	go run main.go [flags]
//
// Flags:
//
//	-server     Gateway server address (default: localhost:50051)
//	-workspace  Workspace to connect to (default: default)
//	-impl       Task implementation name (default: go-demo-task)
//	-spec       Task specifier (empty for non-unique task)
//	-unique     Create a unique task (default: true)
//
// Example (unique task):
//
//	go run main.go -server=localhost:50051 -workspace=prod -impl=processor -spec=instance-1
//
// Example (non-unique task / worker pool):
//
//	go run main.go -server=localhost:50051 -workspace=prod -impl=worker -unique=false
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
	Unique         bool
	TLSEnabled     bool
	TLSCertPath    string
	TLSKeyPath     string
	TLSCAPath      string
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.ServerAddr, "server", "localhost:50051", "Gateway server address")
	flag.StringVar(&cfg.Workspace, "workspace", "default", "Workspace to connect to")
	flag.StringVar(&cfg.Implementation, "impl", "go-demo-task", "Task implementation name")
	flag.StringVar(&cfg.Specifier, "spec", "task-01", "Task specifier (for unique tasks)")
	flag.BoolVar(&cfg.Unique, "unique", true, "Create a unique task (false for non-unique/worker pool)")
	flag.BoolVar(&cfg.TLSEnabled, "tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS client certificate path (for mTLS)")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS client key path (for mTLS)")
	flag.StringVar(&cfg.TLSCAPath, "tls-ca", "", "TLS CA certificate path")

	flag.Parse()

	// If not unique, clear specifier
	if !cfg.Unique {
		cfg.Specifier = ""
	}

	return cfg
}

// =============================================================================
// Main
// =============================================================================

func main() {
	cfg := parseFlags()

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Aether Go SDK - Task Client Example")
	fmt.Println("============================================================")
	fmt.Println()
	fmt.Printf("Server:         %s\n", cfg.ServerAddr)
	fmt.Printf("Workspace:      %s\n", cfg.Workspace)
	fmt.Printf("Implementation: %s\n", cfg.Implementation)
	if cfg.Unique {
		fmt.Printf("Specifier:      %s\n", cfg.Specifier)
		fmt.Printf("Type:           Unique Task\n")
	} else {
		fmt.Printf("Type:           Non-Unique Task (worker pool)\n")
	}
	fmt.Println()

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		fmt.Printf("\n[Task] Received signal %v, shutting down...\n", sig)
		cancel()
	}()

	// Run the task demo
	if err := runTaskDemo(ctx, cfg); err != nil {
		fmt.Printf("[Task] Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("[Task] Disconnected.")
}

// =============================================================================
// Task Demo
// =============================================================================

func runTaskDemo(ctx context.Context, cfg Config) error {
	// Build TLS configuration if enabled
	var tlsConfig *aether.TLSConfig
	if cfg.TLSEnabled {
		var err error
		tlsConfig, err = buildTLSConfig(cfg)
		if err != nil {
			return fmt.Errorf("failed to build TLS config: %w", err)
		}
	}

	// Create the task client
	client, err := aether.NewTaskClient(aether.TaskOptions{
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
		Specifier:      cfg.Specifier, // Empty for non-unique tasks
	})
	if err != nil {
		return fmt.Errorf("failed to create task client: %w", err)
	}

	// Set up handler callbacks
	setupHandlers(client)

	// Connect to the gateway
	fmt.Println("[Task] Connecting to gateway...")
	if err := client.Connect(ctx); err != nil {
		return handleConnectionError(err)
	}
	fmt.Println("[Task] Connected!")

	// Print task identity information
	fmt.Println()
	fmt.Println("--- Task Identity ---")
	fmt.Printf("Workspace:      %s\n", client.Workspace())
	fmt.Printf("Implementation: %s\n", client.Implementation())
	fmt.Printf("Is Unique:      %v\n", client.IsUnique())
	if client.IsUnique() {
		fmt.Printf("Specifier:      %s\n", client.Specifier())
		fmt.Printf("Topic:          %s\n", client.Topic())
	} else {
		fmt.Printf("Assigned ID:    %s\n", client.AssignedID())
		fmt.Printf("Topic:          %s\n", client.Topic())
		fmt.Printf("Broadcast Topic: %s\n", client.BroadcastTopic())
	}

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

func setupHandlers(client *aether.TaskClient) {
	// Message handler - receives incoming messages
	// For non-unique tasks, this includes broadcast messages for work claiming
	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
		fmt.Printf("[Task] Message from %s: %s\n", msg.SourceTopic, string(msg.Payload))

		// Example: parse work assignment from broadcast topic
		if !client.IsUnique() {
			// Non-unique tasks receive work from broadcast topic
			// You would typically parse the message and process the work item
			fmt.Println("[Task] Processing work item from broadcast...")
		}

		return nil
	})

	// Config handler - receives workspace configuration on connect
	client.OnConfig(func(ctx context.Context, config *aether.ConfigSnapshot) error {
		fmt.Println("[Task] Config snapshot received:")
		fmt.Printf("  Workspace KV: %v\n", config.KV)
		fmt.Printf("  Global KV:    %v\n", config.GlobalKV)
		return nil
	})

	// Task assignment handler - receives task assignments
	client.OnTaskAssignment(func(ctx context.Context, task *aether.TaskAssignment) error {
		fmt.Println("[Task] Task assignment received:")
		fmt.Printf("  Task ID:    %s\n", task.TaskID)
		fmt.Printf("  Task Type:  %s\n", task.TaskType)
		fmt.Printf("  Assigned To: %s\n", task.AssignedTo)
		fmt.Printf("  Metadata:   %v\n", task.Metadata)
		return nil
	})

	// KV response handler - receives responses to KV operations
	client.OnKVResponse(func(ctx context.Context, resp *aether.KVResponse) error {
		fmt.Printf("[Task] KV Response: success=%v\n", resp.Success)
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
		fmt.Printf("[Task] Error: %s - %s\n", err.Code, err.Message)
		return nil
	})

	// Signal handler - receives signals like FORCE_DISCONNECT or GRACEFUL_DISCONNECT
	client.OnSignal(func(ctx context.Context, sig *aether.Signal) error {
		fmt.Printf("[Task] Signal: %s - %s\n", sig.Type.String(), sig.Reason)
		return nil
	})

	// Connection lifecycle handlers
	client.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
		fmt.Printf("[Task] Connected with session %s (resumed: %v)\n", ack.SessionID, ack.Resumed)
		return nil
	})

	client.OnDisconnect(func(ctx context.Context, reason string) error {
		fmt.Printf("[Task] Disconnected: %s\n", reason)
		return nil
	})

	client.OnReconnecting(func(ctx context.Context, attempt int) error {
		fmt.Printf("[Task] Reconnection attempt %d...\n", attempt)
		return nil
	})
}

// =============================================================================
// Demo Operations
// =============================================================================

func runDemoOperations(ctx context.Context, client *aether.TaskClient, cfg Config) {
	// Demo: Send a message to an agent
	fmt.Println()
	fmt.Println("--- Sending message to agent ---")
	if err := client.SendToAgent(cfg.Workspace, "go-demo", "agent-01", []byte("Hello from Go task!")); err != nil {
		fmt.Printf("[Task] Error sending message: %v\n", err)
	} else {
		fmt.Println("[Task] Message sent to agent")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Send a message to another task (unique)
	fmt.Println()
	fmt.Println("--- Sending message to unique task ---")
	if err := client.SendToTask(cfg.Workspace, "data-processor", "main", []byte("Task-to-task message")); err != nil {
		fmt.Printf("[Task] Error sending message: %v\n", err)
	} else {
		fmt.Println("[Task] Message sent to unique task")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Send a message to non-unique task pool (broadcast)
	fmt.Println()
	fmt.Println("--- Sending message to task pool (broadcast) ---")
	workItem := map[string]interface{}{
		"job_id":   "job-123",
		"data":     "process this",
		"priority": "high",
	}
	workPayload, _ := json.Marshal(workItem)
	// Empty specifier = broadcast to task pool
	if err := client.SendToTask(cfg.Workspace, "worker-pool", "", workPayload); err != nil {
		fmt.Printf("[Task] Error sending to task pool: %v\n", err)
	} else {
		fmt.Println("[Task] Work item sent to task pool")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Send a message to a user
	fmt.Println()
	fmt.Println("--- Sending message to user ---")
	if err := client.SendToUser("user-123", "window-1", []byte("Task completed notification")); err != nil {
		fmt.Printf("[Task] Error sending to user: %v\n", err)
	} else {
		fmt.Println("[Task] Notification sent to user")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Send events and metrics
	fmt.Println()
	fmt.Println("--- Events and Metrics ---")

	// Send an event to the workflow engine
	eventPayload, _ := json.Marshal(map[string]interface{}{
		"event_type": "task_progress",
		"task_impl":  cfg.Implementation,
		"progress":   50,
		"timestamp":  time.Now().Unix(),
	})
	if err := client.SendEvent(eventPayload); err != nil {
		fmt.Printf("[Task] Error sending event: %v\n", err)
	} else {
		fmt.Println("Sent event to workflow engine")
	}

	// Send a metric to the metrics bridge
	metric := aether.NewMetric().
		Add("items_processed", "", 100).
		Tag("task", cfg.Implementation).
		Tag("workspace", cfg.Workspace).
		Build()
	if err := client.SendMetric(metric); err != nil {
		fmt.Printf("[Task] Error sending metric: %v\n", err)
	} else {
		fmt.Println("Sent metric to metrics bridge")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Workspace switching (for tasks that need to change workspace)
	fmt.Println()
	fmt.Println("--- Workspace Operations ---")
	fmt.Printf("Current workspace: %s\n", client.Workspace())
	// Note: Uncomment to test workspace switching:
	// if err := client.SwitchWorkspace("another-workspace"); err != nil {
	//     fmt.Printf("[Task] Error switching workspace: %v\n", err)
	// } else {
	//     fmt.Printf("Switched to workspace: %s\n", client.Workspace())
	// }

	time.Sleep(1 * time.Second)

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Demo operations complete!")
	if client.IsUnique() {
		fmt.Println("Waiting for messages (Ctrl+C to exit)...")
	} else {
		fmt.Println("Waiting for work items on broadcast topic (Ctrl+C to exit)...")
	}
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
		fmt.Printf("[Task] Authentication failed: %v\n", e)
		return err

	case *aether.DuplicateIdentityError:
		fmt.Printf("[Task] Identity already in use: %v\n", e.Identity)
		return err

	case *aether.ReconnectionError:
		fmt.Printf("[Task] Could not reconnect after %d attempts\n", e.Attempts)
		return err

	case *aether.ConnectionError:
		fmt.Printf("[Task] Connection failed: %v\n", e)
		return err

	case *aether.TimeoutError:
		fmt.Printf("[Task] Connection timed out: %v\n", e)
		return err

	default:
		// Check if it's an AetherError
		if aetherErr, ok := err.(*aether.AetherError); ok {
			fmt.Printf("[Task] Aether error: %v\n", aetherErr)
			return err
		}

		// Unknown error
		return fmt.Errorf("unexpected error: %w", err)
	}
}
