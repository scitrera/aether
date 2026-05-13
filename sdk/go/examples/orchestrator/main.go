// Package main provides an example orchestrator client demonstrating the Aether Go SDK.
//
// This example demonstrates orchestrator-specific features of the Aether Go SDK, including:
//   - Orchestrator client creation with supported profiles
//   - Task assignment handling for starting agents/tasks
//   - Sending status messages to agents and tasks
//   - Integration with the orchestration lifecycle
//   - Error handling and graceful shutdown
//
// Orchestrators are responsible for managing agent/task lifecycle:
//   - Receiving startup requests when targeted agents are offline
//   - Launching compute resources (containers, VMs, processes)
//   - Managing agent pools and scaling
//   - Processing task assignments based on supported profiles
//
// Usage:
//
//	go run main.go [flags]
//
// Flags:
//
//	-server    Gateway server address (default: localhost:50051)
//	-impl      Orchestrator implementation name (default: go-demo-orchestrator)
//	-spec      Orchestrator specifier (optional)
//	-profiles  Comma-separated list of supported profiles (default: docker,kubernetes)
//
// Example:
//
//	go run main.go -server=localhost:50051 -impl=k8s-orchestrator -profiles=k8s-worker,k8s-agent
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Configuration
// =============================================================================

// Config holds the example configuration.
type Config struct {
	ServerAddr        string
	Implementation    string
	Specifier         string
	SupportedProfiles []string
	TLSEnabled        bool
	TLSCertPath       string
	TLSKeyPath        string
	TLSCAPath         string
}

func parseFlags() Config {
	cfg := Config{}
	var profilesStr string

	flag.StringVar(&cfg.ServerAddr, "server", "localhost:50051", "Gateway server address")
	flag.StringVar(&cfg.Implementation, "impl", "go-demo-orchestrator", "Orchestrator implementation name")
	flag.StringVar(&cfg.Specifier, "spec", "", "Orchestrator specifier (optional)")
	flag.StringVar(&profilesStr, "profiles", "docker,kubernetes", "Comma-separated list of supported profiles")
	flag.BoolVar(&cfg.TLSEnabled, "tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS client certificate path (for mTLS)")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS client key path (for mTLS)")
	flag.StringVar(&cfg.TLSCAPath, "tls-ca", "", "TLS CA certificate path")

	flag.Parse()

	// Parse profiles
	if profilesStr != "" {
		cfg.SupportedProfiles = strings.Split(profilesStr, ",")
		for i, p := range cfg.SupportedProfiles {
			cfg.SupportedProfiles[i] = strings.TrimSpace(p)
		}
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
	fmt.Println("Aether Go SDK - Orchestrator Client Example")
	fmt.Println("============================================================")
	fmt.Println()
	fmt.Printf("Server:            %s\n", cfg.ServerAddr)
	fmt.Printf("Implementation:    %s\n", cfg.Implementation)
	if cfg.Specifier != "" {
		fmt.Printf("Specifier:         %s\n", cfg.Specifier)
	}
	fmt.Printf("Supported Profiles: %v\n", cfg.SupportedProfiles)
	fmt.Println()

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		fmt.Printf("\n[Orchestrator] Received signal %v, shutting down...\n", sig)
		cancel()
	}()

	// Run the orchestrator demo
	if err := runOrchestratorDemo(ctx, cfg); err != nil {
		fmt.Printf("[Orchestrator] Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("[Orchestrator] Disconnected.")
}

// =============================================================================
// Orchestrator Demo
// =============================================================================

func runOrchestratorDemo(ctx context.Context, cfg Config) error {
	// Build TLS configuration if enabled
	var tlsConfig *aether.TLSConfig
	if cfg.TLSEnabled {
		var err error
		tlsConfig, err = buildTLSConfig(cfg)
		if err != nil {
			return fmt.Errorf("failed to build TLS config: %w", err)
		}
	}

	// Create the orchestrator client
	client, err := aether.NewOrchestratorClient(aether.OrchestratorOptions{
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
		Implementation:    cfg.Implementation,
		Specifier:         cfg.Specifier,
		SupportedProfiles: cfg.SupportedProfiles,
	})
	if err != nil {
		return fmt.Errorf("failed to create orchestrator client: %w", err)
	}

	// Set up handler callbacks
	setupHandlers(client)

	// Connect to the gateway
	fmt.Println("[Orchestrator] Connecting to gateway...")
	if err := client.Connect(ctx); err != nil {
		return handleConnectionError(err)
	}
	fmt.Println("[Orchestrator] Connected!")

	// Print orchestrator identity information
	fmt.Println()
	fmt.Println("--- Orchestrator Identity ---")
	fmt.Printf("Implementation:    %s\n", client.Implementation())
	fmt.Printf("Specifier:         %s\n", client.Specifier())
	fmt.Printf("Supported Profiles: %v\n", client.SupportedProfiles())

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

func setupHandlers(client *aether.OrchestratorClient) {
	// Message handler - receives incoming messages (startup requests, etc.)
	client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
		fmt.Printf("[Orchestrator] Message from %s:\n", msg.SourceTopic)

		// Try to parse as JSON for better display
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &data); err == nil {
			fmt.Printf("  Parsed: %v\n", data)
		} else {
			fmt.Printf("  Raw: %s\n", string(msg.Payload))
		}

		return nil
	})

	// Task assignment handler - this is the primary handler for orchestrators
	// It receives requests to start new agents/tasks
	client.OnTaskAssignment(func(ctx context.Context, task *aether.TaskAssignment) error {
		fmt.Println()
		fmt.Println("[Orchestrator] *** TASK ASSIGNMENT RECEIVED ***")
		fmt.Printf("  Task ID:              %s\n", task.TaskID)
		fmt.Printf("  Task Type:            %s\n", task.TaskType)
		fmt.Printf("  Profile:              %s\n", task.Profile)
		fmt.Printf("  Target Implementation: %s\n", task.TargetImplementation)
		fmt.Printf("  Workspace:            %s\n", task.Workspace)
		fmt.Printf("  Specifier:            %s\n", task.Specifier)
		fmt.Printf("  Assigned To:          %s\n", task.AssignedTo)
		fmt.Printf("  Launch Params:        %v\n", task.LaunchParams)
		fmt.Printf("  Metadata:             %v\n", task.Metadata)
		fmt.Printf("  Assigned At:          %v\n", task.AssignedAt)
		fmt.Println()

		// In a real orchestrator, you would:
		// 1. Check if you support the requested profile
		if !client.SupportsProfile(task.Profile) {
			fmt.Printf("[Orchestrator] WARNING: Unsupported profile %s\n", task.Profile)
			return nil
		}

		// 2. Launch the appropriate compute resource based on profile
		switch task.Profile {
		case "docker":
			fmt.Println("[Orchestrator] Would launch Docker container...")
			// Example: docker.Run(task.TargetImplementation, task.LaunchParams)
		case "kubernetes":
			fmt.Println("[Orchestrator] Would create Kubernetes pod...")
			// Example: k8s.CreatePod(task.TargetImplementation, task.LaunchParams)
		default:
			fmt.Printf("[Orchestrator] Using default launcher for profile: %s\n", task.Profile)
		}

		// 3. Send status update back to the target agent/task
		statusPayload, _ := json.Marshal(map[string]interface{}{
			"status":  "launching",
			"task_id": task.TaskID,
			"message": "Orchestrator is starting your instance",
		})

		if task.Workspace != "" && task.TargetImplementation != "" && task.Specifier != "" {
			if err := client.SendStatusToAgent(task.Workspace, task.TargetImplementation, task.Specifier, statusPayload); err != nil {
				fmt.Printf("[Orchestrator] Error sending status: %v\n", err)
			} else {
				fmt.Println("[Orchestrator] Sent startup status to agent")
			}
		}

		return nil
	})

	// Config handler
	client.OnConfig(func(ctx context.Context, config *aether.ConfigSnapshot) error {
		fmt.Println("[Orchestrator] Config snapshot received:")
		fmt.Printf("  KV entries: %d\n", len(config.KV))
		fmt.Printf("  Global KV entries: %d\n", len(config.GlobalKV))
		return nil
	})

	// Error handler - receives protocol-level errors
	client.OnError(func(ctx context.Context, err *aether.ErrorInfo) error {
		fmt.Printf("[Orchestrator] Error: %s - %s\n", err.Code, err.Message)
		return nil
	})

	// Signal handler - receives signals like FORCE_DISCONNECT or GRACEFUL_DISCONNECT
	client.OnSignal(func(ctx context.Context, sig *aether.Signal) error {
		fmt.Printf("[Orchestrator] Signal: %s - %s\n", sig.Type.String(), sig.Reason)
		return nil
	})

	// Connection lifecycle handlers
	client.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
		fmt.Printf("[Orchestrator] Connected with session %s (resumed: %v)\n", ack.SessionID, ack.Resumed)
		return nil
	})

	client.OnDisconnect(func(ctx context.Context, reason string) error {
		fmt.Printf("[Orchestrator] Disconnected: %s\n", reason)
		return nil
	})

	client.OnReconnecting(func(ctx context.Context, attempt int) error {
		fmt.Printf("[Orchestrator] Reconnection attempt %d...\n", attempt)
		return nil
	})
}

// =============================================================================
// Demo Operations
// =============================================================================

func runDemoOperations(ctx context.Context, client *aether.OrchestratorClient, cfg Config) {
	// Demo: Check profile support
	fmt.Println()
	fmt.Println("--- Profile Support Check ---")
	testProfiles := []string{"docker", "kubernetes", "aws-lambda", "bare-metal"}
	for _, profile := range testProfiles {
		supported := client.SupportsProfile(profile)
		fmt.Printf("Profile '%s': %v\n", profile, supported)
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Send status message to an agent
	fmt.Println()
	fmt.Println("--- Sending status to agent ---")
	statusPayload, _ := json.Marshal(map[string]interface{}{
		"status":       "ready",
		"orchestrator": cfg.Implementation,
		"profiles":     cfg.SupportedProfiles,
		"timestamp":    time.Now().Unix(),
	})
	if err := client.SendStatusToAgent("default", "go-demo", "agent-01", statusPayload); err != nil {
		fmt.Printf("[Orchestrator] Error sending status: %v\n", err)
	} else {
		fmt.Println("[Orchestrator] Status sent to agent")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Send status to a unique task
	fmt.Println()
	fmt.Println("--- Sending status to unique task ---")
	taskStatus, _ := json.Marshal(map[string]interface{}{
		"status":  "orchestrator_ready",
		"message": "Orchestrator is available for task management",
	})
	if err := client.SendStatusToTask("default", "data-processor", "main", taskStatus); err != nil {
		fmt.Printf("[Orchestrator] Error sending status: %v\n", err)
	} else {
		fmt.Println("[Orchestrator] Status sent to unique task")
	}

	time.Sleep(500 * time.Millisecond)

	// Demo: Broadcast status to task pool (non-unique tasks)
	fmt.Println()
	fmt.Println("--- Broadcasting status to task pool ---")
	poolStatus, _ := json.Marshal(map[string]interface{}{
		"status":  "orchestrator_available",
		"message": "Orchestrator can scale worker pool as needed",
	})
	// Empty specifier = broadcast to task pool
	if err := client.SendStatusToTask("default", "worker-pool", "", poolStatus); err != nil {
		fmt.Printf("[Orchestrator] Error broadcasting status: %v\n", err)
	} else {
		fmt.Println("[Orchestrator] Status broadcast to task pool")
	}

	time.Sleep(1 * time.Second)

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Demo operations complete!")
	fmt.Println()
	fmt.Println("Orchestrator is now listening for task assignments.")
	fmt.Println("When agents are targeted but offline, the gateway will")
	fmt.Println("send TaskAssignment messages to this orchestrator based")
	fmt.Println("on the supported profiles.")
	fmt.Println()
	fmt.Println("Waiting for task assignments (Ctrl+C to exit)...")
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
		fmt.Printf("[Orchestrator] Authentication failed: %v\n", e)
		return err

	case *aether.DuplicateIdentityError:
		fmt.Printf("[Orchestrator] Identity already in use: %v\n", e.Identity)
		return err

	case *aether.ReconnectionError:
		fmt.Printf("[Orchestrator] Could not reconnect after %d attempts\n", e.Attempts)
		return err

	case *aether.ConnectionError:
		fmt.Printf("[Orchestrator] Connection failed: %v\n", e)
		return err

	case *aether.TimeoutError:
		fmt.Printf("[Orchestrator] Connection timed out: %v\n", e)
		return err

	default:
		// Check if it's an AetherError
		if aetherErr, ok := err.(*aether.AetherError); ok {
			fmt.Printf("[Orchestrator] Aether error: %v\n", aetherErr)
			return err
		}

		// Unknown error
		return fmt.Errorf("unexpected error: %w", err)
	}
}
