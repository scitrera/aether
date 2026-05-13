// Package echo provides an example Echo Orchestrator implementation.
//
// The Echo Orchestrator demonstrates how to build an orchestrator using the
// Aether Go SDK. It receives task assignments from the gateway and "handles"
// them by simply logging the assignment details - making it useful for testing
// and as a template for building real orchestrators.
//
// # Usage
//
// The EchoOrchestrator can be used directly or extended for custom behavior:
//
//	orch, err := echo.NewEchoOrchestrator(echo.Options{
//	    ServerAddr: "localhost:50051",
//	    Specifier:  "echo-1",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Optional: Add custom assignment handler
//	orch.OnAssignment(func(ctx context.Context, a *aether.TaskAssignment) error {
//	    fmt.Printf("Custom handler: task %s\n", a.TaskID)
//	    return nil
//	})
//
//	ctx := context.Background()
//	if err := orch.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// # Architecture
//
// The Echo Orchestrator follows the same pattern as the Python SDK's
// BaseOrchestrator class:
//
//   - Wraps the OrchestratorClient for gateway communication
//   - Provides lifecycle management (Run, Close, Shutdown)
//   - Tracks "launched" processes (simulated for echo)
//   - Supports callback hooks for customization
//
// For production orchestrators, extend this pattern to launch actual
// containers, VMs, or processes based on the task assignment parameters.
package echo

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Types
// =============================================================================

// LaunchedProcess tracks information about a "launched" process.
//
// In a real orchestrator, this would contain actual process handles
// (e.g., container ID, PID, VM instance). For the echo orchestrator,
// it just tracks assignment metadata.
type LaunchedProcess struct {
	// TaskID is the unique identifier for the task.
	TaskID string

	// Workspace is the workspace the task operates in.
	Workspace string

	// Implementation is the agent/task implementation type.
	Implementation string

	// Specifier is the agent/task instance identifier.
	Specifier string

	// Profile is the orchestration profile that matched.
	Profile string

	// StartedAt is when the process was "launched".
	StartedAt time.Time

	// Metadata contains additional assignment metadata.
	Metadata map[string]string

	// LaunchParams contains the launch parameters from the assignment.
	LaunchParams map[string]string
}

// AssignmentHandler is called when a task assignment is received.
//
// Return an error to indicate the assignment could not be handled.
// The error will be logged but does not affect the orchestrator.
type AssignmentHandler func(ctx context.Context, assignment *aether.TaskAssignment) error

// ConnectHandler is called when the orchestrator connects to the gateway.
type ConnectHandler func(ctx context.Context, ack *aether.ConnectionAck) error

// DisconnectHandler is called when the orchestrator disconnects.
type DisconnectHandler func(ctx context.Context, reason string) error

// ErrorHandler is called when an error is received from the gateway.
type ErrorHandler func(ctx context.Context, err *aether.ErrorInfo) error

// Options configures the Echo Orchestrator.
type Options struct {
	// ServerAddr is the gateway address (host:port).
	// Default: "localhost:50051"
	ServerAddr string

	// Specifier is an optional unique identifier for this orchestrator instance.
	// If empty, the server generates one.
	Specifier string

	// SupportedProfiles is the list of profiles this orchestrator handles.
	// Default: ["echo"]
	SupportedProfiles []string

	// Implementation is the orchestrator implementation name.
	// Default: "echo-orchestrator"
	Implementation string

	// TLS configures TLS/mTLS for the connection.
	TLS *aether.TLSConfig

	// Credentials for authentication.
	Credentials map[string]string

	// Connection configures connection behavior (retry, backoff, etc.)
	Connection aether.ConnectionOptions

	// Logger is the logger to use. If nil, uses the standard log package.
	Logger Logger
}

// Logger defines the logging interface used by the orchestrator.
type Logger interface {
	Printf(format string, v ...any)
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		ServerAddr:        "localhost:50051",
		Implementation:    "echo-orchestrator",
		SupportedProfiles: []string{"echo"},
	}
}

// =============================================================================
// Echo Orchestrator
// =============================================================================

// EchoOrchestrator is an example orchestrator that echoes task assignments.
//
// It demonstrates the orchestrator pattern:
//   - Connects to the gateway as an orchestrator principal
//   - Receives task assignments for supported profiles
//   - Tracks "launched" processes
//   - Provides lifecycle management
//
// For production use, extend this pattern to actually spawn agents/tasks
// using containers, processes, or other compute resources.
type EchoOrchestrator struct {
	// Client is the underlying orchestrator client.
	Client *aether.OrchestratorClient

	// Configuration
	opts   Options
	logger Logger

	// Process tracking
	processes   map[string]*LaunchedProcess
	processesMu sync.RWMutex

	// Handlers
	onAssignment AssignmentHandler
	onConnect    ConnectHandler
	onDisconnect DisconnectHandler
	onError      ErrorHandler

	// Lifecycle
	running    bool
	runningMu  sync.RWMutex
	shutdownCh chan struct{}
}

// NewEchoOrchestrator creates a new Echo Orchestrator with the given options.
//
// The orchestrator is created but not connected. Call Run() to connect
// and start processing task assignments.
func NewEchoOrchestrator(opts Options) (*EchoOrchestrator, error) {
	// Apply defaults
	if opts.ServerAddr == "" {
		opts.ServerAddr = "localhost:50051"
	}
	if opts.Implementation == "" {
		opts.Implementation = "echo-orchestrator"
	}
	if len(opts.SupportedProfiles) == 0 {
		opts.SupportedProfiles = []string{"echo"}
	}

	// Create the underlying client
	clientOpts := aether.OrchestratorOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr:  opts.ServerAddr,
			TLS:         opts.TLS,
			Credentials: opts.Credentials,
			Connection:  opts.Connection,
		},
		Implementation:    opts.Implementation,
		SupportedProfiles: opts.SupportedProfiles,
		Specifier:         opts.Specifier,
	}

	client, err := aether.NewOrchestratorClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator client: %w", err)
	}

	// Create logger
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}

	orch := &EchoOrchestrator{
		Client:     client,
		opts:       opts,
		logger:     logger,
		processes:  make(map[string]*LaunchedProcess),
		shutdownCh: make(chan struct{}),
	}

	// Setup default handlers
	orch.setupHandlers()

	return orch, nil
}

// setupHandlers configures the client callback handlers.
func (o *EchoOrchestrator) setupHandlers() {
	// Task assignment handler
	o.Client.OnTaskAssignment(func(ctx context.Context, assignment *aether.TaskAssignment) error {
		return o.handleAssignment(ctx, assignment)
	})

	// Connect handler
	o.Client.OnConnect(func(ctx context.Context, ack *aether.ConnectionAck) error {
		o.log("Connected to gateway (session: %s, resumed: %v)", ack.SessionID, ack.Resumed)
		if o.onConnect != nil {
			return o.onConnect(ctx, ack)
		}
		return nil
	})

	// Disconnect handler
	o.Client.OnDisconnect(func(ctx context.Context, reason string) error {
		o.log("Disconnected from gateway: %s", reason)
		if o.onDisconnect != nil {
			return o.onDisconnect(ctx, reason)
		}
		return nil
	})

	// Error handler
	o.Client.OnError(func(ctx context.Context, err *aether.ErrorInfo) error {
		o.log("Error from gateway: [%s] %s", err.Code, err.Message)
		if o.onError != nil {
			return o.onError(ctx, err)
		}
		return nil
	})

	// Message handler (log incoming messages)
	o.Client.OnMessage(func(ctx context.Context, msg *aether.Message) error {
		o.log("Message from %s: %d bytes", msg.SourceTopic, len(msg.Payload))
		return nil
	})

	// Config handler
	o.Client.OnConfig(func(ctx context.Context, config *aether.ConfigSnapshot) error {
		o.log("Received config snapshot: %d workspace keys, %d global keys",
			len(config.KV), len(config.GlobalKV))
		return nil
	})
}

// handleAssignment processes a task assignment.
func (o *EchoOrchestrator) handleAssignment(ctx context.Context, assignment *aether.TaskAssignment) error {
	o.log("Task assignment received:")
	o.log("  Task ID: %s", assignment.TaskID)
	o.log("  Task Type: %s", assignment.TaskType)
	o.log("  Profile: %s", assignment.Profile)
	o.log("  Target Implementation: %s", assignment.TargetImplementation)
	o.log("  Workspace: %s", assignment.Workspace)
	o.log("  Specifier: %s", assignment.Specifier)

	// Create a "launched process" record
	process := &LaunchedProcess{
		TaskID:         assignment.TaskID,
		Workspace:      assignment.Workspace,
		Implementation: assignment.TargetImplementation,
		Specifier:      assignment.Specifier,
		Profile:        assignment.Profile,
		StartedAt:      time.Now(),
		Metadata:       assignment.Metadata,
		LaunchParams:   assignment.LaunchParams,
	}

	// Track the process
	o.TrackProcess(process)
	o.log("Tracked process for task %s (simulated launch)", assignment.TaskID)

	// Call custom handler if registered
	if o.onAssignment != nil {
		if err := o.onAssignment(ctx, assignment); err != nil {
			o.log("Custom assignment handler error: %v", err)
			return err
		}
	}

	return nil
}

// =============================================================================
// Handler Registration
// =============================================================================

// OnAssignment registers a custom handler for task assignments.
//
// This handler is called after the default logging and tracking.
// Use this to implement custom assignment processing logic.
func (o *EchoOrchestrator) OnAssignment(handler AssignmentHandler) {
	o.onAssignment = handler
}

// OnConnect registers a handler for successful connections.
func (o *EchoOrchestrator) OnConnect(handler ConnectHandler) {
	o.onConnect = handler
}

// OnDisconnect registers a handler for disconnections.
func (o *EchoOrchestrator) OnDisconnect(handler DisconnectHandler) {
	o.onDisconnect = handler
}

// OnError registers a handler for error responses.
func (o *EchoOrchestrator) OnError(handler ErrorHandler) {
	o.onError = handler
}

// =============================================================================
// Process Tracking
// =============================================================================

// TrackProcess adds a process to the tracking map.
func (o *EchoOrchestrator) TrackProcess(process *LaunchedProcess) {
	o.processesMu.Lock()
	defer o.processesMu.Unlock()
	o.processes[process.TaskID] = process
}

// UntrackProcess removes a process from tracking and returns it.
func (o *EchoOrchestrator) UntrackProcess(taskID string) *LaunchedProcess {
	o.processesMu.Lock()
	defer o.processesMu.Unlock()
	process := o.processes[taskID]
	delete(o.processes, taskID)
	return process
}

// GetProcess returns a tracked process by task ID.
func (o *EchoOrchestrator) GetProcess(taskID string) *LaunchedProcess {
	o.processesMu.RLock()
	defer o.processesMu.RUnlock()
	return o.processes[taskID]
}

// GetAllProcesses returns a copy of all tracked processes.
func (o *EchoOrchestrator) GetAllProcesses() map[string]*LaunchedProcess {
	o.processesMu.RLock()
	defer o.processesMu.RUnlock()

	result := make(map[string]*LaunchedProcess, len(o.processes))
	for k, v := range o.processes {
		result[k] = v
	}
	return result
}

// ProcessCount returns the number of tracked processes.
func (o *EchoOrchestrator) ProcessCount() int {
	o.processesMu.RLock()
	defer o.processesMu.RUnlock()
	return len(o.processes)
}

// =============================================================================
// Lifecycle Management
// =============================================================================

// Run starts the orchestrator and blocks until shutdown.
//
// This method:
//  1. Connects to the gateway
//  2. Runs the message loop (processing task assignments)
//  3. Handles graceful shutdown on SIGINT/SIGTERM
//
// Call Close() or Shutdown() to stop the orchestrator.
func (o *EchoOrchestrator) Run(ctx context.Context) error {
	o.runningMu.Lock()
	if o.running {
		o.runningMu.Unlock()
		return fmt.Errorf("orchestrator is already running")
	}
	o.running = true
	o.shutdownCh = make(chan struct{})
	o.runningMu.Unlock()

	o.log("Starting %s orchestrator", o.opts.Implementation)
	o.log("Supported profiles: %v", o.opts.SupportedProfiles)
	o.log("Connecting to gateway at %s...", o.opts.ServerAddr)

	// Connect to the gateway
	if err := o.Client.Connect(ctx); err != nil {
		o.setRunning(false)
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Create a context that cancels on shutdown
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Error channel for the message loop
	errCh := make(chan error, 1)

	// Start the message loop in a goroutine
	go func() {
		errCh <- o.Client.Run(runCtx)
	}()

	// Wait for shutdown signal, context cancellation, or error
	select {
	case <-o.shutdownCh:
		o.log("Shutdown requested")
	case sig := <-sigCh:
		o.log("Received signal: %v", sig)
	case err := <-errCh:
		if err != nil && ctx.Err() == nil {
			o.log("Message loop error: %v", err)
			o.setRunning(false)
			return err
		}
	case <-ctx.Done():
		o.log("Context canceled")
	}

	// Perform shutdown
	o.shutdown()

	// Cancel the run context to stop the message loop
	cancel()

	// Wait for the message loop to finish (with timeout)
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		o.log("Timeout waiting for message loop to stop")
	}

	return nil
}

// RunWithSignalHandling is a convenience method that sets up signal handling
// and runs the orchestrator until interrupted.
//
// Deprecated: Use Run() which now includes signal handling.
func (o *EchoOrchestrator) RunWithSignalHandling(ctx context.Context) error {
	return o.Run(ctx)
}

// Close closes the gateway connection.
func (o *EchoOrchestrator) Close() error {
	o.setRunning(false)
	return o.Client.Close()
}

// Shutdown requests a graceful shutdown of the orchestrator.
//
// This signals the Run() method to stop and perform cleanup.
func (o *EchoOrchestrator) Shutdown() {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()

	if o.running && o.shutdownCh != nil {
		close(o.shutdownCh)
	}
}

// IsRunning returns true if the orchestrator is currently running.
func (o *EchoOrchestrator) IsRunning() bool {
	o.runningMu.RLock()
	defer o.runningMu.RUnlock()
	return o.running
}

// shutdown performs graceful shutdown cleanup.
func (o *EchoOrchestrator) shutdown() {
	o.log("Shutting down...")

	// Log tracked processes
	processes := o.GetAllProcesses()
	if len(processes) > 0 {
		o.log("Tracked processes at shutdown:")
		for taskID, process := range processes {
			o.log("  - Task %s (%s/%s) started at %s",
				taskID,
				process.Implementation,
				process.Specifier,
				process.StartedAt.Format(time.RFC3339),
			)
		}
	}

	// Close the connection
	if err := o.Client.Close(); err != nil {
		o.log("Error closing connection: %v", err)
	}

	o.setRunning(false)
	o.log("Orchestrator stopped")
}

// setRunning sets the running state.
func (o *EchoOrchestrator) setRunning(running bool) {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	o.running = running
}

// =============================================================================
// Accessors
// =============================================================================

// Implementation returns the orchestrator implementation name.
func (o *EchoOrchestrator) Implementation() string {
	return o.opts.Implementation
}

// SupportedProfiles returns the list of supported profiles.
func (o *EchoOrchestrator) SupportedProfiles() []string {
	return o.Client.SupportedProfiles()
}

// Specifier returns the orchestrator's specifier.
func (o *EchoOrchestrator) Specifier() string {
	return o.Client.Specifier()
}

// =============================================================================
// Logging
// =============================================================================

// log writes a timestamped log message.
func (o *EchoOrchestrator) log(format string, args ...any) {
	timestamp := time.Now().Format("2006-01-02T15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	o.logger.Printf("[%s] [EchoOrchestrator] %s", timestamp, msg)
}

// =============================================================================
// Message Sending
// =============================================================================

// SendStatusToAgent sends a status message to a specific agent.
//
// This can be used to notify agents about orchestration status,
// such as startup confirmations or health check responses.
func (o *EchoOrchestrator) SendStatusToAgent(workspace, implementation, specifier string, payload []byte) error {
	return o.Client.SendStatusToAgent(workspace, implementation, specifier, payload)
}

// SendStatusToTask sends a status message to a specific task.
//
// For unique tasks, set the specifier. For non-unique tasks,
// leave specifier empty to broadcast to the task pool.
func (o *EchoOrchestrator) SendStatusToTask(workspace, implementation, specifier string, payload []byte) error {
	return o.Client.SendStatusToTask(workspace, implementation, specifier, payload)
}
