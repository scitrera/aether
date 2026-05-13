// Docker orchestrator implementation for Aether.
//
// This file provides the DockerOrchestrator type which manages agent/task
// containers in response to task assignments from the gateway.

package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Docker Orchestrator
// =============================================================================

// DockerOrchestrator manages Docker containers for Aether agents and tasks.
//
// It provides a complete orchestrator implementation that:
//   - Connects to the Aether gateway as an orchestrator principal
//   - Receives task assignments for supported profiles
//   - Launches Docker containers with appropriate configuration
//   - Monitors container lifecycle and handles exits
//   - Streams container logs for debugging
//   - Provides graceful shutdown with configurable timeouts
//
// For basic usage:
//
//	orch, err := docker.NewDockerOrchestrator(docker.Options{
//	    ServerAddr:        "localhost:50051",
//	    SupportedProfiles: []string{"docker-agent"},
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err := orch.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// For customizing container configuration:
//
//	orch.OnContainerConfig(func(assignment interface{}, cfg *docker.ContainerConfig) error {
//	    // Customize container config
//	    cfg.Memory = 512 * 1024 * 1024
//	    return nil
//	})
type DockerOrchestrator struct {
	// Client is the underlying orchestrator client for gateway communication.
	Client *aether.OrchestratorClient

	// Docker client for container operations
	docker *client.Client

	// Configuration
	opts   Options
	logger Logger

	// Container tracking
	containers   map[string]*ContainerInfo // keyed by TaskID
	containersMu sync.RWMutex

	// Handlers
	onAssignment      AssignmentHandler
	onContainerConfig ContainerConfigBuilder
	onContainerEvent  ContainerEventHandler
	onConnect         ConnectHandler
	onDisconnect      DisconnectHandler
	onError           ErrorHandler

	// Lifecycle management
	running    bool
	runningMu  sync.RWMutex
	shutdownCh chan struct{}
	stopOnce   sync.Once

	// Context for background operations
	bgCtx    context.Context
	bgCancel context.CancelFunc
}

// AssignmentHandler is called when a task assignment is received.
//
// This is called after the default container creation. Return an error
// to indicate the assignment could not be handled.
type AssignmentHandler func(ctx context.Context, assignment *aether.TaskAssignment, container *ContainerInfo) error

// ConnectHandler is called when the orchestrator connects to the gateway.
type ConnectHandler func(ctx context.Context, ack *aether.ConnectionAck) error

// DisconnectHandler is called when the orchestrator disconnects.
type DisconnectHandler func(ctx context.Context, reason string) error

// ErrorHandler is called when an error is received from the gateway.
type ErrorHandler func(ctx context.Context, err *aether.ErrorInfo) error

// =============================================================================
// Constructor
// =============================================================================

// NewDockerOrchestrator creates a new Docker Orchestrator with the given options.
//
// The orchestrator is created but not connected. Call Run() to connect
// and start processing task assignments.
//
// This method validates options and creates the orchestrator client, but does
// not initialize the Docker client until Run() is called.
func NewDockerOrchestrator(opts Options) (*DockerOrchestrator, error) {
	// Apply defaults
	if opts.ServerAddr == "" {
		opts.ServerAddr = "localhost:50051"
	}
	if opts.Implementation == "" {
		opts.Implementation = "docker-orchestrator"
	}
	if len(opts.SupportedProfiles) == 0 {
		return nil, fmt.Errorf("at least one supported profile is required")
	}
	if opts.DefaultNetwork == "" {
		opts.DefaultNetwork = "bridge"
	}
	if opts.ContainerPrefix == "" {
		opts.ContainerPrefix = "aether-"
	}
	if opts.StopTimeout <= 0 {
		opts.StopTimeout = 30
	}
	if opts.KillTimeout <= 0 {
		opts.KillTimeout = 10
	}
	if opts.LogPrefix == "" {
		opts.LogPrefix = "Container"
	}

	// Create the underlying orchestrator client
	clientOpts := aether.OrchestratorOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr:  opts.ServerAddr,
			Credentials: opts.Credentials,
		},
		Implementation:    opts.Implementation,
		SupportedProfiles: opts.SupportedProfiles,
		Specifier:         opts.Specifier,
	}

	// Handle TLS config if provided
	if opts.TLS != nil {
		if tlsCfg, ok := opts.TLS.(*aether.TLSConfig); ok {
			clientOpts.TLS = tlsCfg
		}
	}

	// Handle connection options if provided
	if opts.Connection != nil {
		if connOpts, ok := opts.Connection.(aether.ConnectionOptions); ok {
			clientOpts.Connection = connOpts
		}
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

	// Create background context
	bgCtx, bgCancel := context.WithCancel(context.Background())

	orch := &DockerOrchestrator{
		Client:     client,
		opts:       opts,
		logger:     logger,
		containers: make(map[string]*ContainerInfo),
		shutdownCh: make(chan struct{}),
		bgCtx:      bgCtx,
		bgCancel:   bgCancel,
	}

	// Setup client handlers
	orch.setupHandlers()

	return orch, nil
}

// setupHandlers configures the client callback handlers.
func (o *DockerOrchestrator) setupHandlers() {
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

// =============================================================================
// Assignment Handling
// =============================================================================

// handleAssignment processes a task assignment by creating a container.
func (o *DockerOrchestrator) handleAssignment(ctx context.Context, assignment *aether.TaskAssignment) error {
	o.log("Task assignment received:")
	o.log("  Task ID: %s", assignment.TaskID)
	o.log("  Task Type: %s", assignment.TaskType)
	o.log("  Profile: %s", assignment.Profile)
	o.log("  Target Implementation: %s", assignment.TargetImplementation)
	o.log("  Workspace: %s", assignment.Workspace)
	o.log("  Specifier: %s", assignment.Specifier)

	// Build container configuration from assignment
	config, err := o.buildContainerConfig(assignment)
	if err != nil {
		o.log("Failed to build container config: %v", err)
		return fmt.Errorf("failed to build container config: %w", err)
	}

	// Allow customization via callback
	if o.onContainerConfig != nil {
		if err := o.onContainerConfig(assignment, config); err != nil {
			o.log("Container config callback rejected assignment: %v", err)
			return fmt.Errorf("container config callback error: %w", err)
		}
	}

	// Create and track container info
	info := &ContainerInfo{
		TaskID:         assignment.TaskID,
		Workspace:      assignment.Workspace,
		Implementation: assignment.TargetImplementation,
		Specifier:      assignment.Specifier,
		Profile:        assignment.Profile,
		Image:          config.Image,
		StartedAt:      time.Now(),
		ExitCode:       -1,
		State:          ContainerStateCreated,
		Metadata:       assignment.Metadata,
		LaunchParams:   assignment.LaunchParams,
		Environment:    config.Environment,
	}

	// Track the container
	o.TrackContainer(info)
	o.log("Tracked container for task %s (pending creation)", assignment.TaskID)

	// Emit created event
	o.emitEvent(info, ContainerEventCreated)

	// Call custom handler if registered
	if o.onAssignment != nil {
		if err := o.onAssignment(ctx, assignment, info); err != nil {
			o.log("Assignment handler error: %v", err)
			// Don't return error - container is tracked, handler can deal with it
		}
	}

	return nil
}

// buildContainerConfig creates a ContainerConfig from a task assignment.
func (o *DockerOrchestrator) buildContainerConfig(assignment *aether.TaskAssignment) (*ContainerConfig, error) {
	config := &ContainerConfig{
		Environment: make(map[string]string),
		Labels:      make(map[string]string),
	}

	// Get image from launch params or default
	if image, ok := assignment.LaunchParams[LaunchParamImage]; ok && image != "" {
		config.Image = image
	} else if o.opts.DefaultImage != "" {
		config.Image = o.opts.DefaultImage
	} else {
		return nil, fmt.Errorf("no image specified in launch params and no default image configured")
	}

	// Set Aether environment variables
	config.Environment[EnvGateway] = o.opts.ServerAddr
	config.Environment[EnvWorkspace] = assignment.Workspace
	config.Environment[EnvImplementation] = assignment.TargetImplementation
	config.Environment[EnvSpecifier] = assignment.Specifier
	config.Environment[EnvTaskID] = assignment.TaskID
	config.Environment[EnvProfile] = assignment.Profile

	// Generate auth token if not provided
	authToken := generateAuthToken()
	config.Environment[EnvAuthToken] = authToken

	// Process additional launch params
	for key, value := range assignment.LaunchParams {
		switch key {
		case LaunchParamCommand:
			// Command should be JSON array, parse it
			config.Command = parseJSONStringArray(value)
		case LaunchParamEntrypoint:
			// Entrypoint should be JSON array, parse it
			config.Entrypoint = parseJSONStringArray(value)
		case LaunchParamWorkingDir:
			config.WorkingDir = value
		case LaunchParamNetwork:
			config.NetworkMode = value
		case LaunchParamUser:
			config.User = value
		case LaunchParamPrivileged:
			config.Privileged = strings.ToLower(value) == "true"
		case LaunchParamMemory:
			config.Memory = parseMemoryString(value)
		case LaunchParamCPUs:
			config.NanoCPUs = parseCPUString(value)
		default:
			// Check for environment variable prefix
			if strings.HasPrefix(key, LaunchParamEnvPrefix) {
				envKey := strings.TrimPrefix(key, LaunchParamEnvPrefix)
				config.Environment[envKey] = value
			}
		}
	}

	// Set network mode default
	if config.NetworkMode == "" {
		config.NetworkMode = o.opts.DefaultNetwork
	}

	// Set labels
	config.Labels["aether.task_id"] = assignment.TaskID
	config.Labels["aether.workspace"] = assignment.Workspace
	config.Labels["aether.implementation"] = assignment.TargetImplementation
	config.Labels["aether.specifier"] = assignment.Specifier
	config.Labels["aether.profile"] = assignment.Profile
	config.Labels["aether.orchestrator"] = o.opts.Implementation

	// Set stop timeout
	if o.opts.StopTimeout > 0 {
		config.StopTimeout = o.opts.StopTimeout
	}

	// Set auto-remove based on options
	config.AutoRemove = o.opts.AutoRemoveContainers

	return config, nil
}

// =============================================================================
// Handler Registration
// =============================================================================

// OnAssignment registers a handler for task assignments.
//
// This handler is called after the container info is created but before
// the actual Docker container is started. Use this to perform additional
// operations or customization.
func (o *DockerOrchestrator) OnAssignment(handler AssignmentHandler) {
	o.onAssignment = handler
}

// OnContainerConfig registers a handler for customizing container configuration.
//
// This handler is called before the container is created, allowing you to
// modify the ContainerConfig based on the assignment. Return an error to
// reject the assignment.
func (o *DockerOrchestrator) OnContainerConfig(handler ContainerConfigBuilder) {
	o.onContainerConfig = handler
}

// OnContainerEvent registers a handler for container lifecycle events.
//
// This handler is called when containers are created, started, stopped,
// or when health events occur.
func (o *DockerOrchestrator) OnContainerEvent(handler ContainerEventHandler) {
	o.onContainerEvent = handler
}

// OnConnect registers a handler for successful connections.
func (o *DockerOrchestrator) OnConnect(handler ConnectHandler) {
	o.onConnect = handler
}

// OnDisconnect registers a handler for disconnections.
func (o *DockerOrchestrator) OnDisconnect(handler DisconnectHandler) {
	o.onDisconnect = handler
}

// OnError registers a handler for error responses.
func (o *DockerOrchestrator) OnError(handler ErrorHandler) {
	o.onError = handler
}

// =============================================================================
// Container Tracking
// =============================================================================

// TrackContainer adds a container to the tracking map.
func (o *DockerOrchestrator) TrackContainer(info *ContainerInfo) {
	o.containersMu.Lock()
	defer o.containersMu.Unlock()
	o.containers[info.TaskID] = info
}

// UntrackContainer removes a container from tracking and returns it.
func (o *DockerOrchestrator) UntrackContainer(taskID string) *ContainerInfo {
	o.containersMu.Lock()
	defer o.containersMu.Unlock()
	info := o.containers[taskID]
	delete(o.containers, taskID)
	return info
}

// GetContainer returns a tracked container by task ID.
func (o *DockerOrchestrator) GetContainer(taskID string) *ContainerInfo {
	o.containersMu.RLock()
	defer o.containersMu.RUnlock()
	return o.containers[taskID]
}

// GetContainerByID returns a tracked container by Docker container ID.
func (o *DockerOrchestrator) GetContainerByID(containerID string) *ContainerInfo {
	o.containersMu.RLock()
	defer o.containersMu.RUnlock()
	for _, info := range o.containers {
		if info.ContainerID == containerID {
			return info
		}
	}
	return nil
}

// GetAllContainers returns a copy of all tracked containers.
func (o *DockerOrchestrator) GetAllContainers() map[string]*ContainerInfo {
	o.containersMu.RLock()
	defer o.containersMu.RUnlock()

	result := make(map[string]*ContainerInfo, len(o.containers))
	for k, v := range o.containers {
		result[k] = v
	}
	return result
}

// ContainerCount returns the number of tracked containers.
func (o *DockerOrchestrator) ContainerCount() int {
	o.containersMu.RLock()
	defer o.containersMu.RUnlock()
	return len(o.containers)
}

// RunningContainerCount returns the number of running containers.
func (o *DockerOrchestrator) RunningContainerCount() int {
	o.containersMu.RLock()
	defer o.containersMu.RUnlock()

	count := 0
	for _, info := range o.containers {
		if info.IsRunning() {
			count++
		}
	}
	return count
}

// =============================================================================
// Lifecycle Management
// =============================================================================

// Run starts the orchestrator and blocks until shutdown.
//
// This method:
//  1. Initializes the Docker client
//  2. Connects to the gateway
//  3. Runs the message loop (processing task assignments)
//  4. Handles graceful shutdown on SIGINT/SIGTERM
//
// Call Close() or Shutdown() to stop the orchestrator.
func (o *DockerOrchestrator) Run(ctx context.Context) error {
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

	// Initialize Docker client
	o.log("Connecting to Docker daemon...")
	if err := o.initDockerClient(ctx); err != nil {
		o.setRunning(false)
		return fmt.Errorf("failed to initialize Docker client: %w", err)
	}

	o.log("Connecting to gateway at %s...", o.opts.ServerAddr)

	// Connect to the gateway
	if err := o.Client.Connect(ctx); err != nil {
		o.docker.Close()
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

// Close closes the gateway connection without stopping containers.
func (o *DockerOrchestrator) Close() error {
	o.setRunning(false)
	o.bgCancel()

	// Close Docker client
	if o.docker != nil {
		o.docker.Close()
	}

	return o.Client.Close()
}

// Shutdown requests a graceful shutdown of the orchestrator.
//
// This signals the Run() method to stop and perform cleanup,
// including stopping all tracked containers.
func (o *DockerOrchestrator) Shutdown() {
	o.stopOnce.Do(func() {
		o.runningMu.Lock()
		defer o.runningMu.Unlock()

		if o.running && o.shutdownCh != nil {
			close(o.shutdownCh)
		}
	})
}

// IsRunning returns true if the orchestrator is currently running.
func (o *DockerOrchestrator) IsRunning() bool {
	o.runningMu.RLock()
	defer o.runningMu.RUnlock()
	return o.running
}

// shutdown performs graceful shutdown cleanup.
func (o *DockerOrchestrator) shutdown() {
	o.log("Shutting down...")

	// Cancel background operations
	o.bgCancel()

	// Get all tracked containers
	containers := o.GetAllContainers()
	if len(containers) > 0 {
		o.log("Stopping %d tracked containers...", len(containers))

		// Create a context with timeout for shutdown operations
		shutdownCtx, cancel := context.WithTimeout(context.Background(),
			time.Duration(o.opts.StopTimeout+o.opts.KillTimeout)*time.Second)
		defer cancel()

		// Stop containers concurrently with a wait group
		var wg sync.WaitGroup
		for taskID, info := range containers {
			if info.State != ContainerStateRunning && info.State != ContainerStateCreated {
				continue // Skip non-running containers
			}

			wg.Add(1)
			go func(taskID string, info *ContainerInfo) {
				defer wg.Done()

				o.log("  - Stopping container %s (task %s, state: %s)",
					truncateID(info.ContainerID),
					taskID,
					info.State,
				)

				// Close log reader if open
				if info.LogReader != nil {
					info.LogReader.Close()
				}

				// Stop the container
				if err := o.StopContainer(shutdownCtx, info); err != nil {
					o.log("    Warning: failed to stop container %s: %v", truncateID(info.ContainerID), err)
					// Try to force remove
					if err := o.RemoveContainer(shutdownCtx, info, true); err != nil {
						o.log("    Warning: failed to remove container %s: %v", truncateID(info.ContainerID), err)
					}
				} else if o.opts.AutoRemoveContainers {
					// Remove if auto-remove is enabled
					if err := o.RemoveContainer(shutdownCtx, info, false); err != nil {
						o.log("    Warning: failed to remove container %s: %v", truncateID(info.ContainerID), err)
					}
				}
			}(taskID, info)
		}

		// Wait for all containers to stop (with timeout)
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			o.log("All containers stopped successfully")
		case <-shutdownCtx.Done():
			o.log("Timeout waiting for containers to stop")
		}
	}

	// Close the Docker client
	if o.docker != nil {
		if err := o.docker.Close(); err != nil {
			o.log("Error closing Docker client: %v", err)
		}
	}

	// Close the gateway connection
	if err := o.Client.Close(); err != nil {
		o.log("Error closing connection: %v", err)
	}

	o.setRunning(false)
	o.log("Orchestrator stopped")
}

// setRunning sets the running state.
func (o *DockerOrchestrator) setRunning(running bool) {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	o.running = running
}

// emitEvent calls the container event handler if registered.
func (o *DockerOrchestrator) emitEvent(info *ContainerInfo, event ContainerEvent) {
	if o.onContainerEvent != nil {
		o.onContainerEvent(info, event)
	}
}

// =============================================================================
// Accessors
// =============================================================================

// Implementation returns the orchestrator implementation name.
func (o *DockerOrchestrator) Implementation() string {
	return o.opts.Implementation
}

// SupportedProfiles returns the list of supported profiles.
func (o *DockerOrchestrator) SupportedProfiles() []string {
	return o.Client.SupportedProfiles()
}

// Specifier returns the orchestrator's specifier.
func (o *DockerOrchestrator) Specifier() string {
	return o.Client.Specifier()
}

// Options returns a copy of the orchestrator options.
func (o *DockerOrchestrator) Options() Options {
	return o.opts
}

// =============================================================================
// Message Sending
// =============================================================================

// SendStatusToAgent sends a status message to a specific agent.
func (o *DockerOrchestrator) SendStatusToAgent(workspace, implementation, specifier string, payload []byte) error {
	return o.Client.SendStatusToAgent(workspace, implementation, specifier, payload)
}

// SendStatusToTask sends a status message to a specific task.
func (o *DockerOrchestrator) SendStatusToTask(workspace, implementation, specifier string, payload []byte) error {
	return o.Client.SendStatusToTask(workspace, implementation, specifier, payload)
}

// =============================================================================
// Docker Container Lifecycle
// =============================================================================

// initDockerClient initializes the Docker client connection.
//
// This is called internally when the orchestrator starts running.
// The client is configured based on the DockerClientOptions in Options.
func (o *DockerOrchestrator) initDockerClient(ctx context.Context) error {
	opts := []client.Opt{
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	}

	// Apply custom host if specified
	if o.opts.Docker.Host != "" {
		opts = append(opts, client.WithHost(o.opts.Docker.Host))
	}

	// Apply API version if specified
	if o.opts.Docker.APIVersion != "" {
		opts = append(opts, client.WithVersion(o.opts.Docker.APIVersion))
	}

	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %w", err)
	}

	// Verify connection with a ping
	if _, err := cli.Ping(ctx); err != nil {
		cli.Close()
		return fmt.Errorf("failed to connect to Docker daemon: %w", err)
	}

	o.docker = cli
	o.log("Docker client connected (API version: %s)", cli.ClientVersion())
	return nil
}

// CreateContainer creates a Docker container for the given ContainerInfo.
//
// The container is created but not started. Call StartContainer to start it.
// The ContainerInfo is updated with the ContainerID on success.
func (o *DockerOrchestrator) CreateContainer(ctx context.Context, info *ContainerInfo, config *ContainerConfig) error {
	if o.docker == nil {
		return fmt.Errorf("Docker client not initialized")
	}

	// Pull image if not available locally (best effort)
	if err := o.ensureImage(ctx, config.Image); err != nil {
		o.log("Warning: could not ensure image %s: %v", config.Image, err)
		// Continue anyway - image might exist locally
	}

	// Build container name
	name := o.GenerateContainerName(info.TaskID, info.Workspace, info.Implementation, info.Specifier)

	// Build environment variables
	env := make([]string, 0, len(config.Environment))
	for k, v := range config.Environment {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	// Build container config
	containerConfig := &container.Config{
		Image:        config.Image,
		Env:          env,
		Labels:       config.Labels,
		WorkingDir:   config.WorkingDir,
		Entrypoint:   config.Entrypoint,
		Cmd:          config.Command,
		User:         config.User,
		Hostname:     config.Hostname,
		AttachStdout: true,
		AttachStderr: true,
	}

	// Set stop timeout on container config (in seconds)
	if config.StopTimeout > 0 {
		timeout := config.StopTimeout
		containerConfig.StopTimeout = &timeout
	}

	// Build health check if configured
	if config.HealthCheck != nil {
		containerConfig.Healthcheck = &container.HealthConfig{
			Test:        config.HealthCheck.Test,
			Interval:    config.HealthCheck.Interval,
			Timeout:     config.HealthCheck.Timeout,
			StartPeriod: config.HealthCheck.StartPeriod,
			Retries:     config.HealthCheck.Retries,
		}
	}

	// Build host config
	hostConfig := &container.HostConfig{
		NetworkMode: container.NetworkMode(config.NetworkMode),
		Privileged:  config.Privileged,
		AutoRemove:  config.AutoRemove,
		Resources: container.Resources{
			Memory:     config.Memory,
			MemorySwap: config.MemorySwap,
			CPUShares:  config.CPUShares,
			CPUQuota:   config.CPUQuota,
			CPUPeriod:  config.CPUPeriod,
			NanoCPUs:   config.NanoCPUs,
		},
	}

	// Build mounts
	if len(config.Mounts) > 0 {
		mounts := make([]mount.Mount, 0, len(config.Mounts))
		for _, m := range config.Mounts {
			mounts = append(mounts, mount.Mount{
				Type:        mount.Type(m.Type),
				Source:      m.Source,
				Target:      m.Target,
				ReadOnly:    m.ReadOnly,
				Consistency: mount.Consistency(m.Consistency),
			})
		}
		hostConfig.Mounts = mounts
	}

	// Set restart policy
	if config.RestartPolicy.Name != "" {
		hostConfig.RestartPolicy = container.RestartPolicy{
			Name:              container.RestartPolicyMode(config.RestartPolicy.Name),
			MaximumRetryCount: config.RestartPolicy.MaximumRetryCount,
		}
	}

	// Set log config if specified
	if config.LogConfig != nil {
		hostConfig.LogConfig = container.LogConfig{
			Type:   config.LogConfig.Type,
			Config: config.LogConfig.Config,
		}
	}

	// Build network config
	networkConfig := &network.NetworkingConfig{}

	// Create the container
	resp, err := o.docker.ContainerCreate(ctx, containerConfig, hostConfig, networkConfig, nil, name)
	if err != nil {
		info.State = ContainerStateDead
		info.Error = err
		return fmt.Errorf("failed to create container: %w", err)
	}

	// Update container info
	info.ContainerID = resp.ID
	info.State = ContainerStateCreated
	info.Error = nil

	// Log warnings if any
	for _, warning := range resp.Warnings {
		o.log("Container warning: %s", warning)
	}

	o.log("Created container %s for task %s", truncateID(resp.ID), info.TaskID)
	return nil
}

// StartContainer starts a previously created container.
//
// The container must have been created with CreateContainer first.
// On success, updates the ContainerInfo state to Running.
func (o *DockerOrchestrator) StartContainer(ctx context.Context, info *ContainerInfo) error {
	if o.docker == nil {
		return fmt.Errorf("Docker client not initialized")
	}

	if info.ContainerID == "" {
		return fmt.Errorf("container has not been created yet")
	}

	// Start the container
	if err := o.docker.ContainerStart(ctx, info.ContainerID, container.StartOptions{}); err != nil {
		info.State = ContainerStateDead
		info.Error = err
		o.emitEvent(info, ContainerEventDied)
		return fmt.Errorf("failed to start container: %w", err)
	}

	// Update state
	info.State = ContainerStateRunning
	info.StartedAt = time.Now()
	info.Error = nil

	o.log("Started container %s for task %s", truncateID(info.ContainerID), info.TaskID)
	o.emitEvent(info, ContainerEventStarted)

	// Start log streaming if enabled
	if o.opts.StreamLogs {
		go o.streamContainerLogs(info)
	}

	return nil
}

// StopContainer stops a running container gracefully.
//
// It sends a SIGTERM and waits for the container to stop. If the container
// doesn't stop within the configured timeout, it's forcefully killed.
func (o *DockerOrchestrator) StopContainer(ctx context.Context, info *ContainerInfo) error {
	if o.docker == nil {
		return fmt.Errorf("Docker client not initialized")
	}

	if info.ContainerID == "" {
		return fmt.Errorf("container has not been created yet")
	}

	// Check if already stopped
	if info.State == ContainerStateExited || info.State == ContainerStateDead {
		return nil
	}

	o.log("Stopping container %s for task %s...", truncateID(info.ContainerID), info.TaskID)

	// Stop with timeout
	timeout := o.opts.StopTimeout
	if err := o.docker.ContainerStop(ctx, info.ContainerID, container.StopOptions{Timeout: &timeout}); err != nil {
		// Check if container is already stopped
		if client.IsErrNotFound(err) {
			info.State = ContainerStateExited
			return nil
		}
		return fmt.Errorf("failed to stop container: %w", err)
	}

	// Wait for the container to actually stop
	waitCh, errCh := o.docker.ContainerWait(ctx, info.ContainerID, container.WaitConditionNotRunning)
	select {
	case result := <-waitCh:
		info.ExitCode = int(result.StatusCode)
		info.State = ContainerStateExited
		info.ExitedAt = time.Now()
		o.log("Container %s stopped with exit code %d", truncateID(info.ContainerID), info.ExitCode)
	case err := <-errCh:
		if !client.IsErrNotFound(err) {
			return fmt.Errorf("failed waiting for container to stop: %w", err)
		}
		info.State = ContainerStateExited
	case <-ctx.Done():
		return ctx.Err()
	}

	o.emitEvent(info, ContainerEventStopped)
	return nil
}

// RemoveContainer removes a stopped container.
//
// If force is true, the container will be killed first if still running.
func (o *DockerOrchestrator) RemoveContainer(ctx context.Context, info *ContainerInfo, force bool) error {
	if o.docker == nil {
		return fmt.Errorf("Docker client not initialized")
	}

	if info.ContainerID == "" {
		return nil // Nothing to remove
	}

	o.log("Removing container %s for task %s", truncateID(info.ContainerID), info.TaskID)

	if err := o.docker.ContainerRemove(ctx, info.ContainerID, container.RemoveOptions{
		Force:         force,
		RemoveVolumes: true,
	}); err != nil {
		if client.IsErrNotFound(err) {
			// Container already removed
			info.State = ContainerStateRemoving
			o.emitEvent(info, ContainerEventRemoved)
			return nil
		}
		return fmt.Errorf("failed to remove container: %w", err)
	}

	info.State = ContainerStateRemoving
	o.emitEvent(info, ContainerEventRemoved)
	o.log("Removed container %s", truncateID(info.ContainerID))
	return nil
}

// GetContainerLogs retrieves logs from a container.
//
// This method returns all logs (stdout and stderr) from the container.
// For real-time streaming, use StreamLogs instead.
func (o *DockerOrchestrator) GetContainerLogs(ctx context.Context, info *ContainerInfo, tail string) (string, error) {
	if o.docker == nil {
		return "", fmt.Errorf("Docker client not initialized")
	}

	if info.ContainerID == "" {
		return "", fmt.Errorf("container has not been created yet")
	}

	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       tail,
		Timestamps: true,
	}

	reader, err := o.docker.ContainerLogs(ctx, info.ContainerID, options)
	if err != nil {
		return "", fmt.Errorf("failed to get container logs: %w", err)
	}
	defer reader.Close()

	// Read all logs
	var stdout, stderr strings.Builder
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		// Try reading as raw stream (for containers with TTY)
		data, err := io.ReadAll(reader)
		if err != nil {
			return "", fmt.Errorf("failed to read container logs: %w", err)
		}
		return string(data), nil
	}

	// Combine stdout and stderr
	result := stdout.String()
	if stderr.Len() > 0 {
		result += "\n--- stderr ---\n" + stderr.String()
	}
	return result, nil
}

// StreamLogs starts streaming logs from a container to a writer.
//
// This method streams logs in real-time. It blocks until the container
// exits or the context is canceled. The logs are demultiplexed into
// stdout and stderr streams.
func (o *DockerOrchestrator) StreamLogs(ctx context.Context, info *ContainerInfo, stdout, stderr io.Writer) error {
	if o.docker == nil {
		return fmt.Errorf("Docker client not initialized")
	}

	if info.ContainerID == "" {
		return fmt.Errorf("container has not been created yet")
	}

	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
	}

	reader, err := o.docker.ContainerLogs(ctx, info.ContainerID, options)
	if err != nil {
		return fmt.Errorf("failed to stream container logs: %w", err)
	}
	defer reader.Close()

	// Store reader for later cleanup
	info.LogReader = reader

	// Demultiplex the stream
	if _, err := stdcopy.StdCopy(stdout, stderr, reader); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Check if it's an expected EOF
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("failed to stream logs: %w", err)
	}

	return nil
}

// streamContainerLogs streams container logs to the logger (internal use).
func (o *DockerOrchestrator) streamContainerLogs(info *ContainerInfo) {
	ctx := o.bgCtx

	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: false,
	}

	reader, err := o.docker.ContainerLogs(ctx, info.ContainerID, options)
	if err != nil {
		o.log("Failed to stream logs for %s: %v", truncateID(info.ContainerID), err)
		return
	}
	defer reader.Close()

	// Store for later cleanup
	info.LogReader = reader

	// Create a pipe to capture output line by line
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		stdcopy.StdCopy(pw, pw, reader)
	}()

	// Read and log each line
	buf := make([]byte, 4096)
	prefix := o.opts.LogPrefix
	if prefix == "" {
		prefix = "Container"
	}

	for {
		n, err := pr.Read(buf)
		if n > 0 {
			line := strings.TrimSuffix(string(buf[:n]), "\n")
			for _, l := range strings.Split(line, "\n") {
				if l != "" {
					o.log("[%s %s] %s", prefix, truncateID(info.ContainerID), l)
				}
			}
		}
		if err != nil {
			if err != io.EOF && ctx.Err() == nil {
				o.log("Log stream error for %s: %v", truncateID(info.ContainerID), err)
			}
			return
		}
	}
}

// ensureImage pulls the image if it doesn't exist locally.
func (o *DockerOrchestrator) ensureImage(ctx context.Context, imageName string) error {
	// Check if image exists locally
	_, _, err := o.docker.ImageInspectWithRaw(ctx, imageName)
	if err == nil {
		return nil // Image exists
	}

	// Pull the image
	o.log("Pulling image %s...", imageName)
	reader, err := o.docker.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image %s: %w", imageName, err)
	}
	defer reader.Close()

	// Wait for pull to complete (drain the reader)
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return fmt.Errorf("failed while pulling image %s: %w", imageName, err)
	}

	o.log("Pulled image %s", imageName)
	return nil
}

// WaitForContainer waits for a container to exit and returns the exit code.
func (o *DockerOrchestrator) WaitForContainer(ctx context.Context, info *ContainerInfo) (int, error) {
	if o.docker == nil {
		return -1, fmt.Errorf("Docker client not initialized")
	}

	if info.ContainerID == "" {
		return -1, fmt.Errorf("container has not been created yet")
	}

	waitCh, errCh := o.docker.ContainerWait(ctx, info.ContainerID, container.WaitConditionNotRunning)

	select {
	case result := <-waitCh:
		info.ExitCode = int(result.StatusCode)
		info.State = ContainerStateExited
		info.ExitedAt = time.Now()

		if result.StatusCode == 0 {
			o.emitEvent(info, ContainerEventStopped)
		} else {
			o.emitEvent(info, ContainerEventDied)
		}

		return info.ExitCode, nil

	case err := <-errCh:
		if client.IsErrNotFound(err) {
			info.State = ContainerStateExited
			return info.ExitCode, nil
		}
		return -1, fmt.Errorf("failed waiting for container: %w", err)

	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

// InspectContainer updates ContainerInfo with current container state from Docker.
func (o *DockerOrchestrator) InspectContainer(ctx context.Context, info *ContainerInfo) error {
	if o.docker == nil {
		return fmt.Errorf("Docker client not initialized")
	}

	if info.ContainerID == "" {
		return fmt.Errorf("container has not been created yet")
	}

	inspectData, err := o.docker.ContainerInspect(ctx, info.ContainerID)
	if err != nil {
		if client.IsErrNotFound(err) {
			info.State = ContainerStateRemoving
			return nil
		}
		return fmt.Errorf("failed to inspect container: %w", err)
	}

	// Map Docker state to our state
	switch inspectData.State.Status {
	case "created":
		info.State = ContainerStateCreated
	case "running":
		info.State = ContainerStateRunning
	case "paused":
		info.State = ContainerStatePaused
	case "restarting":
		info.State = ContainerStateRestarting
	case "exited":
		info.State = ContainerStateExited
		info.ExitCode = inspectData.State.ExitCode
	case "dead":
		info.State = ContainerStateDead
	case "removing":
		info.State = ContainerStateRemoving
	default:
		info.State = ContainerStateUnknown
	}

	// Check for OOM kill
	if inspectData.State.OOMKilled {
		o.emitEvent(info, ContainerEventOOM)
	}

	// Check health if available
	if inspectData.State.Health != nil {
		switch inspectData.State.Health.Status {
		case "healthy":
			o.emitEvent(info, ContainerEventHealthy)
		case "unhealthy":
			o.emitEvent(info, ContainerEventUnhealthy)
		}
	}

	return nil
}

// CreateAndStartContainer is a convenience method that creates and starts a container.
//
// This combines CreateContainer and StartContainer into a single call.
// It also emits appropriate lifecycle events.
func (o *DockerOrchestrator) CreateAndStartContainer(ctx context.Context, info *ContainerInfo, config *ContainerConfig) error {
	// Create the container
	if err := o.CreateContainer(ctx, info, config); err != nil {
		return err
	}

	// Emit created event
	o.emitEvent(info, ContainerEventCreated)

	// Start the container
	if err := o.StartContainer(ctx, info); err != nil {
		// Try to clean up the created container
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		o.RemoveContainer(cleanupCtx, info, true)
		return err
	}

	return nil
}

// StopAndRemoveContainer is a convenience method that stops and removes a container.
//
// This combines StopContainer and RemoveContainer into a single call.
// It's useful for cleanup operations.
func (o *DockerOrchestrator) StopAndRemoveContainer(ctx context.Context, info *ContainerInfo) error {
	// Stop if running
	if info.State == ContainerStateRunning {
		if err := o.StopContainer(ctx, info); err != nil {
			o.log("Warning: failed to stop container %s: %v", truncateID(info.ContainerID), err)
			// Continue to removal with force
		}
	}

	// Remove the container
	return o.RemoveContainer(ctx, info, true)
}

// =============================================================================
// Container Name Generation
// =============================================================================

// GenerateContainerName creates a container name for a task assignment.
func (o *DockerOrchestrator) GenerateContainerName(taskID, workspace, implementation, specifier string) string {
	// Format: {prefix}{workspace}-{implementation}-{specifier}-{taskID_suffix}
	name := fmt.Sprintf("%s%s-%s-%s-%s",
		o.opts.ContainerPrefix,
		sanitizeName(workspace),
		sanitizeName(implementation),
		sanitizeName(specifier),
		taskID[:min(8, len(taskID))],
	)
	return name
}

// =============================================================================
// Logging
// =============================================================================

// log writes a timestamped log message.
func (o *DockerOrchestrator) log(format string, args ...any) {
	timestamp := time.Now().Format("2006-01-02T15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	o.logger.Printf("[%s] [DockerOrchestrator] %s", timestamp, msg)
}

// =============================================================================
// Helper Functions
// =============================================================================

// generateAuthToken creates a random authentication token.
func generateAuthToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp-based token
		return fmt.Sprintf("token-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// parseJSONStringArray attempts to parse a JSON string array.
// Falls back to splitting by spaces if not valid JSON.
func parseJSONStringArray(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	// Check if it looks like JSON array
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		// Simple JSON array parsing (for basic cases)
		inner := strings.Trim(s, "[]")
		if inner == "" {
			return nil
		}
		parts := strings.Split(inner, ",")
		result := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			part = strings.Trim(part, `"'`)
			if part != "" {
				result = append(result, part)
			}
		}
		return result
	}

	// Fall back to space-separated
	return strings.Fields(s)
}

// parseMemoryString parses a memory string (e.g., "512m", "1g") to bytes.
func parseMemoryString(s string) int64 {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return 0
	}

	var multiplier int64 = 1
	if strings.HasSuffix(s, "k") || strings.HasSuffix(s, "kb") {
		multiplier = 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "kb"), "k")
	} else if strings.HasSuffix(s, "m") || strings.HasSuffix(s, "mb") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "mb"), "m")
	} else if strings.HasSuffix(s, "g") || strings.HasSuffix(s, "gb") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "gb"), "g")
	}

	var value int64
	if _, err := fmt.Sscanf(s, "%d", &value); err != nil {
		return 0
	}
	return value * multiplier
}

// parseCPUString parses a CPU string (e.g., "0.5", "2.0") to nanoCPUs.
func parseCPUString(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	var cpus float64
	if _, err := fmt.Sscanf(s, "%f", &cpus); err != nil {
		return 0
	}

	// Convert to nanoCPUs (10^9 = 1 CPU)
	return int64(cpus * 1e9)
}

// sanitizeName sanitizes a string for use in container names.
func sanitizeName(s string) string {
	// Replace non-alphanumeric characters with dashes
	var result strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			result.WriteRune(r)
		} else if r == '-' || r == '_' || r == '.' {
			result.WriteRune('-')
		}
	}
	return strings.ToLower(result.String())
}

// truncateID truncates a container ID for display.
func truncateID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// min returns the minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
