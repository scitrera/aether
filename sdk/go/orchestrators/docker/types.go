// Package docker provides a Docker-based orchestrator implementation for Aether.
//
// The Docker Orchestrator launches agents and tasks as Docker containers,
// providing lifecycle management including container creation, startup,
// health monitoring, log streaming, and graceful termination.
//
// # Architecture
//
// The Docker Orchestrator follows the same pattern as the Python SDK's
// MultiprocessOrchestrator but substitutes containers for local processes:
//
//   - Receives task assignments from the gateway
//   - Launches containers with appropriate environment configuration
//   - Streams container logs for debugging
//   - Monitors container health and handles exits
//   - Supports graceful shutdown with configurable timeouts
//
// # Container Configuration
//
// When launching a container, the orchestrator:
//
//  1. Uses launch parameters from the task assignment (image, args, etc.)
//  2. Sets Aether environment variables (gateway, workspace, credentials)
//  3. Optionally mounts volumes and sets resource constraints
//  4. Tracks the container for lifecycle management
//
// # Usage
//
//	orch, err := docker.NewDockerOrchestrator(docker.Options{
//	    ServerAddr:        "localhost:50051",
//	    Implementation:    "my-docker-orchestrator",
//	    SupportedProfiles: []string{"docker-agent"},
//	    Docker: docker.ClientOptions{
//	        Host: "unix:///var/run/docker.sock",
//	    },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Optional: Add custom container config
//	orch.OnAssignment(func(ctx context.Context, a *aether.TaskAssignment, cfg *docker.ContainerConfig) error {
//	    // Customize container config based on assignment
//	    cfg.Memory = 512 * 1024 * 1024 // 512MB
//	    return nil
//	})
//
//	ctx := context.Background()
//	if err := orch.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
package docker

import (
	"io"
	"time"
)

// =============================================================================
// Container Info Types
// =============================================================================

// ContainerInfo tracks information about a launched container.
//
// This struct is used to track all containers spawned by the orchestrator
// in response to task assignments. It provides access to container state,
// exit status, and the ability to interact with the container.
type ContainerInfo struct {
	// TaskID is the unique identifier of the task assignment.
	TaskID string

	// ContainerID is the Docker container ID (full 64-character ID).
	ContainerID string

	// Workspace is the workspace the agent/task operates in.
	Workspace string

	// Implementation is the agent/task implementation type.
	Implementation string

	// Specifier is the unique specifier for this agent/task instance.
	Specifier string

	// Profile is the orchestration profile that matched.
	Profile string

	// Image is the Docker image used for the container.
	Image string

	// StartedAt is when the container was created/started.
	StartedAt time.Time

	// ExitedAt is when the container exited (zero if still running).
	ExitedAt time.Time

	// ExitCode is the container's exit code (-1 if still running or unknown).
	ExitCode int

	// State is the current container state (created, running, exited, etc.).
	State ContainerState

	// Metadata contains additional assignment metadata.
	Metadata map[string]string

	// LaunchParams contains the launch parameters from the assignment.
	LaunchParams map[string]string

	// Environment contains the environment variables set for the container.
	Environment map[string]string

	// Error contains any error that occurred during container operations.
	Error error

	// LogReader is the stream for container logs (set if log streaming enabled).
	LogReader io.ReadCloser
}

// IsRunning returns true if the container is currently running.
func (c *ContainerInfo) IsRunning() bool {
	return c.State == ContainerStateRunning
}

// ContainerState represents the state of a Docker container.
type ContainerState string

const (
	// ContainerStateCreated indicates the container has been created but not started.
	ContainerStateCreated ContainerState = "created"

	// ContainerStateRunning indicates the container is running.
	ContainerStateRunning ContainerState = "running"

	// ContainerStatePaused indicates the container is paused.
	ContainerStatePaused ContainerState = "paused"

	// ContainerStateRestarting indicates the container is restarting.
	ContainerStateRestarting ContainerState = "restarting"

	// ContainerStateExited indicates the container has exited.
	ContainerStateExited ContainerState = "exited"

	// ContainerStateDead indicates the container is dead.
	ContainerStateDead ContainerState = "dead"

	// ContainerStateRemoving indicates the container is being removed.
	ContainerStateRemoving ContainerState = "removing"

	// ContainerStateUnknown indicates an unknown state.
	ContainerStateUnknown ContainerState = "unknown"
)

// =============================================================================
// Container Configuration Types
// =============================================================================

// ContainerConfig specifies how to create and run a Docker container.
//
// This configuration is built from the task assignment parameters and
// can be customized via the OnAssignment callback before the container
// is created.
type ContainerConfig struct {
	// Image is the Docker image to use (required).
	// Can be overridden by launch_params["image"].
	Image string

	// Command is the command to run in the container (optional).
	// If empty, the image's default entrypoint/cmd is used.
	Command []string

	// Entrypoint overrides the image's default entrypoint (optional).
	Entrypoint []string

	// WorkingDir sets the working directory inside the container.
	WorkingDir string

	// Environment contains environment variables for the container.
	// Aether-specific variables are automatically added.
	Environment map[string]string

	// Labels are Docker labels to apply to the container.
	Labels map[string]string

	// Mounts specifies volume mounts for the container.
	Mounts []MountConfig

	// Network configuration
	NetworkMode string   // e.g., "bridge", "host", "none", or network name
	Networks    []string // Additional networks to connect to

	// Resource constraints
	Memory     int64 // Memory limit in bytes (0 = unlimited)
	MemorySwap int64 // Total memory including swap (0 = unlimited)
	CPUShares  int64 // CPU shares (relative weight)
	CPUQuota   int64 // CPU quota in microseconds
	CPUPeriod  int64 // CPU period in microseconds
	NanoCPUs   int64 // CPU quota in units of 10^-9 CPUs

	// User sets the user to run as inside the container.
	User string

	// Hostname sets the container's hostname.
	Hostname string

	// Privileged runs the container in privileged mode.
	// WARNING: Use with caution - grants full host access.
	Privileged bool

	// AutoRemove removes the container when it exits.
	AutoRemove bool

	// RestartPolicy configures container restart behavior.
	RestartPolicy RestartPolicy

	// HealthCheck configures container health checking.
	HealthCheck *HealthCheckConfig

	// StopTimeout is the timeout for stopping the container (seconds).
	// If zero, uses Docker's default (10 seconds).
	StopTimeout int

	// LogConfig configures container logging.
	LogConfig *LogConfig
}

// MountConfig specifies a volume mount for a container.
type MountConfig struct {
	// Type is the mount type: "bind", "volume", or "tmpfs".
	Type string

	// Source is the source path for bind mounts or volume name.
	Source string

	// Target is the mount path inside the container.
	Target string

	// ReadOnly mounts the volume as read-only.
	ReadOnly bool

	// Consistency specifies the mount consistency (for bind mounts).
	// One of: "default", "consistent", "cached", "delegated".
	Consistency string
}

// RestartPolicy configures container restart behavior.
type RestartPolicy struct {
	// Name is the restart policy name:
	// "no", "always", "on-failure", "unless-stopped"
	Name string

	// MaximumRetryCount is the max restart count for "on-failure" policy.
	MaximumRetryCount int
}

// HealthCheckConfig configures container health checking.
type HealthCheckConfig struct {
	// Test is the health check command.
	// Format: ["CMD", "arg1", "arg2"] or ["CMD-SHELL", "command"]
	Test []string

	// Interval is the time between health checks.
	Interval time.Duration

	// Timeout is the time to wait for a health check to complete.
	Timeout time.Duration

	// StartPeriod is the grace period before starting health checks.
	StartPeriod time.Duration

	// Retries is the number of consecutive failures before unhealthy.
	Retries int
}

// LogConfig configures container logging.
type LogConfig struct {
	// Type is the logging driver type (e.g., "json-file", "syslog", "none").
	Type string

	// Config contains driver-specific configuration options.
	Config map[string]string
}

// =============================================================================
// Client Configuration Types
// =============================================================================

// ClientOptions configures the Docker client connection.
type ClientOptions struct {
	// Host is the Docker daemon socket address.
	// Examples:
	//   - "unix:///var/run/docker.sock" (Unix socket, default)
	//   - "tcp://localhost:2375" (unencrypted TCP)
	//   - "tcp://localhost:2376" (TLS TCP)
	// If empty, uses DOCKER_HOST environment variable or default socket.
	Host string

	// APIVersion is the Docker API version to use.
	// If empty, uses the client's default version negotiation.
	// Example: "1.41"
	APIVersion string

	// CertPath is the path to TLS certificates for secure connections.
	// If set, expects ca.pem, cert.pem, and key.pem files in this directory.
	CertPath string

	// TLSVerify enables TLS verification (requires CertPath).
	TLSVerify bool
}

// =============================================================================
// Orchestrator Options
// =============================================================================

// Options configures the Docker Orchestrator.
type Options struct {
	// ServerAddr is the Aether gateway address (host:port).
	// Default: "localhost:50051"
	ServerAddr string

	// Implementation is the orchestrator implementation name.
	// Default: "docker-orchestrator"
	Implementation string

	// Specifier is an optional unique identifier for this orchestrator instance.
	// If empty, the server generates one.
	Specifier string

	// SupportedProfiles is the list of profiles this orchestrator handles.
	// Required: at least one profile must be specified.
	SupportedProfiles []string

	// Docker configures the Docker client connection.
	Docker ClientOptions

	// DefaultImage is the default Docker image when none is specified
	// in the task assignment launch params.
	DefaultImage string

	// DefaultNetwork is the default network mode for containers.
	// Default: "bridge"
	DefaultNetwork string

	// ContainerPrefix is a prefix added to container names.
	// Default: "aether-"
	ContainerPrefix string

	// Credentials for Aether gateway authentication.
	Credentials map[string]string

	// TLS configures TLS/mTLS for the Aether gateway connection.
	TLS interface{} // *aether.TLSConfig

	// Connection configures connection behavior (retry, backoff, etc.).
	Connection interface{} // aether.ConnectionOptions

	// StopTimeout is the default timeout for stopping containers (seconds).
	// Default: 30
	StopTimeout int

	// KillTimeout is the timeout after stop before force killing (seconds).
	// Default: 10
	KillTimeout int

	// AutoRemoveContainers removes containers after they exit.
	// Default: false (containers are kept for debugging)
	AutoRemoveContainers bool

	// StreamLogs enables automatic log streaming from containers.
	// Default: true
	StreamLogs bool

	// LogPrefix is the prefix for container log output.
	// Default: "Container"
	LogPrefix string

	// Logger is the logger to use. If nil, uses the standard log package.
	Logger Logger
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		ServerAddr:           "localhost:50051",
		Implementation:       "docker-orchestrator",
		SupportedProfiles:    []string{"docker"},
		DefaultNetwork:       "bridge",
		ContainerPrefix:      "aether-",
		StopTimeout:          30,
		KillTimeout:          10,
		AutoRemoveContainers: false,
		StreamLogs:           true,
		LogPrefix:            "Container",
	}
}

// Logger defines the logging interface used by the orchestrator.
type Logger interface {
	Printf(format string, v ...any)
}

// =============================================================================
// Handler Types
// =============================================================================

// ContainerConfigBuilder is called to customize container configuration
// before the container is created.
//
// Return an error to reject the assignment and prevent container creation.
type ContainerConfigBuilder func(assignment interface{}, config *ContainerConfig) error

// ContainerEventHandler is called when container lifecycle events occur.
type ContainerEventHandler func(info *ContainerInfo, event ContainerEvent)

// ContainerEvent represents a container lifecycle event.
type ContainerEvent string

const (
	// ContainerEventCreated is emitted when a container is created.
	ContainerEventCreated ContainerEvent = "created"

	// ContainerEventStarted is emitted when a container starts.
	ContainerEventStarted ContainerEvent = "started"

	// ContainerEventStopped is emitted when a container stops.
	ContainerEventStopped ContainerEvent = "stopped"

	// ContainerEventDied is emitted when a container dies unexpectedly.
	ContainerEventDied ContainerEvent = "died"

	// ContainerEventRemoved is emitted when a container is removed.
	ContainerEventRemoved ContainerEvent = "removed"

	// ContainerEventOOM is emitted when a container is killed due to OOM.
	ContainerEventOOM ContainerEvent = "oom"

	// ContainerEventHealthy is emitted when a container becomes healthy.
	ContainerEventHealthy ContainerEvent = "healthy"

	// ContainerEventUnhealthy is emitted when a container becomes unhealthy.
	ContainerEventUnhealthy ContainerEvent = "unhealthy"
)

// =============================================================================
// Environment Variable Constants
// =============================================================================

const (
	// EnvGateway is the environment variable for the Aether gateway address.
	EnvGateway = "AETHER_GATEWAY"

	// EnvWorkspace is the environment variable for the workspace name.
	EnvWorkspace = "AETHER_WORKSPACE"

	// EnvImplementation is the environment variable for the implementation type.
	EnvImplementation = "AETHER_IMPLEMENTATION"

	// EnvSpecifier is the environment variable for the instance specifier.
	EnvSpecifier = "AETHER_SPECIFIER"

	// EnvAuthToken is the environment variable for the authentication token.
	EnvAuthToken = "AETHER_AUTH_TOKEN"

	// EnvTaskID is the environment variable for the task ID.
	EnvTaskID = "AETHER_TASK_ID"

	// EnvProfile is the environment variable for the orchestration profile.
	EnvProfile = "AETHER_PROFILE"
)

// =============================================================================
// Launch Parameter Keys
// =============================================================================

const (
	// LaunchParamImage is the key for the Docker image in launch params.
	LaunchParamImage = "image"

	// LaunchParamCommand is the key for the command in launch params.
	// Value should be a JSON-encoded string array.
	LaunchParamCommand = "command"

	// LaunchParamEntrypoint is the key for the entrypoint in launch params.
	// Value should be a JSON-encoded string array.
	LaunchParamEntrypoint = "entrypoint"

	// LaunchParamWorkingDir is the key for the working directory.
	LaunchParamWorkingDir = "working_dir"

	// LaunchParamMemory is the key for memory limit in launch params.
	// Value should be a string representing bytes (e.g., "512m", "1g").
	LaunchParamMemory = "memory"

	// LaunchParamCPUs is the key for CPU limit in launch params.
	// Value should be a decimal string (e.g., "0.5", "2.0").
	LaunchParamCPUs = "cpus"

	// LaunchParamNetwork is the key for network mode in launch params.
	LaunchParamNetwork = "network"

	// LaunchParamEnvPrefix is the prefix for custom environment variables.
	// Variables with this prefix are added to the container environment.
	LaunchParamEnvPrefix = "env."

	// LaunchParamPrivileged is the key for privileged mode (true/false).
	LaunchParamPrivileged = "privileged"

	// LaunchParamUser is the key for the user to run as.
	LaunchParamUser = "user"
)
