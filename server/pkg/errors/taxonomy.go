package errors

import "fmt"

// ErrorCategory represents the high-level category of an error
type ErrorCategory string

const (
	// CategoryOrchestration represents orchestration errors (agent startup, task assignment)
	CategoryOrchestration ErrorCategory = "Orchestration"
)

// Error codes - unique identifiers for each error type
const (
	// Session errors (ERR_SESSION_xxx)
	ErrSessionDuplicate = "ERR_SESSION_001"

	// Orchestration errors (ERR_ORCH_xxx)
	ErrOrchAgentNotFound         = "ERR_ORCH_001"
	ErrOrchUnavailable           = "ERR_ORCH_002"
	ErrOrchTaskAssignment        = "ERR_ORCH_003"
	ErrOrchDuplicateRegistration = "ERR_ORCH_004"

	// Quota errors (ERR_QUOTA_xxx)
	ErrQuotaExceeded = "ERR_QUOTA_001"
)

// Session Error Types

// DuplicateIdentityError indicates an identity is already connected
type DuplicateIdentityError struct {
	Identity          string
	ExistingSessionID string
}

func (e *DuplicateIdentityError) Error() string {
	if e.ExistingSessionID != "" {
		return fmt.Sprintf("identity '%s' is already connected (session: %s)", e.Identity, e.ExistingSessionID)
	}
	return fmt.Sprintf("identity '%s' is already connected", e.Identity)
}

// Orchestration Error Types

// AgentNotFoundError indicates an agent implementation is not registered
type AgentNotFoundError struct {
	Implementation string
}

func (e *AgentNotFoundError) Error() string {
	return fmt.Sprintf("agent implementation '%s' not found in registry", e.Implementation)
}

// OrchestratorNotFoundError indicates no orchestrator is available for a profile
type OrchestratorNotFoundError struct {
	Profile   string
	Workspace string
}

func (e *OrchestratorNotFoundError) Error() string {
	return fmt.Sprintf("no active orchestrators for profile '%s' in workspace '%s'", e.Profile, e.Workspace)
}

// InvalidAssignmentModeError indicates an unsupported task assignment mode
type InvalidAssignmentModeError struct {
	Mode string
}

func (e *InvalidAssignmentModeError) Error() string {
	return fmt.Sprintf("unknown assignment mode: %s", e.Mode)
}

// TargetAgentRequiredError indicates a targeted task is missing target_agent_id
type TargetAgentRequiredError struct{}

func (e *TargetAgentRequiredError) Error() string {
	return "target_agent_id is required for targeted assignment mode"
}

// ProfileRequiredError indicates launch_params missing required 'profile' field
type ProfileRequiredError struct{}

func (e *ProfileRequiredError) Error() string {
	return "launch_params must contain 'profile' field"
}

// TaskNotFoundError indicates a task ID does not exist
type TaskNotFoundError struct {
	TaskID string
}

func (e *TaskNotFoundError) Error() string {
	return fmt.Sprintf("task '%s' not found", e.TaskID)
}

// DuplicateRegistrationError indicates an agent implementation is already registered
type DuplicateRegistrationError struct {
	Implementation string
}

func (e *DuplicateRegistrationError) Error() string {
	return fmt.Sprintf("agent implementation '%s' is already registered", e.Implementation)
}

// InitializationError indicates a orchestration service failed to initialize
type InitializationError struct {
	Component string
	Err       error
}

func (e *InitializationError) Error() string {
	return fmt.Sprintf("failed to initialize %s: %v", e.Component, e.Err)
}

func (e *InitializationError) Unwrap() error {
	return e.Err
}

// Quota Error Types

// QuotaExceededError indicates a resource quota has been exceeded
type QuotaExceededError struct {
	Resource  string // "connections", "message_rate", "kv_keys", "kv_value_size"
	Workspace string
	Identity  string
	Current   int
	Limit     int
}

func (e *QuotaExceededError) Error() string {
	if e.Identity != "" {
		return fmt.Sprintf("quota exceeded for %s in workspace '%s' (identity: %s): current %d, limit %d",
			e.Resource, e.Workspace, e.Identity, e.Current, e.Limit)
	}
	return fmt.Sprintf("quota exceeded for %s in workspace '%s': current %d, limit %d",
		e.Resource, e.Workspace, e.Current, e.Limit)
}
