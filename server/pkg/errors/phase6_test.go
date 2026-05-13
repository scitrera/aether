package errors

import (
	"errors"
	"fmt"
	"testing"
)

func TestAgentNotFoundError(t *testing.T) {
	err := &AgentNotFoundError{Implementation: "my-agent"}

	expected := "agent implementation 'my-agent' not found in registry"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	// Test that it implements error interface
	var _ error = err
}

func TestOrchestratorNotFoundError(t *testing.T) {
	err := &OrchestratorNotFoundError{
		Profile:   "kubernetes",
		Workspace: "prod",
	}

	expected := "no active orchestrators for profile 'kubernetes' in workspace 'prod'"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	var _ error = err
}

func TestInvalidAssignmentModeError(t *testing.T) {
	err := &InvalidAssignmentModeError{Mode: "unknown_mode"}

	expected := "unknown assignment mode: unknown_mode"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	var _ error = err
}

func TestTargetAgentRequiredError(t *testing.T) {
	err := &TargetAgentRequiredError{}

	expected := "target_agent_id is required for targeted assignment mode"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	var _ error = err
}

func TestProfileRequiredError(t *testing.T) {
	err := &ProfileRequiredError{}

	expected := "launch_params must contain 'profile' field"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	var _ error = err
}

func TestTaskNotFoundError(t *testing.T) {
	err := &TaskNotFoundError{TaskID: "task-123"}

	expected := "task 'task-123' not found"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	var _ error = err
}

func TestDuplicateRegistrationError(t *testing.T) {
	err := &DuplicateRegistrationError{Implementation: "existing-agent"}

	expected := "agent implementation 'existing-agent' is already registered"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	var _ error = err
}

func TestInitializationError(t *testing.T) {
	innerErr := fmt.Errorf("connection refused")
	err := &InitializationError{
		Component: "TaskStore",
		Err:       innerErr,
	}

	expected := "failed to initialize TaskStore: connection refused"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	// Test Unwrap
	if err.Unwrap() != innerErr {
		t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), innerErr)
	}

	// Test errors.Is with wrapped error
	if !errors.Is(err, innerErr) {
		t.Errorf("errors.Is() should return true for wrapped error")
	}

	var _ error = err
}

func TestInitializationError_NilError(t *testing.T) {
	err := &InitializationError{
		Component: "TaskStore",
		Err:       nil,
	}

	expected := "failed to initialize TaskStore: <nil>"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	if err.Unwrap() != nil {
		t.Errorf("Unwrap() = %v, want nil", err.Unwrap())
	}
}

// Test that errors can be used with errors.As for type checking
func TestErrorTypeAssertions(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"AgentNotFoundError", &AgentNotFoundError{Implementation: "test"}},
		{"OrchestratorNotFoundError", &OrchestratorNotFoundError{Profile: "test", Workspace: "test"}},
		{"InvalidAssignmentModeError", &InvalidAssignmentModeError{Mode: "test"}},
		{"TargetAgentRequiredError", &TargetAgentRequiredError{}},
		{"ProfileRequiredError", &ProfileRequiredError{}},
		{"TaskNotFoundError", &TaskNotFoundError{TaskID: "test"}},
		{"DuplicateRegistrationError", &DuplicateRegistrationError{Implementation: "test"}},
		{"InitializationError", &InitializationError{Component: "test", Err: nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Wrap the error
			wrapped := fmt.Errorf("wrapped: %w", tt.err)

			// Should be able to extract original error type
			switch tt.name {
			case "AgentNotFoundError":
				var target *AgentNotFoundError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			case "OrchestratorNotFoundError":
				var target *OrchestratorNotFoundError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			case "InvalidAssignmentModeError":
				var target *InvalidAssignmentModeError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			case "TargetAgentRequiredError":
				var target *TargetAgentRequiredError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			case "ProfileRequiredError":
				var target *ProfileRequiredError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			case "TaskNotFoundError":
				var target *TaskNotFoundError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			case "DuplicateRegistrationError":
				var target *DuplicateRegistrationError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			case "InitializationError":
				var target *InitializationError
				if !errors.As(wrapped, &target) {
					t.Errorf("errors.As failed for %s", tt.name)
				}
			}
		})
	}
}
