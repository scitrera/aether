package errors

import (
	"testing"
)

// TestDuplicateIdentityError tests DuplicateIdentityError formatting
func TestDuplicateIdentityError(t *testing.T) {
	err := &DuplicateIdentityError{
		Identity:          "ag::prod::my-agent::v1",
		ExistingSessionID: "session-789",
	}

	expected := "identity 'ag::prod::my-agent::v1' is already connected (session: session-789)"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}

	var _ error = err
}

func TestDuplicateIdentityError_NoSessionID(t *testing.T) {
	err := &DuplicateIdentityError{
		Identity: "ag::prod::my-agent::v1",
	}

	expected := "identity 'ag::prod::my-agent::v1' is already connected"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

// TestQuotaExceededError tests QuotaExceededError formatting
func TestQuotaExceededError(t *testing.T) {
	err := &QuotaExceededError{
		Resource:  "connections",
		Workspace: "prod",
		Identity:  "ag::prod::worker::v1",
		Current:   100,
		Limit:     100,
	}

	expected := "quota exceeded for connections in workspace 'prod' (identity: ag::prod::worker::v1): current 100, limit 100"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

func TestQuotaExceededError_NoIdentity(t *testing.T) {
	err := &QuotaExceededError{
		Resource:  "connections",
		Workspace: "prod",
		Current:   50,
		Limit:     100,
	}

	expected := "quota exceeded for connections in workspace 'prod': current 50, limit 100"
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

// TestErrorCodeConstants verifies error code constants exist and have correct prefixes
func TestErrorCodeConstants(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		prefix string
	}{
		{"ErrSessionDuplicate", ErrSessionDuplicate, "ERR_SESSION_"},
		{"ErrOrchAgentNotFound", ErrOrchAgentNotFound, "ERR_ORCH_"},
		{"ErrOrchUnavailable", ErrOrchUnavailable, "ERR_ORCH_"},
		{"ErrOrchTaskAssignment", ErrOrchTaskAssignment, "ERR_ORCH_"},
		{"ErrOrchDuplicateRegistration", ErrOrchDuplicateRegistration, "ERR_ORCH_"},
		{"ErrQuotaExceeded", ErrQuotaExceeded, "ERR_QUOTA_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.code) < len(tt.prefix) || tt.code[:len(tt.prefix)] != tt.prefix {
				t.Errorf("Error code %q does not have expected prefix %q", tt.code, tt.prefix)
			}
		})
	}
}

// TestCategoryOrchestration verifies the category constant
func TestCategoryOrchestration(t *testing.T) {
	if string(CategoryOrchestration) != "Orchestration" {
		t.Errorf("CategoryOrchestration = %q, want %q", CategoryOrchestration, "Orchestration")
	}
}
