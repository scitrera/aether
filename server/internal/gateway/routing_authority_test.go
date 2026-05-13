package gateway

import (
	"testing"

	"github.com/scitrera/aether/pkg/tasks"
)

// TestApplyAuthorityToTaskContext verifies that applyAuthorityToTaskContext
// populates "user" and "root_user" in the task context map only when the
// corresponding subject type is exactly "user" with a non-empty ID.

func TestApplyAuthorityToTaskContext(t *testing.T) {
	tests := []struct {
		name         string
		auth         tasks.TaskAuthorityInfo
		wantUser     string // empty means key must NOT be present
		wantRootUser string // empty means key must NOT be present
	}{
		{
			name: "user task: subject is user with no root",
			auth: tasks.TaskAuthorityInfo{
				SubjectType:     "user",
				SubjectID:       "alice@example.com",
				RootSubjectType: "",
				RootSubjectID:   "",
			},
			wantUser:     "alice@example.com",
			wantRootUser: "",
		},
		{
			name: "nested task: subject is task, root is user",
			auth: tasks.TaskAuthorityInfo{
				SubjectType:     "task",
				SubjectID:       "task-xyz",
				RootSubjectType: "user",
				RootSubjectID:   "bob@example.com",
			},
			wantUser:     "",
			wantRootUser: "bob@example.com",
		},
		{
			name: "service task: neither subject nor root is user",
			auth: tasks.TaskAuthorityInfo{
				SubjectType:     "service",
				SubjectID:       "orchestrator-svc",
				RootSubjectType: "",
				RootSubjectID:   "",
			},
			wantUser:     "",
			wantRootUser: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := make(map[string]string)
			applyAuthorityToTaskContext(tc, tt.auth)

			// Check "user" key
			gotUser, hasUser := tc["user"]
			if tt.wantUser != "" {
				if !hasUser {
					t.Errorf("expected tc[\"user\"] = %q, but key is absent", tt.wantUser)
				} else if gotUser != tt.wantUser {
					t.Errorf("tc[\"user\"] = %q, want %q", gotUser, tt.wantUser)
				}
			} else {
				if hasUser {
					t.Errorf("expected tc[\"user\"] to be absent, got %q", gotUser)
				}
			}

			// Check "root_user" key
			gotRootUser, hasRootUser := tc["root_user"]
			if tt.wantRootUser != "" {
				if !hasRootUser {
					t.Errorf("expected tc[\"root_user\"] = %q, but key is absent", tt.wantRootUser)
				} else if gotRootUser != tt.wantRootUser {
					t.Errorf("tc[\"root_user\"] = %q, want %q", gotRootUser, tt.wantRootUser)
				}
			} else {
				if hasRootUser {
					t.Errorf("expected tc[\"root_user\"] to be absent, got %q", gotRootUser)
				}
			}
		})
	}
}
