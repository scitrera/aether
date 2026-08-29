package gateway

import (
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/tasks"
)

func TestApplyTaskAuthorizationToAssignmentUsesPersistedAuthority(t *testing.T) {
	assignment := &pb.TaskAssignment{}
	task := &tasks.ExtendedTask{
		Authority: tasks.TaskAuthorityInfo{
			Mode:             "on_behalf_of",
			SubjectType:      "user",
			SubjectID:        "alice",
			AuthorityGrantID: "grant-assignee",
		},
		Metadata: map[string]interface{}{
			"authority_grant_id": "forged-metadata-grant",
			"subject_id":         "mallory",
		},
	}
	applyTaskAuthorizationToAssignment(assignment, task)

	if assignment.Authorization == nil {
		t.Fatal("authorization was not projected")
	}
	if got := assignment.Authorization.GetGrantId(); got != "grant-assignee" {
		t.Fatalf("grant id = %q", got)
	}
	if got := assignment.Authorization.GetSubject().GetPrincipalId(); got != "alice" {
		t.Fatalf("subject id = %q", got)
	}
}

func TestApplyTaskAuthorizationToAssignmentOmitsDirectTask(t *testing.T) {
	assignment := &pb.TaskAssignment{}
	applyTaskAuthorizationToAssignment(assignment, &tasks.ExtendedTask{})
	if assignment.Authorization != nil {
		t.Fatalf("authorization = %#v", assignment.Authorization)
	}
}
