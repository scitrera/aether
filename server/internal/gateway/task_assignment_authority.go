package gateway

import (
	"fmt"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/pkg/models"
)

func validateTaskAuthorityAssignment(actor models.Identity, req *pb.CreateTaskRequest) error {
	assignment := req.GetAuthorityAssignment()
	if assignment == nil {
		return nil
	}
	if actor.Type != models.PrincipalUser || req.GetAuthorization() != nil || req.GetParentTaskId() != "" || req.GetOriginatingScheduleId() != "" {
		return fmt.Errorf("task authority must be explicitly assigned by the authenticated user")
	}
	if assignment.ExpiresInSeconds == 0 || assignment.ExpiresInSeconds > 86400 {
		return fmt.Errorf("task authority duration must be between 1 and 86400 seconds")
	}
	if assignment.MaxAccessLevel != acl.AccessRead && assignment.MaxAccessLevel != acl.AccessReadWrite {
		return fmt.Errorf("task authority permits only read or read/write access")
	}
	return nil
}
