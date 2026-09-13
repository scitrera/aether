// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/models"
	"github.com/scitrera/aether/server/pkg/tasks"
)

// resolveTaskInput routes a subject's opaque response to its running task's
// current assignee. The task workspace remains the ACL target; the internal
// worker workspace is never granted to the user. Payload bytes are untouched.
// This route cannot launch a worker or carry delegated/checked authority.
func (s *GatewayServer) resolveTaskInput(ctx context.Context, sender models.Identity, msg *pb.SendMessage) (string, bool, error) {
	parts := strings.Split(msg.TargetTopic, models.IdentitySep)
	if len(parts) < 4 || parts[0] != "tk" || parts[3] != "input" {
		return "", false, nil
	}
	denied := func() (string, bool, error) {
		return "", true, fmt.Errorf("task input is unavailable or not authorized")
	}
	if len(parts) != 4 || parts[1] == "" || parts[2] == "" ||
		strings.ContainsAny(msg.TargetTopic, "*?") ||
		sender.Type != models.PrincipalUser || sender.ID == "" ||
		sender.Workspace != parts[1] ||
		msg.MessageType != pb.MessageType_OPAQUE ||
		msg.Authorization != nil || msg.CheckedAccess != nil || msg.AuthorityContinuation != nil ||
		(msg.AppWorkspace != "" && msg.AppWorkspace != parts[1]) || s.taskStore == nil {
		return denied()
	}
	task, err := s.taskStore.GetTask(ctx, parts[2])
	if err != nil || task == nil || task.TaskID != parts[2] || task.Workspace != parts[1] ||
		task.Status != tasks.TaskStatusRunning ||
		task.Authority.SubjectType != string(models.PrincipalUser) ||
		task.Authority.SubjectID != sender.ID ||
		!taskMetadataTruthy(task.Metadata["subject_participant"]) {
		return denied()
	}
	// A client-bound turn names its exact initiating user session. Preserve
	// that restriction even when another window belongs to the same subject.
	if host := metadataString(task.Metadata, "turn_tool_host_id"); host != "" && host != sender.ToTopic() {
		return denied()
	}
	target := strings.Split(task.AssignedTo, models.IdentitySep)
	if len(target) != 4 || target[0] != "ag" || target[1] == "" || target[2] == "" || target[3] == "" ||
		strings.ContainsAny(task.AssignedTo, "*?") {
		return denied()
	}
	return task.AssignedTo, true, nil
}
