package gateway

import (
	"encoding/json"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/pkg/tasks"
)

func TestTaskToProtoRetainsReportedProgress(t *testing.T) {
	now := time.Unix(12345, 0)
	task := &tasks.Task{TaskID: "task", Status: tasks.TaskStatusRunning, LastHeartbeat: &now,
		Metadata: map[string]interface{}{"title": "Document review", "completion": "0.1"},
		HeartbeatDetails: map[string]interface{}{"completion": 0.65, "summary": "Source review",
			"source": "sv::worker::one", "step_name": "Review 12/20 claims", "step_sequence": float64(12), "step_total": float64(20)}}
	got := taskToProto(task)
	if got.Status != pb.TaskStatus_TASK_STATUS_RUNNING || got.Metadata["completion"] != "0.65" || got.Metadata["updated_at"] != "12345" {
		t.Fatalf("bad snapshot: %v", got)
	}
	if got.Metadata["title"] != "Document review" || task.Metadata["completion"] != "0.1" {
		t.Fatal("metadata lost or mutated")
	}
	var step map[string]interface{}
	if err := json.Unmarshal([]byte(got.Metadata["step"]), &step); err != nil {
		t.Fatal(err)
	}
	if step["name"] != "Review 12/20 claims" {
		t.Fatalf("lost step: %v", step)
	}
	task.Status = tasks.TaskStatusCompleted
	if taskToProto(task).Status != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatal("progress replaced lifecycle status")
	}
}

func TestTaskProgressSnapshotPreservesStepDetail(t *testing.T) {
	task := &tasks.Task{HeartbeatDetails: map[string]interface{}{"step": map[string]interface{}{"name": "Review", "detail": "Source check", "type": "stage"}}}
	var step map[string]interface{}
	if err := json.Unmarshal([]byte(taskToProto(task).Metadata["step"]), &step); err != nil {
		t.Fatal(err)
	}
	if step["detail"] != "Source check" || step["type"] != "stage" {
		t.Fatal(step)
	}
	if taskToProto(&tasks.Task{}).Metadata != nil {
		t.Fatal("empty task got invented progress")
	}
}
