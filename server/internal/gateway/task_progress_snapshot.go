package gateway

import (
	"encoding/json"
	"math"
	"strconv"

	"github.com/scitrera/aether/server/pkg/tasks"
)

// taskProgressMetadata exposes the existing persisted heartbeat progress through
// the metadata keys consumed by task-list clients. It does not alter lifecycle
// status, authority, or the stored task metadata.
func taskProgressMetadata(task *tasks.Task, metadata map[string]string) map[string]string {
	details := task.HeartbeatDetails
	if len(details) == 0 {
		return metadata
	}
	if metadata == nil {
		metadata = map[string]string{}
	}
	if completion, ok := details["completion"].(float64); ok && !math.IsNaN(completion) && !math.IsInf(completion, 0) {
		metadata["completion"] = strconv.FormatFloat(completion, 'f', -1, 64)
	}
	if source, ok := details["source"].(string); ok {
		metadata["agent"] = source
	}
	if summary, ok := details["summary"].(string); ok {
		metadata["summary"] = summary
	}
	step := details["step"]
	if step == nil {
		name, _ := details["step_name"].(string)
		if name == "" {
			name, _ = details["summary"].(string)
		}
		if name != "" {
			step = map[string]interface{}{"name": name, "sequence": details["step_sequence"], "total_steps": details["step_total"]}
		}
	}
	if step != nil {
		if raw, err := json.Marshal(step); err == nil {
			metadata["step"] = string(raw)
		}
	}
	if task.LastHeartbeat != nil {
		metadata["updated_at"] = strconv.FormatInt(task.LastHeartbeat.Unix(), 10)
	}
	return metadata
}
