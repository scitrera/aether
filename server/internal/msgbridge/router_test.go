package msgbridge

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseWorkflowConfig_Enabled(t *testing.T) {
	metadata := json.RawMessage(`{"workflow_enabled": true, "workflow_events": ["message.received", "ops.command"]}`)
	cfg := parseWorkflowConfig(metadata)
	require.NotNil(t, cfg)
	assert.True(t, cfg.WorkflowEnabled)
	assert.Equal(t, []string{"message.received", "ops.command"}, cfg.WorkflowEvents)
}

func TestParseWorkflowConfig_Disabled(t *testing.T) {
	metadata := json.RawMessage(`{"workflow_enabled": false}`)
	cfg := parseWorkflowConfig(metadata)
	assert.Nil(t, cfg)
}

func TestParseWorkflowConfig_NotPresent(t *testing.T) {
	metadata := json.RawMessage(`{"some_other_field": "value"}`)
	cfg := parseWorkflowConfig(metadata)
	assert.Nil(t, cfg, "workflow_enabled defaults to false, so config should be nil")
}

func TestParseWorkflowConfig_EmptyMetadata(t *testing.T) {
	cfg := parseWorkflowConfig(nil)
	assert.Nil(t, cfg)

	cfg = parseWorkflowConfig(json.RawMessage(`{}`))
	assert.Nil(t, cfg)
}

func TestParseWorkflowConfig_InvalidJSON(t *testing.T) {
	cfg := parseWorkflowConfig(json.RawMessage(`not json`))
	assert.Nil(t, cfg)
}

func TestParseWorkflowConfig_EnabledNoEvents(t *testing.T) {
	metadata := json.RawMessage(`{"workflow_enabled": true}`)
	cfg := parseWorkflowConfig(metadata)
	require.NotNil(t, cfg)
	assert.True(t, cfg.WorkflowEnabled)
	assert.Empty(t, cfg.WorkflowEvents, "empty events list means defaults will be used at publish time")
}

func TestSplitTwo(t *testing.T) {
	impl, spec, ok := splitTwo("data-processor.instance-1")
	assert.True(t, ok)
	assert.Equal(t, "data-processor", impl)
	assert.Equal(t, "instance-1", spec)

	impl, spec, ok = splitTwo("no-dot")
	assert.False(t, ok)
	assert.Equal(t, "no-dot", impl)
	assert.Equal(t, "", spec)
}
