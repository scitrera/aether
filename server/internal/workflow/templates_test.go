package workflow

import (
	"testing"
)

func TestTemplateEngine_Transform(t *testing.T) {
	eng := NewTemplateEngine(100)

	tests := []struct {
		name    string
		tmpl    string
		data    map[string]any
		wantErr bool
		checkFn func(*TransformResult) error
	}{
		{
			name: "basic agent and tool",
			tmpl: `agent: "TestAgent"
tool_name: process
arguments:
  key: value`,
			data: map[string]any{},
			checkFn: func(r *TransformResult) error {
				if r.Agent != "TestAgent" {
					t.Errorf("Agent = %q, want %q", r.Agent, "TestAgent")
				}
				if r.ToolName != "process" {
					t.Errorf("ToolName = %q, want %q", r.ToolName, "process")
				}
				if r.Arguments["key"] != "value" {
					t.Errorf("Arguments[key] = %v, want %q", r.Arguments["key"], "value")
				}
				return nil
			},
		},
		{
			name: "template interpolation",
			tmpl: `agent: "{{ .input.target }}"
tool_name: run
arguments:
  data: "{{ .input.payload }}"`,
			data: map[string]any{
				"input": map[string]any{
					"target":  "MyAgent",
					"payload": "hello",
				},
			},
			checkFn: func(r *TransformResult) error {
				if r.Agent != "MyAgent" {
					t.Errorf("Agent = %q, want %q", r.Agent, "MyAgent")
				}
				if r.Arguments["data"] != "hello" {
					t.Errorf("Arguments[data] = %v, want %q", r.Arguments["data"], "hello")
				}
				return nil
			},
		},
		{
			name: "workspace from source",
			tmpl: `agent: "Worker"
tool_name: do
workspace: "{{ .source.workspace }}"`,
			data: map[string]any{
				"source": map[string]any{"workspace": "production"},
			},
			checkFn: func(r *TransformResult) error {
				if r.Workspace != "production" {
					t.Errorf("Workspace = %q, want %q", r.Workspace, "production")
				}
				return nil
			},
		},
		{
			name:    "invalid template syntax",
			tmpl:    `agent: "{{ .bad`,
			data:    map[string]any{},
			wantErr: true,
		},
		{
			name:    "invalid YAML output",
			tmpl:    `[invalid yaml`,
			data:    map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eng.Transform(tt.tmpl, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Transform() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.checkFn != nil {
				tt.checkFn(result)
			}
		})
	}
}

// TestTemplateEngine_Transform_CreateTask validates that a create_task
// destination template (as registered by the platform's kb-update-on-ingest
// rule) parses into a TransformResult carrying the create_task fields, so the
// router can dispatch an Aether POOL task in response to an event.
func TestTemplateEngine_Transform_CreateTask(t *testing.T) {
	eng := NewTemplateEngine(100)

	tmpl := "type: create_task\n" +
		"task_type: memorylayer-task.kb_update\n" +
		"target_implementation: memorylayer-worker\n" +
		"workspace: \"{{ .source.workspace }}\"\n" +
		"payload:\n" +
		"  workspace_id: \"{{ .source.workspace }}\"\n" +
		"metadata:\n" +
		"  bg_kind: kb\n" +
		"  visibility: workspace\n" +
		"  title: \"Updating knowledge base\"\n"

	r, err := eng.Transform(tmpl, map[string]any{
		"source": map[string]any{"workspace": "ws-default"},
		"input":  map[string]any{"workspace_id": "ws-default"},
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if r.Type != "create_task" {
		t.Errorf("Type = %q, want %q", r.Type, "create_task")
	}
	if r.TaskType != "memorylayer-task.kb_update" {
		t.Errorf("TaskType = %q, want %q", r.TaskType, "memorylayer-task.kb_update")
	}
	if r.TargetImplementation != "memorylayer-worker" {
		t.Errorf("TargetImplementation = %q, want %q", r.TargetImplementation, "memorylayer-worker")
	}
	if r.Workspace != "ws-default" {
		t.Errorf("Workspace = %q, want %q", r.Workspace, "ws-default")
	}
	if r.Metadata["bg_kind"] != "kb" {
		t.Errorf("Metadata[bg_kind] = %q, want %q", r.Metadata["bg_kind"], "kb")
	}
	payload, ok := r.Payload.(map[string]any)
	if !ok {
		t.Fatalf("Payload type = %T, want map[string]any", r.Payload)
	}
	if payload["workspace_id"] != "ws-default" {
		t.Errorf("Payload[workspace_id] = %v, want %q", payload["workspace_id"], "ws-default")
	}
}

func TestTemplateEngine_Caching(t *testing.T) {
	eng := NewTemplateEngine(100)

	tmpl := `agent: "A"
tool_name: test`
	data := map[string]any{}

	_, err := eng.Transform(tmpl, data)
	if err != nil {
		t.Fatal(err)
	}

	// Second call uses cached template
	_, err = eng.Transform(tmpl, data)
	if err != nil {
		t.Fatal(err)
	}

	if len(eng.cache) != 1 {
		t.Errorf("cache size = %d, want 1", len(eng.cache))
	}
}

func TestTemplateEngine_TransformJSON(t *testing.T) {
	eng := NewTemplateEngine(100)

	tmpl := `agent: "Worker"
tool_name: run`

	jsonBytes, err := eng.TransformJSON(tmpl, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	if len(jsonBytes) == 0 {
		t.Error("TransformJSON returned empty bytes")
	}
}
