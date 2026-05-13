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
