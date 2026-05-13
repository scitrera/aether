package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"text/template"

	"gopkg.in/yaml.v3"
)

// TemplateEngine handles Go text/template rendering and YAML parsing.
type TemplateEngine struct {
	mu    sync.RWMutex
	cache map[string]*template.Template
	max   int
}

func NewTemplateEngine(maxCacheSize int) *TemplateEngine {
	if maxCacheSize <= 0 {
		maxCacheSize = 2048
	}
	return &TemplateEngine{
		cache: make(map[string]*template.Template, maxCacheSize),
		max:   maxCacheSize,
	}
}

// TransformResult is the parsed output of a template transformation.
type TransformResult struct {
	Agent     string            `yaml:"agent" json:"agent"`
	ToolName  string            `yaml:"tool_name" json:"tool_name"`
	Arguments map[string]any    `yaml:"arguments" json:"arguments"`
	Workspace string            `yaml:"workspace" json:"workspace"`
	Metadata  map[string]string `yaml:"metadata" json:"metadata"`
}

// Transform renders a Go text/template with the given data, then parses
// the result as YAML into a TransformResult.
func (e *TemplateEngine) Transform(templateStr string, data map[string]any) (*TransformResult, error) {
	tmpl, err := e.getOrParse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("template parse error: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("template execute error: %w", err)
	}

	var result TransformResult
	if err := yaml.Unmarshal(buf.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("template output YAML parse error: %w", err)
	}

	return &result, nil
}

// TransformRaw renders a template and returns the raw YAML-parsed map.
func (e *TemplateEngine) TransformRaw(templateStr string, data map[string]any) (map[string]any, error) {
	tmpl, err := e.getOrParse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("template parse error: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("template execute error: %w", err)
	}

	var result map[string]any
	if err := yaml.Unmarshal(buf.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("template output YAML parse error: %w", err)
	}

	return result, nil
}

// TransformJSON renders a template and returns the result as JSON bytes.
func (e *TemplateEngine) TransformJSON(templateStr string, data map[string]any) ([]byte, error) {
	result, err := e.TransformRaw(templateStr, data)
	if err != nil {
		return nil, err
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("JSON marshal error: %w", err)
	}
	return jsonBytes, nil
}

func (e *TemplateEngine) getOrParse(templateStr string) (*template.Template, error) {
	e.mu.RLock()
	if t, ok := e.cache[templateStr]; ok {
		e.mu.RUnlock()
		return t, nil
	}
	e.mu.RUnlock()

	tmpl, err := template.New("").Option("missingkey=zero").Parse(templateStr)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	if len(e.cache) >= e.max {
		count := 0
		for k := range e.cache {
			delete(e.cache, k)
			count++
			if count >= e.max/4 {
				break
			}
		}
	}
	e.cache[templateStr] = tmpl
	e.mu.Unlock()

	return tmpl, nil
}
