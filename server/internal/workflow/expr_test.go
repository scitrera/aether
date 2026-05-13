package workflow

import (
	"testing"
)

func TestExprEngine_Evaluate(t *testing.T) {
	eng := NewExprEngine(100)

	tests := []struct {
		name    string
		expr    string
		env     map[string]any
		want    bool
		wantErr bool
	}{
		{
			name: "empty expression returns true",
			expr: "",
			env:  nil,
			want: true,
		},
		{
			name: "simple true",
			expr: "true",
			env:  nil,
			want: true,
		},
		{
			name: "simple false",
			expr: "false",
			env:  nil,
			want: false,
		},
		{
			name: "variable comparison",
			expr: `input.status == "completed"`,
			env: map[string]any{
				"input": map[string]any{"status": "completed"},
			},
			want: true,
		},
		{
			name: "variable comparison false",
			expr: `input.status == "completed"`,
			env: map[string]any{
				"input": map[string]any{"status": "running"},
			},
			want: false,
		},
		{
			name: "numeric comparison",
			expr: "input.count > 5",
			env: map[string]any{
				"input": map[string]any{"count": 10},
			},
			want: true,
		},
		{
			name: "compound condition",
			expr: `input.status == "done" && input.count > 0`,
			env: map[string]any{
				"input": map[string]any{"status": "done", "count": 3},
			},
			want: true,
		},
		{
			name: "compound condition partial false",
			expr: `input.status == "done" && input.count > 0`,
			env: map[string]any{
				"input": map[string]any{"status": "done", "count": 0},
			},
			want: false,
		},
		{
			name:    "non-bool result",
			expr:    `"hello"`,
			env:     nil,
			wantErr: true,
		},
		{
			name:    "syntax error",
			expr:    `input.status ==`,
			env:     nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eng.Evaluate(tt.expr, tt.env)
			if (err != nil) != tt.wantErr {
				t.Errorf("Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExprEngine_Caching(t *testing.T) {
	eng := NewExprEngine(100)
	env := map[string]any{"x": 1}

	// First call compiles
	got, err := eng.Evaluate("x == 1", env)
	if err != nil || !got {
		t.Fatalf("first call: got=%v err=%v", got, err)
	}

	// Second call uses cache
	got, err = eng.Evaluate("x == 1", env)
	if err != nil || !got {
		t.Fatalf("cached call: got=%v err=%v", got, err)
	}

	if len(eng.cache) != 1 {
		t.Errorf("cache size = %d, want 1", len(eng.cache))
	}
}

func TestExprEngine_CacheEviction(t *testing.T) {
	eng := NewExprEngine(4) // small cache

	env := map[string]any{"x": true}
	for i := 0; i < 10; i++ {
		expr := "x == true"
		if i > 0 {
			// Create distinct expressions
			expr = "x == true" + string(rune('a'+i))
			// These will fail but that's fine - we just want to fill cache
		}
		eng.Evaluate(expr, env)
	}

	// Cache should not exceed max
	if len(eng.cache) > 4 {
		t.Errorf("cache size = %d, exceeds max 4", len(eng.cache))
	}
}
