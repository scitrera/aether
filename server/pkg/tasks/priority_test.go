package tasks

import (
	"strings"
	"testing"
)

func TestTaskPriority_Normalize(t *testing.T) {
	cases := []struct {
		name string
		in   TaskPriority
		want TaskPriority
	}{
		{"unspecified maps to normal", PriorityUnspecified, PriorityNormal},
		{"xlow passes through", PriorityXLow, PriorityXLow},
		{"low passes through", PriorityLow, PriorityLow},
		{"normal passes through", PriorityNormal, PriorityNormal},
		{"high passes through", PriorityHigh, PriorityHigh},
		{"preempt passes through", PriorityPreempt, PriorityPreempt},
		{"unknown spaced value passes through", TaskPriority(35), TaskPriority(35)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.Normalize(); got != tc.want {
				t.Errorf("Normalize(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestTaskPriority_Ordering(t *testing.T) {
	// The numeric weights must be strictly increasing so they can be used
	// directly as a descending dispatch sort key.
	ordered := []TaskPriority{PriorityXLow, PriorityLow, PriorityNormal, PriorityHigh, PriorityPreempt}
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1] >= ordered[i] {
			t.Errorf("priority weights not strictly increasing: %d (%s) !< %d (%s)",
				ordered[i-1], ordered[i-1], ordered[i], ordered[i])
		}
	}
}

func TestTaskPriority_String(t *testing.T) {
	cases := map[TaskPriority]string{
		PriorityUnspecified: "unspecified",
		PriorityXLow:        "xlow",
		PriorityLow:         "low",
		PriorityNormal:      "normal",
		PriorityHigh:        "high",
		PriorityPreempt:     "preempt",
		TaskPriority(35):    "priority(35)",
	}
	for in, want := range cases {
		if got := in.String(); got != want {
			t.Errorf("TaskPriority(%d).String() = %q, want %q", int(in), got, want)
		}
	}
}

func TestBuildTaskFilterClauses_Priority(t *testing.T) {
	t.Run("exact priority", func(t *testing.T) {
		f := &TaskFilter{Priority: int32(PriorityHigh)}
		clause, _, args := buildTaskFilterClauses(f, 1, nil, false)
		if !strings.Contains(clause, "priority = $1") {
			t.Errorf("expected exact priority clause, got %q", clause)
		}
		if len(args) != 1 || args[0] != int32(PriorityHigh) {
			t.Errorf("expected args [40], got %v", args)
		}
	})

	t.Run("min priority threshold", func(t *testing.T) {
		f := &TaskFilter{MinPriority: int32(PriorityHigh)}
		clause, _, args := buildTaskFilterClauses(f, 1, nil, false)
		if !strings.Contains(clause, "priority >= $1") {
			t.Errorf("expected min priority clause, got %q", clause)
		}
		if len(args) != 1 || args[0] != int32(PriorityHigh) {
			t.Errorf("expected args [40], got %v", args)
		}
	})

	t.Run("both exact and min", func(t *testing.T) {
		f := &TaskFilter{Priority: int32(PriorityNormal), MinPriority: int32(PriorityLow)}
		clause, _, args := buildTaskFilterClauses(f, 1, nil, false)
		if !strings.Contains(clause, "priority = $1") || !strings.Contains(clause, "priority >= $2") {
			t.Errorf("expected both clauses, got %q", clause)
		}
		if len(args) != 2 {
			t.Errorf("expected 2 args, got %v", args)
		}
	})

	t.Run("zero priority means no filter", func(t *testing.T) {
		f := &TaskFilter{}
		clause, _, args := buildTaskFilterClauses(f, 1, nil, false)
		if strings.Contains(clause, "priority") {
			t.Errorf("expected no priority clause for zero filter, got %q", clause)
		}
		if len(args) != 0 {
			t.Errorf("expected no args, got %v", args)
		}
	})
}
