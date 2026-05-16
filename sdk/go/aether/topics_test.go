package aether

import (
	"testing"
)

func TestTopicPrefixConstants(t *testing.T) {
	// Verify topic prefix constants are set correctly
	tests := []struct {
		name     string
		prefix   string
		expected string
	}{
		{"agent prefix", TopicPrefixAgent, "ag"},
		{"unique task prefix", TopicPrefixUniqueTask, "tu"},
		{"task prefix", TopicPrefixTask, "ta"},
		{"task broadcast prefix", TopicPrefixTaskBroadcast, "tb"},
		{"user prefix", TopicPrefixUser, "us"},
		{"user workspace prefix", TopicPrefixUserWorkspace, "uw"},
		{"global agents prefix", TopicPrefixGlobalAgents, "ga"},
		{"global users prefix", TopicPrefixGlobalUsers, "gu"},
		{"event prefix", TopicPrefixEvent, "event"},
		{"metric prefix", TopicPrefixMetric, "metric"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.prefix != tt.expected {
				t.Errorf("prefix = %q, want %q", tt.prefix, tt.expected)
			}
		})
	}
}

func TestAgentTopic(t *testing.T) {
	tests := []struct {
		name           string
		workspace      string
		implementation string
		specifier      string
		want           string
	}{
		{
			name:           "simple agent topic",
			workspace:      "prod",
			implementation: "worker",
			specifier:      "inst-1",
			want:           "ag::prod::worker::inst-1",
		},
		{
			name:           "agent with dots in implementation",
			workspace:      "prod",
			implementation: "claude.code",
			specifier:      "inst-1",
			want:           "ag::prod::claude.code::inst-1",
		},
		{
			name:           "agent with dashes",
			workspace:      "staging-env",
			implementation: "data-processor",
			specifier:      "instance-42",
			want:           "ag::staging-env::data-processor::instance-42",
		},
		{
			name:           "agent with underscores",
			workspace:      "dev_test",
			implementation: "batch_worker",
			specifier:      "worker_1",
			want:           "ag::dev_test::batch_worker::worker_1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AgentTopic(tt.workspace, tt.implementation, tt.specifier)
			if got != tt.want {
				t.Errorf("AgentTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGlobalAgentsTopic(t *testing.T) {
	tests := []struct {
		name      string
		workspace string
		want      string
	}{
		{
			name:      "simple workspace",
			workspace: "prod",
			want:      "ga::prod",
		},
		{
			name:      "workspace with dash",
			workspace: "staging-env",
			want:      "ga::staging-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GlobalAgentsTopic(tt.workspace)
			if got != tt.want {
				t.Errorf("GlobalAgentsTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUniqueTaskTopic(t *testing.T) {
	tests := []struct {
		name           string
		workspace      string
		implementation string
		specifier      string
		want           string
	}{
		{
			name:           "simple unique task",
			workspace:      "prod",
			implementation: "report-generator",
			specifier:      "daily-report",
			want:           "tu::prod::report-generator::daily-report",
		},
		{
			name:           "unique task with dots",
			workspace:      "staging",
			implementation: "my.complex.impl",
			specifier:      "task-id",
			want:           "tu::staging::my.complex.impl::task-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UniqueTaskTopic(tt.workspace, tt.implementation, tt.specifier)
			if got != tt.want {
				t.Errorf("UniqueTaskTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTaskTopic(t *testing.T) {
	tests := []struct {
		name           string
		workspace      string
		implementation string
		id             string
		want           string
	}{
		{
			name:           "simple non-unique task",
			workspace:      "prod",
			implementation: "data-processor",
			id:             "abc123",
			want:           "ta::prod::data-processor::abc123",
		},
		{
			name:           "non-unique task with uuid",
			workspace:      "dev",
			implementation: "batch-job",
			id:             "uuid-123-456",
			want:           "ta::dev::batch-job::uuid-123-456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TaskTopic(tt.workspace, tt.implementation, tt.id)
			if got != tt.want {
				t.Errorf("TaskTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTaskBroadcastTopic(t *testing.T) {
	tests := []struct {
		name           string
		workspace      string
		implementation string
		want           string
	}{
		{
			name:           "simple broadcast topic",
			workspace:      "prod",
			implementation: "data-processor",
			want:           "tb::prod::data-processor",
		},
		{
			name:           "broadcast with dashes",
			workspace:      "staging-env",
			implementation: "batch-worker",
			want:           "tb::staging-env::batch-worker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TaskBroadcastTopic(tt.workspace, tt.implementation)
			if got != tt.want {
				t.Errorf("TaskBroadcastTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserTopic(t *testing.T) {
	tests := []struct {
		name     string
		userID   string
		windowID string
		want     string
	}{
		{
			name:     "simple user topic",
			userID:   "alice",
			windowID: "tab-1",
			want:     "us::alice::tab-1",
		},
		{
			name:     "user with uuid",
			userID:   "user-456",
			windowID: "window-1",
			want:     "us::user-456::window-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UserTopic(tt.userID, tt.windowID)
			if got != tt.want {
				t.Errorf("UserTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserWorkspaceTopic(t *testing.T) {
	tests := []struct {
		name      string
		userID    string
		workspace string
		want      string
	}{
		{
			name:      "simple user workspace topic",
			userID:    "alice",
			workspace: "prod",
			want:      "uw::alice::prod",
		},
		{
			name:      "user workspace with dashes",
			userID:    "user-123",
			workspace: "staging-env",
			want:      "uw::user-123::staging-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UserWorkspaceTopic(tt.userID, tt.workspace)
			if got != tt.want {
				t.Errorf("UserWorkspaceTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGlobalUsersTopic(t *testing.T) {
	tests := []struct {
		name      string
		workspace string
		want      string
	}{
		{
			name:      "simple workspace",
			workspace: "prod",
			want:      "gu::prod",
		},
		{
			name:      "workspace with dash",
			workspace: "staging-env",
			want:      "gu::staging-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GlobalUsersTopic(tt.workspace)
			if got != tt.want {
				t.Errorf("GlobalUsersTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventTopic(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		want      string
	}{
		{
			name:      "simple event type",
			eventType: "task.completed",
			want:      "event.task.completed",
		},
		{
			name:      "workflow event",
			eventType: "workflow.started",
			want:      "event.workflow.started",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EventTopic(tt.eventType)
			if got != tt.want {
				t.Errorf("EventTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventWildcardTopic(t *testing.T) {
	want := "event.*"
	got := EventWildcardTopic()
	if got != want {
		t.Errorf("EventWildcardTopic() = %v, want %v", got, want)
	}
}

func TestMetricTopic(t *testing.T) {
	tests := []struct {
		name       string
		metricType string
		want       string
	}{
		{
			name:       "performance metric",
			metricType: "performance",
			want:       "metric.performance",
		},
		{
			name:       "latency metric",
			metricType: "latency",
			want:       "metric.latency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MetricTopic(tt.metricType)
			if got != tt.want {
				t.Errorf("MetricTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetricWildcardTopic(t *testing.T) {
	want := "metric.*"
	got := MetricWildcardTopic()
	if got != want {
		t.Errorf("MetricWildcardTopic() = %v, want %v", got, want)
	}
}

func TestCreateTopicAgent(t *testing.T) {
	// CreateTopicAgent is an alias for AgentTopic
	got := CreateTopicAgent("prod", "worker", "inst-1")
	want := AgentTopic("prod", "worker", "inst-1")
	if got != want {
		t.Errorf("CreateTopicAgent() = %v, want %v", got, want)
	}
}

func TestCreateTopicTask(t *testing.T) {
	tests := []struct {
		name           string
		workspace      string
		implementation string
		specifier      string
		want           string
	}{
		{
			name:           "unique task with specifier",
			workspace:      "prod",
			implementation: "processor",
			specifier:      "unique-1",
			want:           "tu::prod::processor::unique-1",
		},
		{
			name:           "non-unique task without specifier",
			workspace:      "prod",
			implementation: "processor",
			specifier:      "",
			want:           "ta::prod::processor::",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CreateTopicTask(tt.workspace, tt.implementation, tt.specifier)
			if got != tt.want {
				t.Errorf("CreateTopicTask() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateTopicTaskBroadcast(t *testing.T) {
	// CreateTopicTaskBroadcast is an alias for TaskBroadcastTopic
	got := CreateTopicTaskBroadcast("prod", "worker")
	want := TaskBroadcastTopic("prod", "worker")
	if got != want {
		t.Errorf("CreateTopicTaskBroadcast() = %v, want %v", got, want)
	}
}

func TestCreateTopicUser(t *testing.T) {
	// CreateTopicUser is an alias for UserTopic
	got := CreateTopicUser("alice", "tab-1")
	want := UserTopic("alice", "tab-1")
	if got != want {
		t.Errorf("CreateTopicUser() = %v, want %v", got, want)
	}
}

func TestCreateTopicUserWorkspace(t *testing.T) {
	// CreateTopicUserWorkspace is an alias for UserWorkspaceTopic
	got := CreateTopicUserWorkspace("alice", "prod")
	want := UserWorkspaceTopic("alice", "prod")
	if got != want {
		t.Errorf("CreateTopicUserWorkspace() = %v, want %v", got, want)
	}
}

func TestCreateTopicGlobalAgents(t *testing.T) {
	// CreateTopicGlobalAgents is an alias for GlobalAgentsTopic
	got := CreateTopicGlobalAgents("prod")
	want := GlobalAgentsTopic("prod")
	if got != want {
		t.Errorf("CreateTopicGlobalAgents() = %v, want %v", got, want)
	}
}

func TestCreateTopicGlobalUsers(t *testing.T) {
	// CreateTopicGlobalUsers is an alias for GlobalUsersTopic
	got := CreateTopicGlobalUsers("prod")
	want := GlobalUsersTopic("prod")
	if got != want {
		t.Errorf("CreateTopicGlobalUsers() = %v, want %v", got, want)
	}
}

func TestTopicConsistency(t *testing.T) {
	// Verify that alias functions produce the same results as primary functions
	t.Run("agent consistency", func(t *testing.T) {
		primary := AgentTopic("ws", "impl", "spec")
		alias := CreateTopicAgent("ws", "impl", "spec")
		if primary != alias {
			t.Errorf("AgentTopic = %q, CreateTopicAgent = %q, want same", primary, alias)
		}
	})

	t.Run("task broadcast consistency", func(t *testing.T) {
		primary := TaskBroadcastTopic("ws", "impl")
		alias := CreateTopicTaskBroadcast("ws", "impl")
		if primary != alias {
			t.Errorf("TaskBroadcastTopic = %q, CreateTopicTaskBroadcast = %q, want same", primary, alias)
		}
	})

	t.Run("user consistency", func(t *testing.T) {
		primary := UserTopic("user", "window")
		alias := CreateTopicUser("user", "window")
		if primary != alias {
			t.Errorf("UserTopic = %q, CreateTopicUser = %q, want same", primary, alias)
		}
	})

	t.Run("user workspace consistency", func(t *testing.T) {
		primary := UserWorkspaceTopic("user", "ws")
		alias := CreateTopicUserWorkspace("user", "ws")
		if primary != alias {
			t.Errorf("UserWorkspaceTopic = %q, CreateTopicUserWorkspace = %q, want same", primary, alias)
		}
	})

	t.Run("global agents consistency", func(t *testing.T) {
		primary := GlobalAgentsTopic("ws")
		alias := CreateTopicGlobalAgents("ws")
		if primary != alias {
			t.Errorf("GlobalAgentsTopic = %q, CreateTopicGlobalAgents = %q, want same", primary, alias)
		}
	})

	t.Run("global users consistency", func(t *testing.T) {
		primary := GlobalUsersTopic("ws")
		alias := CreateTopicGlobalUsers("ws")
		if primary != alias {
			t.Errorf("GlobalUsersTopic = %q, CreateTopicGlobalUsers = %q, want same", primary, alias)
		}
	})
}
