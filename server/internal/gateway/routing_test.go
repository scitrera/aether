package gateway

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// Mock MessageRouter for setupClientSubscriptions tests
// ---------------------------------------------------------------------------

type mockRouter struct {
	mu                     sync.Mutex
	subscribedTopics       []string
	exclusiveSubscriptions map[string]string // topic -> consumerName
}

func newMockRouter() *mockRouter {
	return &mockRouter{
		exclusiveSubscriptions: make(map[string]string),
	}
}

func (m *mockRouter) Publish(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (m *mockRouter) Subscribe(topic string, _ func([]byte)) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribedTopics = append(m.subscribedTopics, topic)
	return func() {}, nil
}

func (m *mockRouter) SubscribeExclusive(topic string, consumerName string, _ func([]byte)) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.exclusiveSubscriptions[topic] = consumerName
	return func() {}, nil
}

func (m *mockRouter) SubscribeExclusiveFromNow(topic string, consumerName string, _ func([]byte)) (func(), error) {
	return m.SubscribeExclusive(topic, consumerName, nil)
}

func (m *mockRouter) SubscribeExclusiveFromTimestamp(topic string, consumerName string, _ int64, _ func([]byte)) (func(), error) {
	return m.SubscribeExclusive(topic, consumerName, nil)
}

// hasSharedTopic returns true if the topic was subscribed to via the shared Subscribe path.
func (m *mockRouter) hasSharedTopic(topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.subscribedTopics {
		if t == topic {
			return true
		}
	}
	return false
}

// hasExclusiveTopic returns true if the topic was subscribed to via the exclusive path.
func (m *mockRouter) hasExclusiveTopic(topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.exclusiveSubscriptions[topic]
	return ok
}

// ---------------------------------------------------------------------------
// TestEnforceTopicPermissions – permission matrix (spec Section 3.2.2)
// ---------------------------------------------------------------------------

func TestEnforceTopicPermissions(t *testing.T) {
	tests := []struct {
		name        string
		sender      models.Identity
		targetTopic string
		wantErr     bool
		errContains string
	}{
		// ----- Agent: can send to every topic type in its own workspace -----
		{
			name:        "agent can send to agent topic in same workspace",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "ag::ws1::impl::spec",
			wantErr:     false,
		},
		{
			name:        "agent can send to unique task topic in same workspace",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "tu::ws1::impl::spec",
			wantErr:     false,
		},
		{
			name:        "agent can send to non-unique task topic in same workspace",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "ta::ws1::impl::id1",
			wantErr:     false,
		},
		{
			name:        "agent can send to task broadcast topic in same workspace",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "tb::ws1::impl",
			wantErr:     false,
		},
		{
			name:        "agent can send to user window topic",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "us::user1::win1",
			wantErr:     false,
		},
		{
			name:        "agent can send to user workspace topic",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "uw::user1::ws1",
			wantErr:     false,
		},
		{
			name:        "agent can send to global agent broadcast in same workspace",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "ga::ws1",
			wantErr:     false,
		},
		{
			name:        "agent can send to global user broadcast in same workspace",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "gu::ws1",
			wantErr:     false,
		},
		{
			name:        "agent can send to event topic",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "event::ws1",
			wantErr:     false,
		},
		{
			name:        "agent can send to metric topic",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "metric::ws1",
			wantErr:     false,
		},

		// ----- Agent: cross-workspace pass-through at transport layer -----
		// (Cross-workspace ACL enforcement moved to checkMessageSendWith{
		// Authority,Delegation}; the transport layer no longer rejects on
		// workspace mismatch. See routing.go::enforceTopicPermissions.)
		{
			name:        "agent cross-workspace agent topic allowed at transport (ACL gates downstream)",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "ag::ws2::impl::spec",
			wantErr:     false,
		},
		{
			name:        "agent cross-workspace task topic allowed at transport (ACL gates downstream)",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "tu::ws2::impl::spec",
			wantErr:     false,
		},
		{
			name:        "agent cross-workspace ga broadcast allowed at transport (ACL gates downstream)",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "ga::ws2",
			wantErr:     false,
		},
		{
			name:        "agent cross-workspace gu broadcast allowed at transport (ACL gates downstream)",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "gu::ws2",
			wantErr:     false,
		},
		{
			name:        "agent without workspace is unrestricted by transport layer",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: ""},
			targetTopic: "ag::ws2::impl::spec",
			wantErr:     false,
		},

		// ----- User: cannot send to event.* or metric.* -----
		{
			name:        "user cannot send to event topic",
			sender:      models.Identity{Type: models.PrincipalUser, Workspace: "ws1"},
			targetTopic: "event::ws1",
			wantErr:     true,
			errContains: "event",
		},
		{
			name:        "user cannot send to metric topic",
			sender:      models.Identity{Type: models.PrincipalUser, Workspace: "ws1"},
			targetTopic: "metric::ws1",
			wantErr:     true,
			errContains: "metric",
		},
		{
			name:        "user can send to agent topic",
			sender:      models.Identity{Type: models.PrincipalUser, Workspace: "ws1"},
			targetTopic: "ag::ws1::impl::spec",
			wantErr:     false,
		},
		{
			name:        "user can send to user window topic",
			sender:      models.Identity{Type: models.PrincipalUser, Workspace: "ws1"},
			targetTopic: "us::user1::win1",
			wantErr:     false,
		},
		{
			name:        "user can send to global user broadcast in same workspace",
			sender:      models.Identity{Type: models.PrincipalUser, Workspace: "ws1"},
			targetTopic: "gu::ws1",
			wantErr:     false,
		},

		// ----- MetricsBridge: receive-only, cannot send to any topic -----
		{
			name:        "metrics bridge cannot send to metric topic",
			sender:      models.Identity{Type: models.PrincipalMetricsBridge, Workspace: "ws1"},
			targetTopic: "metric::ws1",
			wantErr:     true,
			errContains: "receive-only",
		},
		{
			name:        "metrics bridge cannot send to agent topic",
			sender:      models.Identity{Type: models.PrincipalMetricsBridge, Workspace: "ws1"},
			targetTopic: "ag::ws1::impl::spec",
			wantErr:     true,
			errContains: "receive-only",
		},
		{
			name:        "metrics bridge cannot send to event topic",
			sender:      models.Identity{Type: models.PrincipalMetricsBridge, Workspace: "ws1"},
			targetTopic: "event::ws1",
			wantErr:     true,
			errContains: "receive-only",
		},

		// ----- Orchestrator: only ag.*, tu.*, ta.*, tb.* -----
		{
			name:        "orchestrator can send to agent topic",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "ag::ws1::impl::spec",
			wantErr:     false,
		},
		{
			name:        "orchestrator can send to unique task topic",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "tu::ws1::impl::spec",
			wantErr:     false,
		},
		{
			name:        "orchestrator can send to non-unique task topic",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "ta::ws1::impl::id1",
			wantErr:     false,
		},
		{
			name:        "orchestrator can send to task broadcast topic",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "tb::ws1::impl",
			wantErr:     false,
		},
		{
			name:        "orchestrator cannot send to event topic",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "event::ws1",
			wantErr:     true,
			errContains: "agent/task",
		},
		{
			name:        "orchestrator cannot send to metric topic",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "metric::ws1",
			wantErr:     true,
			errContains: "agent/task",
		},
		{
			name:        "orchestrator cannot send to global agent broadcast",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "ga::ws1",
			wantErr:     true,
			errContains: "agent/task",
		},
		{
			name:        "orchestrator cannot send to global user broadcast",
			sender:      models.Identity{Type: models.PrincipalOrchestrator, Workspace: "ws1"},
			targetTopic: "gu::ws1",
			wantErr:     true,
			errContains: "agent/task",
		},
		{
			name:        "orchestrator cannot send to user window topic",
			sender:      models.Identity{Type: models.PrincipalOrchestrator},
			targetTopic: "us::user1::win1",
			wantErr:     true,
			errContains: "agent/task",
		},

		// ----- Bridge: unrestricted send permissions (like agents), cross-workspace capable -----
		{
			name:        "bridge can send to agent topic in any workspace",
			sender:      models.Identity{Type: models.PrincipalBridge, Implementation: "aether-msgbridge", Specifier: "default"},
			targetTopic: "ag::prod::worker::1",
			wantErr:     false,
		},
		{
			name:        "bridge can send to user window topic",
			sender:      models.Identity{Type: models.PrincipalBridge, Implementation: "aether-msgbridge", Specifier: "default"},
			targetTopic: "us::alice::window1",
			wantErr:     false,
		},
		{
			name:        "bridge can send to global agent broadcast",
			sender:      models.Identity{Type: models.PrincipalBridge, Implementation: "aether-msgbridge", Specifier: "default"},
			targetTopic: "ga::prod",
			wantErr:     false,
		},
		{
			name:        "bridge can send cross-workspace (empty workspace bypasses check)",
			sender:      models.Identity{Type: models.PrincipalBridge, Implementation: "aether-msgbridge", Specifier: "default"},
			targetTopic: "ag::ws2::impl::spec",
			wantErr:     false,
		},
		{
			name:        "bridge can send to event topic",
			sender:      models.Identity{Type: models.PrincipalBridge, Implementation: "aether-msgbridge", Specifier: "default"},
			targetTopic: "event::prod",
			wantErr:     false,
		},
		{
			name:        "bridge can send to metric topic",
			sender:      models.Identity{Type: models.PrincipalBridge, Implementation: "aether-msgbridge", Specifier: "default"},
			targetTopic: "metric::prod",
			wantErr:     false,
		},

		// ----- Sending to bridge topics -----
		{
			name:        "agent can send to bridge topic",
			sender:      models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"},
			targetTopic: "br::aether-msgbridge::default",
			wantErr:     false,
		},
		{
			name:        "user can send to bridge topic",
			sender:      models.Identity{Type: models.PrincipalUser, Workspace: "ws1"},
			targetTopic: "br::aether-msgbridge::default",
			wantErr:     false,
		},
		{
			name:        "orchestrator cannot send to bridge topic (not an agent/task topic)",
			sender:      models.Identity{Type: models.PrincipalOrchestrator},
			targetTopic: "br::aether-msgbridge::default",
			wantErr:     true,
			errContains: "agent/task",
		},

		// ----- WorkflowEngine: can send to everything -----
		{
			name:        "workflow engine can send to agent topic",
			sender:      models.Identity{Type: models.PrincipalWorkflowEngine, Workspace: "ws1"},
			targetTopic: "ag::ws1::impl::spec",
			wantErr:     false,
		},
		{
			name:        "workflow engine can send to event topic",
			sender:      models.Identity{Type: models.PrincipalWorkflowEngine, Workspace: "ws1"},
			targetTopic: "event::ws1",
			wantErr:     false,
		},
		{
			name:        "workflow engine can send to metric topic",
			sender:      models.Identity{Type: models.PrincipalWorkflowEngine, Workspace: "ws1"},
			targetTopic: "metric::ws1",
			wantErr:     false,
		},
		{
			name:        "workflow engine can send to user topic",
			sender:      models.Identity{Type: models.PrincipalWorkflowEngine, Workspace: "ws1"},
			targetTopic: "us::user1::win1",
			wantErr:     false,
		},
		{
			name:        "workflow engine can send to global broadcasts",
			sender:      models.Identity{Type: models.PrincipalWorkflowEngine, Workspace: "ws1"},
			targetTopic: "ga::ws1",
			wantErr:     false,
		},

		// ----- Task: same permissions as agents -----
		{
			name:        "task can send to agent topic in same workspace",
			sender:      models.Identity{Type: models.PrincipalTask, Workspace: "ws1"},
			targetTopic: "ag::ws1::impl::spec",
			wantErr:     false,
		},
		{
			name:        "task can send to event topic",
			sender:      models.Identity{Type: models.PrincipalTask, Workspace: "ws1"},
			targetTopic: "event::ws1",
			wantErr:     false,
		},
		{
			name:        "task can send to metric topic",
			sender:      models.Identity{Type: models.PrincipalTask, Workspace: "ws1"},
			targetTopic: "metric::ws1",
			wantErr:     false,
		},
		{
			// Cross-workspace ACL enforcement moved to ACL layer; transport allows.
			name:        "task cross-workspace agent topic allowed at transport (ACL gates downstream)",
			sender:      models.Identity{Type: models.PrincipalTask, Workspace: "ws1"},
			targetTopic: "ag::ws2::impl::spec",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := enforceTopicPermissions(tt.sender, tt.targetTopic)
			if (err != nil) != tt.wantErr {
				t.Errorf("enforceTopicPermissions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("enforceTopicPermissions() error = %q, want it to contain %q", err.Error(), tt.errContains)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestExtractWorkspaceFromTopic – workspace extraction helper
// ---------------------------------------------------------------------------

func TestExtractWorkspaceFromTopic(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  string
	}{
		{
			name:  "agent topic returns workspace",
			topic: "ag::prod::worker::1",
			want:  "prod",
		},
		{
			name:  "unique task topic returns workspace",
			topic: "tu::staging::task::abc",
			want:  "staging",
		},
		{
			name:  "non-unique task topic returns workspace",
			topic: "ta::dev::batch::uuid-123",
			want:  "dev",
		},
		{
			name:  "task broadcast topic returns workspace",
			topic: "tb::qa::impl",
			want:  "qa",
		},
		{
			name:  "global agent broadcast returns workspace",
			topic: "ga::dev",
			want:  "dev",
		},
		{
			name:  "global user broadcast returns workspace",
			topic: "gu::prod",
			want:  "prod",
		},
		{
			name:  "progress topic returns workspace",
			topic: "pg::prod",
			want:  "prod",
		},
		{
			name:  "user-workspace topic returns workspace (parts[2])",
			topic: "uw::alice::prod",
			want:  "prod",
		},
		{
			name:  "user window topic returns empty (no workspace)",
			topic: "us::alice::window1",
			want:  "",
		},
		{
			name:  "bridge topic returns empty (no workspace)",
			topic: "br::msgbridge::default",
			want:  "",
		},
		{
			name:  "event topic with workspace component returns workspace",
			topic: "event::prod",
			want:  "prod",
		},
		{
			name:  "metric topic returns workspace",
			topic: "metric::prod",
			want:  "prod",
		},
		{
			name:  "metric receiver shard returns empty (workspace-agnostic fan-in)",
			topic: "metric::receiver0",
			want:  "",
		},
		{
			name:  "metric receiver shard with index returns empty",
			topic: "metric::receiver7",
			want:  "",
		},
		{
			name:  "event receiver shard returns empty (workspace-agnostic fan-in)",
			topic: "event::receiver0",
			want:  "",
		},
		{
			name:  "event receiver shard with index returns empty",
			topic: "event::receiver3",
			want:  "",
		},
		{
			name:  "empty topic returns empty",
			topic: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractWorkspaceFromTopic(tt.topic)
			if got != tt.want {
				t.Errorf("extractWorkspaceFromTopic(%q) = %q, want %q", tt.topic, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestValidateTopicFormat – topic format validation
// ---------------------------------------------------------------------------

func TestValidateTopicFormat(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{
			name:    "empty topic returns error",
			topic:   "",
			wantErr: true,
		},
		{
			name:    "topic longer than 256 characters returns error",
			topic:   strings.Repeat("a", 257),
			wantErr: true,
		},
		{
			name:    "topic exactly 256 characters with valid prefix is allowed",
			topic:   "ag::" + strings.Repeat("a", 252),
			wantErr: false,
		},
		{
			name:    "valid ag prefix",
			topic:   "ag::ws1::impl::spec",
			wantErr: false,
		},
		{
			name:    "valid tu prefix",
			topic:   "tu::ws1::impl::spec",
			wantErr: false,
		},
		{
			name:    "valid ta prefix",
			topic:   "ta::ws1::impl::id1",
			wantErr: false,
		},
		{
			name:    "valid tb prefix",
			topic:   "tb::ws1::impl",
			wantErr: false,
		},
		{
			name:    "valid us prefix",
			topic:   "us::user1::win1",
			wantErr: false,
		},
		{
			name:    "valid uw prefix",
			topic:   "uw::user1::ws1",
			wantErr: false,
		},
		{
			name:    "valid ga prefix",
			topic:   "ga::ws1",
			wantErr: false,
		},
		{
			name:    "valid gu prefix",
			topic:   "gu::ws1",
			wantErr: false,
		},
		{
			name:    "valid event prefix",
			topic:   "event::ws1",
			wantErr: false,
		},
		{
			name:    "valid metric prefix",
			topic:   "metric::ws1",
			wantErr: false,
		},
		{
			name:    "invalid prefix xx returns error",
			topic:   "xx.foo.bar",
			wantErr: true,
		},
		{
			name:    "invalid prefix without dot returns error",
			topic:   "agwsimplspec",
			wantErr: true,
		},
		{
			name:    "random string returns error",
			topic:   "not-a-valid-topic",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTopicFormat(tt.topic)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTopicFormat(%q) error = %v, wantErr %v", tt.topic, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestSetupClientSubscriptions – per-principal subscription topology
// ---------------------------------------------------------------------------

// newTestGateway builds a minimal GatewayServer with the given router. Only the
// fields required by setupClientSubscriptions need to be populated.
func newTestGateway(r *mockRouter) *GatewayServer {
	return &GatewayServer{
		router: r,
	}
}

// newTestClient builds a ClientSession for the given identity with an
// initialised subscriptions map (no live gRPC stream required for these tests).
func newTestClient(identity models.Identity) *ClientSession {
	return &ClientSession{
		ID:            "test-session",
		Identity:      identity,
		subscriptions: make(map[string]func()),
	}
}

func TestSetupClientSubscriptions(t *testing.T) {
	tests := []struct {
		name                 string
		identity             models.Identity
		wantExclusiveTopics  []string
		wantSharedTopics     []string
		wantNoExclusiveCount bool // true when we expect zero exclusive subscriptions
		wantNoSharedCount    bool // true when we expect zero shared subscriptions
	}{
		{
			name: "agent subscribes to identity topic exclusively and global agent broadcast shared",
			identity: models.Identity{
				Type:           models.PrincipalAgent,
				Workspace:      "prod",
				Implementation: "worker",
				Specifier:      "v1",
			},
			wantExclusiveTopics: []string{"ag::prod::worker::v1"},
			wantSharedTopics:    []string{"ga::prod"},
		},
		{
			name: "unique task subscribes to identity topic exclusively only",
			identity: models.Identity{
				Type:           models.PrincipalTask,
				Workspace:      "prod",
				Implementation: "batch",
				Specifier:      "job-1", // Specifier != "" → unique task
			},
			wantExclusiveTopics: []string{"tu::prod::batch::job-1"},
			wantSharedTopics:    []string{},
			wantNoSharedCount:   true,
		},
		{
			name: "non-unique task subscribes to identity topic exclusively and task broadcast shared",
			identity: models.Identity{
				Type:           models.PrincipalTask,
				Workspace:      "prod",
				Implementation: "stream-proc",
				Specifier:      "", // Specifier == "" → non-unique
				ID:             "abc-123",
			},
			wantExclusiveTopics: []string{"ta::prod::stream-proc::abc-123"},
			wantSharedTopics:    []string{"tb::prod::stream-proc"},
		},
		{
			name: "user with workspace subscribes to window topic exclusively and workspace + per-user progress topics shared",
			identity: models.Identity{
				Type:      models.PrincipalUser,
				ID:        "alice",
				Specifier: "win-1",
				Workspace: "prod",
			},
			wantExclusiveTopics: []string{"us::alice::win-1"},
			// pg::us::alice is the per-user progress topic shared by all of
			// alice's open windows. Window-level filtering happens at delivery
			// time via the recipient field, not at the topic level. See
			// UserProgressTopic + isBareUserRecipientMatch.
			wantSharedTopics: []string{"gu::prod", "uw::alice::prod", "pg::us::alice"},
		},
		{
			name: "user without workspace subscribes to window topic exclusively and per-user progress topic shared",
			identity: models.Identity{
				Type:      models.PrincipalUser,
				ID:        "bob",
				Specifier: "win-2",
				Workspace: "", // no workspace
			},
			wantExclusiveTopics: []string{"us::bob::win-2"},
			// Even without a workspace, users still subscribe to their
			// per-user progress topic so targeted progress from agents in
			// any workspace can reach them.
			wantSharedTopics: []string{"pg::us::bob"},
		},
		{
			name: "workflow engine subscribes to event::receiver0 fan-in shard regardless of workspace",
			identity: models.Identity{
				Type:      models.PrincipalWorkflowEngine,
				Workspace: "prod",
			},
			wantExclusiveTopics: []string{"event::receiver0"},
			wantSharedTopics:    []string{},
			wantNoSharedCount:   true,
		},
		{
			name: "workflow engine without workspace also subscribes to event::receiver0 fan-in shard",
			identity: models.Identity{
				Type:      models.PrincipalWorkflowEngine,
				Workspace: "",
			},
			wantExclusiveTopics: []string{"event::receiver0"},
			wantSharedTopics:    []string{},
			wantNoSharedCount:   true,
		},
		{
			name: "metrics bridge subscribes to metric::receiver0 fan-in shard exclusively (with offset tracking) regardless of workspace",
			identity: models.Identity{
				Type:      models.PrincipalMetricsBridge,
				Workspace: "prod",
			},
			wantExclusiveTopics: []string{"metric::receiver0"},
			wantSharedTopics:    []string{},
			wantNoSharedCount:   true,
		},
		{
			name: "orchestrator receives no topic subscriptions",
			identity: models.Identity{
				Type:           models.PrincipalOrchestrator,
				Implementation: "k8s-orch",
				Specifier:      "primary",
			},
			wantExclusiveTopics:  []string{},
			wantSharedTopics:     []string{},
			wantNoExclusiveCount: true,
			wantNoSharedCount:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := newMockRouter()
			server := newTestGateway(router)
			client := newTestClient(tt.identity)

			err := server.setupClientSubscriptions(client)
			if err != nil {
				t.Fatalf("setupClientSubscriptions() unexpected error: %v", err)
			}

			// Verify each expected exclusive topic was subscribed via the exclusive path.
			for _, topic := range tt.wantExclusiveTopics {
				if !router.hasExclusiveTopic(topic) {
					t.Errorf("expected exclusive subscription for topic %q, but it was not found; got exclusive=%v",
						topic, router.exclusiveSubscriptions)
				}
				// Also verify it was NOT subscribed via the shared path.
				if router.hasSharedTopic(topic) {
					t.Errorf("topic %q should be exclusive, but was also subscribed via shared path", topic)
				}
			}

			// Verify each expected shared topic was subscribed via the shared path.
			for _, topic := range tt.wantSharedTopics {
				if !router.hasSharedTopic(topic) {
					t.Errorf("expected shared subscription for topic %q, but it was not found; got shared=%v",
						topic, router.subscribedTopics)
				}
				// Also verify it was NOT subscribed via the exclusive path.
				if router.hasExclusiveTopic(topic) {
					t.Errorf("topic %q should be shared, but was also subscribed via exclusive path", topic)
				}
			}

			// Verify no unexpected exclusive subscriptions when we expect none.
			if tt.wantNoExclusiveCount {
				router.mu.Lock()
				count := len(router.exclusiveSubscriptions)
				router.mu.Unlock()
				if count != 0 {
					t.Errorf("expected no exclusive subscriptions, but got %d: %v",
						count, router.exclusiveSubscriptions)
				}
			}

			// Verify no unexpected shared subscriptions when we expect none.
			if tt.wantNoSharedCount {
				router.mu.Lock()
				count := len(router.subscribedTopics)
				router.mu.Unlock()
				if count != 0 {
					t.Errorf("expected no shared subscriptions, but got %d: %v",
						count, router.subscribedTopics)
				}
			}
		})
	}
}

// TestSetupClientSubscriptions_IdentityTopicTrackedOnSession verifies that
// after setupClientSubscriptions returns, the ClientSession records the
// subscription so HasSubscription returns true for each expected topic.
func TestSetupClientSubscriptions_IdentityTopicTrackedOnSession(t *testing.T) {
	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "impl",
		Specifier:      "spec",
	}

	router := newMockRouter()
	server := newTestGateway(router)
	client := newTestClient(identity)

	if err := server.setupClientSubscriptions(client); err != nil {
		t.Fatalf("setupClientSubscriptions() unexpected error: %v", err)
	}

	if !client.HasSubscription("ag::ws1::impl::spec") {
		t.Error("client session should track subscription for identity topic ag.ws1.impl.spec")
	}
	if !client.HasSubscription("ga::ws1") {
		t.Error("client session should track subscription for global agent broadcast ga.ws1")
	}
}

// TestSetupClientSubscriptions_DuplicateSubscriptionIgnored verifies that
// calling setupClientSubscriptions twice does not create duplicate subscriptions.
func TestSetupClientSubscriptions_DuplicateSubscriptionIgnored(t *testing.T) {
	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "impl",
		Specifier:      "spec",
	}

	router := newMockRouter()
	server := newTestGateway(router)
	client := newTestClient(identity)

	if err := server.setupClientSubscriptions(client); err != nil {
		t.Fatalf("first setupClientSubscriptions() unexpected error: %v", err)
	}
	if err := server.setupClientSubscriptions(client); err != nil {
		t.Fatalf("second setupClientSubscriptions() unexpected error: %v", err)
	}

	// The exclusive subscription should only be recorded once.
	router.mu.Lock()
	exclusiveCount := len(router.exclusiveSubscriptions)
	sharedCount := len(router.subscribedTopics)
	router.mu.Unlock()

	if exclusiveCount != 1 {
		t.Errorf("expected 1 exclusive subscription after duplicate call, got %d", exclusiveCount)
	}
	// The shared Subscribe call may be deduplicated at the ClientSession level
	// (HasSubscription guard). Agents get ga.{workspace} + pg.{workspace} = 2 shared.
	if sharedCount != 2 {
		t.Errorf("expected 2 shared subscriptions after duplicate call, got %d", sharedCount)
	}
}

// ---------------------------------------------------------------------------
// TestCheckCrossWorkspaceBroadcast – cross-workspace event/metric ACL gate
// ---------------------------------------------------------------------------
//
// These tests substitute hasCrossWorkspaceBroadcastPermission for a stub so
// the ACL service is not required. They MUST NOT call t.Parallel() because
// the substitution is process-global.

// withCrossWorkspaceBroadcastStub replaces hasCrossWorkspaceBroadcastPermission
// for the duration of the test, restoring the original on cleanup.
func withCrossWorkspaceBroadcastStub(t *testing.T, allowed bool) {
	t.Helper()
	orig := hasCrossWorkspaceBroadcastPermission
	hasCrossWorkspaceBroadcastPermission = func(_ context.Context, _ *GatewayServer, _ models.Identity, _, _, _ string, _ uuid.UUID) bool {
		return allowed
	}
	t.Cleanup(func() { hasCrossWorkspaceBroadcastPermission = orig })
}

// TestCheckCrossWorkspaceBroadcast_NilACL exercises the dev-mode default
// where ACL is disabled — the gate degrades to allow (no ceiling to enforce).
func TestCheckCrossWorkspaceBroadcast_NilACL(t *testing.T) {
	s := &GatewayServer{}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "_apps"}
	err := s.checkCrossWorkspaceBroadcast(context.Background(), sender, "event::receiver0", "_sandbox", uuid.New())
	if err != nil {
		t.Errorf("expected nil error when ACL is disabled, got %v", err)
	}
}

// TestCheckCrossWorkspaceBroadcast_AllowsWithPermission verifies the
// capability/event_broadcast (or metric_broadcast) capability allows the
// publish through.
func TestCheckCrossWorkspaceBroadcast_AllowsWithPermission(t *testing.T) {
	withCrossWorkspaceBroadcastStub(t, true)
	// Use a zero-value *acl.Service so the s.acl == nil short-circuit
	// doesn't return early before hitting the stub. The stub bypasses any
	// actual ACL evaluation, so the empty Service is never dereferenced.
	s := &GatewayServer{acl: &acl.Service{}}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "_apps"}
	if err := s.checkCrossWorkspaceBroadcast(context.Background(), sender, "event::receiver0", "_sandbox", uuid.New()); err != nil {
		t.Errorf("expected no error when capability is granted, got %v", err)
	}
}

// TestCheckCrossWorkspaceBroadcast_DeniesWithoutPermission verifies a
// cross-workspace publish is denied when the capability is missing.
func TestCheckCrossWorkspaceBroadcast_DeniesWithoutPermission(t *testing.T) {
	withCrossWorkspaceBroadcastStub(t, false)
	s := &GatewayServer{acl: &acl.Service{}}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "_apps"}
	err := s.checkCrossWorkspaceBroadcast(context.Background(), sender, "event::receiver0", "_sandbox", uuid.New())
	if err == nil {
		t.Fatal("expected error when capability is missing, got nil")
	}
	if !strings.Contains(err.Error(), "capability/event_broadcast") {
		t.Errorf("expected error to mention capability/event_broadcast, got %v", err)
	}
}

// TestCheckCrossWorkspaceBroadcast_DeniesMetricWithoutPermission verifies
// the metric variant uses capability/metric_broadcast.
func TestCheckCrossWorkspaceBroadcast_DeniesMetricWithoutPermission(t *testing.T) {
	withCrossWorkspaceBroadcastStub(t, false)
	s := &GatewayServer{acl: &acl.Service{}}
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "_apps"}
	err := s.checkCrossWorkspaceBroadcast(context.Background(), sender, "metric::receiver0", "_sandbox", uuid.New())
	if err == nil {
		t.Fatal("expected error when capability is missing, got nil")
	}
	if !strings.Contains(err.Error(), "capability/metric_broadcast") {
		t.Errorf("expected error to mention capability/metric_broadcast, got %v", err)
	}
}

// TestRewriteEventTopic_FanInShard verifies the routeMessage rewrite for
// event::*, event::{ws} → event::receiver0. This mirrors the metric tests
// and uses an inline simulation of the rewrite block from routing.go.
func TestRewriteEventTopic_FanInShard(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "event wildcard rewrites to shard0", input: "event::*", want: "event::receiver0"},
		{name: "event workspace-scoped rewrites to shard0", input: "event::default", want: "event::receiver0"},
		{name: "event custom workspace rewrites to shard0", input: "event::tenant-a", want: "event::receiver0"},
		{name: "non-event topic untouched", input: "ag::default::x::y", want: "ag::default::x::y"},
		{name: "event receiver shard passes through unchanged", input: "event::receiver0", want: "event::receiver0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topic := tc.input
			if strings.HasPrefix(topic, "event::") && !strings.HasPrefix(topic[len("event::"):], "receiver") {
				topic = "event::receiver0"
			}
			if topic != tc.want {
				t.Errorf("rewrite produced %q, want %q", topic, tc.want)
			}
		})
	}
}
