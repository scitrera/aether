package models

import (
	"testing"
)

func TestCleanupLeaderIdentity(t *testing.T) {
	identity := CleanupLeaderIdentity()

	if identity.Type != PrincipalTask {
		t.Errorf("CleanupLeaderIdentity Type = %v, want %v", identity.Type, PrincipalTask)
	}
	if identity.Workspace != SystemWorkspace {
		t.Errorf("CleanupLeaderIdentity Workspace = %v, want %v", identity.Workspace, SystemWorkspace)
	}
	if identity.Implementation != CleanupLeaderImplementation {
		t.Errorf("CleanupLeaderIdentity Implementation = %v, want %v", identity.Implementation, CleanupLeaderImplementation)
	}
	if identity.Specifier != CleanupLeaderSpecifier {
		t.Errorf("CleanupLeaderIdentity Specifier = %v, want %v", identity.Specifier, CleanupLeaderSpecifier)
	}

	// Verify it produces a valid topic
	topic := identity.ToTopic()
	expected := "tu::_system::_cleanup::leader"
	if topic != expected {
		t.Errorf("CleanupLeaderIdentity ToTopic = %v, want %v", topic, expected)
	}
}

func TestIdentity_ToTopic(t *testing.T) {
	tests := []struct {
		name     string
		identity Identity
		want     string
	}{
		{
			name: "agent",
			identity: Identity{
				Type:           PrincipalAgent,
				Workspace:      "prod",
				Implementation: "worker",
				Specifier:      "inst-1",
			},
			want: "ag::prod::worker::inst-1",
		},
		{
			name: "agent with dots in implementation",
			identity: Identity{
				Type:           PrincipalAgent,
				Workspace:      "prod",
				Implementation: "claude.code",
				Specifier:      "inst-1",
			},
			want: "ag::prod::claude.code::inst-1",
		},
		{
			name: "unique task",
			identity: Identity{
				Type:           PrincipalTask,
				Workspace:      "staging",
				Implementation: "processor",
				Specifier:      "unique-task-1",
			},
			want: "tu::staging::processor::unique-task-1",
		},
		{
			name: "non-unique task",
			identity: Identity{
				Type:           PrincipalTask,
				Workspace:      "dev",
				Implementation: "batch-job",
				ID:             "uuid-123",
			},
			want: "ta::dev::batch-job::uuid-123",
		},
		{
			name: "user",
			identity: Identity{
				Type:      PrincipalUser,
				ID:        "user-456",
				Specifier: "window-1",
			},
			want: "us::user-456::window-1",
		},
		{
			name: "service",
			identity: Identity{
				Type:           PrincipalService,
				Implementation: "frontend-api",
				Specifier:      "pod-1",
			},
			want: "sv::frontend-api::pod-1",
		},
		{
			name: "workflow engine with workspace subscribes to fan-in shard (workspace ignored)",
			identity: Identity{
				Type:      PrincipalWorkflowEngine,
				Workspace: "prod",
			},
			want: "event::receiver0",
		},
		{
			name: "workflow engine without workspace subscribes to fan-in shard",
			identity: Identity{
				Type: PrincipalWorkflowEngine,
			},
			want: "event::receiver0",
		},
		{
			name: "metrics bridge with workspace",
			identity: Identity{
				Type:      PrincipalMetricsBridge,
				Workspace: "prod",
			},
			want: "metric::receiver0",
		},
		{
			name: "metrics bridge without workspace",
			identity: Identity{
				Type: PrincipalMetricsBridge,
			},
			want: "metric::receiver0",
		},
		{
			name: "orchestrator returns empty",
			identity: Identity{
				Type:           PrincipalOrchestrator,
				Implementation: "kubernetes",
				Specifier:      "cluster-1",
			},
			want: "",
		},
		{
			name: "unknown type returns empty",
			identity: Identity{
				Type: "UnknownType",
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.identity.ToTopic()
			if got != tt.want {
				t.Errorf("ToTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIdentity_String(t *testing.T) {
	tests := []struct {
		name     string
		identity Identity
		want     string
	}{
		{
			name: "agent uses topic format",
			identity: Identity{
				Type:           PrincipalAgent,
				Workspace:      "prod",
				Implementation: "worker",
				Specifier:      "inst-1",
			},
			want: "ag::prod::worker::inst-1",
		},
		{
			name: "unique task uses topic format",
			identity: Identity{
				Type:           PrincipalTask,
				Workspace:      "staging",
				Implementation: "processor",
				Specifier:      "unique-1",
			},
			want: "tu::staging::processor::unique-1",
		},
		{
			name: "non-unique task uses topic format",
			identity: Identity{
				Type:           PrincipalTask,
				Workspace:      "dev",
				Implementation: "batch",
				ID:             "uuid-789",
			},
			want: "ta::dev::batch::uuid-789",
		},
		{
			name: "user uses topic format",
			identity: Identity{
				Type:      PrincipalUser,
				ID:        "user-1",
				Specifier: "window-2",
			},
			want: "us::user-1::window-2",
		},
		{
			name: "service uses topic format",
			identity: Identity{
				Type:           PrincipalService,
				Implementation: "frontend-api",
				Specifier:      "pod-1",
			},
			want: "sv::frontend-api::pod-1",
		},
		{
			name: "orchestrator with specifier",
			identity: Identity{
				Type:           PrincipalOrchestrator,
				Implementation: "kubernetes",
				Specifier:      "cluster-1",
			},
			want: "orc::kubernetes::cluster-1",
		},
		{
			name: "orchestrator without specifier",
			identity: Identity{
				Type:           PrincipalOrchestrator,
				Implementation: "docker",
			},
			want: "orc::docker",
		},
		{
			name: "workflow engine with implementation collapses to shard0 singleton",
			identity: Identity{
				Type:           PrincipalWorkflowEngine,
				Implementation: "temporal",
			},
			want: "wfe::shard0",
		},
		{
			name: "workflow engine without implementation collapses to shard0 singleton",
			identity: Identity{
				Type: PrincipalWorkflowEngine,
			},
			want: "wfe::shard0",
		},
		{
			name: "metrics bridge with implementation",
			identity: Identity{
				Type:           PrincipalMetricsBridge,
				Implementation: "prometheus",
			},
			want: "metrics::prometheus",
		},
		{
			name: "metrics bridge without implementation",
			identity: Identity{
				Type: PrincipalMetricsBridge,
			},
			want: "metrics::default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.identity.String()
			if got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseIdentity(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		want        Identity
		wantErr     bool
		errContains string
	}{
		{
			name:  "parse agent",
			input: "ag::prod::worker::inst-1",
			want: Identity{
				Type:           PrincipalAgent,
				Workspace:      "prod",
				Implementation: "worker",
				Specifier:      "inst-1",
			},
		},
		{
			name:  "parse agent with dots in implementation",
			input: "ag::prod::claude.code::inst-1",
			want: Identity{
				Type:           PrincipalAgent,
				Workspace:      "prod",
				Implementation: "claude.code",
				Specifier:      "inst-1",
			},
		},
		{
			name:  "parse unique task",
			input: "tu::staging::processor::unique-task-1",
			want: Identity{
				Type:           PrincipalTask,
				Workspace:      "staging",
				Implementation: "processor",
				Specifier:      "unique-task-1",
			},
		},
		{
			name:  "parse unique task with dots in implementation",
			input: "tu::dev::my.complex.impl::task-id",
			want: Identity{
				Type:           PrincipalTask,
				Workspace:      "dev",
				Implementation: "my.complex.impl",
				Specifier:      "task-id",
			},
		},
		{
			name:  "parse non-unique task",
			input: "ta::dev::batch-job::uuid-123",
			want: Identity{
				Type:           PrincipalTask,
				Workspace:      "dev",
				Implementation: "batch-job",
				ID:             "uuid-123",
			},
		},
		{
			name:  "parse user",
			input: "us::user-456::window-1",
			want: Identity{
				Type:      PrincipalUser,
				ID:        "user-456",
				Specifier: "window-1",
			},
		},
		{
			name:  "parse service",
			input: "sv::frontend.api::pod-1",
			want: Identity{
				Type:           PrincipalService,
				Implementation: "frontend.api",
				Specifier:      "pod-1",
			},
		},
		// Error cases
		{
			name:        "too short",
			input:       "ab",
			wantErr:     true,
			errContains: "too short",
		},
		{
			name:        "missing separator",
			input:       "agprodworker",
			wantErr:     true,
			errContains: "missing \"::\" separator",
		},
		{
			name:        "unknown prefix",
			input:       "xx::something::else",
			wantErr:     true,
			errContains: "unknown identity prefix",
		},
		{
			name:        "agent too few parts",
			input:       "ag::prod::worker",
			wantErr:     true,
			errContains: "expected 4 parts",
		},
		{
			name:        "unique task too few parts",
			input:       "tu::staging::proc",
			wantErr:     true,
			errContains: "expected 4 parts",
		},
		{
			name:        "non-unique task too few parts",
			input:       "ta::dev::batch",
			wantErr:     true,
			errContains: "expected 4 parts",
		},
		{
			name:        "user wrong part count - too few",
			input:       "us::user-id",
			wantErr:     true,
			errContains: "expected 3 parts",
		},
		{
			name:        "user wrong part count - too many",
			input:       "us::user-id::window::extra",
			wantErr:     true,
			errContains: "expected 3 parts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseIdentity(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseIdentity() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("ParseIdentity() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseIdentity() unexpected error = %v", err)
				return
			}

			if got.Type != tt.want.Type {
				t.Errorf("ParseIdentity() Type = %v, want %v", got.Type, tt.want.Type)
			}
			if got.Workspace != tt.want.Workspace {
				t.Errorf("ParseIdentity() Workspace = %v, want %v", got.Workspace, tt.want.Workspace)
			}
			if got.Implementation != tt.want.Implementation {
				t.Errorf("ParseIdentity() Implementation = %v, want %v", got.Implementation, tt.want.Implementation)
			}
			if got.Specifier != tt.want.Specifier {
				t.Errorf("ParseIdentity() Specifier = %v, want %v", got.Specifier, tt.want.Specifier)
			}
			if got.ID != tt.want.ID {
				t.Errorf("ParseIdentity() ID = %v, want %v", got.ID, tt.want.ID)
			}
		})
	}
}

func TestParseIdentity_Roundtrip(t *testing.T) {
	// Test that parsing an identity string and converting back produces the same result
	tests := []struct {
		name     string
		identity Identity
	}{
		{
			name: "agent roundtrip",
			identity: Identity{
				Type:           PrincipalAgent,
				Workspace:      "prod",
				Implementation: "worker",
				Specifier:      "inst-1",
			},
		},
		{
			name: "agent with dots roundtrip",
			identity: Identity{
				Type:           PrincipalAgent,
				Workspace:      "staging",
				Implementation: "claude.code",
				Specifier:      "inst-2",
			},
		},
		{
			name: "unique task roundtrip",
			identity: Identity{
				Type:           PrincipalTask,
				Workspace:      "dev",
				Implementation: "processor",
				Specifier:      "unique-123",
			},
		},
		{
			name: "non-unique task roundtrip",
			identity: Identity{
				Type:           PrincipalTask,
				Workspace:      "qa",
				Implementation: "batch",
				ID:             "uuid-456",
			},
		},
		{
			name: "user roundtrip",
			identity: Identity{
				Type:      PrincipalUser,
				ID:        "user-789",
				Specifier: "window-3",
			},
		},
		{
			name: "service roundtrip",
			identity: Identity{
				Type:           PrincipalService,
				Implementation: "frontend.api",
				Specifier:      "pod-1",
			},
		},
		// New: dotted impl AND dotted specifier round-trip correctly
		{
			name: "agent with dotted impl and email specifier roundtrip",
			identity: Identity{
				Type:           PrincipalAgent,
				Workspace:      "_apps",
				Implementation: "scitrera_ai_runtime.cowork.aether_bridge.CoworkAgent",
				Specifier:      "development@scitrera.com",
			},
		},
		// New: user identity with email as user_id round-trips correctly
		{
			name: "user with email user_id roundtrip",
			identity: Identity{
				Type:      PrincipalUser,
				ID:        "alice@example.com",
				Specifier: "wnd_abc",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert to string
			str := tt.identity.String()

			// Parse back
			parsed, err := ParseIdentity(str)
			if err != nil {
				t.Errorf("ParseIdentity(%q) error = %v", str, err)
				return
			}

			// Convert parsed back to string
			str2 := parsed.String()

			// Strings should match
			if str != str2 {
				t.Errorf("Roundtrip mismatch: original %q, after roundtrip %q", str, str2)
			}

			// Fields should match
			if tt.identity.Type != parsed.Type {
				t.Errorf("Type mismatch: original %v, parsed %v", tt.identity.Type, parsed.Type)
			}
			if tt.identity.Implementation != parsed.Implementation {
				t.Errorf("Implementation mismatch: original %q, parsed %q", tt.identity.Implementation, parsed.Implementation)
			}
			if tt.identity.Specifier != parsed.Specifier {
				t.Errorf("Specifier mismatch: original %q, parsed %q", tt.identity.Specifier, parsed.Specifier)
			}
			if tt.identity.ID != parsed.ID {
				t.Errorf("ID mismatch: original %q, parsed %q", tt.identity.ID, parsed.ID)
			}
			if tt.identity.Workspace != parsed.Workspace {
				t.Errorf("Workspace mismatch: original %q, parsed %q", tt.identity.Workspace, parsed.Workspace)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Bridge identity tests
// ---------------------------------------------------------------------------

func TestBridgeIdentity_ToTopic(t *testing.T) {
	identity := Identity{
		Type:           PrincipalBridge,
		Implementation: "aether-msgbridge",
		Specifier:      "default",
	}
	got := identity.ToTopic()
	want := "br::aether-msgbridge::default"
	if got != want {
		t.Errorf("ToTopic() = %q, want %q", got, want)
	}
}

func TestBridgeIdentity_String(t *testing.T) {
	identity := Identity{
		Type:           PrincipalBridge,
		Implementation: "aether-msgbridge",
		Specifier:      "default",
	}
	got := identity.String()
	want := "br::aether-msgbridge::default"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestParseBridgeIdentity(t *testing.T) {
	got, err := ParseIdentity("br::aether-msgbridge::default")
	if err != nil {
		t.Fatalf("ParseIdentity() unexpected error: %v", err)
	}
	if got.Type != PrincipalBridge {
		t.Errorf("Type = %v, want %v", got.Type, PrincipalBridge)
	}
	if got.Implementation != "aether-msgbridge" {
		t.Errorf("Implementation = %q, want %q", got.Implementation, "aether-msgbridge")
	}
	if got.Specifier != "default" {
		t.Errorf("Specifier = %q, want %q", got.Specifier, "default")
	}
	if got.Workspace != "" {
		t.Errorf("Workspace = %q, want empty (bridges have no workspace)", got.Workspace)
	}
}

// TestParseBridgeIdentity_WithDottedImpl verifies that a dotted impl value is a plain
// single-segment field (no joining/splitting). The `::` separator means `.` is not special.
func TestParseBridgeIdentity_WithDottedImpl(t *testing.T) {
	got, err := ParseIdentity("br::claude.code::spec")
	if err != nil {
		t.Fatalf("ParseIdentity() unexpected error: %v", err)
	}
	if got.Type != PrincipalBridge {
		t.Errorf("Type = %v, want %v", got.Type, PrincipalBridge)
	}
	if got.Implementation != "claude.code" {
		t.Errorf("Implementation = %q, want %q", got.Implementation, "claude.code")
	}
	if got.Specifier != "spec" {
		t.Errorf("Specifier = %q, want %q", got.Specifier, "spec")
	}
}

func TestParseBridgeIdentity_Invalid(t *testing.T) {
	_, err := ParseIdentity("br::onlyonepart")
	if err == nil {
		t.Error("ParseIdentity() expected error for bridge with too few parts, got nil")
	}
	if !contains(err.Error(), "expected 3 parts") {
		t.Errorf("ParseIdentity() error = %q, want it to contain %q", err.Error(), "expected 3 parts")
	}
}

// TestParseIdentity_DotsInFieldValues confirms that `.` inside field values no longer
// causes ambiguity now that `::` is the segment separator.
func TestParseIdentity_DotsInFieldValues(t *testing.T) {
	t.Run("agent with dotted impl and email specifier", func(t *testing.T) {
		ws := "_apps"
		impl := "scitrera_ai_runtime.cowork.aether_bridge.CoworkAgent"
		spec := "development@scitrera.com"

		identity := Identity{
			Type:           PrincipalAgent,
			Workspace:      ws,
			Implementation: impl,
			Specifier:      spec,
		}
		str := identity.String()

		parsed, err := ParseIdentity(str)
		if err != nil {
			t.Fatalf("ParseIdentity(%q) unexpected error: %v", str, err)
		}
		if parsed.Workspace != ws {
			t.Errorf("Workspace = %q, want %q", parsed.Workspace, ws)
		}
		if parsed.Implementation != impl {
			t.Errorf("Implementation = %q, want %q", parsed.Implementation, impl)
		}
		if parsed.Specifier != spec {
			t.Errorf("Specifier = %q, want %q", parsed.Specifier, spec)
		}
	})

	t.Run("user with email user_id", func(t *testing.T) {
		userID := "alice@example.com"
		window := "wnd_abc"

		identity := Identity{
			Type:      PrincipalUser,
			ID:        userID,
			Specifier: window,
		}
		str := identity.String()

		parsed, err := ParseIdentity(str)
		if err != nil {
			t.Fatalf("ParseIdentity(%q) unexpected error: %v", str, err)
		}
		if parsed.ID != userID {
			t.Errorf("ID = %q, want %q", parsed.ID, userID)
		}
		if parsed.Specifier != window {
			t.Errorf("Specifier = %q, want %q", parsed.Specifier, window)
		}
	})
}

func TestCanonicalPrincipalID(t *testing.T) {
	tests := []struct {
		name     string
		identity Identity
		want     string
	}{
		{
			name: "user uses stable user ID",
			identity: Identity{
				Type:      PrincipalUser,
				ID:        "user-123",
				Specifier: "window-a",
			},
			want: "user-123",
		},
		{
			name: "service uses canonical topic form",
			identity: Identity{
				Type:           PrincipalService,
				Implementation: "frontend-api",
				Specifier:      "pod-1",
			},
			want: "sv::frontend-api::pod-1",
		},
		{
			name: "stored structured principal falls back to ID",
			identity: Identity{
				Type: PrincipalAgent,
				ID:   "ag::prod::worker::inst-1",
			},
			want: "ag::prod::worker::inst-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.identity.CanonicalPrincipalID(); got != tt.want {
				t.Errorf("CanonicalPrincipalID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPrincipalRef(t *testing.T) {
	identity := Identity{
		Type:           PrincipalService,
		Implementation: "frontend-api",
		Specifier:      "pod-1",
	}

	ref := identity.PrincipalRef()
	if ref.Type != PrincipalService {
		t.Errorf("PrincipalRef().Type = %v, want %v", ref.Type, PrincipalService)
	}
	if ref.ID != "sv::frontend-api::pod-1" {
		t.Errorf("PrincipalRef().ID = %q, want %q", ref.ID, "sv::frontend-api::pod-1")
	}
	if ref.IsZero() {
		t.Error("expected non-zero principal ref")
	}

	if !(PrincipalRef{}).IsZero() {
		t.Error("expected empty principal ref to be zero")
	}
}

// helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
