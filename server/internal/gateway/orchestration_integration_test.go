package gateway

import (
	"testing"

	"github.com/scitrera/aether/pkg/models"
)

func TestPhase6Integration_ParseIdentity(t *testing.T) {
	// Test identity parsing which is critical for targeted assignments

	tests := []struct {
		name          string
		identityStr   string
		wantType      models.PrincipalType
		wantWorkspace string
		wantImpl      string
		wantSpec      string
		wantErr       bool
	}{
		{
			name:          "agent identity",
			identityStr:   "ag::production::python-worker::instance-1",
			wantType:      models.PrincipalAgent,
			wantWorkspace: "production",
			wantImpl:      "python-worker",
			wantSpec:      "instance-1",
			wantErr:       false,
		},
		{
			name:          "unique task identity",
			identityStr:   "tu::staging::processor::job-123",
			wantType:      models.PrincipalTask,
			wantWorkspace: "staging",
			wantImpl:      "processor",
			wantSpec:      "job-123",
			wantErr:       false,
		},
		{
			name:        "user identity",
			identityStr: "us::alice::window-1",
			wantType:    models.PrincipalUser,
			wantErr:     false,
		},
		{
			name:        "invalid identity",
			identityStr: "invalid",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity, err := models.ParseIdentity(tt.identityStr)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for %s, got nil", tt.identityStr)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if identity.Type != tt.wantType {
				t.Errorf("Expected type %v, got %v", tt.wantType, identity.Type)
			}

			if tt.wantType == models.PrincipalAgent || tt.wantType == models.PrincipalTask {
				if identity.Workspace != tt.wantWorkspace {
					t.Errorf("Expected workspace %s, got %s", tt.wantWorkspace, identity.Workspace)
				}
				if identity.Implementation != tt.wantImpl {
					t.Errorf("Expected implementation %s, got %s", tt.wantImpl, identity.Implementation)
				}
				if identity.Specifier != tt.wantSpec {
					t.Errorf("Expected specifier %s, got %s", tt.wantSpec, identity.Specifier)
				}
			}
		})
	}
}

func TestPhase6Integration_MetadataConversion(t *testing.T) {
	// Test metadata conversion between types

	metadata := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
		"key3": 123, // Non-string value should be skipped
	}

	converted := convertMetadataToString(metadata)

	if converted["key1"] != "value1" {
		t.Errorf("Expected key1='value1', got '%s'", converted["key1"])
	}

	if converted["key2"] != "value2" {
		t.Errorf("Expected key2='value2', got '%s'", converted["key2"])
	}

	if _, exists := converted["key3"]; exists {
		t.Error("Expected key3 to be skipped (non-string value)")
	}
}

func TestPhase6Integration_ClientLookup(t *testing.T) {
	// Test client lookup by identity

	server := &GatewayServer{}

	identity1 := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "test",
		Implementation: "worker",
		Specifier:      "inst-1",
	}

	identity2 := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "test",
		Implementation: "worker",
		Specifier:      "inst-2",
	}

	// Store mock clients
	client1 := &ClientSession{
		ID:       "session-1",
		Identity: identity1,
	}
	client2 := &ClientSession{
		ID:       "session-2",
		Identity: identity2,
	}

	server.activeStreams.Store("session-1", client1)
	server.activeStreams.Store("session-2", client2)
	server.identityIndex.Store(identity1.String(), "session-1")
	server.identityIndex.Store(identity2.String(), "session-2")

	// Test lookup
	found := server.getClientByIdentity(identity1)
	if found == nil {
		t.Error("Expected to find client1")
	} else if found.ID != "session-1" {
		t.Errorf("Expected session-1, got %s", found.ID)
	}

	// Test not found
	notFound := server.getClientByIdentity(models.Identity{
		Type:      models.PrincipalAgent,
		Workspace: "nonexistent",
	})
	if notFound != nil {
		t.Error("Expected nil for nonexistent identity")
	}
}
