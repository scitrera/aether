package auth

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/scitrera/aether/pkg/models"
)

// makeUnsignedJWT builds a JWT with the given claims, signed with "none".
func makeUnsignedJWT(t *testing.T, claims map[string]interface{}) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("failed to marshal claims: %v", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	return header + "." + payloadB64 + "."
}

func TestParseUnverified(t *testing.T) {
	auth := &OAuthAuthenticator{verifySignature: false}

	token := makeUnsignedJWT(t, map[string]interface{}{
		"sub":  "user-123",
		"iss":  "https://auth.example.com",
		"name": "Test User",
	})

	claims, err := auth.parseUnverified(token)
	if err != nil {
		t.Fatalf("parseUnverified() error = %v", err)
	}

	if sub, _ := claims["sub"].(string); sub != "user-123" {
		t.Errorf("sub = %q, want user-123", sub)
	}
	if iss, _ := claims["iss"].(string); iss != "https://auth.example.com" {
		t.Errorf("iss = %q, want https://auth.example.com", iss)
	}
}

func TestParseUnverified_InvalidToken(t *testing.T) {
	auth := &OAuthAuthenticator{verifySignature: false}

	_, err := auth.parseUnverified("not-a-jwt")
	if err == nil {
		t.Error("expected error for invalid token")
	}
}

func TestMapClaimsToIdentity_Defaults(t *testing.T) {
	auth := &OAuthAuthenticator{}
	provider := OAuthProviderConfig{
		Name:             "test",
		DefaultPrincipal: "User",
		DefaultWorkspace: "default-ws",
		ClaimsMapping: ClaimsMapping{
			PrincipalType: "principal_type",
			Workspace:     "workspace",
			Identity:      "sub",
		},
	}

	claims := jwt.MapClaims{
		"sub": "user-456",
	}

	identity, err := auth.mapClaimsToIdentity(claims, provider)
	if err != nil {
		t.Fatalf("mapClaimsToIdentity() error = %v", err)
	}

	if identity.Type != models.PrincipalUser {
		t.Errorf("Type = %v, want User", identity.Type)
	}
	if identity.ID != "user-456" {
		t.Errorf("ID = %q, want user-456", identity.ID)
	}
	if identity.Workspace != "default-ws" {
		t.Errorf("Workspace = %q, want default-ws", identity.Workspace)
	}
}

func TestMapClaimsToIdentity_ClaimsOverrideDefaults(t *testing.T) {
	auth := &OAuthAuthenticator{}
	provider := OAuthProviderConfig{
		Name:             "test",
		DefaultPrincipal: "User",
		DefaultWorkspace: "default-ws",
		ClaimsMapping: ClaimsMapping{
			PrincipalType: "principal_type",
			Workspace:     "workspace",
			Identity:      "sub",
		},
	}

	claims := jwt.MapClaims{
		"sub":            "agent-789",
		"principal_type": "Agent",
		"workspace":      "prod",
	}

	identity, err := auth.mapClaimsToIdentity(claims, provider)
	if err != nil {
		t.Fatalf("mapClaimsToIdentity() error = %v", err)
	}

	if identity.Type != models.PrincipalAgent {
		t.Errorf("Type = %v, want Agent", identity.Type)
	}
	if identity.ID != "agent-789" {
		t.Errorf("ID = %q, want agent-789", identity.ID)
	}
	if identity.Workspace != "prod" {
		t.Errorf("Workspace = %q, want prod", identity.Workspace)
	}
}

func TestMapClaimsToIdentity_InvalidPrincipalType(t *testing.T) {
	auth := &OAuthAuthenticator{}
	provider := OAuthProviderConfig{
		Name: "test",
		ClaimsMapping: ClaimsMapping{
			PrincipalType: "principal_type",
			Identity:      "sub",
		},
	}

	claims := jwt.MapClaims{
		"sub":            "user-1",
		"principal_type": "InvalidType",
	}

	_, err := auth.mapClaimsToIdentity(claims, provider)
	if err == nil {
		t.Error("expected error for invalid principal type")
	}
}

func TestMapClaimsToIdentity_FallbackSubject(t *testing.T) {
	auth := &OAuthAuthenticator{}
	provider := OAuthProviderConfig{
		Name:             "test",
		DefaultPrincipal: "User",
		ClaimsMapping: ClaimsMapping{
			Identity: "custom_id", // not present in claims
		},
	}

	claims := jwt.MapClaims{
		"sub": "fallback-user",
	}

	identity, err := auth.mapClaimsToIdentity(claims, provider)
	if err != nil {
		t.Fatalf("mapClaimsToIdentity() error = %v", err)
	}

	if identity.ID != "fallback-user" {
		t.Errorf("ID = %q, want fallback-user (from sub)", identity.ID)
	}
}

func TestAudienceMatches(t *testing.T) {
	tests := []struct {
		name     string
		claims   jwt.MapClaims
		audience string
		want     bool
	}{
		{
			name:     "string match",
			claims:   jwt.MapClaims{"aud": "my-api"},
			audience: "my-api",
			want:     true,
		},
		{
			name:     "string mismatch",
			claims:   jwt.MapClaims{"aud": "other-api"},
			audience: "my-api",
			want:     false,
		},
		{
			name:     "array match",
			claims:   jwt.MapClaims{"aud": []interface{}{"api-1", "my-api", "api-3"}},
			audience: "my-api",
			want:     true,
		},
		{
			name:     "array no match",
			claims:   jwt.MapClaims{"aud": []interface{}{"api-1", "api-2"}},
			audience: "my-api",
			want:     false,
		},
		{
			name:     "missing aud",
			claims:   jwt.MapClaims{},
			audience: "my-api",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := audienceMatches(tt.claims, tt.audience)
			if got != tt.want {
				t.Errorf("audienceMatches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePrincipalType(t *testing.T) {
	tests := []struct {
		input   string
		want    models.PrincipalType
		wantErr bool
	}{
		{"Agent", models.PrincipalAgent, false},
		{"Task", models.PrincipalTask, false},
		{"User", models.PrincipalUser, false},
		{"WorkflowEngine", models.PrincipalWorkflowEngine, false},
		{"MetricsBridge", models.PrincipalMetricsBridge, false},
		{"Orchestrator", models.PrincipalOrchestrator, false},
		{"Bridge", models.PrincipalBridge, false},
		{"Service", models.PrincipalService, false},
		{"Invalid", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parsePrincipalType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePrincipalType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parsePrincipalType(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestMapClaimsToIdentity_RejectsServicePrincipalType(t *testing.T) {
	auth := &OAuthAuthenticator{}
	provider := OAuthProviderConfig{
		Name: "test",
		ClaimsMapping: ClaimsMapping{
			PrincipalType: "principal_type",
			Identity:      "sub",
		},
	}

	claims := jwt.MapClaims{
		"sub":            "service-1",
		"principal_type": "Service",
	}

	_, err := auth.mapClaimsToIdentity(claims, provider)
	if err == nil {
		t.Fatal("expected error for service principal type via OAuth")
	}
}

func TestOAuthAuthenticator_Authenticate_NoCredentials(t *testing.T) {
	noVerify := false
	auth := NewOAuthAuthenticator(nil, &noVerify)

	result, err := auth.Authenticate(t.Context(), map[string]string{"api_key": "something"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil result when no bearer_token credential")
	}
}

func TestOAuthAuthenticator_Authenticate_EmptyBearer(t *testing.T) {
	noVerify := false
	auth := NewOAuthAuthenticator(nil, &noVerify)

	_, err := auth.Authenticate(t.Context(), map[string]string{"bearer_token": ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOAuthAuthenticator_Close(t *testing.T) {
	noVerify := false
	auth := NewOAuthAuthenticator(nil, &noVerify)
	// Should not panic
	auth.Close()
}
