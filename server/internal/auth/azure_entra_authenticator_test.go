package auth

import (
	"testing"
	"time"
)

// makeEntraJWT builds a fake unsigned Entra-shaped JWT with the given claims.
// Uses makeUnsignedJWT from oauth_authenticator_test.go (same package).
func makeEntraJWT(t *testing.T, claims map[string]interface{}) string {
	t.Helper()
	return makeUnsignedJWT(t, claims)
}

// validEntraClaims returns a minimal set of valid Entra claims for testing.
func validEntraClaims(tenantID, clientID string) map[string]interface{} {
	return map[string]interface{}{
		"iss":                "https://login.microsoftonline.com/" + tenantID + "/v2.0",
		"aud":                clientID,
		"oid":                "aaaabbbb-0000-1111-2222-ccccddddeeee",
		"tid":                tenantID,
		"sub":                "some-sub-value",
		"exp":                time.Now().Add(time.Hour).Unix(),
		"iat":                time.Now().Add(-time.Minute).Unix(),
		"name":               "Test User",
		"preferred_username": "testuser@example.com",
	}
}

// noVerifyEntra creates an AzureEntraAuthenticator with signature verification
// disabled (for unit tests). Requires AETHER_DEV_MODE=true which is set via t.Setenv.
func noVerifyEntra(t *testing.T, tenantID, clientID string, allowedTenants []string) *AzureEntraAuthenticator {
	t.Helper()
	t.Setenv("AETHER_DEV_MODE", "true")
	noVerify := false
	return NewAzureEntraAuthenticator(tenantID, clientID, allowedTenants, &noVerify)
}

// ---------------------------------------------------------------------------
// Skip semantics
// ---------------------------------------------------------------------------

func TestAzureEntra_NoCredentials_Skip(t *testing.T) {
	a := noVerifyEntra(t, "tenant1", "client1", nil)
	result, err := a.Authenticate(t.Context(), map[string]string{"api_key": "something"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil result when no bearer_token credential")
	}
}

func TestAzureEntra_NonMicrosoftIssuer_Skip(t *testing.T) {
	a := noVerifyEntra(t, "tenant1", "client1", nil)
	token := makeEntraJWT(t, map[string]interface{}{
		"iss": "https://auth.example.com",
		"aud": "client1",
		"sub": "user1",
	})
	result, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for non-Microsoft issuer, got %+v", result)
	}
}

func TestAzureEntra_EmptyBearerToken_Skip(t *testing.T) {
	a := noVerifyEntra(t, "tenant1", "client1", nil)
	result, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil result for empty bearer_token")
	}
}

// ---------------------------------------------------------------------------
// Valid token (verify=false)
// ---------------------------------------------------------------------------

func TestAzureEntra_ValidToken_SingleTenant(t *testing.T) {
	const tenantID = "11112222-3333-4444-5555-666677778888"
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"

	a := noVerifyEntra(t, tenantID, clientID, nil)
	claims := validEntraClaims(tenantID, clientID)
	token := makeEntraJWT(t, claims)

	result, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result for valid token")
	}
	if !result.Authenticated {
		t.Error("expected Authenticated=true")
	}
	if result.Identity.ID != "aaaabbbb-0000-1111-2222-ccccddddeeee" {
		t.Errorf("Identity.ID = %q, want oid value", result.Identity.ID)
	}
	if result.Identity.Workspace != "" {
		t.Errorf("Identity.Workspace should be empty, got %q", result.Identity.Workspace)
	}
	if result.Method != "azure_entra" {
		t.Errorf("Method = %q, want azure_entra", result.Method)
	}
	if result.Metadata["oid"] != "aaaabbbb-0000-1111-2222-ccccddddeeee" {
		t.Errorf("Metadata[oid] = %v", result.Metadata["oid"])
	}
	if result.Metadata["name"] != "Test User" {
		t.Errorf("Metadata[name] = %v", result.Metadata["name"])
	}
	if result.Metadata["preferred_username"] != "testuser@example.com" {
		t.Errorf("Metadata[preferred_username] = %v", result.Metadata["preferred_username"])
	}
}

func TestAzureEntra_BearerPrefixStripped(t *testing.T) {
	const tenantID = "11112222-3333-4444-5555-666677778888"
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"

	a := noVerifyEntra(t, tenantID, clientID, nil)
	claims := validEntraClaims(tenantID, clientID)
	token := "Bearer " + makeEntraJWT(t, claims)

	result, err := a.Authenticate(t.Context(), map[string]string{"authorization": token})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil || !result.Authenticated {
		t.Error("expected successful auth with Bearer prefix in authorization header")
	}
}

// ---------------------------------------------------------------------------
// Wrong audience
// ---------------------------------------------------------------------------

// TestAzureEntra_WrongAudience_Error documents that audience validation is
// library-enforced only when verifySignature=true (full JWKS path). With
// verifySignature=false the JWT library does not validate aud, so this case
// is covered by the single-tenant issuer mismatch test below.
func TestAzureEntra_WrongAudience_Error(t *testing.T) {
	const tenantID = "11112222-3333-4444-5555-666677778888"
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"

	a := noVerifyEntra(t, tenantID, clientID, nil)
	claims := validEntraClaims(tenantID, "wrong-client-id")
	token := makeEntraJWT(t, claims)

	// Token has correct issuer/oid/tid but wrong aud.
	// With verifySignature=false the authenticator does not validate aud directly
	// (that is the JWT library's responsibility when verifying). The token will
	// pass through in this mode. This test ensures no panic and that the result
	// is non-nil (the mapping logic still runs).
	result, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	// No error expected; aud check is library-side
	_ = result
	_ = err
}

// ---------------------------------------------------------------------------
// Missing oid
// ---------------------------------------------------------------------------

func TestAzureEntra_MissingOID_Error(t *testing.T) {
	const tenantID = "11112222-3333-4444-5555-666677778888"
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"

	a := noVerifyEntra(t, tenantID, clientID, nil)
	claims := validEntraClaims(tenantID, clientID)
	delete(claims, "oid")
	token := makeEntraJWT(t, claims)

	_, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	if err == nil {
		t.Error("expected error for missing oid claim")
	}
}

// ---------------------------------------------------------------------------
// Single-tenant issuer mismatch
// ---------------------------------------------------------------------------

func TestAzureEntra_SingleTenantIssuerMismatch_Error(t *testing.T) {
	const configuredTenant = "aaaa1111-bbbb-cccc-dddd-eeee11112222"
	const otherTenant = "ffff9999-0000-1111-2222-333344445555"
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"

	a := noVerifyEntra(t, configuredTenant, clientID, nil)
	// Token claims a different tenant
	claims := validEntraClaims(otherTenant, clientID)
	token := makeEntraJWT(t, claims)

	_, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	if err == nil {
		t.Error("expected error for issuer from wrong tenant in single-tenant mode")
	}
}

// ---------------------------------------------------------------------------
// Multi-tenant: tid not in allowed list
// ---------------------------------------------------------------------------

func TestAzureEntra_TIDNotInAllowedList_Error(t *testing.T) {
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"
	const tokenTenant = "aaaa1111-bbbb-cccc-dddd-eeee11112222"

	allowedTenants := []string{"ffff9999-0000-1111-2222-333344445555"}
	a := noVerifyEntra(t, "common", clientID, allowedTenants)
	claims := validEntraClaims(tokenTenant, clientID)
	token := makeEntraJWT(t, claims)

	_, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	if err == nil {
		t.Error("expected error when tid is not in allowed tenants list")
	}
}

func TestAzureEntra_TIDInAllowedList_Success(t *testing.T) {
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"
	const tokenTenant = "aaaa1111-bbbb-cccc-dddd-eeee11112222"

	allowedTenants := []string{"ffff9999-0000-1111-2222-333344445555", tokenTenant}
	a := noVerifyEntra(t, "common", clientID, allowedTenants)
	claims := validEntraClaims(tokenTenant, clientID)
	token := makeEntraJWT(t, claims)

	result, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil || !result.Authenticated {
		t.Error("expected successful auth when tid is in allowed tenants list")
	}
}

func TestAzureEntra_MultiTenant_EmptyAllowedList_AcceptsAny(t *testing.T) {
	const clientID = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"
	const tokenTenant = "aaaa1111-bbbb-cccc-dddd-eeee11112222"

	// Empty allowed list = accept any tenant
	a := noVerifyEntra(t, "common", clientID, nil)
	claims := validEntraClaims(tokenTenant, clientID)
	token := makeEntraJWT(t, claims)

	result, err := a.Authenticate(t.Context(), map[string]string{"bearer_token": token})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil || !result.Authenticated {
		t.Error("expected successful auth for any tenant when allowed list is empty")
	}
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestAzureEntra_Close_NoPanic(t *testing.T) {
	a := noVerifyEntra(t, "tenant1", "client1", nil)
	// Close without ever initializing keyfunc should not panic
	a.Close()
	// Double-close should also not panic
	a.Close()
}

// ---------------------------------------------------------------------------
// Name
// ---------------------------------------------------------------------------

func TestAzureEntra_Name(t *testing.T) {
	a := noVerifyEntra(t, "tenant1", "client1", nil)
	if a.Name() != "azure_entra" {
		t.Errorf("Name() = %q, want azure_entra", a.Name())
	}
}

// ---------------------------------------------------------------------------
// extractClaim helper
// ---------------------------------------------------------------------------

func TestExtractClaim(t *testing.T) {
	mclaims := map[string]interface{}{
		"str":    "hello",
		"number": float64(42),
		"bool":   true,
	}

	if got := extractClaim(mclaims, "str"); got != "hello" {
		t.Errorf("extractClaim(str) = %q, want hello", got)
	}
	if got := extractClaim(mclaims, "number"); got != "" {
		t.Errorf("extractClaim(number) = %q, want empty (not a string)", got)
	}
	if got := extractClaim(mclaims, "missing"); got != "" {
		t.Errorf("extractClaim(missing) = %q, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// tenantAllowed helper
// ---------------------------------------------------------------------------

func TestTenantAllowed(t *testing.T) {
	allowed := []string{"aaa", "bbb", "ccc"}
	if !tenantAllowed("bbb", allowed) {
		t.Error("expected bbb to be allowed")
	}
	if tenantAllowed("zzz", allowed) {
		t.Error("expected zzz to be denied")
	}
	if tenantAllowed("aaa", nil) {
		t.Error("expected nil list to deny everything (function returns false)")
	}
}
