package audit

import (
	"testing"
)

// TestSanitizeMetadata_RedactsCredentialKeys verifies the exported
// SanitizeMetadata replaces credential-shaped keys with [REDACTED] without
// mutating the input.
func TestSanitizeMetadata_RedactsCredentialKeys(t *testing.T) {
	in := map[string]interface{}{
		"password":        "hunter2",
		"api_key":         "AKIA-foo",
		"bearer_token":    "ey...",
		"normal":          "value",
		"user":            "alice",
		"authorization":   "Bearer xxx",
		"private_key_pem": "-----BEGIN-----",
	}

	got := SanitizeMetadata(in)

	for _, key := range []string{"password", "api_key", "bearer_token", "authorization", "private_key_pem"} {
		if got[key] != "[REDACTED]" {
			t.Errorf("expected %q to be [REDACTED], got %v", key, got[key])
		}
	}
	if got["normal"] != "value" {
		t.Errorf("expected normal=value, got %v", got["normal"])
	}
	if got["user"] != "alice" {
		t.Errorf("expected user=alice, got %v", got["user"])
	}

	// Ensure original wasn't mutated.
	if in["password"] != "hunter2" {
		t.Error("SanitizeMetadata mutated input map")
	}
}

// TestSanitizeMetadata_NilReturnsNil verifies nil input maps return nil
// (lets callers use the value directly without nil-checking the result).
func TestSanitizeMetadata_NilReturnsNil(t *testing.T) {
	if got := SanitizeMetadata(nil); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

// TestSanitizeMetadata_NestedMapsAreSanitized verifies nested maps are
// recursively sanitized.
func TestSanitizeMetadata_NestedMapsAreSanitized(t *testing.T) {
	in := map[string]interface{}{
		"outer": "ok",
		"nested": map[string]interface{}{
			"password": "leak",
			"safe":     "fine",
		},
	}

	got := SanitizeMetadata(in)

	nested, ok := got["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested map, got %T", got["nested"])
	}
	if nested["password"] != "[REDACTED]" {
		t.Errorf("expected nested.password redacted, got %v", nested["password"])
	}
	if nested["safe"] != "fine" {
		t.Errorf("expected nested.safe preserved, got %v", nested["safe"])
	}
}
