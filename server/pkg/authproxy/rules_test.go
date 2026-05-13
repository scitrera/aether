package authproxy

import (
	"testing"
)

func TestEvaluateAuthRules_AzureTID(t *testing.T) {
	rules := []AuthRule{
		{Claim: "tid", AllowEq: []string{"beeaef61-22c3-4e35-a140-05d2b6615d37"}, Required: true},
	}

	t.Run("matching tid passes", func(t *testing.T) {
		ok, reason := EvaluateAuthRules(map[string]any{
			"tid": "beeaef61-22c3-4e35-a140-05d2b6615d37",
			"oid": "user-1",
		}, rules)
		if !ok {
			t.Fatalf("expected pass, got reject: %s", reason)
		}
	})

	t.Run("foreign tid rejected", func(t *testing.T) {
		ok, reason := EvaluateAuthRules(map[string]any{
			"tid": "00000000-0000-0000-0000-000000000000",
		}, rules)
		if ok {
			t.Fatalf("expected reject, got pass")
		}
		if reason == "" {
			t.Fatal("expected non-empty reason")
		}
	})

	t.Run("missing required tid rejected", func(t *testing.T) {
		ok, _ := EvaluateAuthRules(map[string]any{}, rules)
		if ok {
			t.Fatal("expected reject when required tid missing")
		}
	})
}

func TestEvaluateAuthRules_OptionalClaim(t *testing.T) {
	rules := []AuthRule{
		{Claim: "hd", AllowEq: []string{"scitrera.com"}, Required: false},
	}

	t.Run("missing optional passes", func(t *testing.T) {
		ok, _ := EvaluateAuthRules(map[string]any{}, rules)
		if !ok {
			t.Fatal("missing optional claim must pass")
		}
	})

	t.Run("present non-matching rejected", func(t *testing.T) {
		ok, _ := EvaluateAuthRules(map[string]any{"hd": "evil.com"}, rules)
		if ok {
			t.Fatal("non-matching value must be rejected when claim is present")
		}
	})
}

func TestEvaluateAuthRules_BoolClaim(t *testing.T) {
	rules := []AuthRule{
		{Claim: "email_verified", AllowEq: []string{"true"}, Required: true},
	}
	if ok, reason := EvaluateAuthRules(map[string]any{"email_verified": true}, rules); !ok {
		t.Fatalf("expected pass for bool true, got %s", reason)
	}
	if ok, _ := EvaluateAuthRules(map[string]any{"email_verified": false}, rules); ok {
		t.Fatal("expected reject for bool false")
	}
}

func TestEvaluateAuthRules_PresenceOnly(t *testing.T) {
	rules := []AuthRule{{Claim: "oid", Required: true}}
	if ok, _ := EvaluateAuthRules(map[string]any{"oid": "abc"}, rules); !ok {
		t.Fatal("presence-only rule should pass when claim is present")
	}
	if ok, _ := EvaluateAuthRules(map[string]any{}, rules); ok {
		t.Fatal("presence-only required rule must reject when claim absent")
	}
}

func TestParseRule(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    AuthRule
		wantErr bool
	}{
		{
			name:  "named with required",
			input: "azure_tid=tid:abc,def,REQUIRED",
			want:  AuthRule{Claim: "tid", AllowEq: []string{"abc", "def"}, Required: true},
		},
		{
			name:  "unnamed single value",
			input: "hd:scitrera.com",
			want:  AuthRule{Claim: "hd", AllowEq: []string{"scitrera.com"}},
		},
		{
			name:  "presence only",
			input: "oid:,REQUIRED",
			want:  AuthRule{Claim: "oid", Required: true},
		},
		{
			name:    "missing colon",
			input:   "tid",
			wantErr: true,
		},
		{
			name:    "empty claim",
			input:   ":abc",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRule(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Claim != tc.want.Claim || got.Required != tc.want.Required {
				t.Errorf("Claim/Required mismatch: got %+v, want %+v", got, tc.want)
			}
			if len(got.AllowEq) != len(tc.want.AllowEq) {
				t.Errorf("AllowEq len: got %d, want %d (%+v)", len(got.AllowEq), len(tc.want.AllowEq), got.AllowEq)
			}
			for i := range got.AllowEq {
				if got.AllowEq[i] != tc.want.AllowEq[i] {
					t.Errorf("AllowEq[%d]: got %q, want %q", i, got.AllowEq[i], tc.want.AllowEq[i])
				}
			}
		})
	}
}

func TestNoOpResolver(t *testing.T) {
	r := NoOpResolver{TenantID: "acme"}
	if r.Name() != "noop" {
		t.Errorf("Name: got %q, want %q", r.Name(), "noop")
	}
	resolved, err := r.Resolve(t.Context(), ResolverInput{})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if resolved.DefaultTenantID != "acme" {
		t.Errorf("DefaultTenantID: got %q, want acme", resolved.DefaultTenantID)
	}
	if len(resolved.TenantIDs) != 1 || resolved.TenantIDs[0] != "acme" {
		t.Errorf("TenantIDs: got %v, want [acme]", resolved.TenantIDs)
	}
}

func TestNoOpResolver_EmptyTenantID(t *testing.T) {
	r := NoOpResolver{}
	resolved, err := r.Resolve(t.Context(), ResolverInput{})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if resolved.DefaultTenantID != "" {
		t.Errorf("expected empty tenant id")
	}
	if resolved.TenantIDs != nil {
		t.Errorf("expected nil TenantIDs when no tenant configured, got %v", resolved.TenantIDs)
	}
}
