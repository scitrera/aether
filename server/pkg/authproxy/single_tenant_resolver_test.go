package authproxy

import (
	"context"
	"net/http"
	"testing"

	"github.com/scitrera/aether/pkg/models"
)

func mustParseSpec(t *testing.T, s string) AuthRuleSpec {
	t.Helper()
	spec, err := parseAuthRuleSpec(s)
	if err != nil {
		t.Fatalf("parseAuthRuleSpec(%q): %v", s, err)
	}
	return spec
}

func TestSingleTenant_PassThroughForAPIKey(t *testing.T) {
	r := NewSingleTenantResolver("acme", []AuthRuleSpec{
		mustParseSpec(t, "azure_entra=tid:expected,REQUIRED"),
	}, nil)

	resolved, err := r.Resolve(context.Background(), ResolverInput{
		Identity: models.Identity{Type: models.PrincipalUser, ID: "svc-1"},
		Method:   "api_key",
		// No claims — would fail rules if they applied.
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved.Reject != nil {
		t.Fatalf("api_key must bypass rules; got reject %+v", resolved.Reject)
	}
	if resolved.DefaultTenantID != "acme" {
		t.Errorf("DefaultTenantID: got %q", resolved.DefaultTenantID)
	}
}

func TestSingleTenant_AzureTID_AcceptedWhenMatch(t *testing.T) {
	r := NewSingleTenantResolver("acme", []AuthRuleSpec{
		mustParseSpec(t, "azure_entra=tid:beeaef61-22c3-4e35-a140-05d2b6615d37,REQUIRED"),
	}, nil)

	resolved, err := r.Resolve(context.Background(), ResolverInput{
		Identity: models.Identity{Type: models.PrincipalUser, ID: "user-1"},
		Method:   "azure_entra",
		Claims: map[string]any{
			"tid":   "beeaef61-22c3-4e35-a140-05d2b6615d37",
			"oid":   "user-oid",
			"email": "alice@scitrera.com",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved.Reject != nil {
		t.Fatalf("expected accept, got reject %+v", resolved.Reject)
	}
}

func TestSingleTenant_AzureTID_RejectedOnMismatch(t *testing.T) {
	r := NewSingleTenantResolver("acme", []AuthRuleSpec{
		mustParseSpec(t, "azure_entra=tid:beeaef61-22c3-4e35-a140-05d2b6615d37,REQUIRED"),
	}, nil)

	resolved, err := r.Resolve(context.Background(), ResolverInput{
		Identity: models.Identity{Type: models.PrincipalUser, ID: "user-1"},
		Method:   "azure_entra",
		Claims: map[string]any{
			"tid": "00000000-0000-0000-0000-000000000000",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved.Reject == nil {
		t.Fatal("expected reject for foreign tid")
	}
	if resolved.Reject.Status != http.StatusForbidden {
		t.Errorf("status: got %d, want 403", resolved.Reject.Status)
	}
	if resolved.Reject.Code != "auth_rule_failed" {
		t.Errorf("code: got %q", resolved.Reject.Code)
	}
}

func TestSingleTenant_RuleScopedByMethod(t *testing.T) {
	// "tid" rule scoped to azure_entra only — must not run against oauth.
	r := NewSingleTenantResolver("acme", []AuthRuleSpec{
		mustParseSpec(t, "azure_entra=tid:expected,REQUIRED"),
	}, nil)

	resolved, err := r.Resolve(context.Background(), ResolverInput{
		Identity: models.Identity{Type: models.PrincipalUser, ID: "user-1"},
		Method:   "oauth",
		Claims: map[string]any{
			"sub": "google-sub",
			"hd":  "scitrera.com",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved.Reject != nil {
		t.Fatalf("scoped rule must not run against other method; got reject %+v", resolved.Reject)
	}
}

func TestSingleTenant_GoogleHD(t *testing.T) {
	r := NewSingleTenantResolver("acme", []AuthRuleSpec{
		mustParseSpec(t, "oauth=hd:scitrera.com"),
	}, nil)

	t.Run("matching hd", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "oauth",
			Claims: map[string]any{"hd": "scitrera.com"},
		})
		if resolved.Reject != nil {
			t.Fatalf("expected accept, got %+v", resolved.Reject)
		}
	})

	t.Run("non-matching hd", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "oauth",
			Claims: map[string]any{"hd": "evil.com"},
		})
		if resolved.Reject == nil {
			t.Fatal("expected reject for foreign hd")
		}
	})
}

func TestSingleTenant_EmailVerified_BoolClaim(t *testing.T) {
	r := NewSingleTenantResolver("acme", []AuthRuleSpec{
		mustParseSpec(t, "email_verified:true,REQUIRED"),
	}, nil)

	t.Run("verified", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "oauth",
			Claims: map[string]any{"email_verified": true},
		})
		if resolved.Reject != nil {
			t.Fatalf("expected accept, got %+v", resolved.Reject)
		}
	})

	t.Run("unverified", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "oauth",
			Claims: map[string]any{"email_verified": false},
		})
		if resolved.Reject == nil {
			t.Fatal("expected reject for unverified email")
		}
	})
}

func TestSingleTenant_EmailDomainGate(t *testing.T) {
	r := NewSingleTenantResolver("acme", nil, []string{"scitrera.com", "example.com"})

	t.Run("allowed", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "oauth",
			Claims: map[string]any{"email": "alice@scitrera.com"},
		})
		if resolved.Reject != nil {
			t.Fatalf("expected accept, got %+v", resolved.Reject)
		}
	})

	t.Run("disallowed", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "oauth",
			Claims: map[string]any{"email": "evil@evil.com"},
		})
		if resolved.Reject == nil || resolved.Reject.Code != "domain_not_allowed" {
			t.Fatalf("expected domain_not_allowed reject, got %+v", resolved.Reject)
		}
	})

	t.Run("missing email", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "oauth",
			Claims: map[string]any{},
		})
		if resolved.Reject == nil || resolved.Reject.Code != "missing_email" {
			t.Fatalf("expected missing_email reject, got %+v", resolved.Reject)
		}
	})

	t.Run("upn fallback for azure", func(t *testing.T) {
		// Azure tokens often carry "upn" or "preferred_username" instead of "email".
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "azure_entra",
			Claims: map[string]any{"upn": "alice@example.com"},
		})
		if resolved.Reject != nil {
			t.Fatalf("expected upn fallback to allow domain, got %+v", resolved.Reject)
		}
	})

	t.Run("api_key bypasses domain gate", func(t *testing.T) {
		resolved, _ := r.Resolve(context.Background(), ResolverInput{
			Method: "api_key",
		})
		if resolved.Reject != nil {
			t.Fatalf("api_key must bypass domain gate, got %+v", resolved.Reject)
		}
	})
}

func TestSingleTenant_LoadFromEnv(t *testing.T) {
	t.Setenv("AUTH_PROXY_AUTH_RULE_AZURE_TID", "azure_entra=tid:expected-tid,REQUIRED")
	t.Setenv("AUTH_PROXY_AUTH_RULE_VERIFIED", "email_verified:true,REQUIRED")
	t.Setenv("AUTH_PROXY_ALLOWED_EMAIL_DOMAINS", "scitrera.com, example.com ")

	r, err := LoadSingleTenantResolverFromEnv("acme")
	if err != nil {
		t.Fatalf("LoadSingleTenantResolverFromEnv: %v", err)
	}
	if len(r.rules) != 2 {
		t.Errorf("expected 2 rules, got %d", len(r.rules))
	}
	if len(r.allowedEmailDomains) != 2 {
		t.Errorf("expected 2 domains, got %v", r.allowedEmailDomains)
	}

	// Verify the parsed rule scopes.
	var hasAzureScoped, hasUnscoped bool
	for _, spec := range r.rules {
		if spec.Method == "azure_entra" && spec.Rule.Claim == "tid" {
			hasAzureScoped = true
		}
		if spec.Method == "" && spec.Rule.Claim == "email_verified" {
			hasUnscoped = true
		}
	}
	if !hasAzureScoped {
		t.Errorf("expected an azure_entra-scoped tid rule")
	}
	if !hasUnscoped {
		t.Errorf("expected an unscoped email_verified rule")
	}
}

func TestParseAuthRuleSpec_LeadingMethodScope(t *testing.T) {
	cases := []struct {
		input      string
		wantMethod string
		wantClaim  string
	}{
		{"azure_entra=tid:abc,REQUIRED", "azure_entra", "tid"},
		{"oauth=hd:scitrera.com", "oauth", "hd"},
		{"*=email_verified:true,REQUIRED", "*", "email_verified"},
		// no leading method=
		{"hd:scitrera.com", "", "hd"},
		{"email_verified:true,REQUIRED", "", "email_verified"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			spec, err := parseAuthRuleSpec(tc.input)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if spec.Method != tc.wantMethod {
				t.Errorf("method: got %q, want %q", spec.Method, tc.wantMethod)
			}
			if spec.Rule.Claim != tc.wantClaim {
				t.Errorf("claim: got %q, want %q", spec.Rule.Claim, tc.wantClaim)
			}
		})
	}
}
