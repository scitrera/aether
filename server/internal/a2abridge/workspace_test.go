package a2abridge

import (
	"errors"
	"testing"
)

func TestWorkspaceResolver_Reject(t *testing.T) {
	r, err := NewWorkspaceResolver(WorkspaceConfig{
		DefaultWorkspace: "aether-default",
		Tenants: map[string]string{
			"a2a-prod": "aether-prod",
			"a2a-dev":  "aether-dev",
		},
		UnknownBehavior: UnknownTenantReject,
	})
	if err != nil {
		t.Fatalf("NewWorkspaceResolver: %v", err)
	}

	tests := []struct {
		tenant string
		want   string
		errIs  error
	}{
		{"a2a-prod", "aether-prod", nil},
		{"a2a-dev", "aether-dev", nil},
		{"", "aether-default", nil},
		{"a2a-unknown", "", ErrUnknownTenant},
	}
	for _, tc := range tests {
		got, err := r.Resolve(tc.tenant)
		if tc.errIs != nil {
			if !errors.Is(err, tc.errIs) {
				t.Errorf("Resolve(%q): want err %v, got %v", tc.tenant, tc.errIs, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("Resolve(%q): unexpected error %v", tc.tenant, err)
			continue
		}
		if got != tc.want {
			t.Errorf("Resolve(%q): want %q, got %q", tc.tenant, tc.want, got)
		}
	}
}

func TestWorkspaceResolver_UseDefault(t *testing.T) {
	r, err := NewWorkspaceResolver(WorkspaceConfig{
		DefaultWorkspace: "sandbox",
		Tenants:          map[string]string{"a2a-prod": "aether-prod"},
		UnknownBehavior:  UnknownTenantUseDefault,
	})
	if err != nil {
		t.Fatalf("NewWorkspaceResolver: %v", err)
	}

	got, err := r.Resolve("a2a-prod")
	if err != nil || got != "aether-prod" {
		t.Errorf("Resolve mapped tenant: want (aether-prod, nil), got (%q, %v)", got, err)
	}
	got, err = r.Resolve("a2a-mystery")
	if err != nil || got != "sandbox" {
		t.Errorf("Resolve unmapped tenant: want (sandbox, nil), got (%q, %v)", got, err)
	}
	got, err = r.Resolve("")
	if err != nil || got != "sandbox" {
		t.Errorf("Resolve empty tenant: want (sandbox, nil), got (%q, %v)", got, err)
	}
}

func TestWorkspaceResolver_DefaultsBehavior(t *testing.T) {
	// Empty behavior defaults to reject.
	r, err := NewWorkspaceResolver(WorkspaceConfig{
		DefaultWorkspace: "default",
	})
	if err != nil {
		t.Fatalf("NewWorkspaceResolver: %v", err)
	}
	if _, err := r.Resolve("missing"); !errors.Is(err, ErrUnknownTenant) {
		t.Errorf("default behavior should reject unmapped tenants; got %v", err)
	}
}

func TestWorkspaceResolver_InvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  WorkspaceConfig
	}{
		{"use_default without default workspace", WorkspaceConfig{
			UnknownBehavior: UnknownTenantUseDefault,
		}},
		{"reject with empty mapping and no default", WorkspaceConfig{
			UnknownBehavior: UnknownTenantReject,
		}},
		{"invalid behavior", WorkspaceConfig{
			DefaultWorkspace: "x",
			UnknownBehavior:  "ignore",
		}},
		{"empty tenant key", WorkspaceConfig{
			DefaultWorkspace: "x",
			Tenants:          map[string]string{"": "ws"},
		}},
		{"empty workspace value", WorkspaceConfig{
			DefaultWorkspace: "x",
			Tenants:          map[string]string{"t": ""},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewWorkspaceResolver(tc.cfg); err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
