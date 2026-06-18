// Package a2abridge contains the protocol-translation primitives shared by
// the a2a-gateway and a2a-sidecar binaries. It maps A2A protocol concepts
// (Message, Task, AgentCard, TaskState) onto aether's stateless message
// transport and persistent task store, and provides the workspace and
// authentication glue both processes need.
package a2abridge

import (
	"fmt"
	"strings"
)

// UnknownTenantBehavior controls what a WorkspaceResolver does when an inbound
// A2A request carries a tenant identifier that is not present in the static
// mapping.
type UnknownTenantBehavior string

const (
	// UnknownTenantReject causes the resolver to return ErrUnknownTenant for
	// any tenant outside the mapping. Use in environments where every A2A
	// caller MUST be explicitly enrolled.
	UnknownTenantReject UnknownTenantBehavior = "reject"

	// UnknownTenantUseDefault causes the resolver to fall back to the
	// configured default workspace for any unmapped tenant. Use for trust-
	// boundary setups where unknown tenants land in a sandbox workspace.
	UnknownTenantUseDefault UnknownTenantBehavior = "use_default"
)

// ErrUnknownTenant is returned by WorkspaceResolver.Resolve when the supplied
// tenant is not in the static map and the configured behavior is
// UnknownTenantReject.
var ErrUnknownTenant = fmt.Errorf("a2abridge: unknown tenant")

// WorkspaceConfig is the operator-facing configuration shape parsed from YAML.
// Field names use snake_case in YAML; see configs/a2a-gateway.example.yaml.
type WorkspaceConfig struct {
	// DefaultWorkspace is the aether workspace used when an inbound A2A
	// request omits a tenant entirely, and (when UnknownBehavior is
	// UnknownTenantUseDefault) when the tenant is not mapped.
	DefaultWorkspace string `yaml:"default_workspace"`

	// Tenants maps A2A tenant identifiers to aether workspace names.
	Tenants map[string]string `yaml:"tenants"`

	// UnknownBehavior controls the fallback for unmapped tenants. Defaults
	// to UnknownTenantReject when empty.
	UnknownBehavior UnknownTenantBehavior `yaml:"unknown_tenant_behavior"`
}

// WorkspaceResolver translates A2A tenant strings into aether workspace
// strings. It is safe for concurrent use; the underlying map is read-only
// after construction.
type WorkspaceResolver struct {
	defaultWorkspace string
	tenants          map[string]string
	unknown          UnknownTenantBehavior
}

// NewWorkspaceResolver validates the config and returns a resolver. It
// rejects configurations that cannot satisfy any request (e.g.
// use_default with no default workspace, or an empty mapping with reject
// behavior — which would refuse every request).
func NewWorkspaceResolver(cfg WorkspaceConfig) (*WorkspaceResolver, error) {
	behavior := cfg.UnknownBehavior
	if behavior == "" {
		behavior = UnknownTenantReject
	}
	if behavior != UnknownTenantReject && behavior != UnknownTenantUseDefault {
		return nil, fmt.Errorf("a2abridge: invalid unknown_tenant_behavior %q", behavior)
	}
	if behavior == UnknownTenantUseDefault && cfg.DefaultWorkspace == "" {
		return nil, fmt.Errorf("a2abridge: unknown_tenant_behavior=use_default requires default_workspace")
	}
	if behavior == UnknownTenantReject && cfg.DefaultWorkspace == "" && len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("a2abridge: workspace config is empty (no default and no tenants)")
	}

	tenants := make(map[string]string, len(cfg.Tenants))
	for k, v := range cfg.Tenants {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" {
			return nil, fmt.Errorf("a2abridge: tenant map contains empty key")
		}
		if v == "" {
			return nil, fmt.Errorf("a2abridge: tenant map for %q has empty workspace", k)
		}
		tenants[k] = v
	}

	return &WorkspaceResolver{
		defaultWorkspace: strings.TrimSpace(cfg.DefaultWorkspace),
		tenants:          tenants,
		unknown:          behavior,
	}, nil
}

// Resolve maps an A2A tenant identifier to an aether workspace. A blank
// tenant always resolves to the default workspace (returning ErrUnknownTenant
// only if no default was configured). A non-blank tenant looks up the map,
// then falls back according to the configured UnknownTenantBehavior.
func (r *WorkspaceResolver) Resolve(tenant string) (string, error) {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		if r.defaultWorkspace == "" {
			return "", fmt.Errorf("%w: empty tenant and no default workspace", ErrUnknownTenant)
		}
		return r.defaultWorkspace, nil
	}
	if ws, ok := r.tenants[tenant]; ok {
		return ws, nil
	}
	if r.unknown == UnknownTenantUseDefault {
		return r.defaultWorkspace, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownTenant, tenant)
}

// DefaultWorkspace returns the configured default workspace, or "" if none.
func (r *WorkspaceResolver) DefaultWorkspace() string {
	return r.defaultWorkspace
}
