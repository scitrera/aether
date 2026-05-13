package kv

import (
	"fmt"
	"strings"

	"github.com/scitrera/aether/pkg/models"
)

// KVScope represents the visibility scope of KV data.
//
// Conceptually a scope has two orthogonal axes:
//   - Identity scope: global / workspace / user / user-workspace
//   - Sharing:        shared (cross-agent) / exclusive (per-agent)
//
// The string-typed KVScope below is the canonical wire+ACL+metrics
// label for each of the eight (identity x sharing) cells. ScopeSpec
// is the structured representation used internally by the namespace
// builder, parser, and Store API.
type KVScope string

const (
	// Shared scopes (no agent identity in Redis namespace).
	ScopeGlobal              KVScope = "global"
	ScopeWorkspace           KVScope = "workspace"
	ScopeUserShared          KVScope = "user-shared"
	ScopeUserWorkspaceShared KVScope = "user-workspace-shared"

	// Per-agent (exclusive) scopes (agent impl|spec embedded in namespace).
	ScopeGlobalExclusive    KVScope = "global-exclusive"
	ScopeWorkspaceExclusive KVScope = "workspace-exclusive"
	ScopeUser               KVScope = "user"
	ScopeUserWorkspace      KVScope = "user-workspace"
)

// IdentityScope identifies the visibility tier of a KV entry.
type IdentityScope int

const (
	IdentityScopeGlobal IdentityScope = iota
	IdentityScopeWorkspace
	IdentityScopeUser
	IdentityScopeUserWorkspace
)

// Sharing identifies whether a scope cell is cross-agent shared
// or agent-private (exclusive).
type Sharing int

const (
	SharingShared Sharing = iota
	SharingExclusive
)

// ScopeSpec is the structured form of a KV scope. The Canonical()
// string is what flows through ACL resource IDs, metrics labels, and
// audit metadata.
type ScopeSpec struct {
	Identity IdentityScope
	Sharing  Sharing
}

// Convenience constructors for the eight matrix cells.
var (
	SpecGlobal              = ScopeSpec{Identity: IdentityScopeGlobal, Sharing: SharingShared}
	SpecGlobalExclusive     = ScopeSpec{Identity: IdentityScopeGlobal, Sharing: SharingExclusive}
	SpecWorkspace           = ScopeSpec{Identity: IdentityScopeWorkspace, Sharing: SharingShared}
	SpecWorkspaceExclusive  = ScopeSpec{Identity: IdentityScopeWorkspace, Sharing: SharingExclusive}
	SpecUser                = ScopeSpec{Identity: IdentityScopeUser, Sharing: SharingExclusive}
	SpecUserShared          = ScopeSpec{Identity: IdentityScopeUser, Sharing: SharingShared}
	SpecUserWorkspace       = ScopeSpec{Identity: IdentityScopeUserWorkspace, Sharing: SharingExclusive}
	SpecUserWorkspaceShared = ScopeSpec{Identity: IdentityScopeUserWorkspace, Sharing: SharingShared}
)

// Canonical returns the canonical scope string used by ACL, metrics,
// and audit. Note USER/USER_WORKSPACE keep their original (per-agent)
// labels for backward compatibility with rules and dashboards.
func (s ScopeSpec) Canonical() KVScope {
	switch s.Identity {
	case IdentityScopeGlobal:
		if s.Sharing == SharingExclusive {
			return ScopeGlobalExclusive
		}
		return ScopeGlobal
	case IdentityScopeWorkspace:
		if s.Sharing == SharingExclusive {
			return ScopeWorkspaceExclusive
		}
		return ScopeWorkspace
	case IdentityScopeUser:
		if s.Sharing == SharingShared {
			return ScopeUserShared
		}
		return ScopeUser
	case IdentityScopeUserWorkspace:
		if s.Sharing == SharingShared {
			return ScopeUserWorkspaceShared
		}
		return ScopeUserWorkspace
	}
	return ""
}

// String makes ScopeSpec usable as a fmt %s and metrics label.
func (s ScopeSpec) String() string { return string(s.Canonical()) }

// ScopeSpecFromKVScope converts a canonical KVScope string into ScopeSpec.
// Returns false if the input is not a recognized scope.
func ScopeSpecFromKVScope(s KVScope) (ScopeSpec, bool) {
	switch s {
	case ScopeGlobal:
		return SpecGlobal, true
	case ScopeGlobalExclusive:
		return SpecGlobalExclusive, true
	case ScopeWorkspace:
		return SpecWorkspace, true
	case ScopeWorkspaceExclusive:
		return SpecWorkspaceExclusive, true
	case ScopeUser:
		return SpecUser, true
	case ScopeUserShared:
		return SpecUserShared, true
	case ScopeUserWorkspace:
		return SpecUserWorkspace, true
	case ScopeUserWorkspaceShared:
		return SpecUserWorkspaceShared, true
	}
	return ScopeSpec{}, false
}

// BuildNamespace constructs a namespaced key for agent KV storage.
//
// Two distinct shapes by scope (per Solution A — see
// `docs/specification.md` §KV and the OSS issue trail for context):
//
//   - SHARED scopes (global, workspace) DROP the agent-identity prefix
//     so every agent in the same tenant (global) or same workspace
//     (workspace) sees the same storage key. This is the semantic the
//     scope names already imply ("global" means tenant-wide, "workspace"
//     means workspace-shared) and the only way one agent's writes are
//     ever visible to another agent's reads. ACL is the gate: callers
//     must have grants on `kv_scope:{global,workspace}` (and optionally
//     `kv_key:{key}` for per-key restrictions).
//
//   - PER-AGENT scopes (user, user-workspace) KEEP the
//     `kv:agent:{impl}|{spec}:` prefix because per-user data really IS
//     per-agent — e.g., one agent's per-user notebook tab state should
//     not collide with another agent's per-user data even when the
//     userID matches. These remain isolated by storage layout.
//
// Inside a single segment we use "|" to split impl from spec because
// both may legitimately contain "." or "::". "|" does not appear in
// Python FQNs, emails, or workspace slugs, and is a valid Redis char.
//
// Examples:
//   - kv:global                                          (shared)
//   - kv:ws:production                                   (shared, by workspace)
//   - kv:agent:python-worker|v1:user:alice               (per-agent, per-user)
//   - kv:agent:python-worker|v1:user:alice:ws:production (per-agent, per-user-per-ws)
func BuildNamespace(agent models.Identity, scope KVScope, userID string, workspace string) string {
	spec, ok := ScopeSpecFromKVScope(scope)
	if !ok {
		// Unknown scope — fall through to a debug-friendly default that
		// won't collide with any legitimate namespace. Callers should
		// always use a canonical KVScope value.
		impl := sanitizeNamespaceComponent(agent.Implementation)
		spc := sanitizeNamespaceComponent(agent.Specifier)
		return fmt.Sprintf("kv:agent:%s|%s", impl, spc)
	}
	return BuildNamespaceSpec(agent, spec, userID, workspace)
}

// BuildNamespaceSpec is the structured-form variant of BuildNamespace.
// It accepts the explicit (Identity x Sharing) tuple directly.
func BuildNamespaceSpec(agent models.Identity, spec ScopeSpec, userID, workspace string) string {
	impl := sanitizeNamespaceComponent(agent.Implementation)
	spc := sanitizeNamespaceComponent(agent.Specifier)
	uid := sanitizeNamespaceComponent(userID)
	ws := sanitizeNamespaceComponent(workspace)

	exclusive := spec.Sharing == SharingExclusive
	switch spec.Identity {
	case IdentityScopeGlobal:
		if exclusive {
			return fmt.Sprintf("kv:agent:%s|%s:global", impl, spc)
		}
		return "kv:global"
	case IdentityScopeWorkspace:
		if exclusive {
			return fmt.Sprintf("kv:agent:%s|%s:ws:%s", impl, spc, ws)
		}
		return fmt.Sprintf("kv:ws:%s", ws)
	case IdentityScopeUser:
		if exclusive {
			return fmt.Sprintf("kv:agent:%s|%s:user:%s", impl, spc, uid)
		}
		return fmt.Sprintf("kv:user:%s", uid)
	case IdentityScopeUserWorkspace:
		if exclusive {
			return fmt.Sprintf("kv:agent:%s|%s:user:%s:ws:%s", impl, spc, uid, ws)
		}
		return fmt.Sprintf("kv:user:%s:ws:%s", uid, ws)
	}
	return fmt.Sprintf("kv:agent:%s|%s", impl, spc)
}

func sanitizeNamespaceComponent(c string) string {
	c = strings.ReplaceAll(c, ":", "_")
	c = strings.ReplaceAll(c, "|", "_")
	return c
}

// ParseNamespace extracts components from a namespaced key.
//
// Recognized shapes (mirror BuildNamespace):
//   - SHARED:    kv:global                                     → no agent info
//     kv:ws:{workspace}                              → no agent info
//   - PER-AGENT: kv:agent:{impl}|{spec}:user:{userID}           → impl/spec set
//     kv:agent:{impl}|{spec}:user:{userID}:ws:{ws}   → impl/spec set
//
// For shared scopes “agentImpl“ and “agentSpec“ are returned as the
// empty string (callers that care about ownership should use IsOwner,
// which knows how to interpret an empty agent identity).
//
// Returns: agentImpl, agentSpec, scope, userID, workspace, error.
func ParseNamespace(key string) (agentImpl, agentSpec string, scope KVScope, userID, workspace string, err error) {
	parts := strings.Split(key, ":")
	if len(parts) < 2 || parts[0] != "kv" {
		return "", "", "", "", "", fmt.Errorf("invalid namespace format: %s", key)
	}

	switch parts[1] {
	case "global":
		// kv:global  (shared global)
		if len(parts) != 2 {
			return "", "", "", "", "", fmt.Errorf("invalid global namespace format: %s", key)
		}
		return "", "", ScopeGlobal, "", "", nil

	case "ws":
		// kv:ws:{workspace}  (shared workspace)
		if len(parts) != 3 {
			return "", "", "", "", "", fmt.Errorf("workspace namespace requires exactly one workspace segment: %s", key)
		}
		return "", "", ScopeWorkspace, "", parts[2], nil

	case "user":
		// kv:user:{uid}                  → user-shared
		// kv:user:{uid}:ws:{workspace}   → user-workspace-shared
		if len(parts) == 3 {
			return "", "", ScopeUserShared, parts[2], "", nil
		}
		if len(parts) == 5 && parts[3] == "ws" {
			return "", "", ScopeUserWorkspaceShared, parts[2], parts[4], nil
		}
		return "", "", "", "", "", fmt.Errorf("invalid shared user namespace format: %s", key)

	case "agent":
		// fall through to per-agent shape parsing
	default:
		return "", "", "", "", "", fmt.Errorf("unknown namespace prefix: %s", parts[1])
	}

	// Per-agent (exclusive) shapes: kv:agent:{impl}|{spec}:...
	if len(parts) < 4 {
		return "", "", "", "", "", fmt.Errorf("invalid per-agent namespace format: %s", key)
	}
	implSpec := strings.SplitN(parts[2], "|", 2)
	if len(implSpec) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid agent specifier format: %s", parts[2])
	}
	agentImpl = implSpec[0]
	agentSpec = implSpec[1]

	switch parts[3] {
	case "global":
		// kv:agent:{i}|{s}:global → global-exclusive
		if len(parts) != 4 {
			return "", "", "", "", "", fmt.Errorf("invalid exclusive global namespace format: %s", key)
		}
		return agentImpl, agentSpec, ScopeGlobalExclusive, "", "", nil

	case "ws":
		// kv:agent:{i}|{s}:ws:{workspace} → workspace-exclusive
		if len(parts) != 5 {
			return "", "", "", "", "", fmt.Errorf("invalid exclusive workspace namespace format: %s", key)
		}
		return agentImpl, agentSpec, ScopeWorkspaceExclusive, "", parts[4], nil

	case "user":
		// kv:agent:{i}|{s}:user:{uid}              → user (exclusive)
		// kv:agent:{i}|{s}:user:{uid}:ws:{ws}      → user-workspace (exclusive)
		if len(parts) < 5 {
			return "", "", "", "", "", fmt.Errorf("invalid per-agent user namespace format: %s", key)
		}
		userID = parts[4]
		if len(parts) == 5 {
			return agentImpl, agentSpec, ScopeUser, userID, "", nil
		}
		if len(parts) == 7 && parts[5] == "ws" {
			return agentImpl, agentSpec, ScopeUserWorkspace, userID, parts[6], nil
		}
		return "", "", "", "", "", fmt.Errorf("invalid per-agent user-workspace namespace format: %s", key)

	default:
		return "", "", "", "", "", fmt.Errorf("unknown per-agent namespace segment: %s", parts[3])
	}
}

// IsOwner checks if an identity owns a given KV namespace.
//
// Returns true only when the namespace is one of the four EXCLUSIVE
// shapes AND the embedded `impl|spec` matches the caller. Shared
// shapes (global, workspace, user-shared, user-workspace-shared) have
// no structural owner — those are gated by ACL on the scope/key, not
// by storage-layer ownership. We return false for shared namespaces so
// callers don't accidentally bypass ACL with an "I wrote it" check.
func IsOwner(identity models.Identity, namespace string) bool {
	agentImpl, agentSpec, _, _, _, err := ParseNamespace(namespace)
	if err != nil {
		return false
	}
	if agentImpl == "" || agentSpec == "" {
		// Shared scope. No structural owner.
		return false
	}
	return identity.Implementation == agentImpl && identity.Specifier == agentSpec
}

// ScopeFromString converts a string to KVScope. Recognized inputs are
// the eight canonical labels; anything else falls back to ScopeGlobal
// for safety in legacy code paths.
func ScopeFromString(s string) KVScope {
	switch KVScope(s) {
	case ScopeGlobal,
		ScopeGlobalExclusive,
		ScopeWorkspace,
		ScopeWorkspaceExclusive,
		ScopeUser,
		ScopeUserShared,
		ScopeUserWorkspace,
		ScopeUserWorkspaceShared:
		return KVScope(s)
	default:
		return ScopeGlobal
	}
}

// String returns the string representation of a KVScope.
func (s KVScope) String() string {
	return string(s)
}

// IsExclusive reports whether the scope embeds the agent identity in
// its Redis namespace (i.e., the per-agent variant). Used by the ACL
// owner fast-path to skip DB lookup when the caller writes within
// their own per-agent namespace.
func (s KVScope) IsExclusive() bool {
	switch s {
	case ScopeGlobalExclusive, ScopeWorkspaceExclusive, ScopeUser, ScopeUserWorkspace:
		return true
	}
	return false
}

// ValidateScopeConfig checks that the scope/userID/workspace tuple is
// valid for the given caller identity. For exclusive scopes the caller
// MUST have non-empty Implementation/Specifier — otherwise two empty-
// identity service callers would collide on `kv:agent:|:global`.
func ValidateScopeConfig(scope KVScope, identity models.Identity, userID, workspace string) error {
	spec, ok := ScopeSpecFromKVScope(scope)
	if !ok {
		return fmt.Errorf("unknown scope: %s", scope)
	}
	return ValidateScopeSpec(spec, identity, userID, workspace)
}

// ValidateScopeSpec is the ScopeSpec-typed counterpart to
// ValidateScopeConfig.
func ValidateScopeSpec(spec ScopeSpec, identity models.Identity, userID, workspace string) error {
	switch spec.Identity {
	case IdentityScopeGlobal:
		// no extra requirements on identity-scope axis
	case IdentityScopeWorkspace:
		if workspace == "" {
			return fmt.Errorf("workspace scope requires a workspace")
		}
	case IdentityScopeUser:
		if userID == "" {
			return fmt.Errorf("user scope requires a user ID")
		}
	case IdentityScopeUserWorkspace:
		if userID == "" || workspace == "" {
			return fmt.Errorf("user-workspace scope requires both user ID and workspace")
		}
	default:
		return fmt.Errorf("unknown identity scope: %d", spec.Identity)
	}
	if spec.Sharing == SharingExclusive {
		if identity.Implementation == "" || identity.Specifier == "" {
			return fmt.Errorf("exclusive scopes require non-empty agent implementation and specifier")
		}
	}
	return nil
}
