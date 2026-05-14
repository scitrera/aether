// Package registry defines the storage interface for the agent-implementation
// catalog (agent_registry table) and the live orchestrator-profile fleet
// (orchestrator_profiles table). The two surfaces are bundled into a single
// Store interface because their consumers are always co-resident on the
// gateway: agent dispatch needs to look up an implementation's launch params
// AND pick an orchestrator that supports the implementation's profile, in the
// same call path.
//
// Stage 1 consumers (callers that depend on this interface today):
//   - cmd/gateway/main.go        — constructs the postgres-backed impl with
//     a Redis-backed ProfileStateStore.
//   - cmd/aetherlite/main.go     — constructs the same postgres-backed impl
//     against the sqlite_compat handle, with
//     a Badger-backed ProfileStateStore (until
//     Stage 2 introduces a native sqlite
//     sibling).
//   - internal/gateway/server.go — holds the registry.Store handle, threads
//     it through to admin handlers, the agent
//     dispatch path, and the heartbeat reaper.
//
// The interface intentionally mirrors the legacy
// *internal/registry.AgentRegistry and *internal/registry.OrchestratorProfileManager
// method sets one-for-one (composed under one type). This is the
// mechanical-extraction phase of the storage refactor described in
// `.slop/20260513_native-storage-interfaces.md` §2/§3/§13: postgres impl is
// byte-for-byte the same logic, just re-homed behind an interface so a
// future sqlite-native sibling (Stage 2) can drop in without churning
// callers.
//
// The shipped *internal/registry.ProfileStateStore interface (with Redis +
// Badger impls) is the canonical precedent for this whole refactor. It
// stays in `internal/registry/` — moving it would be churn for no benefit.
// The new registry.Store depends on it as a constructor parameter, accessed
// through the registry.ProfileStateStore type alias defined in types.go.
package registry

import (
	"context"
	"time"
)

// Store is the registry surface consumed by the gateway. It covers the
// agent-implementation catalog (Register/Get/Exists/List/Delete/GetLaunchParams)
// and the live orchestrator-profile fleet (RegisterProfiles, heartbeat,
// selection, listing, cleanup).
//
// Nil-tolerance policy (§14.1 of the storage-interfaces plan): callers MUST
// pass a non-nil implementation. Agent dispatch and orchestrator selection
// are load-bearing — a silent nil-deref hazard (the chat-message SIGSEGV
// pattern that inspired this refactor) is unacceptable here. There is no
// defensible "opt-out" mode for the registry: a deployment that doesn't
// register agents simply has an empty catalog. No NoOp impl is provided in
// this domain.
//
// Lifecycle: the underlying *sql.DB handle is owned by the caller; nothing
// on Store closes it. The ProfileStateStore passed at construction is
// likewise caller-owned. Methods are safe for concurrent use across
// goroutines (they delegate to *sql.DB which manages its own pool).
type Store interface {
	// =========================================================================
	// Agent registry (agent_registry table)
	// =========================================================================

	// Register upserts an agent implementation into the catalog. Validates
	// that reg.LaunchParams contains a "profile" key (returns
	// *errors.ProfileRequiredError otherwise) and stamps CreatedAt /
	// UpdatedAt on the supplied struct.
	Register(ctx context.Context, reg *AgentRegistration) error

	// Get fetches a registration by implementation name. If implementation
	// contains a trailing ":specifier" suffix it is stripped before the
	// lookup. Returns *errors.AgentNotFoundError on miss.
	Get(ctx context.Context, implementation string) (*AgentRegistration, error)

	// Exists reports whether a registration exists for the given
	// implementation name (or its base, with ":specifier" stripped).
	Exists(ctx context.Context, implementation string) (bool, error)

	// List returns all registrations, optionally filtered to those whose
	// launch_params.profile equals the supplied profile string. Pass ""
	// for no filter. Results are ordered by implementation name.
	List(ctx context.Context, profile string) ([]*AgentRegistration, error)

	// Delete removes a registration by exact implementation name (no
	// suffix stripping). Returns *errors.AgentNotFoundError if no row
	// matched, or a wrapped error if a foreign-key constraint blocks the
	// delete (dependent tasks reference the agent).
	Delete(ctx context.Context, implementation string) error

	// GetLaunchParams returns the launch_params map for the given
	// implementation. If implementation includes a ":specifier" suffix,
	// the lookup prefers an exact-match row, falling back to the base
	// (stripped) name — letting per-specifier overrides take precedence
	// when they exist.
	GetLaunchParams(ctx context.Context, implementation string) (map[string]interface{}, error)

	// =========================================================================
	// Orchestrator profile fleet (orchestrator_profiles table)
	// =========================================================================

	// RegisterProfiles atomically replaces the set of profiles supported
	// by orchestratorID with the supplied list, stamping each row's
	// last_heartbeat to NOW(). Empty workspace defaults to
	// models.SystemWorkspace. Called when an orchestrator connects via
	// InitConnection.
	RegisterProfiles(ctx context.Context, orchestratorID string, profiles []string, workspace string) error

	// UnregisterOrchestrator deletes every profile row for the given
	// orchestratorID. Called on orchestrator disconnect.
	UnregisterOrchestrator(ctx context.Context, orchestratorID string) error

	// UpdateHeartbeat bumps last_heartbeat = NOW() for every row owned by
	// orchestratorID. Returns an error if no row matched (the
	// orchestrator never registered, or its profiles were already
	// reaped).
	UpdateHeartbeat(ctx context.Context, orchestratorID string) error

	// GetActiveOrchestratorsForProfile returns orchestrator IDs that
	// support profile and whose last_heartbeat is within the 60-second
	// staleness window. Empty workspace matches models.SystemWorkspace.
	// The 60-second window is hard-coded today; CleanupStaleProfiles is
	// the operator-facing knob.
	GetActiveOrchestratorsForProfile(ctx context.Context, profile string, workspace string) ([]string, error)

	// SelectOrchestrator picks one orchestrator from the active set for
	// (profile, workspace) using round-robin counter state. The counter
	// lives in the ProfileStateStore passed at construction (Redis or
	// Badger). On state-store failure the impl falls back to an
	// in-process counter — so selection always succeeds when at least
	// one orchestrator is live. Returns *errors.OrchestratorNotFoundError
	// when no live orchestrator supports the profile.
	SelectOrchestrator(ctx context.Context, profile string, workspace string) (string, error)

	// GetOrchestratorProfiles returns every profile row owned by
	// orchestratorID, ordered by profile_name. Used by admin surfaces
	// (no heartbeat filter — surfaces stale rows too).
	GetOrchestratorProfiles(ctx context.Context, orchestratorID string) ([]*OrchestratorProfile, error)

	// ListAllProfiles returns every profile row whose last_heartbeat is
	// within the 60-second staleness window, ordered by
	// (profile_name, orchestrator_id).
	ListAllProfiles(ctx context.Context) ([]*OrchestratorProfile, error)

	// OrchestratorSupportsProfile reports whether the given orchestrator
	// has registered the given profile (no heartbeat filter).
	OrchestratorSupportsProfile(ctx context.Context, orchestratorID string, profile string) (bool, error)

	// CleanupStaleProfiles deletes orchestrator_profiles rows whose
	// last_heartbeat is older than maxAge, returning the number of rows
	// removed. Intended to run on a periodic reaper goroutine
	// (typically every ~minute).
	CleanupStaleProfiles(ctx context.Context, maxAge time.Duration) (int64, error)
}
