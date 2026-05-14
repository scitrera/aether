package registry

// This file re-exports the shared registry types from the legacy
// internal/registry package under the new internal/storage/registry
// interface namespace. The legacy package remains the source of truth
// during Stage 1 of the storage-interfaces refactor; Stage 2 will introduce
// a native sqlite sibling and may eventually let us collapse the legacy
// package into this one. For now, downstream callers can
//
//	import "github.com/scitrera/aether/internal/storage/registry"
//
// and find every type, constant, and helper they need to construct registry
// stores and consume their results — no double-import of the legacy package
// required.
//
// ProfileStateStore intentionally STAYS in `internal/registry/`. It is the
// canonical existing precedent for this whole refactor (see Stage 0
// decisions §7) and is already implemented by both
// *RedisProfileStateStore (full gateway) and *BadgerProfileStateStore
// (AetherLite). The new registry.Store depends on it via the constructor
// parameter, accessed through the alias below so callers only import the
// new package.

import (
	legacy "github.com/scitrera/aether/internal/registry"
)

// Core types — aliased so a single import gets callers everything they need.
type (
	// AgentRegistration is one row of the agent_registry table. See
	// legacy.AgentRegistration for field docs.
	AgentRegistration = legacy.AgentRegistration

	// OrchestratorProfile is one row of the orchestrator_profiles table.
	OrchestratorProfile = legacy.OrchestratorProfile

	// ProfileStateStore is the runtime counter substrate used by
	// SelectOrchestrator for round-robin picks across gateway instances.
	// Implementations: legacy.NewRedisProfileStateStore for the full
	// gateway path, legacy.NewBadgerProfileStateStore for AetherLite.
	ProfileStateStore = legacy.ProfileStateStore
)

// Constructors / helpers — re-exported so callers can build state stores
// and registrations without reaching into the legacy package.
var (
	// NewRedisProfileStateStore wraps a redis.UniversalClient as a
	// ProfileStateStore. Used by the full gateway.
	NewRedisProfileStateStore = legacy.NewRedisProfileStateStore

	// NewBadgerProfileStateStore wraps a *badger.DB as a
	// ProfileStateStore. Used by AetherLite.
	NewBadgerProfileStateStore = legacy.NewBadgerProfileStateStore

	// MergeLaunchParams merges default launch params from the registry
	// with caller-supplied overrides; overrides take precedence. Pure
	// helper, no DB dependency.
	MergeLaunchParams = legacy.MergeLaunchParams
)
