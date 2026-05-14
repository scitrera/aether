// Package postgres provides the postgres-backed implementation of
// registry.Store. It is the Stage 1 wrapper that exposes the existing
// internal/registry.AgentRegistry + internal/registry.OrchestratorProfileManager
// logic behind the new storage interface without any behavior change.
//
// Both legacy types already implement, between them, every method on
// registry.Store one-for-one (same signatures, same returns). Their method
// sets are disjoint (no name collisions between AgentRegistry and
// OrchestratorProfileManager), so struct embedding promotes them into a
// single Store type that satisfies the interface with zero forwarders.
//
// Stage 2 of the storage-interfaces plan (see
// `.slop/20260513_native-storage-interfaces.md` §3) introduces a sibling
// package `internal/storage/registry/sqlite` with a native-sqlite Store
// that satisfies the same registry.Store interface. At that point
// AetherLite stops going through dbcompat for registry reads/writes. The
// interface stays unchanged across that transition.
package postgres

import (
	"database/sql"

	legacy "github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/storage/registry"
)

// Store is the postgres-backed registry store. It struct-embeds the two
// legacy types so their method sets are promoted directly onto Store. No
// wrapping methods are written — the embedded types' implementations are
// already byte-for-byte correct for the interface contract.
//
// Method-collision audit: AgentRegistry exposes
// {Register, Get, Exists, List, Delete, GetLaunchParams};
// OrchestratorProfileManager exposes
// {RegisterProfiles, UnregisterOrchestrator, UpdateHeartbeat,
// GetActiveOrchestratorsForProfile, SelectOrchestrator,
// GetOrchestratorProfiles, ListAllProfiles, OrchestratorSupportsProfile,
// CleanupStaleProfiles}. No name appears in both sets, so struct embedding
// resolves cleanly with no ambiguous-selector errors.
type Store struct {
	*legacy.AgentRegistry
	*legacy.OrchestratorProfileManager
}

// New constructs a postgres-backed registry Store on top of the given
// *sql.DB. The stateStore powers SelectOrchestrator's round-robin counter
// across gateway instances — pass NewRedisProfileStateStore for the full
// gateway path, NewBadgerProfileStateStore for AetherLite. Both impls
// already ship in the legacy internal/registry package; nothing new is
// reinvented here.
//
// Callers retain ownership of db and stateStore; nothing on Store closes
// either of them.
func New(db *sql.DB, stateStore registry.ProfileStateStore) *Store {
	return &Store{
		AgentRegistry:              legacy.NewAgentRegistry(db),
		OrchestratorProfileManager: legacy.NewOrchestratorProfileManager(db, stateStore),
	}
}

// Compile-time conformance assert. This is the load-bearing check that
// registry.Store and *Store agree on the full method set. If a method is
// added to registry.Store or its signature changes, the build breaks here.
var _ registry.Store = (*Store)(nil)
