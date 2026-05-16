// Package sqlite provides the native-sqlite implementation of registry.Store.
// It is the Stage 2 counterpart to the postgres wrapper in
// internal/storage/registry/postgres/store.go: same interface, sqlite-native
// SQL, no dbcompat translation layer.
//
// Design decisions:
//
//   - Single *sql.DB handle with SetMaxOpenConns(1) to serialize writes and
//     avoid SQLITE_BUSY in WAL mode (per plan section 14.3). Reads are
//     serialized through the same connection but this is acceptable for
//     the registry domain's low QPS.
//
//   - Timestamps are stored as ISO-8601 TEXT via strftime('%Y-%m-%dT%H:%M:%fZ',
//     'now') in the schema defaults. The implementation formats/parses
//     time.Time inline using the same layout (no driver-level coercion).
//
//   - JSON columns (launch_params) are stored as TEXT and queried with
//     json_extract where needed.
//
//   - The Store struct embeds two sub-implementations (agentRegistry and
//     profileManager) that mirror the legacy AgentRegistry +
//     OrchestratorProfileManager split. Method promotion satisfies the
//     registry.Store interface with zero forwarders, matching the shape
//     of the postgres wrapper.
//
//   - The bare "sqlite" driver (modernc.org/sqlite) is used directly --
//     not "sqlite_compat". This is correct for Stage 2 native impls
//     because we own all our own SQL and do our own timestamp parsing
//     inline (per plan section 15.4).
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	internalregistry "github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/storage/registry"
	"github.com/scitrera/aether/pkg/errors"
	"github.com/scitrera/aether/pkg/models"

	_ "modernc.org/sqlite" // register bare "sqlite" driver
)

// timestampLayout is the canonical ISO-8601 format used for all timestamp
// storage and retrieval. It matches the strftime format in the migration
// schema: strftime('%Y-%m-%dT%H:%M:%fZ', 'now').
const timestampLayout = "2006-01-02T15:04:05.000Z"

// additionalTimestampLayouts are fallback formats for parsing timestamps
// that may have been written by other code paths (e.g. CURRENT_TIMESTAMP
// which produces "YYYY-MM-DD HH:MM:SS" in sqlite).
var additionalTimestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05",
}

// parseTimestamp parses a TEXT timestamp from sqlite into time.Time.
// Tries the canonical layout first, then fallbacks.
func parseTimestamp(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	t, err := time.Parse(timestampLayout, s)
	if err == nil {
		return t, nil
	}
	for _, layout := range additionalTimestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse timestamp %q", s)
}

// nowTimestamp returns the current time formatted in the canonical layout.
func nowTimestamp() string {
	return time.Now().UTC().Format(timestampLayout)
}

// encodePhase5JSON marshals one of the Phase 5 columns (resource_schema /
// capabilities / extensions) to JSON, returning nil — which the database/sql
// driver writes as SQL NULL — when v is empty/nil. This matches the postgres
// path's behavior so the two backends store the same wire representation.
func encodePhase5JSON(v interface{}) (interface{}, error) {
	switch val := v.(type) {
	case nil:
		return nil, nil
	case []registry.AgentResourceSchemaEntry:
		if len(val) == 0 {
			return nil, nil
		}
	case map[string]bool:
		if len(val) == 0 {
			return nil, nil
		}
	case []string:
		if len(val) == 0 {
			return nil, nil
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// scanPhase5JSON decodes the resource_schema / capabilities / extensions
// columns from a row scan into the AgentRegistration. NULL or empty-string
// columns leave the destination fields at their zero values.
func scanPhase5JSON(reg *registry.AgentRegistration, resourceSchema, capabilities, extensions sql.NullString) error {
	if resourceSchema.Valid && resourceSchema.String != "" {
		if err := json.Unmarshal([]byte(resourceSchema.String), &reg.ResourceSchema); err != nil {
			return fmt.Errorf("failed to unmarshal resource_schema: %w", err)
		}
	}
	if capabilities.Valid && capabilities.String != "" {
		if err := json.Unmarshal([]byte(capabilities.String), &reg.Capabilities); err != nil {
			return fmt.Errorf("failed to unmarshal capabilities: %w", err)
		}
	}
	if extensions.Valid && extensions.String != "" {
		if err := json.Unmarshal([]byte(extensions.String), &reg.Extensions); err != nil {
			return fmt.Errorf("failed to unmarshal extensions: %w", err)
		}
	}
	return nil
}

// Store is the native-sqlite registry store. It struct-embeds two
// sub-implementations so their method sets are promoted directly onto
// Store, satisfying registry.Store with zero forwarders.
//
// This mirrors the shape of the postgres wrapper (which embeds
// *legacy.AgentRegistry and *legacy.OrchestratorProfileManager).
type Store struct {
	*agentRegistry
	*profileManager
}

// Compile-time conformance asserts.
var _ registry.Store = (*Store)(nil)
var _ internalregistry.KVSetter = (*Store)(nil)

// SetRegistryKV implements internalregistry.KVSetter. Passing nil disables
// KV propagation (the default). Safe to call after construction and
// concurrently with Register/Delete.
func (s *Store) SetRegistryKV(kv jetstream.KeyValue) {
	s.agentRegistry.kvMu.Lock()
	defer s.agentRegistry.kvMu.Unlock()
	s.agentRegistry.kv = kv
}

// New constructs a native-sqlite registry Store. The caller provides:
//   - db: an already-opened *sql.DB using the bare "sqlite" driver, pointed
//     at the registry.db file. The caller retains ownership; nothing on
//     Store closes it.
//   - profileState: the ProfileStateStore backing SelectOrchestrator's
//     round-robin counter (Badger in lite, Redis in full).
//
// New runs the per-domain migration set from migrations/sqlite_registry/
// against db before returning. It also enforces SetMaxOpenConns(1) on the
// handle to serialize writes (per plan section 14.3).
func New(db *sql.DB, profileState registry.ProfileStateStore, migrationFS embed.FS) (*Store, error) {
	// Enforce single-writer to prevent SQLITE_BUSY in WAL mode.
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := applyMigrations(ctx, db, migrationFS); err != nil {
		return nil, fmt.Errorf("registry sqlite migrations: %w", err)
	}

	return &Store{
		agentRegistry:  &agentRegistry{db: db},
		profileManager: &profileManager{db: db, state: profileState, profileRR: make(map[string]int64)},
	}, nil
}

// =============================================================================
// Migration runner
// =============================================================================

func applyMigrations(ctx context.Context, db *sql.DB, fs embed.FS) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read embed fs: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version,
		).Scan(&count); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}
		content, err := fs.ReadFile(entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("exec %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO schema_migrations (version) VALUES (?)", version,
		); err != nil {
			return fmt.Errorf("record %s: %w", version, err)
		}
	}
	return nil
}

// =============================================================================
// Agent Registry implementation
// =============================================================================

type agentRegistry struct {
	db   *sql.DB
	kvMu sync.RWMutex
	kv   jetstream.KeyValue // nil means KV propagation disabled
}

func (ar *agentRegistry) Register(ctx context.Context, reg *registry.AgentRegistration) error {
	if _, ok := reg.LaunchParams["profile"]; !ok {
		return &errors.ProfileRequiredError{}
	}

	// Phase 5 Stage B: reject self-conflicts (same prefix declared twice in
	// one schema) before opening the transaction; matches the postgres path.
	if err := validateSqliteResourceSchemaSelfDistinct(reg.ResourceSchema); err != nil {
		return err
	}

	launchParamsJSON, err := json.Marshal(reg.LaunchParams)
	if err != nil {
		return fmt.Errorf("failed to marshal launch_params: %w", err)
	}

	// Phase 5 columns are nullable: encode empty/nil as SQL NULL so legacy
	// registrations don't carry sentinel "[]"/"{}" literals.
	resourceSchemaArg, err := encodePhase5JSON(reg.ResourceSchema)
	if err != nil {
		return fmt.Errorf("failed to marshal resource_schema: %w", err)
	}
	capabilitiesArg, err := encodePhase5JSON(reg.Capabilities)
	if err != nil {
		return fmt.Errorf("failed to marshal capabilities: %w", err)
	}
	extensionsArg, err := encodePhase5JSON(reg.Extensions)
	if err != nil {
		return fmt.Errorf("failed to marshal extensions: %w", err)
	}

	now := nowTimestamp()

	// Phase 5 Stage B: open a tx so the prefix-uniqueness scan and the upsert
	// commit together. SQLite has a single-writer model (we set
	// SetMaxOpenConns(1) in New()), so transactions provide strict
	// serialization — no two Register calls can interleave between the
	// scan and the write.
	tx, err := ar.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build the set of prefixes declared by the incoming registration so we
	// can early-out the scan if none are declared.
	if len(reg.ResourceSchema) > 0 {
		// Scan the existing rows (excluding self) and detect any prefix
		// collision. We do this in Go because sqlite's JSON1 path-query
		// syntax is verbose for "array contains an object with key=value"
		// and the registry's row count is small (~10s of rows in
		// production).
		rows, err := tx.QueryContext(ctx, `
			SELECT implementation, resource_schema
			FROM agent_registry
			WHERE implementation != ?
			  AND resource_schema IS NOT NULL
		`, reg.Implementation)
		if err != nil {
			return fmt.Errorf("failed to scan existing resource schemas: %w", err)
		}

		// Build a map of declared prefix -> implementation across the live
		// table, then check each incoming prefix against it.
		existingPrefixes := make(map[string]string)
		for rows.Next() {
			var impl string
			var schemaJSON sql.NullString
			if err := rows.Scan(&impl, &schemaJSON); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan registry row: %w", err)
			}
			if !schemaJSON.Valid || schemaJSON.String == "" {
				continue
			}
			var entries []registry.AgentResourceSchemaEntry
			if err := json.Unmarshal([]byte(schemaJSON.String), &entries); err != nil {
				rows.Close()
				return fmt.Errorf("failed to unmarshal existing resource_schema for %q: %w", impl, err)
			}
			for _, e := range entries {
				if e.ResourceTypePrefix == "" {
					continue
				}
				existingPrefixes[e.ResourceTypePrefix] = impl
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("error iterating existing registrations: %w", err)
		}
		rows.Close()

		for _, entry := range reg.ResourceSchema {
			if entry.ResourceTypePrefix == "" {
				continue
			}
			if owner, claimed := existingPrefixes[entry.ResourceTypePrefix]; claimed {
				return &errors.ResourceTypePrefixConflictError{
					Implementation: reg.Implementation,
					Prefix:         entry.ResourceTypePrefix,
					Existing:       owner,
				}
			}
		}
	}

	// Upsert: INSERT OR REPLACE would reset created_at on update.
	// Use INSERT ... ON CONFLICT ... DO UPDATE to preserve created_at.
	query := `
		INSERT INTO agent_registry (implementation, launch_params, description, resource_schema, capabilities, extensions, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (implementation)
		DO UPDATE SET
			launch_params = excluded.launch_params,
			description = excluded.description,
			resource_schema = excluded.resource_schema,
			capabilities = excluded.capabilities,
			extensions = excluded.extensions,
			updated_at = excluded.updated_at
		RETURNING created_at, updated_at
	`

	var createdStr, updatedStr string
	err = tx.QueryRowContext(ctx, query,
		reg.Implementation,
		string(launchParamsJSON),
		reg.Description,
		resourceSchemaArg,
		capabilitiesArg,
		extensionsArg,
		now,
		now,
	).Scan(&createdStr, &updatedStr)
	if err != nil {
		return fmt.Errorf("failed to register agent: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit agent registration: %w", err)
	}

	// Best-effort KV propagation for cross-gateway PrefixIndex sync.
	// SQLite is canonical; KV publish failure must not block or fail the caller.
	ar.kvMu.RLock()
	kv := ar.kv
	ar.kvMu.RUnlock()
	if kv != nil {
		if err := internalregistry.PublishAgent(ctx, kv, reg); err != nil {
			slog.Warn("registry: KV propagation failed after Register; continuing",
				"implementation", reg.Implementation,
				"error", err,
			)
		}
	}

	reg.CreatedAt, err = parseTimestamp(createdStr)
	if err != nil {
		return fmt.Errorf("failed to parse created_at: %w", err)
	}
	reg.UpdatedAt, err = parseTimestamp(updatedStr)
	if err != nil {
		return fmt.Errorf("failed to parse updated_at: %w", err)
	}

	return nil
}

// validateSqliteResourceSchemaSelfDistinct mirrors the postgres path's
// validateResourceSchemaSelfDistinct (defined in internal/registry) — kept as
// a separate function here to avoid a dependency on the legacy registry
// package from the sqlite native implementation.
func validateSqliteResourceSchemaSelfDistinct(schema []registry.AgentResourceSchemaEntry) error {
	if len(schema) < 2 {
		return nil
	}
	seen := make(map[string]struct{}, len(schema))
	for _, e := range schema {
		if e.ResourceTypePrefix == "" {
			continue
		}
		if _, dup := seen[e.ResourceTypePrefix]; dup {
			return &errors.ResourceTypePrefixConflictError{
				Prefix:   e.ResourceTypePrefix,
				Existing: "(self)",
			}
		}
		seen[e.ResourceTypePrefix] = struct{}{}
	}
	return nil
}

func (ar *agentRegistry) Get(ctx context.Context, implementation string) (*registry.AgentRegistration, error) {
	if idx := strings.LastIndex(implementation, ":"); idx > 0 {
		implementation = implementation[:idx]
	}
	query := `
		SELECT implementation, launch_params, description, resource_schema, capabilities, extensions, created_at, updated_at
		FROM agent_registry
		WHERE implementation = ?
	`

	var reg registry.AgentRegistration
	var launchParamsJSON string
	var resourceSchemaJSON, capabilitiesJSON, extensionsJSON sql.NullString
	var createdStr, updatedStr string

	err := ar.db.QueryRowContext(ctx, query, implementation).Scan(
		&reg.Implementation,
		&launchParamsJSON,
		&reg.Description,
		&resourceSchemaJSON,
		&capabilitiesJSON,
		&extensionsJSON,
		&createdStr,
		&updatedStr,
	)
	if err == sql.ErrNoRows {
		return nil, &errors.AgentNotFoundError{Implementation: implementation}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get agent: %w", err)
	}

	if err := json.Unmarshal([]byte(launchParamsJSON), &reg.LaunchParams); err != nil {
		return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
	}
	if err := scanPhase5JSON(&reg, resourceSchemaJSON, capabilitiesJSON, extensionsJSON); err != nil {
		return nil, err
	}

	reg.CreatedAt, err = parseTimestamp(createdStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse created_at: %w", err)
	}
	reg.UpdatedAt, err = parseTimestamp(updatedStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse updated_at: %w", err)
	}

	return &reg, nil
}

func (ar *agentRegistry) Exists(ctx context.Context, implementation string) (bool, error) {
	if idx := strings.LastIndex(implementation, ":"); idx > 0 {
		implementation = implementation[:idx]
	}
	query := `SELECT EXISTS(SELECT 1 FROM agent_registry WHERE implementation = ?)`
	var exists bool
	err := ar.db.QueryRowContext(ctx, query, implementation).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check agent existence: %w", err)
	}
	return exists, nil
}

func (ar *agentRegistry) List(ctx context.Context, profile string) ([]*registry.AgentRegistration, error) {
	var query string
	var args []interface{}

	if profile != "" {
		// Use json_extract instead of postgres's ->> operator.
		query = `
			SELECT implementation, launch_params, description, resource_schema, capabilities, extensions, created_at, updated_at
			FROM agent_registry
			WHERE json_extract(launch_params, '$.profile') = ?
			ORDER BY implementation
		`
		args = []interface{}{profile}
	} else {
		query = `
			SELECT implementation, launch_params, description, resource_schema, capabilities, extensions, created_at, updated_at
			FROM agent_registry
			ORDER BY implementation
		`
	}

	rows, err := ar.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list agents: %w", err)
	}
	defer rows.Close()

	var registrations []*registry.AgentRegistration
	for rows.Next() {
		var reg registry.AgentRegistration
		var launchParamsJSON string
		var resourceSchemaJSON, capabilitiesJSON, extensionsJSON sql.NullString
		var createdStr, updatedStr string

		if err := rows.Scan(
			&reg.Implementation,
			&launchParamsJSON,
			&reg.Description,
			&resourceSchemaJSON,
			&capabilitiesJSON,
			&extensionsJSON,
			&createdStr,
			&updatedStr,
		); err != nil {
			return nil, fmt.Errorf("failed to scan agent row: %w", err)
		}

		if err := json.Unmarshal([]byte(launchParamsJSON), &reg.LaunchParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
		}
		if err := scanPhase5JSON(&reg, resourceSchemaJSON, capabilitiesJSON, extensionsJSON); err != nil {
			return nil, err
		}

		var parseErr error
		reg.CreatedAt, parseErr = parseTimestamp(createdStr)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse created_at: %w", parseErr)
		}
		reg.UpdatedAt, parseErr = parseTimestamp(updatedStr)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse updated_at: %w", parseErr)
		}

		registrations = append(registrations, &reg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating agent rows: %w", err)
	}

	return registrations, nil
}

func (ar *agentRegistry) Delete(ctx context.Context, implementation string) error {
	query := `DELETE FROM agent_registry WHERE implementation = ?`

	result, err := ar.db.ExecContext(ctx, query, implementation)
	if err != nil {
		// SQLite uses error string matching for constraint violations since
		// there's no typed error like pq.Error. Check for FOREIGN KEY.
		if strings.Contains(err.Error(), "FOREIGN KEY") {
			return fmt.Errorf("cannot delete agent: has dependent tasks or references")
		}
		return fmt.Errorf("failed to delete agent: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return &errors.AgentNotFoundError{Implementation: implementation}
	}

	// Best-effort KV propagation for cross-gateway PrefixIndex sync.
	ar.kvMu.RLock()
	kv := ar.kv
	ar.kvMu.RUnlock()
	if kv != nil {
		if err := internalregistry.DeleteAgent(ctx, kv, implementation); err != nil {
			slog.Warn("registry: KV propagation failed after Delete; continuing",
				"implementation", implementation,
				"error", err,
			)
		}
	}

	return nil
}

func (ar *agentRegistry) GetLaunchParams(ctx context.Context, implementation string) (map[string]interface{}, error) {
	baseImpl := implementation
	if idx := strings.LastIndex(implementation, ":"); idx > 0 {
		baseImpl = implementation[:idx]
	}

	// Native sqlite: use two separate ? placeholders with explicit binding
	// for the IN and ORDER BY CASE. This avoids the repeated-$N problem
	// that dbcompat's rewriter struggled with (plan section 15.4).
	query := `SELECT launch_params FROM agent_registry
		WHERE implementation IN (?, ?)
		ORDER BY CASE WHEN implementation = ? THEN 0 ELSE 1 END
		LIMIT 1`

	var launchParamsJSON string
	err := ar.db.QueryRowContext(ctx, query, implementation, baseImpl, implementation).Scan(&launchParamsJSON)
	if err == sql.ErrNoRows {
		return nil, &errors.AgentNotFoundError{Implementation: implementation}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get launch params: %w", err)
	}

	var launchParams map[string]interface{}
	if err := json.Unmarshal([]byte(launchParamsJSON), &launchParams); err != nil {
		return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
	}

	return launchParams, nil
}

// =============================================================================
// Orchestrator Profile Manager implementation
// =============================================================================

type profileManager struct {
	db        *sql.DB
	state     registry.ProfileStateStore
	mu        sync.RWMutex
	profileRR map[string]int64 // In-memory fallback when state store fails
}

func (pm *profileManager) RegisterProfiles(ctx context.Context, orchestratorID string, profiles []string, workspace string) error {
	if len(profiles) == 0 {
		return fmt.Errorf("no profiles provided for orchestrator %s", orchestratorID)
	}
	if workspace == "" {
		workspace = models.SystemWorkspace
	}

	tx, err := pm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing profiles for this orchestrator.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM orchestrator_profiles WHERE orchestrator_id = ?`,
		orchestratorID,
	); err != nil {
		return fmt.Errorf("failed to clear existing profiles: %w", err)
	}

	now := nowTimestamp()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO orchestrator_profiles (id, orchestrator_id, profile_name, workspace, last_heartbeat)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, profile := range profiles {
		id := uuid.New().String()
		if _, err := stmt.ExecContext(ctx, id, orchestratorID, profile, workspace, now); err != nil {
			return fmt.Errorf("failed to register profile %s: %w", profile, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (pm *profileManager) UnregisterOrchestrator(ctx context.Context, orchestratorID string) error {
	_, err := pm.db.ExecContext(ctx,
		`DELETE FROM orchestrator_profiles WHERE orchestrator_id = ?`,
		orchestratorID,
	)
	if err != nil {
		return fmt.Errorf("failed to unregister orchestrator: %w", err)
	}
	return nil
}

func (pm *profileManager) UpdateHeartbeat(ctx context.Context, orchestratorID string) error {
	now := nowTimestamp()
	result, err := pm.db.ExecContext(ctx,
		`UPDATE orchestrator_profiles SET last_heartbeat = ? WHERE orchestrator_id = ?`,
		now, orchestratorID,
	)
	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("orchestrator %s not found", orchestratorID)
	}

	return nil
}

func (pm *profileManager) GetActiveOrchestratorsForProfile(ctx context.Context, profile string, workspace string) ([]string, error) {
	if workspace == "" {
		workspace = models.SystemWorkspace
	}

	// Native sqlite: compute the staleness cutoff in Go and compare as
	// TEXT. ISO-8601 timestamps are lexicographically sortable, so a
	// simple string comparison against the 60-second-ago cutoff is
	// semantically equivalent to the postgres INTERVAL expression.
	cutoff := time.Now().UTC().Add(-60 * time.Second).Format(timestampLayout)

	query := `
		SELECT orchestrator_id
		FROM orchestrator_profiles
		WHERE profile_name = ?
		  AND (workspace = ? OR workspace = '_system')
		  AND last_heartbeat > ?
		ORDER BY orchestrator_id
	`

	rows, err := pm.db.QueryContext(ctx, query, profile, workspace, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query orchestrators: %w", err)
	}
	defer rows.Close()

	var orchestrators []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan orchestrator ID: %w", err)
		}
		orchestrators = append(orchestrators, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating orchestrators: %w", err)
	}

	return orchestrators, nil
}

func (pm *profileManager) SelectOrchestrator(ctx context.Context, profile string, workspace string) (string, error) {
	orchestrators, err := pm.GetActiveOrchestratorsForProfile(ctx, profile, workspace)
	if err != nil {
		return "", err
	}
	if len(orchestrators) == 0 {
		return "", &errors.OrchestratorNotFoundError{
			Profile:   profile,
			Workspace: workspace,
		}
	}

	key := fmt.Sprintf("orch:rr:%s:%s", workspace, profile)
	counter, err := pm.state.Incr(ctx, key)
	if err != nil {
		// Fallback to in-memory round-robin if state store fails.
		pm.mu.Lock()
		counter = pm.profileRR[key]
		pm.profileRR[key]++
		pm.mu.Unlock()
	}

	index := int(counter) % len(orchestrators)
	return orchestrators[index], nil
}

func (pm *profileManager) GetOrchestratorProfiles(ctx context.Context, orchestratorID string) ([]*registry.OrchestratorProfile, error) {
	query := `
		SELECT orchestrator_id, profile_name, workspace, last_heartbeat
		FROM orchestrator_profiles
		WHERE orchestrator_id = ?
		ORDER BY profile_name
	`

	rows, err := pm.db.QueryContext(ctx, query, orchestratorID)
	if err != nil {
		return nil, fmt.Errorf("failed to query orchestrator profiles: %w", err)
	}
	defer rows.Close()

	var profiles []*registry.OrchestratorProfile
	for rows.Next() {
		var p registry.OrchestratorProfile
		var hbStr string
		if err := rows.Scan(&p.OrchestratorID, &p.ProfileName, &p.Workspace, &hbStr); err != nil {
			return nil, fmt.Errorf("failed to scan profile: %w", err)
		}
		p.LastHeartbeat, err = parseTimestamp(hbStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse last_heartbeat: %w", err)
		}
		profiles = append(profiles, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating profiles: %w", err)
	}

	return profiles, nil
}

func (pm *profileManager) ListAllProfiles(ctx context.Context) ([]*registry.OrchestratorProfile, error) {
	// Native sqlite: compute the 60-second staleness cutoff in Go.
	cutoff := time.Now().UTC().Add(-60 * time.Second).Format(timestampLayout)

	query := `
		SELECT orchestrator_id, profile_name, workspace, last_heartbeat
		FROM orchestrator_profiles
		WHERE last_heartbeat > ?
		ORDER BY profile_name, orchestrator_id
	`

	rows, err := pm.db.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to list profiles: %w", err)
	}
	defer rows.Close()

	var profiles []*registry.OrchestratorProfile
	for rows.Next() {
		var p registry.OrchestratorProfile
		var hbStr string
		if err := rows.Scan(&p.OrchestratorID, &p.ProfileName, &p.Workspace, &hbStr); err != nil {
			return nil, fmt.Errorf("failed to scan profile: %w", err)
		}
		p.LastHeartbeat, err = parseTimestamp(hbStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse last_heartbeat: %w", err)
		}
		profiles = append(profiles, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating profiles: %w", err)
	}

	return profiles, nil
}

func (pm *profileManager) OrchestratorSupportsProfile(ctx context.Context, orchestratorID string, profile string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM orchestrator_profiles
			WHERE orchestrator_id = ? AND profile_name = ?
		)
	`
	var exists bool
	err := pm.db.QueryRowContext(ctx, query, orchestratorID, profile).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check profile support: %w", err)
	}
	return exists, nil
}

func (pm *profileManager) CleanupStaleProfiles(ctx context.Context, maxAge time.Duration) (int64, error) {
	// Native sqlite: compute the cutoff timestamp in Go rather than
	// relying on postgres's interval arithmetic.
	cutoff := time.Now().UTC().Add(-maxAge).Format(timestampLayout)

	query := `DELETE FROM orchestrator_profiles WHERE last_heartbeat < ?`

	result, err := pm.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup stale profiles: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}
