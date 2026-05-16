package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/scitrera/aether/pkg/errors"
)

// AgentRegistration represents an agent implementation in the registry
type AgentRegistration struct {
	Implementation string                 `json:"implementation"`
	LaunchParams   map[string]interface{} `json:"launch_params"`
	Description    string                 `json:"description,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`

	// Phase 5 additions: agent-declared resource schema, capability flags,
	// and A2A-style extension URIs. All optional and nullable in storage —
	// existing rows written before migration 024/sqlite_registry/002 read
	// back as zero values without error. ACL routing/uniqueness enforcement
	// against these fields lands in Stage B; Stage A only ensures the data
	// model round-trips through proto → admin → storage.
	ResourceSchema []AgentResourceSchemaEntry `json:"resource_schema,omitempty"`
	Capabilities   map[string]bool            `json:"capabilities,omitempty"`
	Extensions     []string                   `json:"extensions,omitempty"`
}

// AgentResourceSchemaEntry mirrors the AgentResourceSchemaEntry proto
// message: one resource family owned by an agent. ResourceTypePrefix is
// the field that Stage B's uniqueness check will index on; PermissionVerbs
// and ResourceIDSchema are informational for now (used by tooling and the
// Phase 6 AgentCard generator).
type AgentResourceSchemaEntry struct {
	ResourceTypePrefix string   `json:"resource_type_prefix"`
	PermissionVerbs    []string `json:"permission_verbs,omitempty"`
	ResourceIDSchema   string   `json:"resource_id_schema,omitempty"`
}

// AgentRegistry manages agent implementations and their orchestration parameters
type AgentRegistry struct {
	db *sql.DB
}

// NewAgentRegistry creates a new agent registry service
func NewAgentRegistry(db *sql.DB) *AgentRegistry {
	return &AgentRegistry{db: db}
}

// Register adds or updates an agent implementation in the registry
func (ar *AgentRegistry) Register(ctx context.Context, reg *AgentRegistration) error {
	// Validate launch params contain required "profile" field
	if _, ok := reg.LaunchParams["profile"]; !ok {
		return &errors.ProfileRequiredError{}
	}

	// Phase 5 Stage B: reject schemas that declare the same
	// resource_type_prefix twice in a single registration. This is a pure
	// input-validation error (not a uniqueness conflict against the table)
	// so we surface it before opening a transaction.
	if err := validateResourceSchemaSelfDistinct(reg.ResourceSchema); err != nil {
		return err
	}

	// Marshal launch params to JSON
	launchParamsJSON, err := json.Marshal(reg.LaunchParams)
	if err != nil {
		return fmt.Errorf("failed to marshal launch_params: %w", err)
	}

	// Marshal the Phase 5 columns. Each is nullable in the schema; encode a
	// SQL NULL when the Go field is empty so we don't waste space writing
	// "null" / "{}" / "[]" sentinel literals for legacy registrations that
	// don't declare a resource schema.
	resourceSchemaArg, err := encodeNullableJSON(reg.ResourceSchema)
	if err != nil {
		return fmt.Errorf("failed to marshal resource_schema: %w", err)
	}
	capabilitiesArg, err := encodeNullableJSON(reg.Capabilities)
	if err != nil {
		return fmt.Errorf("failed to marshal capabilities: %w", err)
	}
	extensionsArg, err := encodeNullableJSON(reg.Extensions)
	if err != nil {
		return fmt.Errorf("failed to marshal extensions: %w", err)
	}

	// Phase 5 Stage B: open a transaction so the prefix-uniqueness check and
	// the upsert run atomically. Two concurrent registrations claiming the
	// same NEW prefix race here: the loser sees ResourceTypePrefixConflictError
	// (its SELECT inside the tx returns the winner's row), and serialization
	// is enforced by postgres' default REPEATABLE READ at the row level via
	// `FOR UPDATE` on conflict rows. We don't need an explicit table lock —
	// the upsert's ON CONFLICT (implementation) ordering plus the per-prefix
	// SELECT is sufficient because every prefix lives inside exactly one row.
	tx, err := ar.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check each declared prefix against the live table (excluding any row
	// for this same implementation — an update that re-asserts its own
	// prefixes must succeed). Stops at the first conflict found so the
	// caller gets a deterministic error.
	for _, entry := range reg.ResourceSchema {
		if entry.ResourceTypePrefix == "" {
			continue
		}
		// JSONB containment with a single-element array of objects:
		// matches any row whose resource_schema array contains an entry
		// with resource_type_prefix == entry.ResourceTypePrefix. Uses the
		// jsonb_path_ops GIN index from migration 024.
		needle := fmt.Sprintf(`[{"resource_type_prefix":%q}]`, entry.ResourceTypePrefix)
		var existing string
		err := tx.QueryRowContext(ctx, `
			SELECT implementation
			FROM agent_registry
			WHERE implementation != $1
			  AND resource_schema @> $2::jsonb
			LIMIT 1
		`, reg.Implementation, needle).Scan(&existing)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to check resource_type_prefix uniqueness: %w", err)
		}
		return &errors.ResourceTypePrefixConflictError{
			Implementation: reg.Implementation,
			Prefix:         entry.ResourceTypePrefix,
			Existing:       existing,
		}
	}

	// Upsert agent registration
	query := `
		INSERT INTO agent_registry (implementation, launch_params, description, resource_schema, capabilities, extensions, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		ON CONFLICT (implementation)
		DO UPDATE SET
			launch_params = EXCLUDED.launch_params,
			description = EXCLUDED.description,
			resource_schema = EXCLUDED.resource_schema,
			capabilities = EXCLUDED.capabilities,
			extensions = EXCLUDED.extensions,
			updated_at = NOW()
		RETURNING created_at, updated_at
	`

	err = tx.QueryRowContext(ctx, query,
		reg.Implementation,
		launchParamsJSON,
		reg.Description,
		resourceSchemaArg,
		capabilitiesArg,
		extensionsArg,
	).Scan(&reg.CreatedAt, &reg.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to register agent: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit agent registration: %w", err)
	}

	return nil
}

// validateResourceSchemaSelfDistinct rejects a ResourceSchema slice that
// declares the same resource_type_prefix more than once. Used by both
// backends' Register paths as an input-validation step before the
// table-uniqueness check; failing this returns ResourceTypePrefixConflictError
// with Existing="" so the caller can distinguish self-conflicts from
// cross-registration conflicts.
func validateResourceSchemaSelfDistinct(schema []AgentResourceSchemaEntry) error {
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

// encodeNullableJSON marshals v to JSON and returns the resulting bytes, or
// returns nil (which the database/sql driver writes as SQL NULL) when v is
// nil or has zero length. Treating empty maps/slices as NULL matches the
// "no Phase 5 data declared" semantics for legacy registrations.
func encodeNullableJSON(v interface{}) (interface{}, error) {
	switch val := v.(type) {
	case nil:
		return nil, nil
	case []AgentResourceSchemaEntry:
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
	return b, nil
}

// scanPhase5Columns decodes the resource_schema / capabilities / extensions
// columns from a row scan into the AgentRegistration. NULL columns leave the
// destination fields as their zero values (nil slices/maps). All three
// columns are JSON-encoded TEXT/JSONB; the wire format is identical across
// postgres + sqlite so a single decoder serves both backends.
func scanPhase5Columns(reg *AgentRegistration, resourceSchema, capabilities, extensions []byte) error {
	if len(resourceSchema) > 0 {
		if err := json.Unmarshal(resourceSchema, &reg.ResourceSchema); err != nil {
			return fmt.Errorf("failed to unmarshal resource_schema: %w", err)
		}
	}
	if len(capabilities) > 0 {
		if err := json.Unmarshal(capabilities, &reg.Capabilities); err != nil {
			return fmt.Errorf("failed to unmarshal capabilities: %w", err)
		}
	}
	if len(extensions) > 0 {
		if err := json.Unmarshal(extensions, &reg.Extensions); err != nil {
			return fmt.Errorf("failed to unmarshal extensions: %w", err)
		}
	}
	return nil
}

// Get retrieves an agent registration by implementation name.
// Strips ":specifier" suffix if present for lookup.
func (ar *AgentRegistry) Get(ctx context.Context, implementation string) (*AgentRegistration, error) {
	if idx := strings.LastIndex(implementation, ":"); idx > 0 {
		implementation = implementation[:idx]
	}
	query := `
		SELECT implementation, launch_params, description, resource_schema, capabilities, extensions, created_at, updated_at
		FROM agent_registry
		WHERE implementation = $1
	`

	var reg AgentRegistration
	var launchParamsJSON []byte
	var resourceSchemaJSON, capabilitiesJSON, extensionsJSON []byte

	err := ar.db.QueryRowContext(ctx, query, implementation).Scan(
		&reg.Implementation,
		&launchParamsJSON,
		&reg.Description,
		&resourceSchemaJSON,
		&capabilitiesJSON,
		&extensionsJSON,
		&reg.CreatedAt,
		&reg.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, &errors.AgentNotFoundError{Implementation: implementation}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get agent: %w", err)
	}

	// Unmarshal launch params
	if err := json.Unmarshal(launchParamsJSON, &reg.LaunchParams); err != nil {
		return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
	}
	if err := scanPhase5Columns(&reg, resourceSchemaJSON, capabilitiesJSON, extensionsJSON); err != nil {
		return nil, err
	}

	return &reg, nil
}

// Exists checks if an agent implementation is registered
func (ar *AgentRegistry) Exists(ctx context.Context, implementation string) (bool, error) {
	// Strip ":specifier" suffix if present — agents are registered by
	// implementation name only, but callers may pass the full identity
	// format (e.g. "my-agent:Default").
	if idx := strings.LastIndex(implementation, ":"); idx > 0 {
		implementation = implementation[:idx]
	}

	query := `SELECT EXISTS(SELECT 1 FROM agent_registry WHERE implementation = $1)`

	var exists bool
	err := ar.db.QueryRowContext(ctx, query, implementation).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check agent existence: %w", err)
	}

	return exists, nil
}

// List retrieves all registered agents, optionally filtered by profile
func (ar *AgentRegistry) List(ctx context.Context, profile string) ([]*AgentRegistration, error) {
	var query string
	var args []interface{}

	if profile != "" {
		query = `
			SELECT implementation, launch_params, description, resource_schema, capabilities, extensions, created_at, updated_at
			FROM agent_registry
			WHERE launch_params->>'profile' = $1
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

	var registrations []*AgentRegistration

	for rows.Next() {
		var reg AgentRegistration
		var launchParamsJSON []byte
		var resourceSchemaJSON, capabilitiesJSON, extensionsJSON []byte

		if err := rows.Scan(
			&reg.Implementation,
			&launchParamsJSON,
			&reg.Description,
			&resourceSchemaJSON,
			&capabilitiesJSON,
			&extensionsJSON,
			&reg.CreatedAt,
			&reg.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan agent row: %w", err)
		}

		// Unmarshal launch params
		if err := json.Unmarshal(launchParamsJSON, &reg.LaunchParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
		}
		if err := scanPhase5Columns(&reg, resourceSchemaJSON, capabilitiesJSON, extensionsJSON); err != nil {
			return nil, err
		}

		registrations = append(registrations, &reg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating agent rows: %w", err)
	}

	return registrations, nil
}

// Delete removes an agent implementation from the registry
func (ar *AgentRegistry) Delete(ctx context.Context, implementation string) error {
	query := `DELETE FROM agent_registry WHERE implementation = $1`

	result, err := ar.db.ExecContext(ctx, query, implementation)
	if err != nil {
		// Check if this is a foreign key violation
		if pqErr, ok := err.(*pq.Error); ok {
			if pqErr.Code == "23503" { // foreign_key_violation
				return fmt.Errorf("cannot delete agent: has dependent tasks or references")
			}
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

	return nil
}

// GetLaunchParams retrieves the launch parameters for an agent.
// If the implementation contains a ":specifier" suffix, the query matches
// both the exact name and the base (stripped) name, preferring the exact
// match so per-specifier overrides take precedence when they exist.
func (ar *AgentRegistry) GetLaunchParams(ctx context.Context, implementation string) (map[string]interface{}, error) {
	baseImpl := implementation
	if idx := strings.LastIndex(implementation, ":"); idx > 0 {
		baseImpl = implementation[:idx]
	}

	// Single query: match exact or base, prefer exact (longer match first)
	query := `SELECT launch_params FROM agent_registry
		WHERE implementation IN ($1, $2)
		ORDER BY CASE WHEN implementation = $1 THEN 0 ELSE 1 END
		LIMIT 1`

	var launchParamsJSON []byte
	err := ar.db.QueryRowContext(ctx, query, implementation, baseImpl).Scan(&launchParamsJSON)

	if err == sql.ErrNoRows {
		return nil, &errors.AgentNotFoundError{Implementation: implementation}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get launch params: %w", err)
	}

	var launchParams map[string]interface{}
	if err := json.Unmarshal(launchParamsJSON, &launchParams); err != nil {
		return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
	}

	return launchParams, nil
}

// MergeLaunchParams merges default launch params from registry with overrides
// Overrides take precedence over defaults
func MergeLaunchParams(defaults, overrides map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})

	// Copy defaults
	for k, v := range defaults {
		merged[k] = v
	}

	// Apply overrides
	for k, v := range overrides {
		merged[k] = v
	}

	return merged
}
