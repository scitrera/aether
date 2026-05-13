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

	// Marshal launch params to JSON
	launchParamsJSON, err := json.Marshal(reg.LaunchParams)
	if err != nil {
		return fmt.Errorf("failed to marshal launch_params: %w", err)
	}

	// Upsert agent registration
	query := `
		INSERT INTO agent_registry (implementation, launch_params, description, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
		ON CONFLICT (implementation)
		DO UPDATE SET
			launch_params = EXCLUDED.launch_params,
			description = EXCLUDED.description,
			updated_at = NOW()
		RETURNING created_at, updated_at
	`

	err = ar.db.QueryRowContext(ctx, query,
		reg.Implementation,
		launchParamsJSON,
		reg.Description,
	).Scan(&reg.CreatedAt, &reg.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to register agent: %w", err)
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
		SELECT implementation, launch_params, description, created_at, updated_at
		FROM agent_registry
		WHERE implementation = $1
	`

	var reg AgentRegistration
	var launchParamsJSON []byte

	err := ar.db.QueryRowContext(ctx, query, implementation).Scan(
		&reg.Implementation,
		&launchParamsJSON,
		&reg.Description,
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
			SELECT implementation, launch_params, description, created_at, updated_at
			FROM agent_registry
			WHERE launch_params->>'profile' = $1
			ORDER BY implementation
		`
		args = []interface{}{profile}
	} else {
		query = `
			SELECT implementation, launch_params, description, created_at, updated_at
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

		if err := rows.Scan(
			&reg.Implementation,
			&launchParamsJSON,
			&reg.Description,
			&reg.CreatedAt,
			&reg.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan agent row: %w", err)
		}

		// Unmarshal launch params
		if err := json.Unmarshal(launchParamsJSON, &reg.LaunchParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal launch_params: %w", err)
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
