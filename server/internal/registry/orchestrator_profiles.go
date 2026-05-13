package registry

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/pkg/errors"
	"github.com/scitrera/aether/pkg/models"
)

// OrchestratorProfile represents an orchestrator's profile declaration
type OrchestratorProfile struct {
	OrchestratorID string
	ProfileName    string
	Workspace      string
	LastHeartbeat  time.Time
}

// OrchestratorProfileManager manages orchestrator profile registration and selection
type OrchestratorProfileManager struct {
	db        *sql.DB
	redis     redis.UniversalClient
	mu        sync.RWMutex
	profileRR map[string]int64 // Round-robin counters per profile
}

// NewOrchestratorProfileManager creates a new orchestrator profile manager
func NewOrchestratorProfileManager(db *sql.DB, redisClient redis.UniversalClient) *OrchestratorProfileManager {
	return &OrchestratorProfileManager{
		db:        db,
		redis:     redisClient,
		profileRR: make(map[string]int64),
	}
}

// RegisterProfiles registers an orchestrator's supported profiles
// Called when an orchestrator connects with InitConnection message
func (opm *OrchestratorProfileManager) RegisterProfiles(
	ctx context.Context,
	orchestratorID string,
	profiles []string,
	workspace string,
) error {
	if len(profiles) == 0 {
		return fmt.Errorf("no profiles provided for orchestrator %s", orchestratorID)
	}

	// If workspace not specified, use _system for global orchestrators
	if workspace == "" {
		workspace = models.SystemWorkspace
	}

	// Start transaction to register all profiles atomically
	tx, err := opm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing profiles for this orchestrator
	_, err = tx.ExecContext(ctx,
		`DELETE FROM orchestrator_profiles WHERE orchestrator_id = $1`,
		orchestratorID,
	)
	if err != nil {
		return fmt.Errorf("failed to clear existing profiles: %w", err)
	}

	// Insert new profiles
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO orchestrator_profiles (orchestrator_id, profile_name, workspace, last_heartbeat)
		VALUES ($1, $2, $3, NOW())
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, profile := range profiles {
		if _, err := stmt.ExecContext(ctx, orchestratorID, profile, workspace); err != nil {
			return fmt.Errorf("failed to register profile %s: %w", profile, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// UnregisterOrchestrator removes all profiles for an orchestrator
// Called when orchestrator disconnects
func (opm *OrchestratorProfileManager) UnregisterOrchestrator(ctx context.Context, orchestratorID string) error {
	query := `DELETE FROM orchestrator_profiles WHERE orchestrator_id = $1`

	_, err := opm.db.ExecContext(ctx, query, orchestratorID)
	if err != nil {
		return fmt.Errorf("failed to unregister orchestrator: %w", err)
	}

	return nil
}

// UpdateHeartbeat updates the last heartbeat timestamp for an orchestrator
func (opm *OrchestratorProfileManager) UpdateHeartbeat(ctx context.Context, orchestratorID string) error {
	query := `
		UPDATE orchestrator_profiles
		SET last_heartbeat = NOW()
		WHERE orchestrator_id = $1
	`

	result, err := opm.db.ExecContext(ctx, query, orchestratorID)
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

// GetActiveOrchestratorsForProfile returns orchestrators supporting a profile
// Filters out stale heartbeats (older than 60 seconds)
func (opm *OrchestratorProfileManager) GetActiveOrchestratorsForProfile(
	ctx context.Context,
	profile string,
	workspace string,
) ([]string, error) {
	// If workspace not specified, match _system orchestrators
	if workspace == "" {
		workspace = models.SystemWorkspace
	}

	// Get orchestrators with recent heartbeat (within 60 seconds)
	query := `
		SELECT orchestrator_id
		FROM orchestrator_profiles
		WHERE profile_name = $1
		  AND (workspace = $2 OR workspace = '_system')
		  AND last_heartbeat > NOW() - INTERVAL '60 seconds'
		ORDER BY orchestrator_id
	`

	rows, err := opm.db.QueryContext(ctx, query, profile, workspace)
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

// SelectOrchestrator selects an orchestrator for a profile using round-robin
func (opm *OrchestratorProfileManager) SelectOrchestrator(
	ctx context.Context,
	profile string,
	workspace string,
) (string, error) {
	orchestrators, err := opm.GetActiveOrchestratorsForProfile(ctx, profile, workspace)
	if err != nil {
		return "", err
	}

	if len(orchestrators) == 0 {
		return "", &errors.OrchestratorNotFoundError{
			Profile:   profile,
			Workspace: workspace,
		}
	}

	// Use Redis to maintain round-robin counter across gateway instances
	key := fmt.Sprintf("orch:rr:%s:%s", workspace, profile)

	// Increment and get counter
	counter, err := opm.redis.Incr(ctx, key).Result()
	if err != nil {
		// Fallback to in-memory round-robin if Redis fails
		opm.mu.Lock()
		counter = opm.profileRR[key]
		opm.profileRR[key]++
		opm.mu.Unlock()
	}

	// Select orchestrator using modulo
	index := int(counter) % len(orchestrators)
	return orchestrators[index], nil
}

// GetOrchestratorProfiles returns all profiles supported by an orchestrator
func (opm *OrchestratorProfileManager) GetOrchestratorProfiles(
	ctx context.Context,
	orchestratorID string,
) ([]*OrchestratorProfile, error) {
	query := `
		SELECT orchestrator_id, profile_name, workspace, last_heartbeat
		FROM orchestrator_profiles
		WHERE orchestrator_id = $1
		ORDER BY profile_name
	`

	rows, err := opm.db.QueryContext(ctx, query, orchestratorID)
	if err != nil {
		return nil, fmt.Errorf("failed to query orchestrator profiles: %w", err)
	}
	defer rows.Close()

	var profiles []*OrchestratorProfile
	for rows.Next() {
		var profile OrchestratorProfile
		if err := rows.Scan(
			&profile.OrchestratorID,
			&profile.ProfileName,
			&profile.Workspace,
			&profile.LastHeartbeat,
		); err != nil {
			return nil, fmt.Errorf("failed to scan profile: %w", err)
		}
		profiles = append(profiles, &profile)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating profiles: %w", err)
	}

	return profiles, nil
}

// ListAllProfiles returns all active orchestrator profiles
func (opm *OrchestratorProfileManager) ListAllProfiles(ctx context.Context) ([]*OrchestratorProfile, error) {
	query := `
		SELECT orchestrator_id, profile_name, workspace, last_heartbeat
		FROM orchestrator_profiles
		WHERE last_heartbeat > NOW() - INTERVAL '60 seconds'
		ORDER BY profile_name, orchestrator_id
	`

	rows, err := opm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list profiles: %w", err)
	}
	defer rows.Close()

	var profiles []*OrchestratorProfile
	for rows.Next() {
		var profile OrchestratorProfile
		if err := rows.Scan(
			&profile.OrchestratorID,
			&profile.ProfileName,
			&profile.Workspace,
			&profile.LastHeartbeat,
		); err != nil {
			return nil, fmt.Errorf("failed to scan profile: %w", err)
		}
		profiles = append(profiles, &profile)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating profiles: %w", err)
	}

	return profiles, nil
}

// OrchestratorSupportsProfile checks if an orchestrator supports a given profile
func (opm *OrchestratorProfileManager) OrchestratorSupportsProfile(
	ctx context.Context,
	orchestratorID string,
	profile string,
) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM orchestrator_profiles
			WHERE orchestrator_id = $1 AND profile_name = $2
		)
	`

	var exists bool
	err := opm.db.QueryRowContext(ctx, query, orchestratorID, profile).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check profile support: %w", err)
	}

	return exists, nil
}

// CleanupStaleProfiles removes orchestrator profiles with old heartbeats
// Should be called periodically (e.g., every minute)
func (opm *OrchestratorProfileManager) CleanupStaleProfiles(ctx context.Context, maxAge time.Duration) (int64, error) {
	query := `
		DELETE FROM orchestrator_profiles
		WHERE last_heartbeat < NOW() - $1::interval
	`

	result, err := opm.db.ExecContext(ctx, query, maxAge.String())
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup stale profiles: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}
