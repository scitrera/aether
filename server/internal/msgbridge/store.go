package msgbridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Store provides PostgreSQL operations for all msgbridge tables.
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// =============================================================================
// Channel Mapping types and operations
// =============================================================================

type ChannelMapping struct {
	ID              int64           `json:"id"`
	Name            string          `json:"name"`
	Platform        string          `json:"platform"`
	ChannelID       string          `json:"channel_id"`
	Direction       string          `json:"direction"`   // "inbound", "outbound", "both"
	TargetType      string          `json:"target_type"` // "agent", "user", "broadcast_agents", "broadcast_users"
	TargetWorkspace string          `json:"target_workspace"`
	TargetID        string          `json:"target_id"` // impl.spec for agents, user_id for users
	Enabled         bool            `json:"enabled"`
	Metadata        json.RawMessage `json:"metadata"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

func (s *Store) CreateChannelMapping(ctx context.Context, m *ChannelMapping) error {
	query := `
		INSERT INTO msgbridge_channel_mappings
			(name, platform, channel_id, direction, target_type, target_workspace, target_id, enabled, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, created_at, updated_at
	`
	metadata := m.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage("{}")
	}
	return s.db.QueryRowContext(ctx, query,
		m.Name, m.Platform, m.ChannelID, m.Direction, m.TargetType,
		m.TargetWorkspace, m.TargetID, m.Enabled, metadata,
	).Scan(&m.ID, &m.CreatedAt, &m.UpdatedAt)
}

func (s *Store) GetChannelMapping(ctx context.Context, id int64) (*ChannelMapping, error) {
	query := `
		SELECT id, name, platform, channel_id, direction, target_type,
		       target_workspace, target_id, enabled, metadata, created_at, updated_at
		FROM msgbridge_channel_mappings
		WHERE id = $1
	`
	var m ChannelMapping
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&m.ID, &m.Name, &m.Platform, &m.ChannelID, &m.Direction, &m.TargetType,
		&m.TargetWorkspace, &m.TargetID, &m.Enabled, &m.Metadata, &m.CreatedAt, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get channel mapping: %w", err)
	}
	return &m, nil
}

func (s *Store) GetChannelMappingByName(ctx context.Context, name string) (*ChannelMapping, error) {
	query := `
		SELECT id, name, platform, channel_id, direction, target_type,
		       target_workspace, target_id, enabled, metadata, created_at, updated_at
		FROM msgbridge_channel_mappings
		WHERE name = $1
	`
	var m ChannelMapping
	err := s.db.QueryRowContext(ctx, query, name).Scan(
		&m.ID, &m.Name, &m.Platform, &m.ChannelID, &m.Direction, &m.TargetType,
		&m.TargetWorkspace, &m.TargetID, &m.Enabled, &m.Metadata, &m.CreatedAt, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get channel mapping by name: %w", err)
	}
	return &m, nil
}

func (s *Store) GetChannelMappingByChannel(ctx context.Context, platform, channelID string) (*ChannelMapping, error) {
	query := `
		SELECT id, name, platform, channel_id, direction, target_type,
		       target_workspace, target_id, enabled, metadata, created_at, updated_at
		FROM msgbridge_channel_mappings
		WHERE platform = $1 AND channel_id = $2
	`
	var m ChannelMapping
	err := s.db.QueryRowContext(ctx, query, platform, channelID).Scan(
		&m.ID, &m.Name, &m.Platform, &m.ChannelID, &m.Direction, &m.TargetType,
		&m.TargetWorkspace, &m.TargetID, &m.Enabled, &m.Metadata, &m.CreatedAt, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get channel mapping by channel: %w", err)
	}
	return &m, nil
}

func (s *Store) ListChannelMappings(ctx context.Context) ([]*ChannelMapping, error) {
	query := `
		SELECT id, name, platform, channel_id, direction, target_type,
		       target_workspace, target_id, enabled, metadata, created_at, updated_at
		FROM msgbridge_channel_mappings
		ORDER BY id ASC
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list channel mappings: %w", err)
	}
	defer rows.Close()

	var mappings []*ChannelMapping
	for rows.Next() {
		var m ChannelMapping
		if err := rows.Scan(
			&m.ID, &m.Name, &m.Platform, &m.ChannelID, &m.Direction, &m.TargetType,
			&m.TargetWorkspace, &m.TargetID, &m.Enabled, &m.Metadata, &m.CreatedAt, &m.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan channel mapping: %w", err)
		}
		mappings = append(mappings, &m)
	}
	return mappings, rows.Err()
}

func (s *Store) UpdateChannelMapping(ctx context.Context, m *ChannelMapping) error {
	query := `
		UPDATE msgbridge_channel_mappings
		SET name = $2, platform = $3, channel_id = $4, direction = $5,
		    target_type = $6, target_workspace = $7, target_id = $8,
		    enabled = $9, metadata = $10, updated_at = now()
		WHERE id = $1
		RETURNING updated_at
	`
	metadata := m.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage("{}")
	}
	return s.db.QueryRowContext(ctx, query,
		m.ID, m.Name, m.Platform, m.ChannelID, m.Direction, m.TargetType,
		m.TargetWorkspace, m.TargetID, m.Enabled, metadata,
	).Scan(&m.UpdatedAt)
}

func (s *Store) DeleteChannelMapping(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM msgbridge_channel_mappings WHERE id = $1", id)
	return err
}

// =============================================================================
// User Mapping types and operations
// =============================================================================

type UserMapping struct {
	ID             int64     `json:"id"`
	Platform       string    `json:"platform"`
	PlatformUserID string    `json:"platform_user_id"`
	AetherUserID   string    `json:"aether_user_id"`
	DisplayName    string    `json:"display_name"`
	CreatedAt      time.Time `json:"created_at"`
}

func (s *Store) CreateUserMapping(ctx context.Context, m *UserMapping) error {
	query := `
		INSERT INTO msgbridge_user_mappings (platform, platform_user_id, aether_user_id, display_name)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at
	`
	return s.db.QueryRowContext(ctx, query,
		m.Platform, m.PlatformUserID, m.AetherUserID, m.DisplayName,
	).Scan(&m.ID, &m.CreatedAt)
}

func (s *Store) GetUserMapping(ctx context.Context, platform, platformUserID string) (*UserMapping, error) {
	query := `
		SELECT id, platform, platform_user_id, aether_user_id, display_name, created_at
		FROM msgbridge_user_mappings
		WHERE platform = $1 AND platform_user_id = $2
	`
	var m UserMapping
	err := s.db.QueryRowContext(ctx, query, platform, platformUserID).Scan(
		&m.ID, &m.Platform, &m.PlatformUserID, &m.AetherUserID, &m.DisplayName, &m.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user mapping: %w", err)
	}
	return &m, nil
}

func (s *Store) ListUserMappings(ctx context.Context) ([]*UserMapping, error) {
	query := `
		SELECT id, platform, platform_user_id, aether_user_id, display_name, created_at
		FROM msgbridge_user_mappings
		ORDER BY id ASC
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list user mappings: %w", err)
	}
	defer rows.Close()

	var mappings []*UserMapping
	for rows.Next() {
		var m UserMapping
		if err := rows.Scan(
			&m.ID, &m.Platform, &m.PlatformUserID, &m.AetherUserID, &m.DisplayName, &m.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan user mapping: %w", err)
		}
		mappings = append(mappings, &m)
	}
	return mappings, rows.Err()
}

func (s *Store) DeleteUserMapping(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM msgbridge_user_mappings WHERE id = $1", id)
	return err
}

// =============================================================================
// Message Log types and operations
// =============================================================================

type MessageLogEntry struct {
	ID          int64     `json:"id"`
	Direction   string    `json:"direction"`
	Platform    string    `json:"platform"`
	ChannelID   string    `json:"channel_id"`
	MessageID   string    `json:"message_id"`
	AetherTopic string    `json:"aether_topic"`
	Status      string    `json:"status"`
	ErrorMsg    string    `json:"error_msg"`
	CreatedAt   time.Time `json:"created_at"`
}

func (s *Store) LogMessage(ctx context.Context, entry *MessageLogEntry) error {
	query := `
		INSERT INTO msgbridge_message_log (direction, platform, channel_id, message_id, aether_topic, status, error_msg)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at
	`
	return s.db.QueryRowContext(ctx, query,
		entry.Direction, entry.Platform, entry.ChannelID, entry.MessageID,
		entry.AetherTopic, entry.Status, entry.ErrorMsg,
	).Scan(&entry.ID, &entry.CreatedAt)
}

func (s *Store) QueryMessageLog(ctx context.Context, platform string, channelID string, limit int) ([]*MessageLogEntry, error) {
	if limit <= 0 {
		limit = 100
	}

	var (
		query string
		args  []any
	)

	switch {
	case platform != "" && channelID != "":
		query = `
			SELECT id, direction, platform, channel_id, message_id, aether_topic, status,
			       COALESCE(error_msg, ''), created_at
			FROM msgbridge_message_log
			WHERE platform = $1 AND channel_id = $2
			ORDER BY created_at DESC
			LIMIT $3
		`
		args = []any{platform, channelID, limit}
	case platform != "":
		query = `
			SELECT id, direction, platform, channel_id, message_id, aether_topic, status,
			       COALESCE(error_msg, ''), created_at
			FROM msgbridge_message_log
			WHERE platform = $1
			ORDER BY created_at DESC
			LIMIT $2
		`
		args = []any{platform, limit}
	default:
		query = `
			SELECT id, direction, platform, channel_id, message_id, aether_topic, status,
			       COALESCE(error_msg, ''), created_at
			FROM msgbridge_message_log
			ORDER BY created_at DESC
			LIMIT $1
		`
		args = []any{limit}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query message log: %w", err)
	}
	defer rows.Close()

	var entries []*MessageLogEntry
	for rows.Next() {
		var e MessageLogEntry
		if err := rows.Scan(
			&e.ID, &e.Direction, &e.Platform, &e.ChannelID, &e.MessageID,
			&e.AetherTopic, &e.Status, &e.ErrorMsg, &e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan message log entry: %w", err)
		}
		entries = append(entries, &e)
	}
	return entries, rows.Err()
}
