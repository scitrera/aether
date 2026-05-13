package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/scitrera/aether/internal/logging"

	"github.com/scitrera/aether/pkg/crypto"
)

// TaskAuthToken represents an authentication token for an orchestrated agent
type TaskAuthToken struct {
	Token          string     `json:"token"`           // The actual token value (only returned on creation)
	TokenHash      string     `json:"token_hash"`      // SHA256 hash of token (used as key)
	TaskID         string     `json:"task_id"`         // The task this token authorizes
	TargetIdentity string     `json:"target_identity"` // e.g., "ag::workspace::impl::spec"
	Workspace      string     `json:"workspace"`
	OrchestratorID string     `json:"orchestrator_id"` // Who was assigned to launch this
	CreatedAt      time.Time  `json:"created_at"`
	Revoked        bool       `json:"revoked"`
	RevokedAt      *time.Time `json:"revoked_at,omitempty"`
}

// TokenStore manages task authentication tokens
type TokenStore interface {
	// GenerateToken creates and stores a new task auth token
	GenerateToken(ctx context.Context, taskID, targetIdentity, workspace, orchestratorID string) (*TaskAuthToken, error)

	// ValidateToken validates a token and returns its data if valid
	// Returns error if token is invalid, revoked, or not found
	ValidateToken(ctx context.Context, tokenStr string) (*TaskAuthToken, error)

	// RevokeToken explicitly revokes a token
	RevokeToken(ctx context.Context, tokenStr string) error

	// RevokeTokensForTask revokes all tokens associated with a task
	RevokeTokensForTask(ctx context.Context, taskID string) error

	// ListTokensForTask returns all active tokens for a task (for debugging/admin)
	ListTokensForTask(ctx context.Context, taskID string) ([]*TaskAuthToken, error)
}

// RedisTokenStore implements TokenStore using Redis
type RedisTokenStore struct {
	client      redis.UniversalClient
	tokenLength int // Length in bytes (default 32 = 256 bits)
}

// NewRedisTokenStore creates a new Redis-backed token store
func NewRedisTokenStore(client redis.UniversalClient) *RedisTokenStore {
	return &RedisTokenStore{
		client:      client,
		tokenLength: 32, // 256 bits
	}
}

const (
	tokenKeyPrefix   = "aether:task_token:"  // Primary key: hash -> token data
	taskTokensPrefix = "aether:task_tokens:" // Index: task_id -> set of token hashes
	maxTokenTTL      = 24 * time.Hour        // Token TTL in Redis
)

// GenerateToken creates and stores a new task auth token
func (s *RedisTokenStore) GenerateToken(ctx context.Context, taskID, targetIdentity, workspace, orchestratorID string) (*TaskAuthToken, error) {
	// Generate cryptographically secure random token
	tokenStr, tokenHash, err := crypto.GenerateToken(s.tokenLength)
	if err != nil {
		return nil, err
	}

	token := &TaskAuthToken{
		Token:          tokenStr,
		TokenHash:      tokenHash,
		TaskID:         taskID,
		TargetIdentity: targetIdentity,
		Workspace:      workspace,
		OrchestratorID: orchestratorID,
		CreatedAt:      time.Now(),
		Revoked:        false,
	}

	// Store token data
	tokenData, err := json.Marshal(token)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal token: %w", err)
	}

	// Use pipeline for atomic operation
	pipe := s.client.Pipeline()

	// Store token data with TTL
	tokenKey := tokenKeyPrefix + tokenHash
	pipe.Set(ctx, tokenKey, tokenData, maxTokenTTL)

	// Add to task's token set with same TTL
	taskTokensKey := taskTokensPrefix + taskID
	pipe.SAdd(ctx, taskTokensKey, tokenHash)
	pipe.Expire(ctx, taskTokensKey, maxTokenTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to store token: %w", err)
	}

	return token, nil
}

// ValidateToken validates a token and returns its data if valid
func (s *RedisTokenStore) ValidateToken(ctx context.Context, tokenStr string) (*TaskAuthToken, error) {
	// Hash the provided token
	tokenHash, err := crypto.HashToken(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("failed to hash token: %w", err)
	}

	// Fetch token data
	tokenKey := tokenKeyPrefix + tokenHash
	tokenData, err := s.client.Get(ctx, tokenKey).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("token not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token: %w", err)
	}

	var token TaskAuthToken
	if err := json.Unmarshal(tokenData, &token); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token: %w", err)
	}

	// Check if revoked
	if token.Revoked {
		return nil, fmt.Errorf("token has been revoked")
	}

	// Clear the actual token value from returned data (security)
	token.Token = ""

	return &token, nil
}

// RevokeToken explicitly revokes a token
func (s *RedisTokenStore) RevokeToken(ctx context.Context, tokenStr string) error {
	tokenHash, err := crypto.HashToken(tokenStr)
	if err != nil {
		return fmt.Errorf("failed to hash token: %w", err)
	}
	return s.revokeByHash(ctx, tokenHash)
}

// luaRevokeToken atomically reads a token, sets revoked=true and revoked_at, and
// writes it back with a 1-hour TTL for audit retention. This eliminates the
// TOCTOU race between GET and SET in the previous non-atomic implementation.
// KEYS[1] = token key, ARGV[1] = revoked_at timestamp (RFC3339)
// Returns: 1 if revoked, 0 if token not found.
var luaRevokeToken = redis.NewScript(`
	local data = redis.call("GET", KEYS[1])
	if not data then return 0 end
	local token = cjson.decode(data)
	token["revoked"] = true
	token["revoked_at"] = ARGV[1]
	redis.call("SET", KEYS[1], cjson.encode(token), "EX", 3600)
	return 1
`)

// revokeByHash atomically revokes a token by its hash using a Lua script.
func (s *RedisTokenStore) revokeByHash(ctx context.Context, tokenHash string) error {
	tokenKey := tokenKeyPrefix + tokenHash
	now := time.Now().Format(time.RFC3339)

	result, err := luaRevokeToken.Run(ctx, s.client, []string{tokenKey}, now).Int64()
	if err != nil {
		return fmt.Errorf("failed to revoke token: %w", err)
	}
	// result == 0 means token didn't exist — not an error, nothing to revoke
	_ = result
	return nil
}

// RevokeTokensForTask revokes all tokens associated with a task.
// Each token is revoked atomically via the luaRevokeToken Lua script to avoid
// the TOCTOU race that a GET-modify-SET pipeline would introduce.
func (s *RedisTokenStore) RevokeTokensForTask(ctx context.Context, taskID string) error {
	key := taskTokensPrefix + taskID

	tokenHashes, err := s.client.SMembers(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get tokens for task %s: %w", taskID, err)
	}

	for _, hash := range tokenHashes {
		if err := s.revokeByHash(ctx, hash); err != nil {
			logging.Logger.Warn().Err(err).Str("token_hash", hash[:8]).Str("task_id", taskID).Msg("failed to revoke token for task; continuing")
		}
	}

	// Clean up the task index key (retain for audit with a 1-hour TTL)
	s.client.Expire(ctx, key, time.Hour)

	return nil
}

// ListTokensForTask returns all active tokens for a task
func (s *RedisTokenStore) ListTokensForTask(ctx context.Context, taskID string) ([]*TaskAuthToken, error) {
	taskTokensKey := taskTokensPrefix + taskID

	// Get all token hashes for this task
	tokenHashes, err := s.client.SMembers(ctx, taskTokensKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get tokens for task: %w", err)
	}

	if len(tokenHashes) == 0 {
		return nil, nil
	}

	// Build full keys and MGET all token data in one round-trip
	keys := make([]string, len(tokenHashes))
	for i, hash := range tokenHashes {
		keys[i] = tokenKeyPrefix + hash
	}

	results, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tokens for task: %w", err)
	}

	var tokens []*TaskAuthToken
	for _, val := range results {
		if val == nil {
			continue // Skip missing/expired tokens
		}

		strVal, ok := val.(string)
		if !ok {
			continue
		}

		var token TaskAuthToken
		if err := json.Unmarshal([]byte(strVal), &token); err != nil {
			continue
		}

		// Clear actual token value
		token.Token = ""
		tokens = append(tokens, &token)
	}

	return tokens, nil
}
