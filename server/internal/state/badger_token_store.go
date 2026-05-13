package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/internal/lite"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/crypto"
)

const (
	tokenDataSubPrefix = "data:"
	tokenTaskSubPrefix = "task:"

	// revokedTokenRetentionTTL is the TTL applied to a revoked token for audit retention.
	revokedTokenRetentionTTL = 1 * time.Hour
)

// BadgerTokenStore implements TokenStore using a local Badger database.
// It is intended for AetherLite (single-node) deployments.
//
// Key layout (all under the "tok:" prefix):
//
//	tok:data:{tokenHash}          → JSON-encoded TaskAuthToken
//	tok:task:{taskID}:{tokenHash} → empty value (task → token index)
type BadgerTokenStore struct {
	db *badger.DB
}

// NewBadgerTokenStore creates a new Badger-backed token store.
func NewBadgerTokenStore(db *badger.DB) *BadgerTokenStore {
	return &BadgerTokenStore{db: db}
}

// tokenDataKey returns the primary key for a token's data.
func tokenDataKey(tokenHash string) []byte {
	return []byte(lite.PrefixToken + tokenDataSubPrefix + tokenHash)
}

// tokenTaskIndexKey returns the secondary index key linking a task to a token.
func tokenTaskIndexKey(taskID, tokenHash string) []byte {
	return []byte(lite.PrefixToken + tokenTaskSubPrefix + taskID + ":" + tokenHash)
}

// tokenTaskIndexPrefix returns the key prefix used to scan all tokens for a task.
func tokenTaskIndexPrefix(taskID string) []byte {
	return []byte(lite.PrefixToken + tokenTaskSubPrefix + taskID + ":")
}

// GenerateToken creates and stores a new task auth token.
func (s *BadgerTokenStore) GenerateToken(ctx context.Context, taskID, targetIdentity, workspace, orchestratorID string) (*TaskAuthToken, error) {
	tokenStr, tokenHash, err := crypto.GenerateToken(32)
	if err != nil {
		return nil, fmt.Errorf("failed to generate token: %w", err)
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

	tokenData, err := json.Marshal(token)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal token: %w", err)
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		// Primary data entry.
		dataEntry := badger.NewEntry(tokenDataKey(tokenHash), tokenData).WithTTL(maxTokenTTL)
		if err := txn.SetEntry(dataEntry); err != nil {
			return err
		}

		// Task index entry — empty value; TTL matches data entry.
		idxEntry := badger.NewEntry(tokenTaskIndexKey(taskID, tokenHash), []byte{}).WithTTL(maxTokenTTL)
		return txn.SetEntry(idxEntry)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to store token: %w", err)
	}

	return token, nil
}

// ValidateToken validates a token string and returns its data if valid.
// Returns an error if the token is not found, expired, or revoked.
func (s *BadgerTokenStore) ValidateToken(ctx context.Context, tokenStr string) (*TaskAuthToken, error) {
	tokenHash, err := crypto.HashToken(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("failed to hash token: %w", err)
	}

	var token TaskAuthToken
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(tokenDataKey(tokenHash))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("token not found")
		}
		if err != nil {
			return fmt.Errorf("failed to fetch token: %w", err)
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &token)
		})
	})
	if err != nil {
		return nil, err
	}

	if token.Revoked {
		return nil, fmt.Errorf("token has been revoked")
	}

	// Clear plaintext token value before returning (security).
	token.Token = ""
	return &token, nil
}

// RevokeToken explicitly revokes a token by its plaintext value.
func (s *BadgerTokenStore) RevokeToken(ctx context.Context, tokenStr string) error {
	tokenHash, err := crypto.HashToken(tokenStr)
	if err != nil {
		return fmt.Errorf("failed to hash token: %w", err)
	}
	return s.revokeByHash(ctx, tokenHash)
}

// revokeByHash marks a token as revoked and shortens its TTL to
// revokedTokenRetentionTTL for audit retention.
func (s *BadgerTokenStore) revokeByHash(ctx context.Context, tokenHash string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := tokenDataKey(tokenHash)

		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil // Nothing to revoke.
		}
		if err != nil {
			return fmt.Errorf("failed to fetch token for revocation: %w", err)
		}

		var token TaskAuthToken
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &token)
		}); err != nil {
			return fmt.Errorf("failed to unmarshal token for revocation: %w", err)
		}

		if token.Revoked {
			return nil // Already revoked.
		}

		now := time.Now()
		token.Revoked = true
		token.RevokedAt = &now

		updated, err := json.Marshal(&token)
		if err != nil {
			return fmt.Errorf("failed to marshal revoked token: %w", err)
		}

		return txn.SetEntry(badger.NewEntry(key, updated).WithTTL(revokedTokenRetentionTTL))
	})
}

// RevokeTokensForTask revokes all tokens associated with a task.
func (s *BadgerTokenStore) RevokeTokensForTask(ctx context.Context, taskID string) error {
	hashes, err := s.listHashesForTask(taskID)
	if err != nil {
		return fmt.Errorf("failed to list tokens for task %s: %w", taskID, err)
	}

	for _, hash := range hashes {
		if err := s.revokeByHash(ctx, hash); err != nil {
			logging.Logger.Warn().Err(err).Str("token_hash", hash[:8]).Str("task_id", taskID).
				Msg("badger token store: failed to revoke token for task; continuing")
		}
	}

	// Shorten the task-index entries to audit-retention TTL.
	_ = s.db.Update(func(txn *badger.Txn) error {
		for _, hash := range hashes {
			idxKey := tokenTaskIndexKey(taskID, hash)
			_ = txn.SetEntry(badger.NewEntry(idxKey, []byte{}).WithTTL(revokedTokenRetentionTTL))
		}
		return nil
	})

	return nil
}

// ListTokensForTask returns all tokens (active and revoked) stored for a task.
func (s *BadgerTokenStore) ListTokensForTask(ctx context.Context, taskID string) ([]*TaskAuthToken, error) {
	hashes, err := s.listHashesForTask(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to list tokens for task %s: %w", taskID, err)
	}

	if len(hashes) == 0 {
		return nil, nil
	}

	var tokens []*TaskAuthToken
	err = s.db.View(func(txn *badger.Txn) error {
		for _, hash := range hashes {
			item, err := txn.Get(tokenDataKey(hash))
			if err == badger.ErrKeyNotFound {
				continue // Expired or already purged.
			}
			if err != nil {
				return fmt.Errorf("failed to fetch token data: %w", err)
			}

			var token TaskAuthToken
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &token)
			}); err != nil {
				continue
			}

			token.Token = "" // Never expose plaintext token.
			tokens = append(tokens, &token)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tokens, nil
}

// listHashesForTask scans the task index and returns all token hashes for a task.
func (s *BadgerTokenStore) listHashesForTask(taskID string) ([]string, error) {
	prefix := tokenTaskIndexPrefix(taskID)
	prefixLen := len(prefix)

	var hashes []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Keys only.
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) <= prefixLen {
				continue
			}
			hash := string(key[prefixLen:])
			hashes = append(hashes, hash)
		}
		return nil
	})
	return hashes, err
}
