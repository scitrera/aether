package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/pkg/models"
)

// BadgerCheckpointStore is a Badger-backed implementation of the CheckpointManager interface.
// Keys are stored as: ckpt:{identity}:{key}  (matching lite.PrefixCheckpoint + identity + key).
type BadgerCheckpointStore struct {
	db *badger.DB
}

// NewBadgerCheckpointStore creates a new BadgerCheckpointStore backed by the given Badger database.
func NewBadgerCheckpointStore(db *badger.DB) *BadgerCheckpointStore {
	return &BadgerCheckpointStore{db: db}
}

// badgerKey constructs the full Badger key for a checkpoint.
// Format: ckpt:{identity}:{key}
func badgerCheckpointKey(identity models.Identity, key string) []byte {
	if key == "" {
		key = DefaultKey
	}
	return []byte(fmt.Sprintf("ckpt:%s:%s", identity.String(), key))
}

// badgerCheckpointPrefix returns the prefix used to iterate all checkpoints for an identity.
func badgerCheckpointPrefix(identity models.Identity) []byte {
	return []byte(fmt.Sprintf("ckpt:%s:", identity.String()))
}

// Save stores a checkpoint for an identity.
func (s *BadgerCheckpointStore) Save(ctx context.Context, identity models.Identity, key string, data []byte, ttl time.Duration) error {
	if key == "" {
		key = DefaultKey
	}

	cp := Checkpoint{
		Data:     data,
		SavedAt:  time.Now(),
		Identity: identity.String(),
		Key:      key,
	}
	if ttl > 0 {
		cp.ExpiresAt = time.Now().Add(ttl)
	}

	jsonData, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	bKey := badgerCheckpointKey(identity, key)
	err = s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(bKey, jsonData)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}
	return nil
}

// Load retrieves a checkpoint for an identity. Returns nil, nil if not found.
func (s *BadgerCheckpointStore) Load(ctx context.Context, identity models.Identity, key string) (*Checkpoint, error) {
	if key == "" {
		key = DefaultKey
	}

	bKey := badgerCheckpointKey(identity, key)
	var cp Checkpoint
	found := false

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(bKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if unmarshalErr := json.Unmarshal(v, &cp); unmarshalErr != nil {
				return fmt.Errorf("failed to unmarshal checkpoint: %w", unmarshalErr)
			}
			found = true
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}
	if !found {
		return nil, nil
	}
	return &cp, nil
}

// Delete removes a checkpoint for an identity.
func (s *BadgerCheckpointStore) Delete(ctx context.Context, identity models.Identity, key string) error {
	if key == "" {
		key = DefaultKey
	}

	bKey := badgerCheckpointKey(identity, key)
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(bKey)
	})
	if err != nil {
		return fmt.Errorf("failed to delete checkpoint: %w", err)
	}
	return nil
}

// List returns all checkpoint key suffixes for an identity.
func (s *BadgerCheckpointStore) List(ctx context.Context, identity models.Identity) ([]string, error) {
	prefix := badgerCheckpointPrefix(identity)
	var keys []string

	err := s.db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.PrefetchValues = false
		iterOpts.Prefix = prefix

		it := txn.NewIterator(iterOpts)
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			rawKey := string(it.Item().KeyCopy(nil))
			keys = append(keys, rawKey[len(prefix):])
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}
	return keys, nil
}
