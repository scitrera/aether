package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/pkg/models"
)

// BadgerKVStore is a Badger-backed implementation of the KVReadWriter interface.
// All keys are stored with the prefix from lite.PrefixKV ("kv:") followed by
// the same namespace scheme used by the Redis Store.
type BadgerKVStore struct {
	db *badger.DB
}

// NewBadgerKVStore creates a new BadgerKVStore backed by the given Badger database.
func NewBadgerKVStore(db *badger.DB) *BadgerKVStore {
	return &BadgerKVStore{db: db}
}

// badgerKey constructs a full Badger key for a KV entry.
// Format: kv:agent:{impl}.{spec}:{scope}[:{context}]:{key}
// The "kv:" prefix is already part of BuildNamespace's output (it starts with "kv:agent:…").
func (s *BadgerKVStore) badgerKey(agent models.Identity, scope KVScope, key string, userID string, workspace string) []byte {
	namespace := BuildNamespace(agent, scope, userID, workspace)
	return []byte(fmt.Sprintf("%s:%s", namespace, key))
}

// badgerPrefix returns the Badger key prefix for iterating all keys in a namespace.
func (s *BadgerKVStore) badgerPrefix(agent models.Identity, scope KVScope, userID string, workspace string) []byte {
	namespace := BuildNamespace(agent, scope, userID, workspace)
	return []byte(namespace + ":")
}

// Get retrieves a value from the agent's KV store in the specified scope.
func (s *BadgerKVStore) Get(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	fullKey := s.badgerKey(agent, scope, key, userID, workspace)

	var val string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(fullKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("%w: %s", ErrKeyNotFound, key)
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			val = string(v)
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", err
		}
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}
	return val, nil
}

// Set stores a value in the agent's KV store in the specified scope.
func (s *BadgerKVStore) Set(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	value string,
	userID string,
	workspace string,
	ttl time.Duration,
) error {
	if err := validateKey(key); err != nil {
		return err
	}
	fullKey := s.badgerKey(agent, scope, key, userID, workspace)

	err := s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(fullKey, []byte(value))
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
	if err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}
	return nil
}

// Delete removes a key from the agent's KV store.
func (s *BadgerKVStore) Delete(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) error {
	if err := validateKey(key); err != nil {
		return err
	}
	fullKey := s.badgerKey(agent, scope, key, userID, workspace)

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(fullKey)
	})
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// List returns all keys in a namespace with their values, capped at DefaultListLimit.
func (s *BadgerKVStore) List(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	userID string,
	workspace string,
) (map[string]string, error) {
	res, err := s.ListPaginated(ctx, agent, scope, userID, workspace, &ListOptions{Limit: DefaultListLimit})
	if err != nil {
		return nil, err
	}
	return res.Items, nil
}

// ListPaginated returns up to opts.Limit keys in a namespace with their values.
// Cursor-based pagination: the cursor is the last full Badger key seen in the
// previous page. Pass "" to start from the beginning.
func (s *BadgerKVStore) ListPaginated(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	userID string,
	workspace string,
	opts *ListOptions,
) (*ListResult, error) {
	limit := DefaultListLimit
	var startAfter []byte
	if opts != nil {
		if opts.Limit > 0 {
			limit = opts.Limit
		}
		if opts.Cursor != "" && opts.Cursor != "0" {
			startAfter = []byte(opts.Cursor)
		}
	}

	prefix := s.badgerPrefix(agent, scope, userID, workspace)

	items := make(map[string]string)
	var nextCursor string
	hasMore := false

	err := s.db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.Prefix = prefix

		it := txn.NewIterator(iterOpts)
		defer it.Close()

		// Seek to the correct start position.
		if len(startAfter) > 0 {
			// Start after the cursor key by seeking to it then advancing.
			it.Seek(startAfter)
			if it.Valid() {
				// Skip the cursor key itself (we already returned it last page).
				it.Next()
			}
		} else {
			it.Seek(prefix)
		}

		count := 0
		for ; it.Valid(); it.Next() {
			if count >= limit {
				hasMore = true
				nextCursor = string(it.Item().KeyCopy(nil))
				break
			}
			item := it.Item()
			rawKey := string(item.KeyCopy(nil))
			shortKey := rawKey[len(prefix):]

			var val string
			if err := item.Value(func(v []byte) error {
				val = string(v)
				return nil
			}); err != nil {
				return fmt.Errorf("failed to read value for key %s: %w", shortKey, err)
			}
			items[shortKey] = val
			count++
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	return &ListResult{Items: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

// Increment atomically increments a key's integer value and returns the new value.
// The value is stored as a little-endian int64 on disk.
func (s *BadgerKVStore) Increment(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	fullKey := s.badgerKey(agent, scope, key, userID, workspace)
	return s.addDelta(fullKey, 1)
}

// Decrement atomically decrements a key's integer value and returns the new value.
func (s *BadgerKVStore) Decrement(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	fullKey := s.badgerKey(agent, scope, key, userID, workspace)
	return s.addDelta(fullKey, -1)
}

// addDelta performs an atomic read-modify-write to add delta to the stored integer.
// Values are stored as their decimal string representation (matching Redis INCR semantics).
func (s *BadgerKVStore) addDelta(fullKey []byte, delta int64) (int64, error) {
	var newVal int64
	err := s.db.Update(func(txn *badger.Txn) error {
		current, err := readBadgerCounter(txn, fullKey)
		if err != nil {
			return err
		}
		newVal = current + delta
		encoded := []byte(strconv.FormatInt(newVal, 10))
		return txn.Set(fullKey, encoded)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to modify counter: %w", err)
	}
	return newVal, nil
}

// readBadgerCounter loads the integer value at fullKey within the given
// transaction. Missing keys yield 0; the legacy 8-byte little-endian
// encoding is accepted for back-compat with old data; new writes use
// the decimal-string format.
func readBadgerCounter(txn *badger.Txn, fullKey []byte) (int64, error) {
	item, err := txn.Get(fullKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var current int64
	if valErr := item.Value(func(v []byte) error {
		if len(v) == 8 {
			current = int64(binary.LittleEndian.Uint64(v))
			return nil
		}
		parsed, parseErr := strconv.ParseInt(string(v), 10, 64)
		if parseErr != nil {
			return fmt.Errorf("value is not an integer: %w", parseErr)
		}
		current = parsed
		return nil
	}); valErr != nil {
		return 0, valErr
	}
	return current, nil
}

// IncrementIf atomically increments a counter by `delta` only when the
// resulting value would not exceed `ceiling`. Returns the (possibly
// unchanged) current value plus a boolean indicating whether the
// mutation was applied.
func (s *BadgerKVStore) IncrementIf(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	ceiling int64,
) (int64, bool, error) {
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	if delta < 0 {
		return 0, false, fmt.Errorf("IncrementIf delta must be non-negative, got %d", delta)
	}
	fullKey := s.badgerKey(agent, scope, key, userID, workspace)
	return s.addDeltaGuarded(fullKey, delta, ceiling, true)
}

// DecrementIf atomically decrements a counter by `delta` only when the
// resulting value would not drop below `floor`. Returns the (possibly
// unchanged) current value plus a boolean indicating whether the
// mutation was applied.
func (s *BadgerKVStore) DecrementIf(
	ctx context.Context,
	agent models.Identity,
	scope KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	floor int64,
) (int64, bool, error) {
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	if delta < 0 {
		return 0, false, fmt.Errorf("DecrementIf delta must be non-negative, got %d", delta)
	}
	fullKey := s.badgerKey(agent, scope, key, userID, workspace)
	return s.addDeltaGuarded(fullKey, -delta, floor, false)
}

// addDeltaGuarded is the shared implementation backing IncrementIf /
// DecrementIf. `isCeiling=true` rejects writes whose result exceeds
// `guard`; `isCeiling=false` rejects writes whose result drops below
// `guard`.
//
// Badger uses optimistic concurrency control, so concurrent guarded
// counter writes against the same key may collide with
// ErrConflict. We retry on conflict; this is safe because the
// transaction is a pure read-modify-write with no side effects.
func (s *BadgerKVStore) addDeltaGuarded(fullKey []byte, delta, guard int64, isCeiling bool) (int64, bool, error) {
	const maxAttempts = 100
	var (
		finalVal int64
		applied  bool
	)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := s.db.Update(func(txn *badger.Txn) error {
			current, err := readBadgerCounter(txn, fullKey)
			if err != nil {
				return err
			}
			proposed := current + delta
			if (isCeiling && proposed > guard) || (!isCeiling && proposed < guard) {
				finalVal = current
				applied = false
				return nil
			}
			finalVal = proposed
			applied = true
			return txn.Set(fullKey, []byte(strconv.FormatInt(proposed, 10)))
		})
		if err == nil {
			return finalVal, applied, nil
		}
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		return 0, false, fmt.Errorf("guarded counter on %s failed: %w", string(fullKey), err)
	}
	return 0, false, fmt.Errorf("guarded counter on %s failed: contention after %d attempts", string(fullKey), maxAttempts)
}
