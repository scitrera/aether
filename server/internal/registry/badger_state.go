package registry

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger/v4"
)

// badgerProfileStatePrefix namespaces ProfileStateStore keys inside the
// shared Badger DB used by AetherLite. Kept registry-local so the package
// owns its key surface end-to-end.
const badgerProfileStatePrefix = "orch:state:"

// BadgerProfileStateStore implements ProfileStateStore on top of an
// embedded Badger database. Used by AetherLite, which has no Redis.
type BadgerProfileStateStore struct {
	db *badger.DB
}

// NewBadgerProfileStateStore wraps the given Badger database. The caller
// retains ownership and must call Close() on the badger.DB itself.
func NewBadgerProfileStateStore(db *badger.DB) *BadgerProfileStateStore {
	return &BadgerProfileStateStore{db: db}
}

// fullKey prepends the package-local prefix so we never collide with other
// subsystems sharing the AetherLite Badger instance.
func (s *BadgerProfileStateStore) fullKey(key string) []byte {
	return []byte(badgerProfileStatePrefix + key)
}

// Incr atomically increments the integer counter at key and returns the
// new value. Missing keys are treated as 0 (matches Redis INCR).
//
// Badger uses optimistic concurrency, so concurrent writers against the
// same key can collide with ErrConflict. We retry the read-modify-write
// transaction on conflict — safe because the txn has no side effects
// outside the single Set.
func (s *BadgerProfileStateStore) Incr(ctx context.Context, key string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	fullKey := s.fullKey(key)
	const maxAttempts = 100
	var newVal int64
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := s.db.Update(func(txn *badger.Txn) error {
			var current int64
			item, err := txn.Get(fullKey)
			switch {
			case errors.Is(err, badger.ErrKeyNotFound):
				current = 0
			case err != nil:
				return err
			default:
				if valErr := item.Value(func(v []byte) error {
					parsed, parseErr := strconv.ParseInt(string(v), 10, 64)
					if parseErr != nil {
						return fmt.Errorf("counter %s is not an integer: %w", key, parseErr)
					}
					current = parsed
					return nil
				}); valErr != nil {
					return valErr
				}
			}
			newVal = current + 1
			return txn.Set(fullKey, []byte(strconv.FormatInt(newVal, 10)))
		})
		if err == nil {
			return newVal, nil
		}
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		return 0, fmt.Errorf("badger Incr on %s failed: %w", key, err)
	}
	return 0, fmt.Errorf("badger Incr on %s failed: contention after %d attempts", key, maxAttempts)
}
