package state

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/internal/lite"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

const (
	lockSubPrefix = "lock:"
	metaSubPrefix = "meta:"

	// metaSep separates the gateway_id from the identity string inside a
	// session metadata value. NUL is safe because it cannot appear in either
	// an identity (constructed from `::`-joined ASCII parts) or a gateway_id
	// (configured ASCII identifier).
	metaSep = "\x00"
)

// encodeMetaValue serialises gatewayID and identity into a single Badger
// value. Format: `{gatewayID}\x00{identity}`. An empty gatewayID is allowed
// (legacy or unset), in which case the value starts with the separator.
func encodeMetaValue(identity, gatewayID string) []byte {
	return []byte(gatewayID + metaSep + identity)
}

// decodeMetaValue splits a session metadata value into (identity, gatewayID).
// For backward compatibility with legacy entries that stored only the
// identity (no separator), a value without `metaSep` is treated as an
// identity-only value with an empty gatewayID.
func decodeMetaValue(raw []byte) (identity, gatewayID string) {
	s := string(raw)
	idx := strings.Index(s, metaSep)
	if idx < 0 {
		return s, ""
	}
	return s[idx+len(metaSep):], s[:idx]
}

// BadgerSessionRegistry implements SessionManager using a local Badger database.
// It is intended for AetherLite (single-node) deployments where distributed
// coordination via Redis is not required. A sync.Mutex serialises all
// acquire/release operations so that there is no TOCTOU race on a single node.
type BadgerSessionRegistry struct {
	db *badger.DB
	mu sync.Mutex
}

// NewBadgerSessionRegistry creates a Badger-backed session registry.
func NewBadgerSessionRegistry(db *badger.DB) *BadgerSessionRegistry {
	return &BadgerSessionRegistry{db: db}
}

// lockKey returns the Badger key for the distributed lock of an identity.
// Format: sess:lock:{identity}
func lockKey(identity string) []byte {
	return []byte(lite.PrefixSession + lockSubPrefix + identity)
}

// metaKey returns the Badger key for session metadata.
// Format: sess:meta:{sessionID}
func metaKey(sessionID string) []byte {
	return []byte(lite.PrefixSession + metaSubPrefix + sessionID)
}

// AcquireOrResumeLock attempts to acquire the identity lock.
//
// Semantics mirror the Redis implementation in SessionRegistry:
//   - No existing lock → acquire fresh.
//   - Existing lock with matching resumeSessionID → resume (take over own lock).
//   - Existing lock with low remaining TTL (< forceTakeoverThresholdMs) → forced takeover.
//   - Existing lock held by another session with healthy TTL → reject (acquired=false).
//
// Returns (acquired, resumed, forced, error).
func (r *BadgerSessionRegistry) AcquireOrResumeLock(
	ctx context.Context,
	identity models.Identity,
	sessionID, resumeSessionID string,
	forceTakeoverThresholdMs int64,
) (acquired bool, resumed bool, forced bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := lockKey(identity.String())
	ttlDuration := LockTTL

	err = r.db.Update(func(txn *badger.Txn) error {
		item, getErr := txn.Get(key)
		if getErr == badger.ErrKeyNotFound {
			// No lock exists — acquire fresh.
			e := badger.NewEntry(key, []byte(sessionID)).WithTTL(ttlDuration)
			if setErr := txn.SetEntry(e); setErr != nil {
				return setErr
			}
			acquired = true
			return nil
		}
		if getErr != nil {
			return getErr
		}

		// Lock exists — read current holder.
		valBytes, valErr := item.ValueCopy(nil)
		if valErr != nil {
			return valErr
		}
		currentHolder := string(valBytes)

		// Resume: matching session ID.
		if resumeSessionID != "" && currentHolder == resumeSessionID {
			e := badger.NewEntry(key, []byte(sessionID)).WithTTL(ttlDuration)
			if setErr := txn.SetEntry(e); setErr != nil {
				return setErr
			}
			acquired = true
			resumed = true
			return nil
		}

		// Force takeover: check remaining TTL.
		expiresAt := item.ExpiresAt() // Unix seconds; 0 means no TTL
		if expiresAt > 0 {
			remainingMs := int64(time.Until(time.Unix(int64(expiresAt), 0)).Milliseconds())
			if remainingMs > 0 && remainingMs < forceTakeoverThresholdMs {
				e := badger.NewEntry(key, []byte(sessionID)).WithTTL(ttlDuration)
				if setErr := txn.SetEntry(e); setErr != nil {
					return setErr
				}
				acquired = true
				forced = true
				logging.Logger.Warn().
					Str("identity", identity.String()).
					Int64("remaining_ms", remainingMs).
					Msg("badger session: forced lock takeover — previous holder missed refresh cycles")
				return nil
			}
		}

		// Lock is held by a different session with healthy TTL — reject.
		acquired = false
		return nil
	})

	return acquired, resumed, forced, err
}

// ReleaseLock releases the lock for the given identity, but only if the caller
// still owns it (sessionID matches the stored value).
func (r *BadgerSessionRegistry) ReleaseLock(ctx context.Context, identity models.Identity, sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := lockKey(identity.String())

	return r.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil // Nothing to release.
		}
		if err != nil {
			return err
		}

		valBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		if string(valBytes) != sessionID {
			// We don't own this lock any more — do nothing.
			return nil
		}

		return txn.Delete(key)
	})
}

// RefreshLock extends the TTL of an existing lock owned by the caller.
// Returns true if the lock was refreshed (we still own it).
func (r *BadgerSessionRegistry) RefreshLock(ctx context.Context, identity models.Identity, sessionID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := lockKey(identity.String())
	refreshed := false

	err := r.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		valBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		if string(valBytes) != sessionID {
			return nil
		}

		e := badger.NewEntry(key, []byte(sessionID)).WithTTL(LockTTL)
		if err := txn.SetEntry(e); err != nil {
			return err
		}
		refreshed = true
		return nil
	})

	return refreshed, err
}

// RefreshLockAndSession atomically refreshes both the lock TTL and the session
// metadata TTL. Returns true if the lock was refreshed (we still own it).
func (r *BadgerSessionRegistry) RefreshLockAndSession(ctx context.Context, identity models.Identity, sessionID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lockK := lockKey(identity.String())
	metaK := metaKey(sessionID)
	refreshed := false

	err := r.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(lockK)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		valBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		if string(valBytes) != sessionID {
			return nil
		}

		// Refresh lock.
		if err := txn.SetEntry(badger.NewEntry(lockK, []byte(sessionID)).WithTTL(LockTTL)); err != nil {
			return err
		}

		// Refresh session metadata if it exists.
		metaItem, err := txn.Get(metaK)
		if err == nil {
			metaVal, err := metaItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := txn.SetEntry(badger.NewEntry(metaK, metaVal).WithTTL(LockTTL)); err != nil {
				return err
			}
		}

		refreshed = true
		return nil
	})

	return refreshed, err
}

// RegisterSession stores session metadata keyed by sessionID.
// The entry expires after LockTTL and must be refreshed alongside the lock.
// gatewayID is recorded so peers can discover which gateway hosts the
// principal's connection (Phase-7 forwarding).
func (r *BadgerSessionRegistry) RegisterSession(ctx context.Context, identity models.Identity, sessionID, gatewayID string) error {
	key := metaKey(sessionID)
	e := badger.NewEntry(key, encodeMetaValue(identity.String(), gatewayID)).WithTTL(LockTTL)
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(e)
	})
}

// GetSessionIdentity resolves the principal identity stored for a session.
func (r *BadgerSessionRegistry) GetSessionIdentity(ctx context.Context, sessionID string) (models.Identity, error) {
	key := metaKey(sessionID)
	var identityStr string
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		identityStr, _ = decodeMetaValue(val)
		return nil
	})
	if err != nil {
		return models.Identity{}, err
	}

	return parseStoredSessionIdentity(identityStr)
}

// GetSessionGateway returns the gateway_id of the gateway hosting the given
// principal's connection. Returns "" with nil error when the principal is
// offline (lock absent) or when the session metadata predates the gateway_id
// field.
func (r *BadgerSessionRegistry) GetSessionGateway(ctx context.Context, identity models.Identity) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lockK := lockKey(identity.String())
	var gatewayID string
	err := r.db.View(func(txn *badger.Txn) error {
		// Read the lock value (sessionID) — absence means principal offline.
		lockItem, err := txn.Get(lockK)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		sidBytes, err := lockItem.ValueCopy(nil)
		if err != nil {
			return err
		}
		// Read the session metadata to extract gateway_id.
		metaItem, err := txn.Get(metaKey(string(sidBytes)))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		val, err := metaItem.ValueCopy(nil)
		if err != nil {
			return err
		}
		_, gatewayID = decodeMetaValue(val)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("badger GetSessionGateway: %w", err)
	}
	return gatewayID, nil
}

// UnregisterSession removes session metadata for the given sessionID.
func (r *BadgerSessionRegistry) UnregisterSession(ctx context.Context, sessionID string) error {
	key := metaKey(sessionID)
	return r.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// RefreshSession extends the TTL of the session metadata entry.
// Should be called alongside RefreshLock.
func (r *BadgerSessionRegistry) RefreshSession(ctx context.Context, sessionID string) error {
	key := metaKey(sessionID)
	return r.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return txn.SetEntry(badger.NewEntry(key, val).WithTTL(LockTTL))
	})
}

// IsActive returns true if an identity lock exists in Badger.
func (r *BadgerSessionRegistry) IsActive(ctx context.Context, identity string) (bool, error) {
	key := lockKey(identity)
	err := r.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("badger session IsActive: %w", err)
	}
	return true, nil
}

// FindHealthyServiceInstances scans the Badger lock keyspace for sv::{impl}::*
// holders. The TTL filter is intentionally a no-op in lite mode: a single-node
// deployment without a Redis lock manager has no notion of "TTL within 5s" and
// the typical lite use case (one sidecar) does not benefit from healthy-set
// pruning. Returns identity strings.
func (r *BadgerSessionRegistry) FindHealthyServiceInstances(ctx context.Context, impl string, _ time.Duration) ([]string, error) {
	prefix := []byte(lite.PrefixSession + lockSubPrefix + "sv" + models.IdentitySep + impl + models.IdentitySep)
	var out []string
	err := r.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			fullKey := string(it.Item().Key())
			identityStr := fullKey[len(lite.PrefixSession+lockSubPrefix):]
			if identityStr != "" {
				out = append(out, identityStr)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badger FindHealthyServiceInstances: %w", err)
	}
	return out, nil
}

// SetTunnelPin records a tunnel pin in Badger. TTL is honoured via Badger's
// built-in entry expiration.
func (r *BadgerSessionRegistry) SetTunnelPin(ctx context.Context, tunnelID, serviceIdentity string, ttl time.Duration) error {
	if tunnelID == "" || serviceIdentity == "" {
		return fmt.Errorf("tunnelID and serviceIdentity must be non-empty")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return r.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(lite.PrefixSession+tunnelPinKeyPrefix+tunnelID), []byte(serviceIdentity)).WithTTL(ttl)
		return txn.SetEntry(entry)
	})
}

// GetTunnelPin returns the bound service identity, or "" if absent/expired.
func (r *BadgerSessionRegistry) GetTunnelPin(ctx context.Context, tunnelID string) (string, error) {
	if tunnelID == "" {
		return "", nil
	}
	var val string
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(lite.PrefixSession + tunnelPinKeyPrefix + tunnelID))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			val = string(v)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("badger GetTunnelPin: %w", err)
	}
	return val, nil
}

// RefreshTunnelPin re-writes the entry with a fresh TTL when the pin still
// exists. Mirrors the Redis EXPIRE semantic.
func (r *BadgerSessionRegistry) RefreshTunnelPin(ctx context.Context, tunnelID string, ttl time.Duration) error {
	if tunnelID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return r.db.Update(func(txn *badger.Txn) error {
		key := []byte(lite.PrefixSession + tunnelPinKeyPrefix + tunnelID)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		var val []byte
		if cpErr := item.Value(func(v []byte) error {
			val = append(val, v...)
			return nil
		}); cpErr != nil {
			return cpErr
		}
		entry := badger.NewEntry(key, val).WithTTL(ttl)
		return txn.SetEntry(entry)
	})
}

// DeleteTunnelPin removes a tunnel pin. Idempotent.
func (r *BadgerSessionRegistry) DeleteTunnelPin(ctx context.Context, tunnelID string) error {
	if tunnelID == "" {
		return nil
	}
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(lite.PrefixSession + tunnelPinKeyPrefix + tunnelID))
	})
}

// SetRequestPin records a request pin in Badger. Mirrors SetTunnelPin
// semantics for the request-pin keyspace.
func (r *BadgerSessionRegistry) SetRequestPin(ctx context.Context, requestID, pinValue string, ttl time.Duration) error {
	if requestID == "" || pinValue == "" {
		return fmt.Errorf("requestID and pinValue must be non-empty")
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return r.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(lite.PrefixSession+requestPinKeyPrefix+requestID), []byte(pinValue)).WithTTL(ttl)
		return txn.SetEntry(entry)
	})
}

// GetRequestPin returns the bound caller|service value, or "" if absent/expired.
func (r *BadgerSessionRegistry) GetRequestPin(ctx context.Context, requestID string) (string, error) {
	if requestID == "" {
		return "", nil
	}
	var val string
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(lite.PrefixSession + requestPinKeyPrefix + requestID))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			val = string(v)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("badger GetRequestPin: %w", err)
	}
	return val, nil
}

// RefreshRequestPin re-writes the entry with a fresh TTL when the pin still
// exists. Mirrors RefreshTunnelPin.
func (r *BadgerSessionRegistry) RefreshRequestPin(ctx context.Context, requestID string, ttl time.Duration) error {
	if requestID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return r.db.Update(func(txn *badger.Txn) error {
		key := []byte(lite.PrefixSession + requestPinKeyPrefix + requestID)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		var val []byte
		if cpErr := item.Value(func(v []byte) error {
			val = append(val, v...)
			return nil
		}); cpErr != nil {
			return cpErr
		}
		entry := badger.NewEntry(key, val).WithTTL(ttl)
		return txn.SetEntry(entry)
	})
}

// DeleteRequestPin removes a request pin. Idempotent.
func (r *BadgerSessionRegistry) DeleteRequestPin(ctx context.Context, requestID string) error {
	if requestID == "" {
		return nil
	}
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(lite.PrefixSession + requestPinKeyPrefix + requestID))
	})
}
