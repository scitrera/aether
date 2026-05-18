package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/pkg/models"
)

const (
	// smallPayloadThreshold is the maximum size (inclusive) for KV storage.
	smallPayloadThreshold = 256 * 1024 // 256 KB

	// Bucket names.
	jsBucketKV  = "aether_checkpoints_kv"
	jsBucketObj = "aether_checkpoints_obj"
	jsBucketIdx = "aether_checkpoints_idx"

	// prunerDefaultInterval is the default interval between pruner sweeps.
	prunerDefaultInterval = 60 * time.Second
)

// jsSidecarEntry is the JSON payload stored in the index KV bucket.
type jsSidecarEntry struct {
	Size             int    `json:"size"`
	Location         string `json:"location"` // "kv" or "obj"
	SavedUnixMs      int64  `json:"saved_unix_ms"`
	TTLExpiresUnixMs int64  `json:"ttl_expires_unix_ms"` // 0 = no TTL
}

// JetStreamCheckpointStore is a JetStream-backed implementation of
// gateway.CheckpointManager. Small payloads (≤256 KB) go to a KV bucket;
// large payloads go to an Object Store bucket. A lightweight sidecar KV
// bucket records metadata and location so Load avoids a double-probe.
type JetStreamCheckpointStore struct {
	js            jetstream.JetStream
	kv            jetstream.KeyValue
	obj           jetstream.ObjectStore
	idx           jetstream.KeyValue
	pruneInterval time.Duration
}

// NewJetStreamCheckpointStore creates buckets (if needed) and starts the
// background pruner goroutine. The provided context controls pruner lifetime —
// cancel it to stop the pruner.
func NewJetStreamCheckpointStore(ctx context.Context, js jetstream.JetStream) (*JetStreamCheckpointStore, error) {
	return newJetStreamCheckpointStoreWithInterval(ctx, js, prunerDefaultInterval)
}

// newJetStreamCheckpointStoreWithInterval is the internal constructor that
// accepts a custom pruner interval. Used by tests to accelerate TTL pruning.
func newJetStreamCheckpointStoreWithInterval(ctx context.Context, js jetstream.JetStream, interval time.Duration) (*JetStreamCheckpointStore, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      jsBucketKV,
		Description: "Aether checkpoint small payloads (≤256 KB)",
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint: create KV bucket: %w", err)
	}

	obj, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      jsBucketObj,
		Description: "Aether checkpoint large payloads (>256 KB)",
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint: create object store bucket: %w", err)
	}

	idx, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      jsBucketIdx,
		Description: "Aether checkpoint sidecar index",
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint: create index bucket: %w", err)
	}

	s := &JetStreamCheckpointStore{
		js:            js,
		kv:            kv,
		obj:           obj,
		idx:           idx,
		pruneInterval: interval,
	}
	s.startPruner(ctx)
	return s, nil
}

// sanitizeNATSKeySegment replaces characters that are invalid in NATS KV keys.
// Valid NATS KV key chars: [-/_=\.a-zA-Z0-9]. The identity separator "::" must
// be replaced since ':' is not in the allowed set.
func sanitizeNATSKeySegment(s string) string {
	// Replace "::" (identity separator) with "." to keep key readable.
	s = strings.ReplaceAll(s, "::", ".")
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '/', r == '=', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// jsKey constructs the storage key used in KV/Object/Index buckets.
// Format: {workspace}/{sanitized_identity}/{sanitized_key}
// NATS KV valid chars: [-/_=\.a-zA-Z0-9]; key must not start/end with '.'.
func jsKey(identity models.Identity, key string) string {
	if key == "" {
		key = DefaultKey
	}
	ws := sanitizeNATSKeySegment(identity.Workspace)
	if ws == "" {
		ws = "_"
	}
	id := sanitizeNATSKeySegment(identity.String())
	k := sanitizeNATSKeySegment(key)
	return fmt.Sprintf("%s/%s/%s", ws, id, k)
}

// jsKeyPrefix constructs the prefix used to list all checkpoints for an identity.
func jsKeyPrefix(identity models.Identity) string {
	ws := sanitizeNATSKeySegment(identity.Workspace)
	if ws == "" {
		ws = "_"
	}
	id := sanitizeNATSKeySegment(identity.String())
	return fmt.Sprintf("%s/%s/", ws, id)
}

// Save stores a checkpoint for an identity, routing by payload size.
func (s *JetStreamCheckpointStore) Save(ctx context.Context, identity models.Identity, key string, data []byte, ttl time.Duration) error {
	if key == "" {
		key = DefaultKey
	}

	storageKey := jsKey(identity, key)
	now := time.Now()

	var location string
	if len(data) <= smallPayloadThreshold {
		location = "kv"
		if _, err := s.kv.Put(ctx, storageKey, data); err != nil {
			return fmt.Errorf("checkpoint: kv put: %w", err)
		}
	} else {
		location = "obj"
		if _, err := s.obj.Put(ctx, jetstream.ObjectMeta{Name: storageKey}, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("checkpoint: object store put: %w", err)
		}
	}

	var ttlExpiresUnixMs int64
	if ttl > 0 {
		ttlExpiresUnixMs = now.Add(ttl).UnixMilli()
	}

	sidecar := jsSidecarEntry{
		Size:             len(data),
		Location:         location,
		SavedUnixMs:      now.UnixMilli(),
		TTLExpiresUnixMs: ttlExpiresUnixMs,
	}
	sidecarJSON, err := json.Marshal(sidecar)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal sidecar: %w", err)
	}
	if _, err := s.idx.Put(ctx, storageKey, sidecarJSON); err != nil {
		return fmt.Errorf("checkpoint: index put: %w", err)
	}

	return nil
}

// Load retrieves a checkpoint for an identity. Returns nil, nil if not found.
func (s *JetStreamCheckpointStore) Load(ctx context.Context, identity models.Identity, key string) (*Checkpoint, error) {
	if key == "" {
		key = DefaultKey
	}

	storageKey := jsKey(identity, key)

	// Read sidecar to determine location.
	entry, err := s.idx.Get(ctx, storageKey)
	if err != nil {
		if isNatsKeyNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("checkpoint: index get: %w", err)
	}

	var sidecar jsSidecarEntry
	if err := json.Unmarshal(entry.Value(), &sidecar); err != nil {
		return nil, fmt.Errorf("checkpoint: unmarshal sidecar: %w", err)
	}

	var data []byte
	switch sidecar.Location {
	case "kv":
		kvEntry, err := s.kv.Get(ctx, storageKey)
		if err != nil {
			if isNatsKeyNotFound(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("checkpoint: kv get: %w", err)
		}
		data = kvEntry.Value()
	case "obj":
		objEntry, err := s.obj.Get(ctx, storageKey)
		if err != nil {
			return nil, fmt.Errorf("checkpoint: object store get: %w", err)
		}
		defer objEntry.Close()
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(objEntry); err != nil {
			return nil, fmt.Errorf("checkpoint: read object store entry: %w", err)
		}
		data = buf.Bytes()
	default:
		return nil, fmt.Errorf("checkpoint: unknown location %q in sidecar", sidecar.Location)
	}

	savedAt := time.UnixMilli(sidecar.SavedUnixMs)
	cp := &Checkpoint{
		Data:     data,
		SavedAt:  savedAt,
		Identity: identity.String(),
		Key:      key,
	}
	if sidecar.TTLExpiresUnixMs > 0 {
		cp.ExpiresAt = time.UnixMilli(sidecar.TTLExpiresUnixMs)
	}

	return cp, nil
}

// Delete removes a checkpoint for an identity from the underlying store and the index.
func (s *JetStreamCheckpointStore) Delete(ctx context.Context, identity models.Identity, key string) error {
	if key == "" {
		key = DefaultKey
	}

	storageKey := jsKey(identity, key)

	// Read sidecar to determine location; tolerate missing.
	entry, err := s.idx.Get(ctx, storageKey)
	if err != nil && !isNatsKeyNotFound(err) {
		return fmt.Errorf("checkpoint: index get for delete: %w", err)
	}

	if err == nil {
		var sidecar jsSidecarEntry
		if jsonErr := json.Unmarshal(entry.Value(), &sidecar); jsonErr == nil {
			switch sidecar.Location {
			case "kv":
				if delErr := s.kv.Delete(ctx, storageKey); delErr != nil && !isNatsKeyNotFound(delErr) {
					return fmt.Errorf("checkpoint: kv delete: %w", delErr)
				}
			case "obj":
				if delErr := s.obj.Delete(ctx, storageKey); delErr != nil {
					return fmt.Errorf("checkpoint: object store delete: %w", delErr)
				}
			}
		}
	}

	if err := s.idx.Delete(ctx, storageKey); err != nil && !isNatsKeyNotFound(err) {
		return fmt.Errorf("checkpoint: index delete: %w", err)
	}

	return nil
}

// List returns all checkpoint key suffixes for an identity by iterating the index.
func (s *JetStreamCheckpointStore) List(ctx context.Context, identity models.Identity) ([]string, error) {
	prefix := jsKeyPrefix(identity)

	watcher, err := s.idx.WatchAll(ctx, jetstream.IgnoreDeletes())
	if err != nil {
		return nil, fmt.Errorf("checkpoint: index watch: %w", err)
	}
	defer func() { _ = watcher.Stop() }()

	var keys []string
	for entry := range watcher.Updates() {
		if entry == nil {
			// nil signals end of initial values.
			break
		}
		k := entry.Key()
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k[len(prefix):])
		}
	}

	return keys, nil
}

// startPruner starts a goroutine that periodically removes expired entries.
func (s *JetStreamCheckpointStore) startPruner(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.pruneInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.pruneOnce(ctx)
			}
		}
	}()
}

// pruneOnce iterates the index and deletes entries whose TTL has expired.
// Exported for test use only (internal-package test hook via same-package tests).
func (s *JetStreamCheckpointStore) pruneOnce(ctx context.Context) error {
	watcher, err := s.idx.WatchAll(ctx, jetstream.IgnoreDeletes())
	if err != nil {
		return fmt.Errorf("checkpoint: pruner watch: %w", err)
	}
	defer func() { _ = watcher.Stop() }()

	now := time.Now().UnixMilli()
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		var sidecar jsSidecarEntry
		if err := json.Unmarshal(entry.Value(), &sidecar); err != nil {
			continue
		}
		if sidecar.TTLExpiresUnixMs == 0 || sidecar.TTLExpiresUnixMs > now {
			continue
		}
		// Expired — delete from underlying store and index.
		storageKey := entry.Key()
		switch sidecar.Location {
		case "kv":
			_ = s.kv.Delete(ctx, storageKey)
		case "obj":
			_ = s.obj.Delete(ctx, storageKey)
		}
		_ = s.idx.Delete(ctx, storageKey)
	}

	return nil
}

// isNatsKeyNotFound returns true for NATS "key not found" errors.
func isNatsKeyNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err == jetstream.ErrKeyNotFound
}
