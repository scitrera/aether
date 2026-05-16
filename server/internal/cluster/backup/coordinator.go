package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

// Logger is a small structured-logger interface; consumers (cmd/aetherlite,
// tests) can pass anything satisfying it (zerolog adapter, std log adapter,
// etc.).
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// nopLogger is used when callers don't supply a Logger.
type nopLogger struct{}

func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// Default bucket / key constants. Exposed so tests can sanity-check.
const (
	BackupMetaBucket  = "_aetherlite_backup_meta"
	BackupIndexBucket = "_aetherlite_backup_index"
	LeaderKey         = "leader"
	// DefaultLeaderTTL is the per-entry TTL for the leader lease.
	DefaultLeaderTTL = 30 * time.Second
)

// leaderRecord is the value stored under BackupMetaBucket/leader.
type leaderRecord struct {
	NodeID         string `json:"node_id"`
	AcquiredUnixMs int64  `json:"acquired_unix_ms"`
	ExpiresUnixMs  int64  `json:"expires_unix_ms"`
}

// indexRecord is the value stored under BackupIndexBucket/<domain>; it
// records when the last successful snapshot for a domain was completed and
// where it lives.
type indexRecord struct {
	Domain         string `json:"domain"`
	LastUnixMs     int64  `json:"last_unix_ms"`
	LastSnapshotID string `json:"last_snapshot_id"`
	LastKey        string `json:"last_key"`
	LastSize       int64  `json:"last_size"`
}

// BackupCoordinator periodically (on a single elected leader) snapshots
// JetStream streams and KV buckets to object storage.
type BackupCoordinator struct {
	js       jetstream.JetStream
	storage  StorageClient
	policies []BackupPolicy
	nodeID   string
	logger   Logger

	leaderTTL time.Duration

	// localState is the in-memory mirror of the index bucket. Only the leader
	// actively writes to this; followers refresh on tick so they have a fresh
	// view when they become leader.
	mu             sync.Mutex
	lastBackupTime map[string]time.Time
	leaderRevision uint64 // current leaderKey revision when we hold the lock; 0 otherwise
	leaderHeldByMe bool
}

// CoordinatorOption tweaks BackupCoordinator settings.
type CoordinatorOption func(*BackupCoordinator)

// WithLeaderTTL overrides the leader-lease TTL (default DefaultLeaderTTL).
func WithLeaderTTL(ttl time.Duration) CoordinatorOption {
	return func(c *BackupCoordinator) {
		if ttl > 0 {
			c.leaderTTL = ttl
		}
	}
}

// NewBackupCoordinator constructs a BackupCoordinator. The caller is
// responsible for ensuring the JetStream context outlives the coordinator.
func NewBackupCoordinator(
	js jetstream.JetStream,
	storage StorageClient,
	policies []BackupPolicy,
	nodeID string,
	logger Logger,
	opts ...CoordinatorOption,
) (*BackupCoordinator, error) {
	if js == nil {
		return nil, errors.New("backup coordinator: js is required")
	}
	if storage == nil {
		return nil, errors.New("backup coordinator: storage is required")
	}
	if nodeID == "" {
		return nil, errors.New("backup coordinator: nodeID is required")
	}
	// Make a copy of the policies slice to avoid the caller mutating it.
	pols := make([]BackupPolicy, len(policies))
	for i := range policies {
		p := policies[i]
		if err := p.Validate(); err != nil {
			return nil, err
		}
		pols[i] = p
	}
	if logger == nil {
		logger = nopLogger{}
	}
	c := &BackupCoordinator{
		js:             js,
		storage:        storage,
		policies:       pols,
		nodeID:         nodeID,
		logger:         logger,
		leaderTTL:      DefaultLeaderTTL,
		lastBackupTime: make(map[string]time.Time),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// IsLeader reports whether this coordinator currently believes it holds the
// leader lease. Useful for tests and metrics.
func (c *BackupCoordinator) IsLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaderHeldByMe
}

// tickInterval picks the timer cadence as (smallest MinInterval) / 4, with a
// floor of 100ms and a ceiling so we never sleep longer than the leader TTL
// (otherwise a leader could lose its lease between ticks).
func (c *BackupCoordinator) tickInterval() time.Duration {
	min := time.Duration(0)
	for _, p := range c.policies {
		if min == 0 || p.MinInterval < min {
			min = p.MinInterval
		}
	}
	if min == 0 {
		min = 1 * time.Second
	}
	t := min / 4
	if t < 100*time.Millisecond {
		t = 100 * time.Millisecond
	}
	// Make sure we re-acquire/refresh leadership well before TTL expiry.
	maxTick := c.leaderTTL / 3
	if maxTick > 0 && t > maxTick {
		t = maxTick
	}
	return t
}

// Run drives the coordinator loop until ctx is cancelled. It is safe to call
// Run from multiple goroutines on different coordinator instances bound to
// the same JetStream cluster: at most one will be the leader at any moment.
func (c *BackupCoordinator) Run(ctx context.Context) error {
	if _, err := c.ensureMetaBucket(ctx); err != nil {
		return fmt.Errorf("ensure meta bucket: %w", err)
	}
	if _, err := c.ensureIndexBucket(ctx); err != nil {
		return fmt.Errorf("ensure index bucket: %w", err)
	}

	interval := c.tickInterval()
	c.logger.Infof("backup coordinator starting (node=%s, tick=%s, policies=%d)", c.nodeID, interval, len(c.policies))

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("backup coordinator shutting down (node=%s)", c.nodeID)
			return ctx.Err()
		case now := <-t.C:
			if err := c.tickOnce(ctx, now); err != nil {
				c.logger.Warnf("backup coordinator tick: %v", err)
			}
		}
	}
}

// tickOnce runs a single iteration of the coordinator loop. Exposed for tests
// that want deterministic step-by-step execution.
func (c *BackupCoordinator) tickOnce(ctx context.Context, now time.Time) error {
	leader, err := c.acquireOrRefreshLeader(ctx, now)
	if err != nil {
		return fmt.Errorf("leader election: %w", err)
	}
	if !leader {
		return nil
	}

	// Refresh local view from the index bucket so a freshly-promoted leader
	// doesn't double-back-up.
	c.refreshIndex(ctx)

	for _, pol := range c.policies {
		last := c.lastBackupTime[pol.Domain]
		if !last.IsZero() && now.Sub(last) < pol.MinInterval {
			continue
		}
		if err := c.runSnapshot(ctx, pol, now); err != nil {
			c.logger.Errorf("snapshot %s: %v", pol.Domain, err)
			continue
		}
		c.lastBackupTime[pol.Domain] = now
	}
	return nil
}

// ensureMetaBucket creates the leader-election KV bucket if needed.
func (c *BackupCoordinator) ensureMetaBucket(ctx context.Context) (jetstream.KeyValue, error) {
	kv, err := c.js.KeyValue(ctx, BackupMetaBucket)
	if err == nil {
		return kv, nil
	}
	return c.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: BackupMetaBucket,
		TTL:    c.leaderTTL,
	})
}

// ensureIndexBucket creates the last-backup-time KV bucket if needed.
func (c *BackupCoordinator) ensureIndexBucket(ctx context.Context) (jetstream.KeyValue, error) {
	kv, err := c.js.KeyValue(ctx, BackupIndexBucket)
	if err == nil {
		return kv, nil
	}
	return c.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: BackupIndexBucket,
	})
}

// acquireOrRefreshLeader runs one round of the leader-election protocol:
//   - If we already hold the lease, attempt to renew via Update(rev).
//   - Otherwise read the current leader: if missing or expired, try Create.
func (c *BackupCoordinator) acquireOrRefreshLeader(ctx context.Context, now time.Time) (bool, error) {
	kv, err := c.ensureMetaBucket(ctx)
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	heldByMe := c.leaderHeldByMe
	revision := c.leaderRevision
	c.mu.Unlock()

	value := leaderRecord{
		NodeID:         c.nodeID,
		AcquiredUnixMs: now.UnixMilli(),
		ExpiresUnixMs:  now.Add(c.leaderTTL).UnixMilli(),
	}

	if heldByMe && revision > 0 {
		buf, _ := json.Marshal(value)
		rev, err := kv.Update(ctx, LeaderKey, buf, revision)
		if err == nil {
			c.mu.Lock()
			c.leaderRevision = rev
			c.mu.Unlock()
			return true, nil
		}
		// Lost the lease; fall through to a fresh attempt.
		c.mu.Lock()
		c.leaderHeldByMe = false
		c.leaderRevision = 0
		c.mu.Unlock()
	}

	entry, err := kv.Get(ctx, LeaderKey)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, err
	}
	if entry != nil {
		var cur leaderRecord
		if jerr := json.Unmarshal(entry.Value(), &cur); jerr == nil {
			if cur.NodeID != c.nodeID && cur.ExpiresUnixMs > now.UnixMilli() {
				// Someone else holds a valid lease.
				return false, nil
			}
		}
	}

	buf, _ := json.Marshal(value)
	if entry == nil {
		rev, err := kv.Create(ctx, LeaderKey, buf)
		if err != nil {
			// Race: another node won.
			return false, nil
		}
		c.mu.Lock()
		c.leaderHeldByMe = true
		c.leaderRevision = rev
		c.mu.Unlock()
		c.logger.Infof("backup coordinator: node=%s acquired leadership", c.nodeID)
		return true, nil
	}
	// Existing entry is expired (or held by us-but-state-was-lost): take over
	// via Update at the current revision.
	rev, err := kv.Update(ctx, LeaderKey, buf, entry.Revision())
	if err != nil {
		return false, nil
	}
	c.mu.Lock()
	c.leaderHeldByMe = true
	c.leaderRevision = rev
	c.mu.Unlock()
	c.logger.Infof("backup coordinator: node=%s took over expired leadership", c.nodeID)
	return true, nil
}

// refreshIndex reads every domain's index record so the leader knows what's
// already been backed up and by whom.
func (c *BackupCoordinator) refreshIndex(ctx context.Context) {
	kv, err := c.ensureIndexBucket(ctx)
	if err != nil {
		return
	}
	for _, pol := range c.policies {
		entry, err := kv.Get(ctx, pol.Domain)
		if err != nil {
			continue
		}
		var rec indexRecord
		if jerr := json.Unmarshal(entry.Value(), &rec); jerr != nil {
			continue
		}
		t := time.UnixMilli(rec.LastUnixMs)
		if cur, ok := c.lastBackupTime[pol.Domain]; !ok || t.After(cur) {
			c.lastBackupTime[pol.Domain] = t
		}
	}
}

// runSnapshot performs one snapshot for one policy. It is responsible for
// uploading the data + sidecar manifest + index update.
func (c *BackupCoordinator) runSnapshot(ctx context.Context, pol BackupPolicy, now time.Time) error {
	snapshotID := fmt.Sprintf("%s_%s", now.UTC().Format("20060102T150405.000"), uuid.NewString())
	dataKey := path.Join(pol.S3Prefix, pol.Domain, snapshotID+".bin")
	manifestKey := path.Join(pol.S3Prefix, pol.Domain, snapshotID+".manifest.json")

	// Serialize the snapshot into a temp file so we can compute the checksum
	// and size before uploading (and so multipart upload has a real size).
	tmp, err := os.CreateTemp("", "aetherlite-backup-*.bin")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	hasher := sha256.New()
	hcw := &hashingCountingWriter{w: tmp, h: hasher}

	var lastIndex uint64
	switch pol.Kind {
	case DomainKindStream:
		idx, serr := snapshotStream(ctx, c.js, pol.Domain, hcw)
		if serr != nil {
			return fmt.Errorf("snapshot stream %s: %w", pol.Domain, serr)
		}
		lastIndex = idx
	case DomainKindKV:
		idx, serr := snapshotKV(ctx, c.js, pol.Domain, hcw)
		if serr != nil {
			return fmt.Errorf("snapshot kv %s: %w", pol.Domain, serr)
		}
		lastIndex = idx
	default:
		return fmt.Errorf("unknown domain kind for %q", pol.Domain)
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	manifest := Manifest{
		Domain:          pol.Domain,
		SnapshotID:      snapshotID,
		RaftIndex:       lastIndex,
		SizeBytes:       hcw.n,
		CreatedAtUnixMs: now.UnixMilli(),
		ChecksumSHA256:  hex.EncodeToString(hasher.Sum(nil)),
		Kind:            pol.Kind.String(),
	}

	meta := map[string]string{
		"x-aetherlite-domain":      manifest.Domain,
		"x-aetherlite-snapshot-id": manifest.SnapshotID,
		"x-aetherlite-checksum":    manifest.ChecksumSHA256,
		"x-aetherlite-kind":        manifest.Kind,
	}

	if err := c.storage.Upload(ctx, dataKey, tmp, manifest.SizeBytes, meta); err != nil {
		return fmt.Errorf("upload data: %w", err)
	}
	mfBytes, _ := manifest.MarshalJSON()
	if err := c.storage.Upload(ctx, manifestKey, bytes.NewReader(mfBytes), int64(len(mfBytes)), meta); err != nil {
		return fmt.Errorf("upload manifest: %w", err)
	}

	// Record in index bucket.
	kv, err := c.ensureIndexBucket(ctx)
	if err == nil {
		rec := indexRecord{
			Domain:         pol.Domain,
			LastUnixMs:     now.UnixMilli(),
			LastSnapshotID: snapshotID,
			LastKey:        dataKey,
			LastSize:       manifest.SizeBytes,
		}
		buf, _ := json.Marshal(rec)
		if _, err := kv.Put(ctx, pol.Domain, buf); err != nil {
			c.logger.Warnf("backup index put %s: %v", pol.Domain, err)
		}
	}
	c.logger.Infof("backup uploaded domain=%s id=%s size=%d sha256=%s", pol.Domain, snapshotID, manifest.SizeBytes, manifest.ChecksumSHA256)
	return nil
}

// hashingCountingWriter mirrors hashingCountingReader for the producer side
// of a snapshot (we write the snapshot to a temp file while hashing).
type hashingCountingWriter struct {
	w io.Writer
	h interface{ Write([]byte) (int, error) }
	n int64
}

func (w *hashingCountingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 {
		_, _ = w.h.Write(p[:n])
		w.n += int64(n)
	}
	return n, err
}

// ---------------------------------------------------------------------------
// Snapshot format
//
// We use a tiny custom framing for both streams and KV dumps so the same
// reader can decode either. Each record is:
//
//	uint32 (big-endian) keyLen
//	uint32 (big-endian) valLen
//	keyLen bytes of key
//	valLen bytes of value
//
// For streams the "key" is the subject and the "value" is the message body.
// For KV the "key" is the entry key and the "value" is the entry value.
// We tolerate a 0/0 record as EOF.
// ---------------------------------------------------------------------------

func writeRecord(w io.Writer, key, val []byte) error {
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(val)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(key) > 0 {
		if _, err := w.Write(key); err != nil {
			return err
		}
	}
	if len(val) > 0 {
		if _, err := w.Write(val); err != nil {
			return err
		}
	}
	return nil
}

func readRecord(r io.Reader) (key, val []byte, err error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, nil, err
	}
	kl := binary.BigEndian.Uint32(hdr[0:4])
	vl := binary.BigEndian.Uint32(hdr[4:8])
	if kl == 0 && vl == 0 {
		return nil, nil, io.EOF
	}
	key = make([]byte, kl)
	val = make([]byte, vl)
	if kl > 0 {
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, nil, err
		}
	}
	if vl > 0 {
		if _, err := io.ReadFull(r, val); err != nil {
			return nil, nil, err
		}
	}
	return key, val, nil
}
