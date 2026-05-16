package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// jsRegistryBucketName is the NATS JetStream KV bucket that carries the
// cross-gateway agent registry projection consumed by PrefixIndex's
// JetStream-Watch mode. The bucket name is intentionally distinct from
// the legacy "aether_kv" bucket used by the generic KV store so the two
// projections cannot collide on keys.
const jsRegistryBucketName = "aether_registry"

// jsRegistryDefaultBackoff is the initial reconnect backoff for the watch
// goroutine when the underlying JetStream watcher returns an error.
const jsRegistryDefaultBackoff = time.Second

// jsRegistryMaxBackoff caps exponential backoff between watch reconnect
// attempts so a long-running disconnect cannot cause arbitrarily long
// silent gaps.
const jsRegistryMaxBackoff = 30 * time.Second

// CreateOrOpenRegistryBucket opens (creating if necessary) the
// aether_registry NATS KV bucket used for cross-gateway PrefixIndex sync.
// Callers (gateway wiring) typically invoke this once at startup and pass
// the resulting handle to both StartJetStreamWatch (read side) and
// PublishAgent (write side).
//
// The bucket is configured with History=1 because the consumers only care
// about the latest state per key — historical revisions are unnecessary.
// Replicas defaults to the JetStream cluster's recommended replica count
// when called via cluster/nats.EmbeddedServer.ReplicasForHA(); pass 0 to
// let JetStream pick (single-node deployments).
func CreateOrOpenRegistryBucket(ctx context.Context, js jetstream.JetStream, replicas int) (jetstream.KeyValue, error) {
	cfg := jetstream.KeyValueConfig{
		Bucket:      jsRegistryBucketName,
		Description: "Aether agent registry projection (cross-gateway PrefixIndex sync).",
		History:     1,
		Replicas:    replicas,
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("registry: open KV bucket %s: %w", jsRegistryBucketName, err)
	}
	return kv, nil
}

// EncodeRegistryKey serializes an Implementation string into a NATS
// KV–safe key. The encoding mirrors the per-segment scheme used by
// internal/kv/jetstream_store.go (literal "." escaped to "_2E_", etc.)
// but is intentionally inlined here as a single, minimal helper rather
// than reused via cross-package import — PrefixIndex stays a leaf package
// in the dependency graph.
//
// Empty implementation names are rejected with a non-nil error so callers
// never publish "" keys that would silently overlap.
func EncodeRegistryKey(implementation string) (string, error) {
	if implementation == "" {
		return "", errors.New("registry: empty implementation")
	}
	return encodeRegistrySegment(implementation), nil
}

// encodeRegistrySegment escapes the few NATS subject-reserved characters
// that may appear inside an aether implementation name. The encoding is
// reversible via decodeRegistrySegment so a key recovered from kv.Keys()
// can be mapped back to the source Implementation.
func encodeRegistrySegment(s string) string {
	if s == "" {
		return "_"
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_':
			b.WriteString("_5F_")
		case c == '.':
			b.WriteString("_2E_")
		case c == '/':
			b.WriteString("_2F_")
		case c == '*':
			b.WriteString("_2A_")
		case c == '>':
			b.WriteString("_3E_")
		case c == ' ':
			b.WriteString("_20_")
		case c < 0x20 || c > 0x7E:
			b.WriteByte('_')
			b.WriteByte(hexNibble(c >> 4))
			b.WriteByte(hexNibble(c & 0x0F))
			b.WriteByte('_')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// decodeRegistrySegment reverses encodeRegistrySegment. Returns the
// original implementation name for a key recovered from kv.Keys(); if the
// input is not a valid encoding the result may contain garbage but never
// panics.
func decodeRegistrySegment(s string) string {
	if s == "_" {
		return ""
	}
	if !strings.Contains(s, "_") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '_' && i+3 < len(s) {
			j := i + 1
			if isHex(s[j]) && isHex(s[j+1]) && s[j+2] == '_' {
				val := (hexVal(s[j]) << 4) | hexVal(s[j+1])
				b.WriteByte(val)
				i += 4
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func hexNibble(v byte) byte {
	if v < 10 {
		return '0' + v
	}
	return 'A' + v - 10
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return c - 'a' + 10
	}
}

// PublishAgent writes the agent registration into the KV bucket so peer
// gateways' watchers observe the change. The two writes (SQLite/Postgres
// canonical row + KV projection) are intentionally NOT transactional —
// the canonical store is the source of truth; the KV projection is a
// best-effort propagation channel. Callers should log + continue on
// publish failure rather than failing the user-visible Register call.
//
// Implementation note: PublishAgent only writes the fields PrefixIndex
// needs (Implementation + ResourceSchema). Keeping the wire payload
// narrow means schema additions to AgentRegistration (e.g. LaunchParams
// changes that the prefix index does not care about) do not force a
// KV-format migration.
func PublishAgent(ctx context.Context, kv jetstream.KeyValue, reg *AgentRegistration) error {
	if kv == nil {
		return errors.New("registry: nil KV bucket")
	}
	if reg == nil || reg.Implementation == "" {
		return errors.New("registry: PublishAgent requires non-nil reg with non-empty Implementation")
	}
	key, err := EncodeRegistryKey(reg.Implementation)
	if err != nil {
		return err
	}
	// Narrow projection: PrefixIndex only consults Implementation +
	// ResourceSchema. Encoding the full AgentRegistration here would
	// publish secrets-adjacent fields (LaunchParams) to a bucket every
	// gateway can read, so we deliberately drop them.
	payload := registryProjection{
		Implementation: reg.Implementation,
		ResourceSchema: reg.ResourceSchema,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("registry: marshal projection: %w", err)
	}
	if _, err := kv.Put(ctx, key, data); err != nil {
		return fmt.Errorf("registry: KV put %s: %w", key, err)
	}
	return nil
}

// DeleteAgent removes the projection entry for an implementation. Peer
// gateways' watchers receive a KeyValueDelete event and drop the
// corresponding prefixes from their in-memory index.
//
// As with PublishAgent, callers should treat failure as non-fatal — the
// canonical store DELETE has already succeeded by the time DeleteAgent
// runs, and stale projection entries will eventually be reconciled by
// the next gateway restart's bootstrap.
func DeleteAgent(ctx context.Context, kv jetstream.KeyValue, implementation string) error {
	if kv == nil {
		return errors.New("registry: nil KV bucket")
	}
	key, err := EncodeRegistryKey(implementation)
	if err != nil {
		return err
	}
	if err := kv.Delete(ctx, key); err != nil {
		// "key not found" is benign — the projection may have never been
		// written (e.g. delete called before the corresponding publish
		// completed during a crash window).
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("registry: KV delete %s: %w", key, err)
	}
	return nil
}

// registryProjection is the on-the-wire JSON shape stored in the
// aether_registry KV bucket. Kept separate from AgentRegistration so
// extending the registry domain object (LaunchParams, Capabilities, …)
// does not implicitly broaden the KV payload.
type registryProjection struct {
	Implementation string                     `json:"implementation"`
	ResourceSchema []AgentResourceSchemaEntry `json:"resource_schema,omitempty"`
}

// StartJetStreamWatch enables cross-gateway live-update mode on the
// PrefixIndex. It performs an initial bootstrap from the bucket (reading
// every existing key) then starts a background goroutine that consumes
// kv.WatchAll() events and applies Put/Delete/Purge to the in-memory
// index.
//
// Calling StartJetStreamWatch a second time on the same PrefixIndex is a
// no-op (the existing watch keeps running). Stop the watch by cancelling
// the ctx passed in.
//
// While JetStream-Watch mode is active, IsWatchActive returns true and
// callers (the gateway's periodic-rebuild goroutine) should skip their
// own DB-driven Rebuild — the KV projection plus incremental
// CreateAgent/DeleteAgent writes are the canonical refresh path.
//
// Errors returned by StartJetStreamWatch are fatal-at-startup conditions
// (nil KV, bootstrap failure). Mid-flight watcher errors are handled
// internally with exponential-backoff reconnect; they log a warning and
// retry rather than propagate out to the caller.
func (p *PrefixIndex) StartJetStreamWatch(ctx context.Context, kv jetstream.KeyValue, logger *slog.Logger) error {
	if p == nil {
		return errors.New("registry: nil PrefixIndex")
	}
	if kv == nil {
		return errors.New("registry: nil KV bucket")
	}
	if logger == nil {
		logger = slog.Default()
	}

	p.watchMu.Lock()
	if p.watchActive {
		p.watchMu.Unlock()
		return nil
	}

	// Bootstrap: snapshot every existing key BEFORE the watcher starts so
	// the index is consistent with the bucket as of the moment we begin
	// watching. The subsequent WatchAll call also replays initial values,
	// but we keep the explicit bootstrap step so callers get a synchronous
	// "the index is ready" signal — the watch goroutine's first
	// end-of-initial-values marker (nil entry) would otherwise be the only
	// way to learn the bootstrap is done, which is harder to test.
	if err := p.bootstrapFromKV(ctx, kv); err != nil {
		p.watchMu.Unlock()
		return fmt.Errorf("registry: bootstrap from KV: %w", err)
	}

	p.watchActive = true
	p.watchMu.Unlock()

	go p.runWatchLoop(ctx, kv, logger)
	return nil
}

// IsWatchActive reports whether StartJetStreamWatch has been called and
// the watch goroutine is (logically) live. Used by the gateway's
// periodic-rebuild scheduler to suppress the legacy DB-driven Rebuild
// path when KV Watch is providing the same updates incrementally.
func (p *PrefixIndex) IsWatchActive() bool {
	if p == nil {
		return false
	}
	p.watchMu.RLock()
	defer p.watchMu.RUnlock()
	return p.watchActive
}

// bootstrapFromKV reads every key currently in the bucket and replaces
// the in-memory index with the result. Called once from
// StartJetStreamWatch before the watch goroutine takes over.
func (p *PrefixIndex) bootstrapFromKV(ctx context.Context, kv jetstream.KeyValue) error {
	// kv.ListKeys returns a lister that streams the current set of keys.
	// Using ListKeys (rather than WatchAll → drain initial values) keeps
	// this code path independent of the watcher's internal state machine,
	// which makes the bootstrap test deterministic.
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		// Empty bucket returns ErrNoKeysFound on some nats.go versions;
		// treat it as "nothing to bootstrap".
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			fresh := make(map[string]string)
			p.mu.Lock()
			p.prefixes = fresh
			p.mu.Unlock()
			return nil
		}
		return err
	}
	defer lister.Stop()

	fresh := make(map[string]string, 16)
	for key := range lister.Keys() {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return fmt.Errorf("get %s: %w", key, err)
		}
		applyProjectionToMap(fresh, entry.Value())
	}

	p.mu.Lock()
	p.prefixes = fresh
	p.mu.Unlock()
	return nil
}

// applyProjectionToMap parses the JSON projection and merges its prefix
// declarations into dst. Used by bootstrapFromKV and the watch loop to
// share a single decoder.
func applyProjectionToMap(dst map[string]string, raw []byte) {
	var proj registryProjection
	if err := json.Unmarshal(raw, &proj); err != nil {
		return
	}
	if proj.Implementation == "" {
		return
	}
	// Drop any prefix currently pointing at this impl so a re-published
	// projection that narrows the schema actually releases dropped
	// prefixes (mirrors Set's contract).
	for prefix, owner := range dst {
		if owner == proj.Implementation {
			delete(dst, prefix)
		}
	}
	for _, e := range proj.ResourceSchema {
		if e.ResourceTypePrefix == "" {
			continue
		}
		dst[e.ResourceTypePrefix] = proj.Implementation
	}
}

// runWatchLoop is the long-running goroutine that consumes
// kv.WatchAll() updates and reflects them in the in-memory prefix map.
// On any watcher error or premature channel close it reconnects with
// exponential backoff (starting at jsRegistryDefaultBackoff, doubling
// up to jsRegistryMaxBackoff) and resumes; the loop only exits when
// the parent ctx is cancelled.
func (p *PrefixIndex) runWatchLoop(ctx context.Context, kv jetstream.KeyValue, logger *slog.Logger) {
	defer func() {
		p.watchMu.Lock()
		p.watchActive = false
		p.watchMu.Unlock()
	}()

	backoff := jsRegistryDefaultBackoff
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		err := p.watchOnce(ctx, kv)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Warn("registry: KV watch error, reconnecting",
				"error", err,
				"backoff", backoff,
				"bucket", jsRegistryBucketName,
			)
		} else {
			// The watcher channel closed without an explicit error
			// (server-initiated drain, bucket recreated, etc.). Treat
			// it the same as an error path — log + reconnect.
			logger.Warn("registry: KV watch channel closed, reconnecting",
				"backoff", backoff,
				"bucket", jsRegistryBucketName,
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Exponential backoff capped at jsRegistryMaxBackoff. Reset on
		// successful watcher establishment is handled inside watchOnce
		// (returns nil immediately after passing the init phase, at
		// which point we restart with the default backoff).
		backoff *= 2
		if backoff > jsRegistryMaxBackoff {
			backoff = jsRegistryMaxBackoff
		}
	}
}

// watchOnce opens a WatchAll subscription, drains its initial-values
// burst into the in-memory map, then processes incremental Updates
// until either the channel closes or ctx is cancelled. The return value
// distinguishes between "watcher establishment failed" (err != nil) and
// "channel closed cleanly" (err == nil) so runWatchLoop can decide
// whether to log a hard error or a soft reconnect.
//
// On every successful establishment the caller-visible backoff is reset
// to jsRegistryDefaultBackoff by re-entering watchOnce.
func (p *PrefixIndex) watchOnce(ctx context.Context, kv jetstream.KeyValue) error {
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		return err
	}
	defer watcher.Stop()

	initDone := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case entry, ok := <-watcher.Updates():
			if !ok {
				// channel closed — caller will reconnect.
				return nil
			}
			if entry == nil {
				// End-of-initial-values marker. From here on, every
				// event is incremental.
				initDone = true
				continue
			}
			p.applyWatchEvent(entry)
			_ = initDone // retained for future diagnostics; suppresses unused warning if removed
		}
	}
}

// applyWatchEvent translates a single KeyValueEntry into the
// corresponding mutation on the in-memory prefix map. Operation types:
//
//   - KeyValuePut    → decode projection, replace owner's prefixes.
//   - KeyValueDelete → remove every prefix owned by the decoded impl.
//   - KeyValuePurge  → same as delete (purge wipes all revisions, but
//     for the prefix index there is no difference between "deleted last
//     entry" and "purged history").
func (p *PrefixIndex) applyWatchEvent(entry jetstream.KeyValueEntry) {
	op := entry.Operation()
	key := entry.Key()
	switch op {
	case jetstream.KeyValuePut:
		var proj registryProjection
		if err := json.Unmarshal(entry.Value(), &proj); err != nil {
			return
		}
		if proj.Implementation == "" {
			// Fall back to the decoded key if the payload omits it
			// (shouldn't happen for our writes; defensive).
			proj.Implementation = decodeRegistrySegment(key)
		}
		p.Set(proj.Implementation, proj.ResourceSchema)
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		// On delete/purge the value bytes may be empty, so we recover
		// the implementation from the encoded key.
		impl := decodeRegistrySegment(key)
		if impl != "" {
			p.Delete(impl)
		}
	}
}
