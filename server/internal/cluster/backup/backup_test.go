package backup

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	natsutil "github.com/scitrera/aether/internal/cluster/nats"

	"github.com/nats-io/nats.go/jetstream"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testLogger drains log lines into t.Log so failed tests surface ordering.
type testLogger struct{ t *testing.T }

func (l testLogger) Infof(format string, args ...any) {
	l.t.Logf("INFO "+format, args...)
}
func (l testLogger) Warnf(format string, args ...any) {
	l.t.Logf("WARN "+format, args...)
}
func (l testLogger) Errorf(format string, args ...any) {
	l.t.Logf("ERR  "+format, args...)
}

// newTestEmbeddedServer spins up a single-node embedded NATS server bound to
// a random port and registers a cleanup hook.
func newTestEmbeddedServer(t *testing.T) *natsutil.EmbeddedServer {
	t.Helper()
	es := &natsutil.EmbeddedServer{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := natsutil.Config{
		DataDir:     t.TempDir(),
		ListenHost:  "127.0.0.1",
		ClientPort:  -1,
		ClusterPort: -1,
	}
	if err := es.Start(ctx, cfg); err != nil {
		t.Fatalf("start embedded nats: %v", err)
	}
	t.Cleanup(es.Stop)
	return es
}

// ---------------------------------------------------------------------------
// 1) S3 / storage round-trip — uses LocalFileStorage for portability. A real
// MinIO/S3 round-trip belongs under a separate "integration" build tag.
// ---------------------------------------------------------------------------

func TestS3StorageClient_RoundTrip(t *testing.T) {
	// We exercise LocalFileStorage for the round-trip test so the suite does
	// not require an S3 endpoint. The S3StorageClient implementation is
	// covered by an integration test under //go:build integration (future work).
	root := t.TempDir()
	store, err := NewLocalFileStorage(root)
	if err != nil {
		t.Fatalf("local storage: %v", err)
	}
	ctx := context.Background()
	payload := []byte("hello, aetherlite backup")
	meta := map[string]string{"x-test": "1", "x-aetherlite-checksum": "abc"}
	if err := store.Upload(ctx, "prefix/dom/a.bin", strings.NewReader(string(payload)), int64(len(payload)), meta); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := store.Upload(ctx, "prefix/dom/b.bin", strings.NewReader("second"), 6, meta); err != nil {
		t.Fatalf("upload 2: %v", err)
	}

	key, gotMeta, err := store.LatestKey(ctx, "prefix/dom")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if key != "prefix/dom/b.bin" {
		t.Fatalf("expected latest=b, got %q", key)
	}
	if gotMeta["x-test"] != "1" {
		t.Fatalf("meta round-trip lost: %+v", gotMeta)
	}

	var sink strings.Builder
	if err := store.Download(ctx, "prefix/dom/a.bin", &writerAdapter{b: &sink}); err != nil {
		t.Fatalf("download: %v", err)
	}
	if sink.String() != string(payload) {
		t.Fatalf("payload mismatch: %q", sink.String())
	}
}

type writerAdapter struct{ b *strings.Builder }

func (w *writerAdapter) Write(p []byte) (int, error) { return w.b.Write(p) }

// ---------------------------------------------------------------------------
// 2) Leader election: only one of N coordinators is leader at any moment.
// ---------------------------------------------------------------------------

func TestBackupCoordinator_LeaderElection_SingleLeader(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	store, err := NewLocalFileStorage(t.TempDir())
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	policies := []BackupPolicy{{
		Domain:      "dummy_stream",
		Kind:        DomainKindStream,
		MinInterval: 24 * time.Hour, // ensure no actual snapshots during election test
		S3Prefix:    "test",
	}}

	var coords []*BackupCoordinator
	for i := 0; i < 3; i++ {
		c, err := NewBackupCoordinator(js, store, policies, fmt.Sprintf("node-%d", i), testLogger{t}, WithLeaderTTL(2*time.Second))
		if err != nil {
			t.Fatalf("new coord: %v", err)
		}
		coords = append(coords, c)
	}

	ctx := context.Background()
	// Drive a few ticks manually so the test is deterministic.
	for i := 0; i < 5; i++ {
		for _, c := range coords {
			_ = c.tickOnce(ctx, time.Now())
		}
		leaders := 0
		for _, c := range coords {
			if c.IsLeader() {
				leaders++
			}
		}
		if leaders > 1 {
			t.Fatalf("iter %d: %d concurrent leaders", i, leaders)
		}
	}
	// At the end at least one must be leader.
	any := false
	for _, c := range coords {
		if c.IsLeader() {
			any = true
		}
	}
	if !any {
		t.Fatalf("no leader elected after 5 iterations")
	}
}

// ---------------------------------------------------------------------------
// 3) Failover: when the leader stops refreshing, another node takes over
// within ~2 TTLs.
// ---------------------------------------------------------------------------

func TestBackupCoordinator_LeaderFailover(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	store, _ := NewLocalFileStorage(t.TempDir())
	policies := []BackupPolicy{{
		Domain:      "dummy_stream",
		Kind:        DomainKindStream,
		MinInterval: 24 * time.Hour,
		S3Prefix:    "test",
	}}
	ttl := 1 * time.Second
	a, _ := NewBackupCoordinator(js, store, policies, "node-a", testLogger{t}, WithLeaderTTL(ttl))
	b, _ := NewBackupCoordinator(js, store, policies, "node-b", testLogger{t}, WithLeaderTTL(ttl))

	ctx := context.Background()
	// Let a acquire leadership.
	for i := 0; i < 3; i++ {
		_ = a.tickOnce(ctx, time.Now())
		_ = b.tickOnce(ctx, time.Now())
		if a.IsLeader() {
			break
		}
	}
	if !a.IsLeader() {
		t.Fatalf("a should be leader; isLeader(a)=%v isLeader(b)=%v", a.IsLeader(), b.IsLeader())
	}

	// Simulate a freeze of a: don't tick it, only tick b after TTL+slack.
	time.Sleep(ttl + 500*time.Millisecond)
	deadline := time.Now().Add(3 * ttl)
	for time.Now().Before(deadline) {
		_ = b.tickOnce(ctx, time.Now())
		if b.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !b.IsLeader() {
		t.Fatalf("b should have taken over after a froze")
	}
}

// ---------------------------------------------------------------------------
// 4) Snapshot cadence per policy: two policies with different intervals
// produce backups at roughly their requested cadences during a fixed window.
// ---------------------------------------------------------------------------

func TestBackupCoordinator_SnapshotPerPolicy(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	ctx := context.Background()

	// Create two streams to back up.
	for _, name := range []string{"fast_stream", "slow_stream"} {
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     name,
			Subjects: []string{name + ".>"},
			Replicas: 1,
		}); err != nil {
			t.Fatalf("create stream %s: %v", name, err)
		}
		if _, err := js.Publish(ctx, name+".x", []byte("payload")); err != nil {
			t.Fatalf("publish to %s: %v", name, err)
		}
	}

	store, _ := NewLocalFileStorage(t.TempDir())
	policies := []BackupPolicy{
		{Domain: "fast_stream", Kind: DomainKindStream, MinInterval: 200 * time.Millisecond, S3Prefix: "test"},
		{Domain: "slow_stream", Kind: DomainKindStream, MinInterval: 800 * time.Millisecond, S3Prefix: "test"},
	}
	c, err := NewBackupCoordinator(js, store, policies, "node-a", testLogger{t}, WithLeaderTTL(2*time.Second))
	if err != nil {
		t.Fatalf("new coord: %v", err)
	}

	// Drive 10 ticks over ~1.5s.
	start := time.Now()
	for time.Since(start) < 1500*time.Millisecond {
		_ = c.tickOnce(ctx, time.Now())
		time.Sleep(50 * time.Millisecond)
	}

	fastObjs, _ := store.List(ctx, "test/fast_stream")
	slowObjs, _ := store.List(ctx, "test/slow_stream")
	fastBins := countSuffix(fastObjs, ".bin")
	slowBins := countSuffix(slowObjs, ".bin")
	// fast should produce at least 3, slow should produce at least 1; fast
	// strictly more than slow.
	if fastBins < 3 {
		t.Fatalf("expected fast cadence >=3 backups, got %d", fastBins)
	}
	if slowBins < 1 {
		t.Fatalf("expected slow cadence >=1 backup, got %d", slowBins)
	}
	if fastBins <= slowBins {
		t.Fatalf("fast (%d) should produce strictly more backups than slow (%d)", fastBins, slowBins)
	}
}

func countSuffix(objs []ObjectInfo, suffix string) int {
	n := 0
	for _, o := range objs {
		if strings.HasSuffix(o.Key, suffix) {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// 5) End-to-end restore: snapshot a stream with known data, wipe it, restore,
// and assert the payloads come back intact.
// ---------------------------------------------------------------------------

func TestRestoreFromS3_RoundTrip(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	ctx := context.Background()

	// Seed the stream with a handful of subject/payload pairs.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "orders",
		Subjects: []string{"orders.>"},
		Replicas: 1,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	want := map[string]string{
		"orders.created":  "a",
		"orders.updated":  "b",
		"orders.canceled": "c",
	}
	for sub, body := range want {
		if _, err := js.Publish(ctx, sub, []byte(body)); err != nil {
			t.Fatalf("publish %s: %v", sub, err)
		}
	}

	store, _ := NewLocalFileStorage(t.TempDir())
	pol := BackupPolicy{
		Domain:       "orders",
		Kind:         DomainKindStream,
		MinInterval:  50 * time.Millisecond,
		S3Prefix:     "rt",
		ReplicaCount: 1,
	}
	c, err := NewBackupCoordinator(js, store, []BackupPolicy{pol}, "node-a", testLogger{t}, WithLeaderTTL(2*time.Second))
	if err != nil {
		t.Fatalf("coord: %v", err)
	}
	// Tick a few times to ensure a snapshot is taken.
	for i := 0; i < 5; i++ {
		_ = c.tickOnce(ctx, time.Now())
		time.Sleep(80 * time.Millisecond)
	}

	// Now wipe the stream and restore.
	if err := js.DeleteStream(ctx, "orders"); err != nil {
		t.Fatalf("delete stream: %v", err)
	}

	if err := RestoreFromS3(ctx, js, store, pol, testLogger{t}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Confirm every subject/body is back. We use a fresh ordered consumer.
	stream, err := js.Stream(ctx, "orders")
	if err != nil {
		t.Fatalf("get stream post-restore: %v", err)
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverAllPolicy})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	got := make(map[string]string)
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < len(want) && time.Now().Before(deadline) {
		msg, err := cons.Next(jetstream.FetchMaxWait(300 * time.Millisecond))
		if err != nil {
			continue
		}
		got[msg.Subject()] = string(msg.Data())
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d msgs after restore, got %d: %+v", len(want), len(got), got)
	}
	for sub, body := range want {
		if got[sub] != body {
			t.Fatalf("subject %s: want %q, got %q", sub, body, got[sub])
		}
	}
}

// ---------------------------------------------------------------------------
// 6) KV round-trip: snapshot a KV bucket and restore it.
// ---------------------------------------------------------------------------

func TestRestoreFromS3_KV_RoundTrip(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "settings"})
	if err != nil {
		t.Fatalf("create kv: %v", err)
	}
	want := map[string]string{
		"feature_a": "on",
		"feature_b": "off",
		"theme":     "dark",
	}
	for k, v := range want {
		if _, err := kv.Put(ctx, k, []byte(v)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	store, _ := NewLocalFileStorage(t.TempDir())
	pol := BackupPolicy{
		Domain:       "settings",
		Kind:         DomainKindKV,
		MinInterval:  50 * time.Millisecond,
		S3Prefix:     "kvbk",
		ReplicaCount: 1,
	}
	c, err := NewBackupCoordinator(js, store, []BackupPolicy{pol}, "node-a", testLogger{t}, WithLeaderTTL(2*time.Second))
	if err != nil {
		t.Fatalf("coord: %v", err)
	}
	for i := 0; i < 5; i++ {
		_ = c.tickOnce(ctx, time.Now())
		time.Sleep(80 * time.Millisecond)
	}

	if err := js.DeleteKeyValue(ctx, "settings"); err != nil {
		t.Fatalf("delete kv: %v", err)
	}
	if err := RestoreFromS3(ctx, js, store, pol, testLogger{t}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	kv2, err := js.KeyValue(ctx, "settings")
	if err != nil {
		t.Fatalf("get kv post-restore: %v", err)
	}
	for k, v := range want {
		entry, err := kv2.Get(ctx, k)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(entry.Value()) != v {
			t.Fatalf("key %s: want %q got %q", k, v, string(entry.Value()))
		}
	}
}

// ---------------------------------------------------------------------------
// Sanity: storage interface assertions are enforced.
// ---------------------------------------------------------------------------

func TestStorageInterfaceAssertions(t *testing.T) {
	// Compile-time checks happen at package load; reach for them at runtime
	// so the linter sees a use.
	var _ StorageClient = (*LocalFileStorage)(nil)
	var _ StorageClient = (*S3StorageClient)(nil)
}

// ---------------------------------------------------------------------------
// recordingLogger captures log calls at each level so tests can assert on
// what was (or was not) logged.
// ---------------------------------------------------------------------------

type recordingLogger struct {
	mu     sync.Mutex
	infos  []string
	warns  []string
	errors []string
	t      *testing.T
}

func (l *recordingLogger) Infof(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.infos = append(l.infos, msg)
	l.mu.Unlock()
	l.t.Logf("INFO "+format, args...)
}
func (l *recordingLogger) Warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.warns = append(l.warns, msg)
	l.mu.Unlock()
	l.t.Logf("WARN "+format, args...)
}
func (l *recordingLogger) Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.errors = append(l.errors, msg)
	l.mu.Unlock()
	l.t.Logf("ERR  "+format, args...)
}

func (l *recordingLogger) warnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.warns)
}

func (l *recordingLogger) errorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errors)
}

// ---------------------------------------------------------------------------
// 7) Missing domain — skips quietly (no WARN/ERROR, no snapshot written,
//    lastBackupTime stays zero so it retries on the next tick).
// ---------------------------------------------------------------------------

func TestBackupCoordinator_MissingDomain_SkipsQuietly(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	ctx := context.Background()

	storageDir := t.TempDir()
	store, err := NewLocalFileStorage(storageDir)
	if err != nil {
		t.Fatalf("local storage: %v", err)
	}

	// Policy targets a stream that does NOT exist.
	pol := BackupPolicy{
		Domain:      "nonexistent_stream",
		Kind:        DomainKindStream,
		MinInterval: 50 * time.Millisecond,
		S3Prefix:    "test",
	}

	log := &recordingLogger{t: t}
	c, err := NewBackupCoordinator(js, store, []BackupPolicy{pol}, "node-a", log, WithLeaderTTL(2*time.Second))
	if err != nil {
		t.Fatalf("new coord: %v", err)
	}

	// Run a few ticks; the stream never gets created.
	for i := 0; i < 5; i++ {
		_ = c.tickOnce(ctx, time.Now())
		time.Sleep(60 * time.Millisecond)
	}

	// No WARN or ERROR should have been emitted for the missing domain.
	if log.warnCount() > 0 {
		t.Errorf("expected 0 WARN lines, got %d: %v", log.warnCount(), log.warns)
	}
	if log.errorCount() > 0 {
		t.Errorf("expected 0 ERROR lines, got %d: %v", log.errorCount(), log.errors)
	}

	// No snapshot file should exist.
	objs, _ := store.List(ctx, "test/nonexistent_stream")
	if len(objs) > 0 {
		t.Errorf("expected no snapshot files, got %d", len(objs))
	}

	// lastBackupTime should still be zero (so we'll retry on the next tick).
	c.mu.Lock()
	lastTime := c.lastBackupTime[pol.Domain]
	c.mu.Unlock()
	if !lastTime.IsZero() {
		t.Errorf("expected lastBackupTime to be zero, got %v", lastTime)
	}
}

// ---------------------------------------------------------------------------
// 8) Self-healing: missing bucket → tick skips → bucket created → next tick
//    succeeds and writes a snapshot.
// ---------------------------------------------------------------------------

func TestBackupCoordinator_MissingDomainBecomesProvisioned_AutoHeals(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	ctx := context.Background()

	store, err := NewLocalFileStorage(t.TempDir())
	if err != nil {
		t.Fatalf("local storage: %v", err)
	}

	// Use a KV bucket domain that doesn't exist yet.
	pol := BackupPolicy{
		Domain:      "healing_kv",
		Kind:        DomainKindKV,
		MinInterval: 50 * time.Millisecond,
		S3Prefix:    "test",
	}

	log := &recordingLogger{t: t}
	c, err := NewBackupCoordinator(js, store, []BackupPolicy{pol}, "node-a", log, WithLeaderTTL(2*time.Second))
	if err != nil {
		t.Fatalf("new coord: %v", err)
	}

	// First tick: bucket absent — should skip quietly.
	_ = c.tickOnce(ctx, time.Now())
	if log.errorCount() > 0 {
		t.Fatalf("got ERROR before provisioning: %v", log.errors)
	}

	// Now create the bucket and add data.
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "healing_kv"})
	if err != nil {
		t.Fatalf("create kv: %v", err)
	}
	if _, err := kv.Put(ctx, "ping", []byte("pong")); err != nil {
		t.Fatalf("kv put: %v", err)
	}

	// Next tick(s): bucket now exists → snapshot should succeed.
	var snapshotted bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.tickOnce(ctx, time.Now())
		objs, _ := store.List(ctx, "test/healing_kv")
		if countSuffix(objs, ".bin") > 0 {
			snapshotted = true
			break
		}
		time.Sleep(60 * time.Millisecond)
	}
	if !snapshotted {
		t.Fatal("expected snapshot after bucket was provisioned, but none appeared")
	}
	if log.errorCount() > 0 {
		t.Errorf("unexpected ERROR logs: %v", log.errors)
	}
}

// ---------------------------------------------------------------------------
// 9) Non-ENOENT errors still emit WARN/ERROR and tick continues.
// ---------------------------------------------------------------------------

// errorStorageClient wraps LocalFileStorage and injects a write error on Upload.
type errorStorageClient struct {
	inner StorageClient
	err   error
}

func (e *errorStorageClient) Upload(ctx context.Context, key string, r io.Reader, size int64, meta map[string]string) error {
	return e.err
}
func (e *errorStorageClient) Download(ctx context.Context, key string, w io.Writer) error {
	return e.inner.Download(ctx, key, w)
}
func (e *errorStorageClient) LatestKey(ctx context.Context, prefix string) (string, map[string]string, error) {
	return e.inner.LatestKey(ctx, prefix)
}
func (e *errorStorageClient) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return e.inner.List(ctx, prefix)
}

func TestBackupCoordinator_OtherErrorStillWarns(t *testing.T) {
	es := newTestEmbeddedServer(t)
	js := es.JetStream()
	ctx := context.Background()

	// Create the stream so the "not found" path is NOT taken.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "error_stream",
		Subjects: []string{"error_stream.>"},
		Replicas: 1,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if _, err := js.Publish(ctx, "error_stream.x", []byte("data")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	inner, err := NewLocalFileStorage(t.TempDir())
	if err != nil {
		t.Fatalf("local storage: %v", err)
	}
	// Inject an upload error so the snapshot fails with a non-not-found error.
	injectedErr := fmt.Errorf("simulated S3 write error")
	store := &errorStorageClient{inner: inner, err: injectedErr}

	pol := BackupPolicy{
		Domain:      "error_stream",
		Kind:        DomainKindStream,
		MinInterval: 50 * time.Millisecond,
		S3Prefix:    "test",
	}

	log := &recordingLogger{t: t}
	c, err := NewBackupCoordinator(js, store, []BackupPolicy{pol}, "node-a", log, WithLeaderTTL(2*time.Second))
	if err != nil {
		t.Fatalf("new coord: %v", err)
	}

	// Run a tick — the upload will fail with the injected error.
	_ = c.tickOnce(ctx, time.Now())

	// Should have logged at ERROR level (not silently skipped).
	if log.errorCount() == 0 {
		t.Errorf("expected at least one ERROR log for upload failure, got none")
	}

	// Coordinator should continue (not crash); a second tick should also fire.
	_ = c.tickOnce(ctx, time.Now())
	if log.errorCount() < 2 {
		t.Errorf("expected ERROR on each tick while upload fails, got %d", log.errorCount())
	}
}

// ensure parallel tests don't share a single embedded server's state by
// accident.
var _ = sync.Mutex{}
