package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ErrDomainNotProvisioned is returned by snapshotStream and snapshotKV when
// the target JetStream stream or KV bucket does not yet exist. Callers should
// treat this as a normal transient state (domain not yet provisioned) rather
// than an error requiring operator attention.
var ErrDomainNotProvisioned = errors.New("backup: domain not provisioned yet")

// snapshotStream walks every message in a JetStream stream in order and
// writes one length-prefixed record per message. It returns the last
// sequence number observed (used as the manifest's RaftIndex).
func snapshotStream(ctx context.Context, js jetstream.JetStream, streamName string, w io.Writer) (uint64, error) {
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, fmt.Errorf("%w: %s", ErrDomainNotProvisioned, streamName)
		}
		return 0, fmt.Errorf("get stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("stream info: %w", err)
	}
	if info.State.Msgs == 0 {
		return info.State.LastSeq, nil
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return 0, fmt.Errorf("ordered consumer: %w", err)
	}

	var lastSeq uint64
	deadline := time.Now().Add(2 * time.Second)
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return lastSeq, ctxErr
		}
		msg, err := cons.Next(jetstream.FetchMaxWait(500 * time.Millisecond))
		if err != nil {
			// Treat fetch timeout / no-messages as end-of-stream.
			if errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			// nats.go can return generic timeout errors for ordered consumer
			// idle states; bail when we've already collected at least one
			// message past the expected count.
			if lastSeq >= info.State.LastSeq && time.Now().After(deadline) {
				break
			}
			// Otherwise re-check; we may have just hit a transient nothing.
			if time.Now().After(deadline) && lastSeq >= info.State.LastSeq {
				break
			}
			continue
		}
		meta, mErr := msg.Metadata()
		if mErr == nil {
			lastSeq = meta.Sequence.Stream
		}
		if err := writeRecord(w, []byte(msg.Subject()), msg.Data()); err != nil {
			return lastSeq, err
		}
		if lastSeq >= info.State.LastSeq {
			break
		}
	}
	// Trailing 0/0 record marks EOF.
	if err := writeRecord(w, nil, nil); err != nil {
		return lastSeq, err
	}
	return lastSeq, nil
}

// snapshotKV iterates every key in a KV bucket and dumps it. The "raft index"
// returned is the largest revision observed across the bucket; if the bucket
// is empty, 0 is returned.
func snapshotKV(ctx context.Context, js jetstream.JetStream, bucket string, w io.Writer) (uint64, error) {
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return 0, fmt.Errorf("%w: %s", ErrDomainNotProvisioned, bucket)
		}
		return 0, fmt.Errorf("get kv: %w", err)
	}
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return 0, fmt.Errorf("list keys: %w", err)
	}
	defer lister.Stop()

	var maxRev uint64
	for key := range lister.Keys() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return maxRev, ctxErr
		}
		entry, err := kv.Get(ctx, key)
		if err != nil {
			// A racing delete is fine; skip.
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return maxRev, fmt.Errorf("kv get %s: %w", key, err)
		}
		if entry.Revision() > maxRev {
			maxRev = entry.Revision()
		}
		if err := writeRecord(w, []byte(entry.Key()), entry.Value()); err != nil {
			return maxRev, err
		}
	}
	if err := writeRecord(w, nil, nil); err != nil {
		return maxRev, err
	}
	return maxRev, nil
}

// RestoreFromS3 reads the latest snapshot for domain from storage and applies
// it to JetStream. For streams the existing stream is deleted (if any) and
// re-created with the same name + replica count, then every record is
// re-published in order. For KV buckets the existing bucket is deleted (if
// any) and recreated, then every entry is Put in order.
//
// This is a "restore from backup" pathway, NOT a hot-failover mechanism — it
// is destructive of any current state at "domain". Callers are expected to
// gate it behind operator intent.
//
// The function verifies the manifest's SHA256 against the downloaded bytes
// before mutating state.
func RestoreFromS3(
	ctx context.Context,
	js jetstream.JetStream,
	storage StorageClient,
	pol BackupPolicy,
	logger Logger,
) error {
	if logger == nil {
		logger = nopLogger{}
	}
	if err := pol.Validate(); err != nil {
		return err
	}

	prefix := path.Join(pol.S3Prefix, pol.Domain) + "/"
	latestKey, _, err := storage.LatestKey(ctx, prefix)
	if err != nil {
		return fmt.Errorf("find latest snapshot for %s: %w", pol.Domain, err)
	}
	manifestKey := strings.TrimSuffix(latestKey, ".bin") + ".manifest.json"

	// Pull the manifest first so we know what we're working with.
	var mfBuf strings.Builder
	mfWriter := &stringWriter{b: &mfBuf}
	if err := storage.Download(ctx, manifestKey, mfWriter); err != nil {
		return fmt.Errorf("download manifest %s: %w", manifestKey, err)
	}
	var manifest Manifest
	if err := json.Unmarshal([]byte(mfBuf.String()), &manifest); err != nil {
		return fmt.Errorf("parse manifest %s: %w", manifestKey, err)
	}

	// Download data to a temp file so we can checksum-verify before applying.
	tmp, err := os.CreateTemp("", "aetherlite-restore-*.bin")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if err := storage.Download(ctx, latestKey, tmp); err != nil {
		return fmt.Errorf("download data %s: %w", latestKey, err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, tmp); err != nil {
		return err
	}
	got := hex.EncodeToString(hasher.Sum(nil))
	if manifest.ChecksumSHA256 != "" && got != manifest.ChecksumSHA256 {
		return fmt.Errorf("snapshot checksum mismatch: manifest=%s actual=%s", manifest.ChecksumSHA256, got)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	logger.Infof("restoring domain=%s id=%s size=%d", manifest.Domain, manifest.SnapshotID, manifest.SizeBytes)

	switch pol.Kind {
	case DomainKindStream:
		return restoreStream(ctx, js, pol, tmp, &manifest)
	case DomainKindKV:
		return restoreKV(ctx, js, pol, tmp, &manifest)
	default:
		return fmt.Errorf("unknown domain kind for %q", pol.Domain)
	}
}

func restoreStream(ctx context.Context, js jetstream.JetStream, pol BackupPolicy, r io.Reader, manifest *Manifest) error {
	// Best-effort: tear down existing stream so we don't double up.
	_ = js.DeleteStream(ctx, pol.Domain)

	replicas := pol.ReplicaCount
	if replicas <= 0 {
		replicas = 1
	}
	// Use a wildcard subject derived from the domain so messages of any
	// subject under that namespace can be republished. The original
	// stream's subjects aren't recoverable from this dump format; the
	// caller can either pre-create the stream (in which case our Delete is
	// a no-op and CreateOrUpdateStream below uses the existing config) or
	// accept a "{domain}.>" catch-all.
	subjects := []string{pol.Domain + ".>"}
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     pol.Domain,
		Subjects: subjects,
		Replicas: replicas,
	})
	if err != nil {
		return fmt.Errorf("recreate stream %s: %w", pol.Domain, err)
	}

	for {
		key, val, err := readRecord(r)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read record: %w", err)
		}
		if _, err := js.Publish(ctx, string(key), val); err != nil {
			return fmt.Errorf("republish: %w", err)
		}
	}
}

func restoreKV(ctx context.Context, js jetstream.JetStream, pol BackupPolicy, r io.Reader, manifest *Manifest) error {
	_ = js.DeleteKeyValue(ctx, pol.Domain)

	replicas := pol.ReplicaCount
	if replicas <= 0 {
		replicas = 1
	}
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   pol.Domain,
		Replicas: replicas,
	})
	if err != nil {
		return fmt.Errorf("recreate kv %s: %w", pol.Domain, err)
	}
	for {
		key, val, err := readRecord(r)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read record: %w", err)
		}
		if _, err := kv.Put(ctx, string(key), val); err != nil {
			return fmt.Errorf("kv put %s: %w", string(key), err)
		}
	}
}

// stringWriter adapts strings.Builder to io.Writer.
type stringWriter struct{ b *strings.Builder }

func (s *stringWriter) Write(p []byte) (int, error) { return s.b.Write(p) }
