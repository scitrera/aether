package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/scitrera/aether/internal/lite"
	"github.com/scitrera/aether/internal/logging"
)

// defaultSubscriberBufferSize is the default channel buffer size for live fan-out
// per subscriber. Messages beyond this are dropped from the live path (they remain
// persisted in Badger and can be replayed on reconnect for named consumers).
const defaultSubscriberBufferSize = 256

// BadgerRouter is a persistent MessageRouter backed by a Badger database.
// It provides append-only per-topic message logs with consumer offset tracking
// and in-process live fan-out to active subscribers.
//
// Delivery semantics:
//   - All messages are durably persisted to Badger before fan-out.
//   - Live fan-out to active subscribers is at-most-once (channel-buffered).
//     If a subscriber's buffer is full, messages are dropped from the live path
//     to avoid head-of-line blocking for other subscribers. Dropped messages
//     remain in Badger and will be replayed on reconnect for named consumers
//     (SubscribeExclusive) via consumer offset tracking. Anonymous subscribers
//     (Subscribe) lose dropped messages permanently until they reconnect and
//     replay from offset 0.
//
// Key layout in Badger:
//
//	msg:{topic}:{sequence:016x}  → payload bytes       (message log)
//	seq:{topic}                  → uint64 big-endian    (next sequence number)
//	off:{topic}:{consumerName}   → uint64 big-endian    (last-read sequence per consumer)
type BadgerRouter struct {
	db *badger.DB

	// subscriberBufferSize is the per-subscriber channel buffer size.
	// Larger values reduce drop risk but use more memory per subscriber.
	subscriberBufferSize int

	// mu protects the subs map.
	mu   sync.RWMutex
	subs map[string][]*subscriber

	// exclusiveLocks tracks which (topic, consumerName) pairs already have an
	// active exclusive subscriber. The stored value is always struct{}{}.
	exclusiveLocks sync.Map // key: topic+"\x00"+consumerName

	// messageTTL bounds how long a published message is retained before Badger
	// expires it (native per-entry TTL, reclaimed by Badger's value-log GC). It
	// caps otherwise-unbounded topic-log growth and the size of any cold-start /
	// full-replay burst, while comfortably exceeding the realistic reconnect gap
	// so a resuming consumer still catches up. The sequence counter and consumer
	// offsets carry NO TTL, so expiry never rewinds them: a consumer resuming
	// past an expired range simply finds fewer messages to replay. Zero disables
	// expiry (retain forever). Set at construction; treat as immutable after use.
	messageTTL time.Duration
}

// defaultMessageRetentionTTL is the default per-message retention. 24h caps
// growth at roughly a day of traffic per topic while dwarfing the real reconnect
// gap (seconds–minutes). Override via SetMessageRetentionTTL before first use.
const defaultMessageRetentionTTL = 24 * time.Hour

// msgWithSeq bundles a message payload with its Badger sequence number so that
// the drain goroutine can persist the exact offset of each processed message.
type msgWithSeq struct {
	payload []byte
	seq     uint64
}

// subscriber represents a single active subscription on a topic.
type subscriber struct {
	handler      func([]byte)
	ch           chan msgWithSeq
	done         chan struct{}
	name         string // empty for non-exclusive
	replayedUpTo uint64 // drain skips messages with seq <= this to avoid replay duplicates
}

// NewBadgerRouter creates a BadgerRouter using the provided Badger database.
// Uses the default subscriber buffer size. For a custom buffer size, use
// NewBadgerRouterWithBufferSize.
func NewBadgerRouter(db *badger.DB) *BadgerRouter {
	return NewBadgerRouterWithBufferSize(db, defaultSubscriberBufferSize)
}

// NewBadgerRouterWithBufferSize creates a BadgerRouter with a custom subscriber
// channel buffer size. Larger values reduce drop risk under bursty loads but
// use more memory per subscriber.
func NewBadgerRouterWithBufferSize(db *badger.DB, bufferSize int) *BadgerRouter {
	if bufferSize <= 0 {
		bufferSize = defaultSubscriberBufferSize
	}
	return &BadgerRouter{
		db:                   db,
		subscriberBufferSize: bufferSize,
		subs:                 make(map[string][]*subscriber),
		messageTTL:           defaultMessageRetentionTTL,
	}
}

// SetMessageRetentionTTL overrides the per-message retention TTL. A value <= 0
// disables expiry (messages retained forever). Call once at startup before the
// router serves traffic; not safe to change concurrently with Publish.
func (r *BadgerRouter) SetMessageRetentionTTL(d time.Duration) {
	if d < 0 {
		d = 0
	}
	r.messageTTL = d
}

// Close shuts down all active subscribers.
func (r *BadgerRouter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, list := range r.subs {
		for _, s := range list {
			select {
			case <-s.done:
				// already closed
			default:
				close(s.done)
			}
		}
	}
	r.subs = make(map[string][]*subscriber)
	return nil
}

// --------------------------------------------------------------------------
// MessageRouter interface
// --------------------------------------------------------------------------

// Publish persists the message to Badger and fans it out to live subscribers.
func (r *BadgerRouter) Publish(_ context.Context, topic string, payload []byte) error {
	seq, err := r.appendMessage(topic, payload)
	if err != nil {
		return fmt.Errorf("badger_router: publish to %q: %w", topic, err)
	}

	r.mu.RLock()
	list := r.subs[topic]
	// Snapshot the slice so we can release the lock before calling handlers.
	snapshot := make([]*subscriber, len(list))
	copy(snapshot, list)
	r.mu.RUnlock()

	for _, s := range snapshot {
		select {
		case <-s.done:
			// subscriber gone; skip
		case s.ch <- msgWithSeq{payload: payload, seq: seq}:
			// delivered to drain goroutine
		default:
			logging.Logger.Warn().Str("topic", topic).Str("consumer", s.name).
				Msg("badger_router: subscriber channel full, dropping message")
		}
	}
	return nil
}

// startPolicy selects where a subscription begins reading when it is created.
type startPolicy int

const (
	// startResume resumes from the named consumer's committed offset; a cold
	// consumer (no committed offset) — and every anonymous subscriber — starts
	// at sequence 0 and replays the entire retained log.
	startResume startPolicy = iota
	// startTail starts at the current tail and never replays, ignoring any
	// committed offset.
	startTail
	// startResumeOrTail resumes from the named consumer's committed offset when
	// one exists, and otherwise starts at the current tail (NOT sequence 0). This
	// is the right default for shared/broadcast lanes a client re-subscribes on
	// every connect: first connect gets no history dump, reconnect replays only
	// the gap since the last committed offset.
	startResumeOrTail
)

// Subscribe creates a subscription with full replay from the consumer's last
// persisted offset (or the beginning of the log if none exists).
// The consumerName is derived from the handler address (not persisted), so
// replay always starts from sequence 0 for anonymous subscribers.
func (r *BadgerRouter) Subscribe(topic string, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, "", handler, startResume)
}

// SubscribeExclusive creates a named exclusive subscription with replay.
// Only one active subscriber per (topic, consumerName) is permitted.
func (r *BadgerRouter) SubscribeExclusive(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, consumerName, handler, startResume)
}

// SubscribeExclusiveFromNow creates a named exclusive subscription that starts
// from the current write position, skipping all previously stored messages.
func (r *BadgerRouter) SubscribeExclusiveFromNow(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, consumerName, handler, startTail)
}

// SubscribeExclusiveResumeOrTail creates a named exclusive subscription that
// resumes from the consumer's last committed offset, or — when no offset has
// been committed yet (a brand-new consumer) — starts at the current tail instead
// of replaying the whole topic from sequence 0. Use it for shared/broadcast
// lanes a client re-subscribes on every (re)connect so a first connect gets no
// history dump and a reconnect replays only the gap. Contrast SubscribeExclusive
// (cold → full replay from 0) and SubscribeExclusiveFromNow (always tail).
func (r *BadgerRouter) SubscribeExclusiveResumeOrTail(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, consumerName, handler, startResumeOrTail)
}

// SubscribeExclusiveFromTimestamp creates an exclusive subscription with full replay
// from the consumer's last persisted offset (or log start if none exists). The
// startTimestampMs parameter is accepted for interface compatibility with the
// RabbitMQ-backed Router, but is intentionally ignored: BadgerRouter indexes
// messages by sequence number, not timestamp, and its default replay behavior
// already returns all messages since the log start for new consumers — a superset
// of timestamp-based replay. The trigger message is guaranteed to be in that replay.
func (r *BadgerRouter) SubscribeExclusiveFromTimestamp(topic string, consumerName string, _ int64, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, consumerName, handler, startResume)
}

// --------------------------------------------------------------------------
// Internal helpers
// --------------------------------------------------------------------------

// subscribe is the shared implementation for all three public Subscribe variants.
func (r *BadgerRouter) subscribe(topic, consumerName string, handler func([]byte), policy startPolicy) (func(), error) {
	exclusive := consumerName != ""

	if exclusive {
		lockKey := topic + "\x00" + consumerName
		if _, loaded := r.exclusiveLocks.LoadOrStore(lockKey, struct{}{}); loaded {
			return nil, fmt.Errorf("badger_router: exclusive consumer %q already active on topic %q", consumerName, topic)
		}
	}
	// releaseLock undoes the exclusive lock reservation on any early-return error
	// path (before the drain goroutine / unsub takes ownership of it).
	releaseLock := func() {
		if exclusive {
			r.exclusiveLocks.Delete(topic + "\x00" + consumerName)
		}
	}

	// Determine replay start sequence before registering in live fan-out so we
	// don't miss any messages published concurrently during replay.
	var startSeq uint64
	switch policy {
	case startTail:
		// Start after the current tail; no replay.
		cur, err := r.currentSequence(topic)
		if err != nil {
			releaseLock()
			return nil, fmt.Errorf("badger_router: read sequence for %q: %w", topic, err)
		}
		startSeq = cur
	case startResumeOrTail:
		if exclusive {
			// Resume from the committed offset if one exists; otherwise start at
			// the current tail (a cold consumer skips the retained backlog rather
			// than replaying from sequence 0).
			last, ok, err := r.loadOffsetOK(topic, consumerName)
			if err != nil {
				releaseLock()
				return nil, fmt.Errorf("badger_router: load offset for %q/%q: %w", topic, consumerName, err)
			}
			if ok {
				startSeq = last // warm: replay only the gap (from last+1 below)
			} else {
				cur, cerr := r.currentSequence(topic)
				if cerr != nil {
					releaseLock()
					return nil, fmt.Errorf("badger_router: read sequence for %q: %w", topic, cerr)
				}
				startSeq = cur // cold: start at tail
			}
		} else {
			// Anonymous resume-or-tail has no offset to resume from → start at tail.
			cur, err := r.currentSequence(topic)
			if err != nil {
				releaseLock()
				return nil, fmt.Errorf("badger_router: read sequence for %q: %w", topic, err)
			}
			startSeq = cur
		}
	default: // startResume
		if exclusive {
			// Resume from persisted offset (cold → 0 → full replay).
			last, err := r.loadOffset(topic, consumerName)
			if err != nil {
				releaseLock()
				return nil, fmt.Errorf("badger_router: load offset for %q/%q: %w", topic, consumerName, err)
			}
			startSeq = last // replay from last+1 below
		}
		// For anonymous Subscribe, startSeq stays 0 → replay from beginning.
	}

	s := &subscriber{
		handler: handler,
		ch:      make(chan msgWithSeq, r.subscriberBufferSize),
		done:    make(chan struct{}),
		name:    consumerName,
	}

	// Register in live fan-out before replay to avoid missing concurrent publishes.
	// Messages published during replay are queued in s.ch; after replay completes
	// we record replayedUpTo so drain can discard duplicates.
	r.mu.Lock()
	r.subs[topic] = append(r.subs[topic], s)
	r.mu.Unlock()

	// Replay historical messages synchronously. Any concurrent Publish calls
	// queue into s.ch. We track the highest sequence replayed so that drain
	// can skip those duplicates.
	var replayedUpTo uint64
	if err := r.replay(topic, consumerName, startSeq, handler, &replayedUpTo); err != nil {
		// Replay failed — remove from fan-out and clean up.
		r.removeSubscriber(topic, s)
		if exclusive {
			r.exclusiveLocks.Delete(topic + "\x00" + consumerName)
		}
		return nil, fmt.Errorf("badger_router: replay for %q: %w", topic, err)
	}
	// Tell drain to skip any live messages that were already delivered by replay.
	s.replayedUpTo = replayedUpTo

	// Persist the consumer offset for the replayed range. drain saves the offset
	// per live message, but replay (the reconnect catch-up path) did not — so a
	// named consumer that caught up via replay never committed its progress, and
	// those messages re-replayed on every reconnect. This is especially harmful
	// for messages that only ever reach a consumer via replay: Publish drops from
	// the live fan-out channel (non-blocking) when it is full, but still persists
	// to badger, so a slow consumer's messages are delivered only via replay and,
	// without this save, re-shed on every reconnect forever. Commit the highest
	// replayed sequence so the next reconnect resumes from head. Named consumers
	// only (anonymous subscribers, consumerName=="", don't track offsets).
	if consumerName != "" && replayedUpTo > startSeq {
		if err := r.saveOffset(topic, consumerName, replayedUpTo); err != nil {
			logging.Logger.Error().Err(err).Str("topic", topic).Str("consumer", consumerName).
				Msg("badger_router: failed to save consumer offset after replay")
		}
	}

	// Start drain goroutine.
	go r.drain(s, topic)

	unsub := func() {
		r.removeSubscriber(topic, s)
		if exclusive {
			r.exclusiveLocks.Delete(topic + "\x00" + consumerName)
		}
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}
	return unsub, nil
}

// drain reads from s.ch and calls s.handler until s.done is closed.
// Messages with seq <= s.replayedUpTo are skipped because they were already
// delivered synchronously during replay.
func (r *BadgerRouter) drain(s *subscriber, topic string) {
	for {
		select {
		case <-s.done:
			return
		case msg, ok := <-s.ch:
			if !ok {
				return
			}
			// Skip messages that were already delivered during replay.
			if msg.seq <= s.replayedUpTo {
				continue
			}
			s.handler(msg.payload)
			if s.name != "" {
				// Best-effort offset update; errors are logged but non-fatal.
				if err := r.saveOffset(topic, s.name, msg.seq); err != nil {
					logging.Logger.Error().Err(err).Str("topic", topic).Str("consumer", s.name).
						Msg("badger_router: failed to save consumer offset")
				}
			}
		}
	}
}

// removeSubscriber removes s from the fan-out list for topic.
func (r *BadgerRouter) removeSubscriber(topic string, s *subscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()

	list := r.subs[topic]
	for i, v := range list {
		if v == s {
			r.subs[topic] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(r.subs[topic]) == 0 {
		delete(r.subs, topic)
	}
}

// --------------------------------------------------------------------------
// Badger key helpers
// --------------------------------------------------------------------------

// messageKey returns the Badger key for a specific message.
func messageKey(topic string, seq uint64) []byte {
	return []byte(fmt.Sprintf("%s%s:%016x", lite.PrefixMessage, topic, seq))
}

// sequenceKey returns the Badger key for a topic's sequence counter.
func sequenceKey(topic string) []byte {
	return []byte(lite.PrefixSequence + topic)
}

// offsetKey returns the Badger key for a consumer's offset on a topic.
func offsetKey(topic, consumerName string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s", lite.PrefixOffset, topic, consumerName))
}

// --------------------------------------------------------------------------
// Badger I/O
// --------------------------------------------------------------------------

// appendMessage atomically increments the topic sequence and writes the
// message payload. Returns the sequence number assigned to this message.
//
// Concurrent publishes to the same topic race on the per-topic sequence key
// (read-then-write inside a single Update txn). Badger surfaces the conflict
// as ErrConflict; we retry a bounded number of times with no backoff because
// the transactions are sub-millisecond and the conflict resolves the moment
// the winning writer commits. The retry cap protects against pathological
// starvation but is intentionally generous — under heavy concurrent fan-in
// to a single sidecar topic (the multicaller scenario) we routinely see 3-4
// conflicts in a row before all writers commit.
const appendMessageMaxRetries = 16

func (r *BadgerRouter) appendMessage(topic string, payload []byte) (uint64, error) {
	seqKey := sequenceKey(topic)

	var (
		seq     uint64
		lastErr error
	)
	for attempt := 0; attempt < appendMessageMaxRetries; attempt++ {
		lastErr = r.db.Update(func(txn *badger.Txn) error {
			// Read current sequence (default 0 if absent).
			var cur uint64
			item, err := txn.Get(seqKey)
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err == nil {
				if err = item.Value(func(val []byte) error {
					if len(val) == 8 {
						cur = binary.BigEndian.Uint64(val)
					}
					return nil
				}); err != nil {
					return err
				}
			}

			seq = cur + 1

			// Write incremented sequence.
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, seq)
			if err = txn.Set(seqKey, buf); err != nil {
				return err
			}

			// Write message. Carries a TTL so Badger natively expires old
			// messages and bounds topic-log growth; the seq counter (above) has
			// no TTL so sequence numbering is never rewound.
			if r.messageTTL > 0 {
				return txn.SetEntry(badger.NewEntry(messageKey(topic, seq), payload).WithTTL(r.messageTTL))
			}
			return txn.Set(messageKey(topic, seq), payload)
		})
		if lastErr == nil {
			return seq, nil
		}
		if lastErr != badger.ErrConflict {
			return 0, lastErr
		}
	}
	return 0, lastErr
}

// currentSequence returns the current (latest) sequence number for a topic.
// Returns 0 if no messages have been published yet.
func (r *BadgerRouter) currentSequence(topic string) (uint64, error) {
	var cur uint64
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(sequenceKey(topic))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) == 8 {
				cur = binary.BigEndian.Uint64(val)
			}
			return nil
		})
	})
	return cur, err
}

// loadOffset returns the last-read sequence number for a named consumer.
// Returns 0 if no offset has been persisted yet.
func (r *BadgerRouter) loadOffset(topic, consumerName string) (uint64, error) {
	off, _, err := r.loadOffsetOK(topic, consumerName)
	return off, err
}

// loadOffsetOK returns the last-read sequence number for a named consumer and
// whether a committed offset actually exists. The bool distinguishes "no offset
// persisted yet" (found=false) from a genuinely committed offset of 0, which
// callers need to decide between full replay and tail on a cold consumer.
func (r *BadgerRouter) loadOffsetOK(topic, consumerName string) (uint64, bool, error) {
	var off uint64
	var found bool
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(offsetKey(topic, consumerName))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			if len(val) == 8 {
				off = binary.BigEndian.Uint64(val)
			}
			return nil
		})
	})
	return off, found, err
}

// saveOffset persists seq as the consumer's last-read offset for topic.
//
// Retries on badger.ErrConflict: the offset key is written from the consumer's
// drain goroutine (per live message) and once from the replay catch-up path, and
// under a connection storm / heavy concurrent publish+commit load the optimistic
// txn can conflict. Without the retry the save silently fails (logged, non-fatal)
// and the offset stalls, so the consumer re-replays the same backlog on the next
// reconnect and re-sheds it under gateway backpressure. Mirrors appendMessage's
// bounded ErrConflict retry (sub-millisecond txns; no backoff needed).
func (r *BadgerRouter) saveOffset(topic, consumerName string, seq uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)
	var lastErr error
	for attempt := 0; attempt < appendMessageMaxRetries; attempt++ {
		lastErr = r.db.Update(func(txn *badger.Txn) error {
			return txn.Set(offsetKey(topic, consumerName), buf)
		})
		if lastErr == nil {
			return nil
		}
		if lastErr != badger.ErrConflict {
			return lastErr
		}
	}
	return lastErr
}

// replay calls handler for every message in the range (startSeq, currentSeq].
// For anonymous subscribers (consumerName == "") startSeq is 0, replaying all.
// For named subscribers startSeq is the last committed offset.
// replayedUpTo is updated to the highest sequence number delivered, so the
// caller can set subscriber.replayedUpTo to suppress duplicates in drain.
func (r *BadgerRouter) replay(topic, consumerName string, startSeq uint64, handler func([]byte), replayedUpTo *uint64) error {
	// Prefix for this topic's messages.
	prefix := []byte(fmt.Sprintf("%s%s:", lite.PrefixMessage, topic))

	// Lower bound key (exclusive): message at startSeq — we start from startSeq+1.
	from := messageKey(topic, startSeq+1)

	// prefixLen is the byte length of the key portion before the hex sequence.
	// key format: PrefixMessage + topic + ":" + 016x
	prefixLen := len(prefix)

	return r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 64
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(from); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			// Extract the sequence number from the key suffix (16 hex chars).
			key := item.Key()
			var seq uint64
			if len(key) >= prefixLen+16 {
				hexPart := key[prefixLen : prefixLen+16]
				for _, b := range hexPart {
					seq <<= 4
					switch {
					case b >= '0' && b <= '9':
						seq |= uint64(b - '0')
					case b >= 'a' && b <= 'f':
						seq |= uint64(b-'a') + 10
					case b >= 'A' && b <= 'F':
						seq |= uint64(b-'A') + 10
					}
				}
			}

			if err := item.Value(func(val []byte) error {
				// Copy the value because it is only valid within the txn.
				cp := make([]byte, len(val))
				copy(cp, val)
				handler(cp)
				return nil
			}); err != nil {
				return err
			}

			if seq > *replayedUpTo {
				*replayedUpTo = seq
			}
		}
		return nil
	})
}
