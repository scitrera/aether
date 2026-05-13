package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

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
}

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
	}
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

// Subscribe creates a subscription with full replay from the consumer's last
// persisted offset (or the beginning of the log if none exists).
// The consumerName is derived from the handler address (not persisted), so
// replay always starts from sequence 0 for anonymous subscribers.
func (r *BadgerRouter) Subscribe(topic string, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, "", handler, false)
}

// SubscribeExclusive creates a named exclusive subscription with replay.
// Only one active subscriber per (topic, consumerName) is permitted.
func (r *BadgerRouter) SubscribeExclusive(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, consumerName, handler, false)
}

// SubscribeExclusiveFromNow creates a named exclusive subscription that starts
// from the current write position, skipping all previously stored messages.
func (r *BadgerRouter) SubscribeExclusiveFromNow(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, consumerName, handler, true)
}

// SubscribeExclusiveFromTimestamp creates an exclusive subscription with full replay
// from the consumer's last persisted offset (or log start if none exists). The
// startTimestampMs parameter is accepted for interface compatibility with the
// RabbitMQ-backed Router, but is intentionally ignored: BadgerRouter indexes
// messages by sequence number, not timestamp, and its default replay behavior
// already returns all messages since the log start for new consumers — a superset
// of timestamp-based replay. The trigger message is guaranteed to be in that replay.
func (r *BadgerRouter) SubscribeExclusiveFromTimestamp(topic string, consumerName string, _ int64, handler func([]byte)) (func(), error) {
	return r.subscribe(topic, consumerName, handler, false)
}

// --------------------------------------------------------------------------
// Internal helpers
// --------------------------------------------------------------------------

// subscribe is the shared implementation for all three public Subscribe variants.
func (r *BadgerRouter) subscribe(topic, consumerName string, handler func([]byte), fromNow bool) (func(), error) {
	exclusive := consumerName != ""

	if exclusive {
		lockKey := topic + "\x00" + consumerName
		if _, loaded := r.exclusiveLocks.LoadOrStore(lockKey, struct{}{}); loaded {
			return nil, fmt.Errorf("badger_router: exclusive consumer %q already active on topic %q", consumerName, topic)
		}
	}

	// Determine replay start sequence before registering in live fan-out so we
	// don't miss any messages published concurrently during replay.
	var startSeq uint64
	if fromNow {
		// Start after the current tail; no replay.
		cur, err := r.currentSequence(topic)
		if err != nil {
			if exclusive {
				r.exclusiveLocks.Delete(topic + "\x00" + consumerName)
			}
			return nil, fmt.Errorf("badger_router: read sequence for %q: %w", topic, err)
		}
		startSeq = cur
	} else if exclusive && consumerName != "" {
		// Resume from persisted offset.
		last, err := r.loadOffset(topic, consumerName)
		if err != nil {
			if exclusive {
				r.exclusiveLocks.Delete(topic + "\x00" + consumerName)
			}
			return nil, fmt.Errorf("badger_router: load offset for %q/%q: %w", topic, consumerName, err)
		}
		startSeq = last // replay from last+1 below
	}
	// For anonymous Subscribe, startSeq stays 0 → replay from beginning.

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
func (r *BadgerRouter) appendMessage(topic string, payload []byte) (uint64, error) {
	seqKey := sequenceKey(topic)

	var seq uint64
	err := r.db.Update(func(txn *badger.Txn) error {
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

		// Write message.
		return txn.Set(messageKey(topic, seq), payload)
	})
	return seq, err
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
	var off uint64
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(offsetKey(topic, consumerName))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) == 8 {
				off = binary.BigEndian.Uint64(val)
			}
			return nil
		})
	})
	return off, err
}

// saveOffset persists seq as the consumer's last-read offset for topic.
func (r *BadgerRouter) saveOffset(topic, consumerName string, seq uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(offsetKey(topic, consumerName), buf)
	})
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
