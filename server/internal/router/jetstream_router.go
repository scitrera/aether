// Package router provides MessageRouter implementations for Aether.
// This file implements JetStreamRouter, a MessageRouter backed by NATS JetStream.
package router

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/router/natscodec"
)

// knownStreams maps the aether topic prefix (first token before "::") to the
// JetStream stream name and its subject filter pattern.
var knownStreams = map[string]struct {
	name     string
	subjects []string
}{
	"ag":     {name: "ag", subjects: []string{"ag.>"}},
	"tu":     {name: "tu", subjects: []string{"tu.>"}},
	"ta":     {name: "ta", subjects: []string{"ta.>"}},
	"tb":     {name: "tb", subjects: []string{"tb.>"}},
	"us":     {name: "us", subjects: []string{"us.>"}},
	"uw":     {name: "uw", subjects: []string{"uw.>"}},
	"ga":     {name: "ga", subjects: []string{"ga.>"}},
	"gu":     {name: "gu", subjects: []string{"gu.>"}},
	"pg":     {name: "pg", subjects: []string{"pg.>"}},
	"br":     {name: "br", subjects: []string{"br.>"}},
	"sv":     {name: "sv", subjects: []string{"sv.>"}},
	"event":  {name: "event", subjects: []string{"event.>"}},
	"metric": {name: "metric", subjects: []string{"metric.>"}},
	"tk":     {name: "tk", subjects: []string{"tk.>"}},
}

// defaultAckWait is how long JetStream waits before redelivering an unacked message.
const defaultAckWait = 30 * time.Second

// streamMaxAge is how long messages are retained in JetStream streams.
const streamMaxAge = 24 * time.Hour

// Logger is the minimal logging interface accepted by JetStreamRouter.
// The global logging.Logger satisfies this interface.
type Logger interface {
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// JetStreamRouter is a persistent MessageRouter backed by NATS JetStream.
// Each aether topic prefix maps to a dedicated JetStream stream with
// 24-hour message retention. Durable (exclusive) consumers resume from
// their stored offset across reconnects; ephemeral consumers start from
// the current tail (Subscribe) or a specified delivery policy.
type JetStreamRouter struct {
	js       jetstream.JetStream
	replicas int
	log      Logger
}

// NewJetStreamRouter creates a JetStreamRouter and ensures all required
// JetStream streams exist. Stream creation is idempotent: existing streams
// whose config matches are left unchanged.
func NewJetStreamRouter(js jetstream.JetStream, replicas int, log Logger) (*JetStreamRouter, error) {
	if replicas <= 0 {
		replicas = 1
	}
	if log == nil {
		log = &zerologAdapter{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for prefix, info := range knownStreams {
		cfg := jetstream.StreamConfig{
			Name:      info.name,
			Subjects:  info.subjects,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    streamMaxAge,
			Storage:   jetstream.FileStorage,
			Replicas:  replicas,
		}
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return nil, fmt.Errorf("jetstream_router: ensure stream %q (prefix %q): %w", info.name, prefix, err)
		}
	}

	return &JetStreamRouter{
		js:       js,
		replicas: replicas,
		log:      log,
	}, nil
}

// --------------------------------------------------------------------------
// MessageRouter interface
// --------------------------------------------------------------------------

// Publish converts the aether topic to a NATS subject and publishes the payload.
func (r *JetStreamRouter) Publish(ctx context.Context, topic string, payload []byte) error {
	subject := natscodec.ToNATSSubject(topic)
	if _, err := r.js.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("jetstream_router: publish to %q (subject %q): %w", topic, subject, err)
	}
	return nil
}

// Subscribe creates an ephemeral ordered consumer that delivers messages from
// the current tail onward. The returned cancel func stops the consumer.
// Note: ephemeral ordered consumers do not persist offsets; reconnecting
// always starts from "new" messages at subscribe time.
func (r *JetStreamRouter) Subscribe(topic string, handler func([]byte)) (func(), error) {
	subject := natscodec.ToNATSSubject(topic)
	streamName, err := streamNameForSubject(subject)
	if err != nil {
		return nil, fmt.Errorf("jetstream_router: subscribe %q: %w", topic, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := r.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("jetstream_router: get stream %q for topic %q: %w", streamName, topic, err)
	}

	consCtx, consCancel := context.WithCancel(context.Background())

	cons, err := stream.OrderedConsumer(consCtx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverNewPolicy,
	})
	if err != nil {
		consCancel()
		return nil, fmt.Errorf("jetstream_router: ordered consumer for %q: %w", topic, err)
	}

	msgCtx, err := cons.Messages()
	if err != nil {
		consCancel()
		return nil, fmt.Errorf("jetstream_router: start message iterator for %q: %w", topic, err)
	}

	go func() {
		defer msgCtx.Stop()
		for {
			msg, err := msgCtx.Next()
			if err != nil {
				// Iterator stopped or context cancelled — normal shutdown.
				return
			}
			handler(msg.Data())
			_ = msg.Ack()
		}
	}()

	return func() {
		consCancel()
		msgCtx.Stop()
	}, nil
}

// SubscribeExclusive creates a durable push consumer that resumes from its
// stored offset on reconnect. Only one active subscription per consumerName
// per stream is recommended (NATS enforces this server-side for durable consumers).
func (r *JetStreamRouter) SubscribeExclusive(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscribeDurable(topic, consumerName, jetstream.DeliverAllPolicy, 0, handler)
}

// SubscribeExclusiveFromNow creates a durable consumer that starts from the
// current write position, ignoring all previously published messages.
func (r *JetStreamRouter) SubscribeExclusiveFromNow(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return r.subscribeDurable(topic, consumerName, jetstream.DeliverNewPolicy, 0, handler)
}

// SubscribeExclusiveFromTimestamp creates a durable consumer. When
// startTimestampMs > 0, new consumers start from messages at or after that
// unix-millisecond timestamp; existing durable consumers resume from their
// stored offset regardless of startTimestampMs (NATS ignores DeliverPolicy
// updates for existing consumers). When startTimestampMs <= 0, falls back to
// DeliverAllPolicy (replay from beginning for new consumers).
func (r *JetStreamRouter) SubscribeExclusiveFromTimestamp(topic string, consumerName string, startTimestampMs int64, handler func([]byte)) (func(), error) {
	if startTimestampMs <= 0 {
		return r.subscribeDurable(topic, consumerName, jetstream.DeliverAllPolicy, 0, handler)
	}
	return r.subscribeDurable(topic, consumerName, jetstream.DeliverByStartTimePolicy, startTimestampMs, handler)
}

// --------------------------------------------------------------------------
// Internal helpers
// --------------------------------------------------------------------------

// subscribeDurable is the shared implementation for all exclusive/durable Subscribe variants.
func (r *JetStreamRouter) subscribeDurable(topic, consumerName string, deliverPolicy jetstream.DeliverPolicy, startTimestampMs int64, handler func([]byte)) (func(), error) {
	subject := natscodec.ToNATSSubject(topic)
	streamName, err := streamNameForSubject(subject)
	if err != nil {
		return nil, fmt.Errorf("jetstream_router: subscribe exclusive %q: %w", topic, err)
	}

	// Aether-form consumerName may carry characters NATS rejects in a durable
	// consumer name (e.g. "us::user@example.com::win-1"). Escape it through
	// the consumer-name namespace so it becomes a valid NATS identifier.
	// The escape is deterministic, so the same input always maps to the same
	// durable name — preserving offset resumption across reconnects.
	natsConsumerName := natscodec.EscapeForConsumerName(consumerName)

	cfg := jetstream.ConsumerConfig{
		Durable:       natsConsumerName,
		FilterSubject: subject,
		DeliverPolicy: deliverPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       defaultAckWait,
	}
	if deliverPolicy == jetstream.DeliverByStartTimePolicy && startTimestampMs > 0 {
		t := time.UnixMilli(startTimestampMs)
		cfg.OptStartTime = &t
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, err := r.js.CreateOrUpdateConsumer(ctx, streamName, cfg)
	if err != nil {
		return nil, fmt.Errorf("jetstream_router: create durable consumer %q on %q: %w", consumerName, topic, err)
	}

	msgCtx, err := cons.Messages()
	if err != nil {
		return nil, fmt.Errorf("jetstream_router: start message iterator for %q/%q: %w", topic, consumerName, err)
	}

	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		defer msgCtx.Stop()
		for {
			msg, err := msgCtx.Next()
			if err != nil {
				// Iterator stopped — normal shutdown.
				return
			}
			handler(msg.Data())
			if err := msg.Ack(); err != nil {
				r.log.Errorf("jetstream_router: ack failed for consumer %q on topic %q: %v", consumerName, topic, err)
			}
		}
	}()

	return func() {
		msgCtx.Stop()
		<-doneCh
	}, nil
}

// streamNameForSubject derives the JetStream stream name from a NATS subject.
// The stream name is the first dot-separated token of the subject (which
// corresponds to the first token of the aether topic before "::").
// Returns an error if the prefix is not one of the known stream prefixes.
func streamNameForSubject(natsSubject string) (string, error) {
	prefix := natsSubject
	if idx := strings.IndexByte(natsSubject, '.'); idx >= 0 {
		prefix = natsSubject[:idx]
	}
	if info, ok := knownStreams[prefix]; ok {
		return info.name, nil
	}
	return "", fmt.Errorf("jetstream_router: no stream registered for subject prefix %q", prefix)
}

// zerologAdapter wraps the package-level zerolog logger to satisfy Logger.
type zerologAdapter struct{}

func (z *zerologAdapter) Warnf(format string, args ...any) {
	logging.Logger.Warn().Msgf(format, args...)
}

func (z *zerologAdapter) Errorf(format string, args ...any) {
	logging.Logger.Error().Msgf(format, args...)
}
