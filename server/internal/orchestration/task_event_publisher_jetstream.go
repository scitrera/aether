// JetStreamTaskEventPublisher publishes Phase 4 task events to NATS JetStream.
//
// Events land on subjects derived from the aether topic
// tk::{workspace}::{task_id}::events via natscodec.ToNATSSubject, which
// produces tk.{ws-esc}.{task_id-esc}.events inside the pre-existing "tk" stream
// (created by JetStreamRouter at startup).
//
// This implementation is a drop-in for the in-process router-backed publisher
// used by the full gateway. In lite/cluster mode, main.go wires this variant
// into TaskAssignmentService via SetEventPublisher so events survive across
// multi-gateway clusters.
//
// The subscribe side gap (post-subscribe children missed when using a snapshot)
// is closed by JetStream durable consumers whose subject filter uses the NATS
// wildcard "tk.{ws}.>" — any child published after consumer creation is still
// delivered because the filter covers the whole workspace subtree.

package orchestration

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/router/natscodec"
	"google.golang.org/protobuf/proto"
)

// jsEventAckWait is how long JetStream waits before redelivering an unacked
// task-event message.
const jsEventAckWait = 30 * time.Second

// Compile-time interface check.
var _ TaskEventPublisher = (*JetStreamTaskEventPublisher)(nil)

// JetStreamTaskEventPublisher satisfies TaskEventPublisher by publishing
// proto-serialized TaskEvent messages directly to NATS JetStream on the
// tk.{ws}.{task_id}.events subject.
//
// The "tk" stream (subjects: "tk.>") must already exist; JetStreamRouter
// creates it idempotently at startup. This publisher does NOT create streams.
type JetStreamTaskEventPublisher struct {
	js jetstream.JetStream
}

// NewJetStreamTaskEventPublisher creates a JetStreamTaskEventPublisher.
// js must be a valid JetStream context; the caller is responsible for ensuring
// the "tk" stream exists (JetStreamRouter does this automatically).
func NewJetStreamTaskEventPublisher(js jetstream.JetStream) *JetStreamTaskEventPublisher {
	return &JetStreamTaskEventPublisher{js: js}
}

// PublishTaskEvent serializes evt and publishes it to
// tk.{ws-esc}.{task_id-esc}.events on the JetStream "tk" stream.
// Returns an error if serialization or publish fails; callers in the service
// layer treat these as non-fatal (best-effort).
func (p *JetStreamTaskEventPublisher) PublishTaskEvent(ctx context.Context, workspace, taskID string, event *pb.TaskEvent) error {
	if workspace == "" || taskID == "" || event == nil {
		return nil
	}
	// Build the aether topic then translate to a NATS subject.
	// tk::{workspace}::{task_id}::events → tk.{ws-esc}.{task_id-esc}.events
	aetherTopic := fmt.Sprintf("tk::%s::%s::events", workspace, taskID)
	subject := natscodec.ToNATSSubject(aetherTopic)

	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("jetstream_task_event_publisher: marshal TaskEvent: %w", err)
	}

	if _, err := p.js.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("jetstream_task_event_publisher: publish to %q: %w", subject, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Subscribe helpers (used by tests and the JetStream-aware subscribe handler)
// ---------------------------------------------------------------------------

// SubscribeTaskEvents creates a durable JetStream consumer for a single task's
// events. The consumer filter is the exact subject for that task:
//
//	tk.{ws-esc}.{task_id-esc}.events
//
// The consumerName must be unique across tasks (e.g. subscription_id). Returns
// a cancel func that stops delivery; the caller must invoke it to release
// server-side resources.
func (p *JetStreamTaskEventPublisher) SubscribeTaskEvents(
	consumerName, workspace, taskID string,
	handler func(*pb.TaskEvent),
) (cancel func(), err error) {
	aetherTopic := fmt.Sprintf("tk::%s::%s::events", workspace, taskID)
	subject := natscodec.ToNATSSubject(aetherTopic)
	return p.subscribeDurable(consumerName, subject, handler)
}

// SubscribeWorkspaceTaskEvents creates a durable JetStream consumer that
// receives events from ALL tasks in a workspace (recursive subscription):
//
//	filter: tk.{ws-esc}.>
//
// Because the wildcard covers every subject under the workspace prefix,
// children spawned AFTER consumer creation are automatically delivered —
// this closes the snapshot-at-subscribe gap present in the in-process
// router implementation.
func (p *JetStreamTaskEventPublisher) SubscribeWorkspaceTaskEvents(
	consumerName, workspace string,
	handler func(*pb.TaskEvent),
) (cancel func(), err error) {
	wsEsc := natscodec.ToNATSSubject(workspace) // escapes workspace token
	// Wildcard: every subject under "tk.{ws-esc}." — covers all task_ids and sub-paths.
	subject := fmt.Sprintf("tk.%s.>", wsEsc)
	return p.subscribeDurable(consumerName, subject, handler)
}

// subscribeDurable is the shared implementation: creates (or resumes) a durable
// consumer with AckExplicit policy, then drives a message-iterator goroutine.
func (p *JetStreamTaskEventPublisher) subscribeDurable(
	consumerName, filterSubject string,
	handler func(*pb.TaskEvent),
) (func(), error) {
	ctx10 := context.Background()
	// The "tk" stream name matches the prefix by the knownStreams convention.
	streamName := "tk"

	// consumerName may be an aether identity or subscription_id carrying
	// characters NATS rejects in a durable consumer name. Escape through the
	// consumer-name namespace; the escape is deterministic so reconnects with
	// the same input resume the same durable consumer.
	natsConsumerName := natscodec.EscapeForConsumerName(consumerName)

	cfg := jetstream.ConsumerConfig{
		Durable:       natsConsumerName,
		FilterSubject: filterSubject,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       jsEventAckWait,
	}

	cons, err := p.js.CreateOrUpdateConsumer(ctx10, streamName, cfg)
	if err != nil {
		return nil, fmt.Errorf("jetstream_task_event_publisher: create consumer %q on %q: %w", consumerName, filterSubject, err)
	}

	msgCtx, err := cons.Messages()
	if err != nil {
		return nil, fmt.Errorf("jetstream_task_event_publisher: start message iterator %q: %w", consumerName, err)
	}

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		defer msgCtx.Stop()
		for {
			msg, err := msgCtx.Next()
			if err != nil {
				return
			}
			var evt pb.TaskEvent
			if err := proto.Unmarshal(msg.Data(), &evt); err != nil {
				logging.Logger.Warn().Err(err).Str("filter", filterSubject).Msg("jetstream_task_event_publisher: unmarshal TaskEvent failed")
				_ = msg.Ack()
				continue
			}
			handler(&evt)
			if err := msg.Ack(); err != nil {
				logging.Logger.Error().Err(err).Str("filter", filterSubject).Msg("jetstream_task_event_publisher: ack failed")
			}
		}
	}()

	return func() {
		msgCtx.Stop()
		<-doneCh
	}, nil
}
