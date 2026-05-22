package gateway

import (
	"context"
	"sync"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
)

// proxyInflightTracker remembers which (caller, service) topics have which
// wireIDs in flight at any given moment. It exists so a session-cleanup
// pass (cleanupSession) can synthesize "the other side went away" errors
// to the surviving peer of every in-flight ProxyHttpRequest that involved
// the departing session.
//
// This addresses two real production gaps documented in the e2e coverage
// matrix as H2 and H3:
//
//   - H2: service disconnects mid-stream → caller's in-flight body reader
//     hangs forever instead of erroring. Fix: on service session cleanup,
//     emit ProxyError{SIDECAR_UNAVAILABLE} (wrapped as a fin chunk so the
//     SDK's streaming-body path unblocks) to every caller with an
//     in-flight pointed at the dead service.
//
//   - H3: caller disconnects mid-stream → service's backend handler keeps
//     running because nothing in the gateway tells the service to stop.
//     Fix: on caller session cleanup, emit ProxyError{SIDECAR_UNAVAILABLE}
//     to the service for every in-flight whose caller is now gone. The
//     terminator's streaming dispatch loop watches for response-send
//     failures and shuts down its backend request when SendProxyHttpResponse
//     returns an error — that's what stops the backend handler's
//     r.Context() since the http.Client request context is derived from
//     the dispatch ctx.
//
// The tracker keeps purely in-memory state — it is not replicated across
// gateway instances. That matches the production deployment shape today
// (one gateway process per node; routing pins live in Redis but in-flight
// state is per-session anyway, so the local view is authoritative).
type proxyInflightTracker struct {
	mu sync.RWMutex
	// byTopic maps an identity topic ("ag::foo" or "sv::impl::spec") to
	// the set of (wireID → inflight) pairs where this topic is either
	// the caller or the service. A single in-flight is double-indexed
	// (both sides) so cleanup-by-topic finds it from either direction.
	byTopic map[string]map[string]*proxyInflightEntry
}

// proxyInflightEntry captures the routing identifiers for a single
// in-flight proxy request. The wireID is the gateway-minted id used as
// the primary pin key; originalID is the caller-side request_id the SDK
// uses to correlate the response.
type proxyInflightEntry struct {
	wireID     string
	originalID string
	caller     string
	service    string
}

func newProxyInflightTracker() *proxyInflightTracker {
	return &proxyInflightTracker{byTopic: make(map[string]map[string]*proxyInflightEntry)}
}

// register records a new in-flight ProxyHttpRequest under both its caller
// and service topics. Safe to call concurrently.
func (t *proxyInflightTracker) register(wireID, originalID, caller, service string) {
	if t == nil || wireID == "" || caller == "" || service == "" {
		return
	}
	entry := &proxyInflightEntry{wireID: wireID, originalID: originalID, caller: caller, service: service}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.byTopic == nil {
		t.byTopic = make(map[string]map[string]*proxyInflightEntry)
	}
	for _, topic := range [2]string{caller, service} {
		set, ok := t.byTopic[topic]
		if !ok {
			set = make(map[string]*proxyInflightEntry)
			t.byTopic[topic] = set
		}
		set[wireID] = entry
	}
}

// unregister drops the in-flight entry for wireID from both the caller's
// and service's per-topic sets. Idempotent.
func (t *proxyInflightTracker) unregister(wireID string) {
	if t == nil || wireID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, set := range t.byTopic {
		if entry, ok := set[wireID]; ok {
			delete(set, wireID)
			if len(set) == 0 {
				delete(t.byTopic, topic)
			}
			// Also clear the entry from the OTHER side's index. We
			// already hold the write lock so this is safe even though
			// we're mutating during iteration — Go's map iteration
			// tolerates deletes of the current key, and we treat the
			// other side as a separate key.
			otherSide := entry.service
			if topic == entry.service {
				otherSide = entry.caller
			}
			if otherSet, ok := t.byTopic[otherSide]; ok {
				delete(otherSet, wireID)
				if len(otherSet) == 0 {
					delete(t.byTopic, otherSide)
				}
			}
			return
		}
	}
}

// drainForTopic returns and removes every in-flight entry where topic is
// either the caller or the service. The returned slice is safe for the
// caller to iterate without holding the tracker lock.
func (t *proxyInflightTracker) drainForTopic(topic string) []proxyInflightEntry {
	if t == nil || topic == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	set, ok := t.byTopic[topic]
	if !ok || len(set) == 0 {
		return nil
	}
	out := make([]proxyInflightEntry, 0, len(set))
	for _, entry := range set {
		out = append(out, *entry)
	}
	// Drop both index entries for each wireID we drained.
	for _, entry := range out {
		if otherTopic := entry.service; topic != otherTopic {
			if otherSet, ok := t.byTopic[otherTopic]; ok {
				delete(otherSet, entry.wireID)
				if len(otherSet) == 0 {
					delete(t.byTopic, otherTopic)
				}
			}
		}
		if otherTopic := entry.caller; topic != otherTopic {
			if otherSet, ok := t.byTopic[otherTopic]; ok {
				delete(otherSet, entry.wireID)
				if len(otherSet) == 0 {
					delete(t.byTopic, otherTopic)
				}
			}
		}
	}
	delete(t.byTopic, topic)
	return out
}

// notifyPeersOfSessionEnd is the cleanup-side entry point. For every
// in-flight ProxyHttpRequest involving departingTopic, emit a synthetic
// error to the OTHER side so the surviving peer unblocks promptly
// instead of waiting for its own context deadline. Caller-side wakeups
// use the caller's originalID (what the SDK's per-client inflight map
// is keyed on); service-side wakeups use the gateway-minted wireID
// (what the terminator's request handlers see). The pin registry
// entries are not cleaned up here — they expire naturally via their
// TTL — because doing so synchronously would require the cleanup ctx
// to outlive the session teardown and the gain is negligible.
//
// publishCallback is invoked once per surviving peer with that peer's
// topic and a DownstreamMessage ready to ship. It returns nil on
// success and a non-nil error if publish failed (logged at debug).
func (t *proxyInflightTracker) notifyPeersOfSessionEnd(ctx context.Context, departingTopic string,
	publishCallback func(ctx context.Context, peerTopic string, downstream *pb.DownstreamMessage) error) {
	if t == nil || publishCallback == nil {
		return
	}
	entries := t.drainForTopic(departingTopic)
	if len(entries) == 0 {
		return
	}
	for _, entry := range entries {
		// peerTopic / requestID: what the SURVIVING peer's response
		// handler is keyed on. Caller-side handler uses originalID;
		// service-side handler uses the gateway-minted wireID.
		var peerTopic, requestID string
		if departingTopic == entry.service {
			// Service died; notify the caller.
			peerTopic = entry.caller
			requestID = entry.originalID
		} else {
			// Caller died; notify the service.
			peerTopic = entry.service
			requestID = entry.wireID
		}
		// First emit an error response header (idempotent: SDK and
		// terminator both drop unknown request_ids quietly). For
		// streaming responses this surfaces as a mid-stream terminal
		// error in streamingBody. For non-streaming inflights it
		// completes the request with an error.
		errResp := &pb.ProxyHttpResponse{
			RequestId: requestID,
			Error: &pb.ProxyError{
				Kind:    pb.ProxyError_SIDECAR_UNAVAILABLE,
				Message: "peer session ended (" + departingTopic + ")",
			},
		}
		errMsg := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_ProxyHttpResponse{ProxyHttpResponse: errResp}}
		if err := publishCallback(ctx, peerTopic, errMsg); err != nil {
			logging.Logger.Debug().
				Err(err).
				Str("peer", peerTopic).
				Str("departing", departingTopic).
				Str("wire_id", entry.wireID).
				Msg("proxy-inflight peer notify publish failed (best effort)")
		}
		// Then emit a fin=true body chunk so any streaming-body reader
		// that already received the response header unblocks. This is
		// belt-and-suspenders: the SDK's resolveProxyResponse closes
		// streamingBody on an error header, but if the header was
		// already consumed (e.g. service had time to send the header
		// before disconnecting), only a fin chunk will wake a Read()
		// blocked on the buffer.
		finChunk := &pb.ProxyHttpBodyChunk{
			RequestId: requestID,
			IsRequest: false,
			Fin:       true,
		}
		finMsg := &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: finChunk}}
		_ = publishCallback(ctx, peerTopic, finMsg)
	}
	logging.Logger.Info().
		Str("departing", departingTopic).
		Int("inflights_notified", len(entries)).
		Msg("proxy-inflight session-end peer notification complete")
}
