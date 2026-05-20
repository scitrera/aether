package aether

import "github.com/scitrera/go-backpressure"

// Priority taxonomy for upstream sends. Lower numerical value = higher
// priority (matches bradenaw/backpressure convention: 0 = highest).
//
// Used by BaseClient.SendWithPriority and by sidecar/gateway callers to
// classify envelopes so that CoDel-driven shedding preserves critical
// control traffic and response headers while shedding best-effort bulk
// data first under sustained load.
const (
	// PriorityControl: session-level critical envelopes (errors,
	// heartbeats, close notices, tunnel-close). Must not be shed —
	// shedding here corrupts session state.
	PriorityControl backpressure.Priority = 0

	// PriorityResponseHeader: ProxyHttpResponse headers, TunnelOpen
	// acknowledgements, tunnel acks. A caller is blocked waiting; a
	// dropped header silently breaks the response.
	PriorityResponseHeader backpressure.Priority = 1

	// PriorityRequest: relay-mediated outbound ProxyHttpRequest /
	// TunnelOpen / request-direction chunks. Caller has a deadline and
	// can retry, so shedding here is recoverable.
	PriorityRequest backpressure.Priority = 2

	// PriorityResponseChunk: ProxyHttpBodyChunk frames (response
	// direction) and tunnel data. Shedding mid-stream truncates the
	// body; the caller will see a clean error on shed because we fail
	// the whole stream rather than silently dropping frames.
	PriorityResponseChunk backpressure.Priority = 3

	// PriorityBestEffort: ProgressReport, fire-and-forget metrics, KV
	// ops without ack semantics. First class to be shed under load.
	PriorityBestEffort backpressure.Priority = 4
)
