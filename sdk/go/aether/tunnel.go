// Package aether tunnel (TunnelDial) support for the Go SDK.
//
// This file provides TunnelDial, allowing callers to open a net.Conn-compatible
// byte-stream tunnel through an Aether connection to a remote service.
// Protocol may be TCP, UDP, or WEBSOCKET (pb.TunnelOpen_Protocol).

package aether

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// tunnelChunkSize is the maximum payload size per TunnelData frame.
const tunnelChunkSize = 32 * 1024

// initialCredits is the number of outbound TunnelData frames the dialer may
// send before waiting for a TunnelAck credit grant from the remote side.
// (credit window = initialCredits frames)
const initialCredits = 16

// inboundAckThreshold is the consumed-byte threshold at which the caller
// emits an upstream TunnelAck granting more bytes to the remote sidecar.
// Sized to match the per-frame cap so we ack roughly once per frame consumed.
const inboundAckThreshold = tunnelChunkSize

// =============================================================================
// TunnelOpt — functional options
// =============================================================================

type tunnelOptions struct {
	idleTimeout  time.Duration
	maxBytes     int64
	metadata     map[string]string
	sessionToken string // reserved v2
	backend      string
}

// TunnelOpt configures a TunnelDial call.
type TunnelOpt func(*tunnelOptions)

// WithIdleTimeout sets an idle timeout on the tunnel (server-side enforcement).
func WithIdleTimeout(d time.Duration) TunnelOpt {
	return func(o *tunnelOptions) { o.idleTimeout = d }
}

// WithMaxBytes sets a byte-quota on the tunnel (server-side enforcement).
func WithMaxBytes(n int64) TunnelOpt {
	return func(o *tunnelOptions) { o.maxBytes = n }
}

// WithMetadata attaches arbitrary metadata to the TunnelOpen frame (e.g. WS
// sub-protocols, routing hints).
func WithMetadata(m map[string]string) TunnelOpt {
	return func(o *tunnelOptions) { o.metadata = m }
}

// WithSessionToken is reserved for v2 reconnect/resume; ignored in v1.
func WithSessionToken(s string) TunnelOpt {
	return func(o *tunnelOptions) { o.sessionToken = s }
}

// WithTunnelBackend selects a named backend on the terminator side. Empty
// string (the default) lets the terminator pick the first backend whose
// allow-list admits the tunnel. The backend's allow-list still applies even
// when an explicit name is supplied.
func WithTunnelBackend(name string) TunnelOpt {
	return func(o *tunnelOptions) { o.backend = name }
}

// =============================================================================
// TunnelClosedError
// =============================================================================

// TunnelClosedError is returned by Read/Write when the remote side has sent a
// TunnelClose frame.
type TunnelClosedError struct {
	Reason string
	Detail string
}

func (e *TunnelClosedError) Error() string {
	return fmt.Sprintf("aether tunnel closed: %s: %s", e.Reason, e.Detail)
}

// =============================================================================
// tunnelConn — net.Conn implementation
// =============================================================================

// tunnelState holds all mutable per-tunnel state.
type tunnelState struct {
	tunnelID string

	// inbound ring-buffer (bytes delivered from downstream TunnelData)
	inMu   sync.Mutex
	inBuf  []byte
	inCond *sync.Cond

	// outbound credit tracking (tokens consumed per TunnelData frame sent)
	outCredits int32 // atomic

	// inboundConsumedSinceAck is the bytes consumed via Read since the most
	// recent upstream TunnelAck was emitted. Used to throttle ack emission
	// at the inboundAckThreshold cadence.
	inboundConsumedSinceAck int64 // atomic

	// sequence counters
	outSeq atomic.Uint32

	// FIN flags
	finIn  atomic.Bool // remote sent fin → Read sees io.EOF after drain
	finOut atomic.Bool // local called Close or half-close write

	// closed by TunnelClose from remote
	closedMu  sync.Mutex
	closedErr error
	closedCh  chan struct{}

	// deadline management
	deadlineMu sync.Mutex
	deadline   time.Time
	rDeadline  time.Time
	wDeadline  time.Time
}

// globalTunnelInflights maps tunnelID → *tunnelState for active tunnels.
var globalTunnelInflights sync.Map

// registerTunnelInflight registers a new in-flight tunnel and returns its state.
func registerTunnelInflight(tunnelID string) *tunnelState {
	ts := &tunnelState{
		tunnelID: tunnelID,
		closedCh: make(chan struct{}),
	}
	ts.inCond = sync.NewCond(&ts.inMu)
	atomic.StoreInt32(&ts.outCredits, initialCredits)
	globalTunnelInflights.Store(tunnelID, ts)
	return ts
}

// deleteTunnelInflight removes the tunnel state after it is done.
func deleteTunnelInflight(tunnelID string) {
	globalTunnelInflights.Delete(tunnelID)
}

// tunnelConn wraps tunnelState and implements net.Conn.
type tunnelConn struct {
	client *BaseClient
	state  *tunnelState
	target string
	proto  string
}

// --- net.Conn interface ---

func (tc *tunnelConn) Read(b []byte) (int, error) {
	ts := tc.state

	ts.inMu.Lock()
	defer ts.inMu.Unlock()

	for {
		// Check deadline.
		dl := tc.readDeadline()
		if !dl.IsZero() && time.Now().After(dl) {
			return 0, &net.OpError{Op: "read", Net: "tunnel", Err: context.DeadlineExceeded}
		}

		// Data available.
		if len(ts.inBuf) > 0 {
			n := copy(b, ts.inBuf)
			ts.inBuf = ts.inBuf[n:]
			// Account for consumed bytes and emit an upstream TunnelAck
			// when the threshold is crossed so the remote sidecar can
			// replenish its outbound credit window. Holding the lock here
			// is fine — Send is non-blocking against the request queue.
			consumed := atomic.AddInt64(&ts.inboundConsumedSinceAck, int64(n))
			if consumed >= int64(inboundAckThreshold) {
				if atomic.CompareAndSwapInt64(&ts.inboundConsumedSinceAck, consumed, 0) {
					_ = tc.client.Send(&pb.UpstreamMessage{
						Payload: &pb.UpstreamMessage_TunnelAck{
							TunnelAck: &pb.TunnelAck{
								TunnelId: ts.tunnelID,
								Credits:  uint32(consumed),
							},
						},
					})
				}
			}
			return n, nil
		}

		// Buffer empty — check terminal states.
		if err := ts.closedError(); err != nil {
			return 0, err
		}
		if ts.finIn.Load() {
			return 0, io.EOF
		}

		// Wait for more data with optional deadline timeout.
		if !dl.IsZero() {
			waitUntil := time.Until(dl)
			if waitUntil <= 0 {
				return 0, &net.OpError{Op: "read", Net: "tunnel", Err: context.DeadlineExceeded}
			}
			// Wake via timer so we can check deadline.
			go func() {
				time.Sleep(waitUntil)
				ts.inCond.Broadcast()
			}()
		}
		ts.inCond.Wait()
	}
}

func (tc *tunnelConn) Write(b []byte) (int, error) {
	ts := tc.state

	if ts.finOut.Load() {
		return 0, fmt.Errorf("aether tunnel: write on closed connection")
	}
	if err := ts.closedError(); err != nil {
		return 0, err
	}

	total := 0
	for total < len(b) {
		// Check write deadline.
		if dl := tc.writeDeadline(); !dl.IsZero() && time.Now().After(dl) {
			return total, &net.OpError{Op: "write", Net: "tunnel", Err: context.DeadlineExceeded}
		}

		// Wait for a credit slot (spin with short sleep to avoid busy loop).
		for atomic.LoadInt32(&ts.outCredits) <= 0 {
			if err := ts.closedError(); err != nil {
				return total, err
			}
			time.Sleep(1 * time.Millisecond)
		}
		atomic.AddInt32(&ts.outCredits, -1)

		end := total + tunnelChunkSize
		if end > len(b) {
			end = len(b)
		}
		chunk := b[total:end]
		seq := ts.outSeq.Add(1) - 1
		fin := (end == len(b)) && ts.finOut.Load()

		if err := tc.client.Send(&pb.UpstreamMessage{
			Payload: &pb.UpstreamMessage_TunnelData{
				TunnelData: &pb.TunnelData{
					TunnelId: ts.tunnelID,
					Seq:      seq,
					Data:     chunk,
					Fin:      fin,
				},
			},
		}); err != nil {
			return total, fmt.Errorf("aether tunnel: sending data: %w", err)
		}
		total = end
	}
	return total, nil
}

func (tc *tunnelConn) Close() error {
	ts := tc.state

	// Mark fin out so further Writes fail.
	ts.finOut.Store(true)

	// Send TunnelClose upstream.
	_ = tc.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelClose{
			TunnelClose: &pb.TunnelClose{
				TunnelId: ts.tunnelID,
				Reason:   pb.TunnelClose_NORMAL,
			},
		},
	})

	// Signal any blocked Reads.
	ts.inMu.Lock()
	ts.inCond.Broadcast()
	ts.inMu.Unlock()

	deleteTunnelInflight(ts.tunnelID)
	return nil
}

func (tc *tunnelConn) LocalAddr() net.Addr {
	return tunnelAddr{network: "tunnel", addr: "local"}
}

func (tc *tunnelConn) RemoteAddr() net.Addr {
	return tunnelAddr{network: "tunnel", addr: tc.target}
}

func (tc *tunnelConn) SetDeadline(t time.Time) error {
	tc.state.deadlineMu.Lock()
	tc.state.deadline = t
	tc.state.rDeadline = t
	tc.state.wDeadline = t
	tc.state.deadlineMu.Unlock()
	tc.state.inCond.Broadcast()
	return nil
}

func (tc *tunnelConn) SetReadDeadline(t time.Time) error {
	tc.state.deadlineMu.Lock()
	tc.state.rDeadline = t
	tc.state.deadlineMu.Unlock()
	tc.state.inCond.Broadcast()
	return nil
}

func (tc *tunnelConn) SetWriteDeadline(t time.Time) error {
	tc.state.deadlineMu.Lock()
	tc.state.wDeadline = t
	tc.state.deadlineMu.Unlock()
	return nil
}

// readDeadline returns the effective read deadline (most restrictive of
// per-op deadline and global deadline).
func (tc *tunnelConn) readDeadline() time.Time {
	tc.state.deadlineMu.Lock()
	defer tc.state.deadlineMu.Unlock()
	dl := tc.state.deadline
	rdl := tc.state.rDeadline
	switch {
	case dl.IsZero():
		return rdl
	case rdl.IsZero():
		return dl
	default:
		if rdl.Before(dl) {
			return rdl
		}
		return dl
	}
}

// writeDeadline returns the effective write deadline.
func (tc *tunnelConn) writeDeadline() time.Time {
	tc.state.deadlineMu.Lock()
	defer tc.state.deadlineMu.Unlock()
	dl := tc.state.deadline
	wdl := tc.state.wDeadline
	switch {
	case dl.IsZero():
		return wdl
	case wdl.IsZero():
		return dl
	default:
		if wdl.Before(dl) {
			return wdl
		}
		return dl
	}
}

// closedError returns the tunnel closed error if the tunnel has been closed
// by a remote TunnelClose, otherwise nil.
func (ts *tunnelState) closedError() error {
	ts.closedMu.Lock()
	defer ts.closedMu.Unlock()
	return ts.closedErr
}

// =============================================================================
// tunnelAddr — synthetic net.Addr for Local/RemoteAddr
// =============================================================================

type tunnelAddr struct {
	network string
	addr    string
}

func (a tunnelAddr) Network() string { return a.network }
func (a tunnelAddr) String() string  { return a.addr }

// =============================================================================
// Downstream dispatch handlers (called from client.go dispatchResponse)
// =============================================================================

// handleTunnelData delivers inbound data to the waiting tunnelConn.Read.
func (c *BaseClient) handleTunnelData(td *pb.TunnelData) {
	val, ok := globalTunnelInflights.Load(td.GetTunnelId())
	if !ok {
		return
	}
	ts := val.(*tunnelState)

	ts.inMu.Lock()
	ts.inBuf = append(ts.inBuf, td.GetData()...)
	if td.GetFin() {
		ts.finIn.Store(true)
	}
	ts.inCond.Broadcast()
	ts.inMu.Unlock()
}

// handleTunnelAck replenishes the outbound credit window.
func (c *BaseClient) handleTunnelAck(ack *pb.TunnelAck) {
	val, ok := globalTunnelInflights.Load(ack.GetTunnelId())
	if !ok {
		return
	}
	ts := val.(*tunnelState)
	atomic.AddInt32(&ts.outCredits, int32(ack.GetCredits()))
}

// handleTunnelClose signals a remote-initiated tunnel teardown.
func (c *BaseClient) handleTunnelClose(tc *pb.TunnelClose) {
	val, ok := globalTunnelInflights.Load(tc.GetTunnelId())
	if !ok {
		return
	}
	ts := val.(*tunnelState)

	ts.closedMu.Lock()
	if ts.closedErr == nil {
		ts.closedErr = &TunnelClosedError{
			Reason: tc.GetReason().String(),
			Detail: tc.GetDetail(),
		}
		select {
		case <-ts.closedCh:
		default:
			close(ts.closedCh)
		}
	}
	ts.closedMu.Unlock()

	// Wake blocked Reads so they surface the error.
	ts.inMu.Lock()
	ts.inCond.Broadcast()
	ts.inMu.Unlock()
}

// =============================================================================
// TunnelDial — main entry point
// =============================================================================

// TunnelDial opens a byte-stream tunnel through the Aether connection to the
// given target topic and returns a net.Conn-compatible wrapper.
//
// target is the Aether service topic (e.g. "sv::proxy::default").
// proto is the wire protocol string: "tcp", "udp", or "ws".
// remoteHint is an optional hint passed to the remote sidecar (e.g. "host:port").
//
// The returned net.Conn supports Read, Write, Close, SetDeadline,
// SetReadDeadline, and SetWriteDeadline. LocalAddr returns a synthetic
// placeholder; RemoteAddr returns the target topic.
func (c *BaseClient) TunnelDial(ctx context.Context, target, proto, remoteHint string, opts ...TunnelOpt) (net.Conn, error) {
	if target == "" {
		return nil, fmt.Errorf("aether tunnel: target topic is required")
	}

	var o tunnelOptions
	for _, opt := range opts {
		opt(&o)
	}

	// Map proto string → pb enum.
	var pbProto pb.TunnelOpen_Protocol
	switch proto {
	case "udp":
		pbProto = pb.TunnelOpen_UDP
	case "ws", "websocket":
		pbProto = pb.TunnelOpen_WEBSOCKET
	default:
		pbProto = pb.TunnelOpen_TCP
	}

	tunnelID := c.NextRequestID()
	ts := registerTunnelInflight(tunnelID)

	openMsg := &pb.TunnelOpen{
		TunnelId:     tunnelID,
		TargetTopic:  target,
		Protocol:     pbProto,
		RemoteHint:   remoteHint,
		Metadata:     o.metadata,
		SessionToken: o.sessionToken,
		BackendName:  o.backend,
	}
	if o.idleTimeout > 0 {
		openMsg.IdleTimeoutMs = o.idleTimeout.Milliseconds()
	}
	if o.maxBytes > 0 {
		openMsg.MaxBytes = o.maxBytes
	}

	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_TunnelOpen{TunnelOpen: openMsg},
	}); err != nil {
		deleteTunnelInflight(tunnelID)
		return nil, fmt.Errorf("aether tunnel: sending TunnelOpen: %w", err)
	}

	conn := &tunnelConn{
		client: c,
		state:  ts,
		target: target,
		proto:  proto,
	}
	return conn, nil
}
