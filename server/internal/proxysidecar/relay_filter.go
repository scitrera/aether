package proxysidecar

import (
	"fmt"
	"sort"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
)

// Op identifiers used in relay.allowed_ops literal lists. They mirror the
// payload variants of UpstreamMessage. The set kept here is intentionally
// scoped to ops a sandbox might legitimately need; admin / orchestration /
// workflow ops are not exposed even if an operator names them.
const (
	OpInitConnection      = "InitConnection"
	OpSendMessage         = "SendMessage"
	OpProgressReport      = "ProgressReport"
	OpKVOperation         = "KVOperation"
	OpCheckpointOperation = "CheckpointOperation"
	OpProxyHttpRequest    = "ProxyHttpRequest"
	OpProxyHttpBodyChunk  = "ProxyHttpBodyChunk"
	OpProxyHttpResponse   = "ProxyHttpResponse"
	OpTunnelOpen          = "TunnelOpen"
	OpTunnelData          = "TunnelData"
	OpTunnelClose         = "TunnelClose"
	OpTunnelAck           = "TunnelAck"
	OpSwitchWorkspace     = "SwitchWorkspace"
)

// allowedOpsSet is an O(1)-membership view of the relay's permitted upstream
// op set. InitConnection is always allowed because the sandbox must complete
// the handshake before any other op is dispatched.
type allowedOpsSet struct {
	ops map[string]struct{}
}

// resolveAllowedOps returns a set built from cfg. Profile names take
// precedence over a literal list when both are accidentally supplied; this
// is enforced by config validation but defended again here so callers do
// not have to remember which one wins.
func resolveAllowedOps(cfg AllowedOpsConfig) (*allowedOpsSet, error) {
	set := &allowedOpsSet{ops: map[string]struct{}{}}
	set.ops[OpInitConnection] = struct{}{}

	if cfg.Profile != "" {
		ops, err := profileOps(cfg.Profile)
		if err != nil {
			return nil, err
		}
		for _, op := range ops {
			set.ops[op] = struct{}{}
		}
		return set, nil
	}

	for _, op := range cfg.Ops {
		op = strings.TrimSpace(op)
		if op == "" {
			continue
		}
		if !knownRelayOp(op) {
			return nil, fmt.Errorf("relay.allowed_ops: unknown op %q", op)
		}
		set.ops[op] = struct{}{}
	}
	return set, nil
}

// profileOps returns the literal op list backing a named profile.
func profileOps(profile string) ([]string, error) {
	switch profile {
	case AllowedOpsProfileSandboxDefault:
		return []string{
			OpSendMessage,
			OpProgressReport,
			OpKVOperation,
		}, nil
	case AllowedOpsProfileSandboxTunnels:
		return []string{
			OpSendMessage,
			OpProgressReport,
			OpKVOperation,
			OpProxyHttpRequest,
			OpProxyHttpBodyChunk,
			OpProxyHttpResponse,
			OpTunnelOpen,
			OpTunnelData,
			OpTunnelClose,
			OpTunnelAck,
		}, nil
	case AllowedOpsProfileToolStubOnly:
		// Only the InitConnection handshake; any other op is denied. The
		// sandbox is expected to *receive* envelopes (e.g. ProxyHttpRequest)
		// and respond, but never to initiate upstream ops itself.
		return nil, nil
	default:
		return nil, fmt.Errorf("relay.allowed_ops.profile=%q: unknown profile", profile)
	}
}

func knownRelayOp(name string) bool {
	switch name {
	case OpInitConnection,
		OpSendMessage,
		OpProgressReport,
		OpKVOperation,
		OpCheckpointOperation,
		OpProxyHttpRequest,
		OpProxyHttpBodyChunk,
		OpProxyHttpResponse,
		OpTunnelOpen,
		OpTunnelData,
		OpTunnelClose,
		OpTunnelAck,
		OpSwitchWorkspace:
		return true
	}
	return false
}

// allows reports whether op is permitted.
func (s *allowedOpsSet) allows(op string) bool {
	if s == nil {
		return false
	}
	_, ok := s.ops[op]
	return ok
}

// list returns the sorted list of allowed ops. Used in logs and ErrorResponse
// detail so operators can see the resolved set without consulting config.
func (s *allowedOpsSet) list() []string {
	out := make([]string, 0, len(s.ops))
	for op := range s.ops {
		out = append(out, op)
	}
	sort.Strings(out)
	return out
}

// upstreamOpName returns the op identifier for an UpstreamMessage payload.
// Returns "" when the payload is nil or unrecognised; callers treat that as
// "deny" so unknown variants cannot slip through.
func upstreamOpName(msg *pb.UpstreamMessage) string {
	if msg == nil {
		return ""
	}
	switch msg.Payload.(type) {
	case *pb.UpstreamMessage_Init:
		return OpInitConnection
	case *pb.UpstreamMessage_Send:
		return OpSendMessage
	case *pb.UpstreamMessage_Progress:
		return OpProgressReport
	case *pb.UpstreamMessage_KvOp:
		return OpKVOperation
	case *pb.UpstreamMessage_CheckpointOp:
		return OpCheckpointOperation
	case *pb.UpstreamMessage_ProxyHttpRequest:
		return OpProxyHttpRequest
	case *pb.UpstreamMessage_ProxyHttpBodyChunk:
		return OpProxyHttpBodyChunk
	case *pb.UpstreamMessage_ProxyHttpResponse:
		return OpProxyHttpResponse
	case *pb.UpstreamMessage_TunnelOpen:
		return OpTunnelOpen
	case *pb.UpstreamMessage_TunnelData:
		return OpTunnelData
	case *pb.UpstreamMessage_TunnelClose:
		return OpTunnelClose
	case *pb.UpstreamMessage_TunnelAck:
		return OpTunnelAck
	case *pb.UpstreamMessage_SwitchWorkspace:
		return OpSwitchWorkspace
	}
	return ""
}
