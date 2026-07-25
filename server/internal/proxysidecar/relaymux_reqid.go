package proxysidecar

import (
	"strconv"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
)

// request_id field map for RelayMux correlation.
//
// RelayMux fans many sub-client streams onto ONE shared upstream gateway
// connection. To route a reply back to the sub-client that asked, every
// CORRELATED upstream op has its request_id rewritten to a globally-unique
// mux id before forwarding; the matching downstream reply is then demuxed by
// reading that id and restoring the sub-client's original value.
//
// "Correlated" means: the upstream payload carries a per-request correlation
// id AND there is a downstream payload that echoes it. The two helpers below
// are the single source of truth for which payloads participate.
//
// Coverage (confirmed against api/proto/aether.proto, field tags noted):
//
//	Correlated (request_id rewritten / restored):
//	  kv_op (KVOperation.request_id=8)              <-> kv (KVResponse.request_id=5)
//	  task_op (TaskOperation.request_id=4)          <-> task_op (TaskOperationResponse.request_id=5)
//	  task_query (TaskQuery.request_id=4)           <-> task_query (TaskQueryResponse.request_id=6)
//	  create_task (CreateTaskRequest.request_id=10) <-> create_task (CreateTaskResponse.request_id=6)
//	  checkpoint_op (CheckpointOperation.request_id=5) <-> checkpoint (CheckpointResponse.request_id=6)
//	  admin_query (AdminQuery.request_id=4)         <-> admin (AdminResponse.request_id=9)
//	  session_op (SessionOperation.request_id=4)    <-> session_response (SessionOperationResponse.request_id=4)
//	  workspace_op (WorkspaceOperation.request_id=5)<-> workspace (WorkspaceResponse.request_id=8)
//	  agent_op (AgentOperation.request_id=6)        <-> agent (AgentResponse.request_id=9)
//	  acl_op (ACLOperation.request_id=19)           <-> acl (ACLResponse.request_id=12)
//	  token_op (TokenOperation.request_id=5)        <-> token (TokenResponse.request_id=9)
//	  audit_query (AuditQuery.request_id=1)         <-> audit_response (AuditQueryResponse.request_id=1)
//	  authority_grant_op (AuthorityGrantOperation.request_id=6) <-> authority_grant (AuthorityGrantResponse.request_id=5)
//	  resolve_authority_request (ResolveAuthorityRequest.request_id=1) <-> resolve_authority_response (ResolveAuthorityResponse.request_id=1)
//	  connection_status_request (ConnectionStatusRequest.request_id=1) <-> connection_status_response (ConnectionStatusResponse.request_id=1)
//	  workflow_op (WorkflowOperation.request_id=6)  <-> workflow_response (WorkflowResponse.request_id=6)
//	  submit_audit_event (SubmitAuditEventRequest.client_request_id=9) <-> submit_audit_event_response (SubmitAuditEventResponse.client_request_id=1)
//	  authority_request_op (AuthorityRequestOperation.client_request_id=6) <-> authority_request_response (AuthorityRequestOperationResponse.client_request_id=3)
//	  task_subscription_op (TaskSubscriptionOperation.client_request_id=4) <-> task_subscription_response (TaskSubscriptionOperationResponse.client_request_id=3)
//
//	NOT correlated (forwarded one-way / broadcast — never tracked):
//	  init (handled locally, never forwarded by the mux)
//	  send (SendMessage — fire-and-forget, no request_id)
//	  switch_workspace (no request_id)
//	  progress (ProgressReport HAS a request_id=7 field, but there is no
//	    matching downstream "progress response": ProgressUpdate is a
//	    gateway-initiated PUSH, not an echo. So ProgressReport is forwarded
//	    one-way and ProgressUpdate is broadcast. This is the one surprising
//	    proto shape — a request_id-bearing op that is nonetheless one-way.)
//	  proxy_http_request / proxy_http_body_chunk / proxy_http_response: these
//	    correlate by request_id between the SANDBOX and a downstream service,
//	    NOT between the sub-client and the gateway in a request/response sense.
//	    Proxy ops are NOT in the correlated (pending-table) map above because
//	    they can be MULTI-FRAME: a chunked ProxyHttpRequest (BodyChunked=true)
//	    is followed by ProxyHttpBodyChunk frames that SHARE one request_id, and a
//	    chunked response is a ProxyHttpResponse header followed by
//	    ProxyHttpBodyChunk frames (Fin on the last) that also share it. A
//	    per-frame muxSeq counter would assign different ids to frames of the same
//	    logical request and break that correlation.
//
//	    Instead, proxy frames use a STATELESS deterministic per-sub-client
//	    request_id PREFIX (see encodeProxyReqID / decodeProxyReqID below):
//	      - Upstream: an outbound proxy frame's request_id is rewritten from
//	        <orig> to encodeProxyReqID(subID, <orig>). Deterministic ⇒ every
//	        frame of one chunked request rewrites identically; no pending entry.
//	      - Downstream: a proxy response/chunk frame's request_id decodes back to
//	        (subID, origID). If subID is a live sub-client, the original id is
//	        restored on the frame and it is delivered to THAT sub-client only. If
//	        it does NOT decode (e.g. the terminator's own "req-N" proxy response),
//	        the frame falls through to broadcast so the SDK's terminator typed
//	        dispatch (OnProxyHttpResponse) still handles it.
//	    This replaces the never-wired "broadcast to sub-clients; the sandbox
//	    picks up its own by request_id" scheme that previously dropped a relay
//	    sub-client's proxy responses.
//	  tunnel_open / tunnel_data / tunnel_close / tunnel_ack: routed by
//	    tunnel_id, handled separately (see RelayMux tunnel routing), not here.
//
// Downstream-only PUSH payloads with no upstream correlated request that are
// always broadcast: msg, config, signal, task_assignment, progress_update,
// proxy_http_request, connection_ack, authority_grant_revocation,
// authority_request_event, task_hibernated, task_event.

// upstreamSetRequestID rewrites the correlation id on a correlated upstream
// payload to newID, returning the original id and whether the payload was a
// correlated type. For non-correlated payloads it returns ("", false) and the
// message is forwarded unchanged (one-way).
func upstreamSetRequestID(msg *pb.UpstreamMessage, newID string) (origID string, correlated bool) {
	if msg == nil {
		return "", false
	}
	switch p := msg.Payload.(type) {
	case *pb.UpstreamMessage_KvOp:
		orig := p.KvOp.GetRequestId()
		p.KvOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_TaskOp:
		orig := p.TaskOp.GetRequestId()
		p.TaskOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_TaskQuery:
		orig := p.TaskQuery.GetRequestId()
		p.TaskQuery.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_CreateTask:
		orig := p.CreateTask.GetRequestId()
		p.CreateTask.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_CheckpointOp:
		orig := p.CheckpointOp.GetRequestId()
		p.CheckpointOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_AdminQuery:
		orig := p.AdminQuery.GetRequestId()
		p.AdminQuery.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_SessionOp:
		orig := p.SessionOp.GetRequestId()
		p.SessionOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_WorkspaceOp:
		orig := p.WorkspaceOp.GetRequestId()
		p.WorkspaceOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_AgentOp:
		orig := p.AgentOp.GetRequestId()
		p.AgentOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_AclOp:
		orig := p.AclOp.GetRequestId()
		p.AclOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_TokenOp:
		orig := p.TokenOp.GetRequestId()
		p.TokenOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_AuditQuery:
		orig := p.AuditQuery.GetRequestId()
		p.AuditQuery.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_AuthorityGrantOp:
		orig := p.AuthorityGrantOp.GetRequestId()
		p.AuthorityGrantOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_ResolveAuthorityRequest:
		orig := p.ResolveAuthorityRequest.GetRequestId()
		p.ResolveAuthorityRequest.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_ConnectionStatusRequest:
		orig := p.ConnectionStatusRequest.GetRequestId()
		p.ConnectionStatusRequest.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_WorkflowOp:
		orig := p.WorkflowOp.GetRequestId()
		p.WorkflowOp.RequestId = newID
		return orig, true
	case *pb.UpstreamMessage_SubmitAuditEvent:
		orig := p.SubmitAuditEvent.GetClientRequestId()
		p.SubmitAuditEvent.ClientRequestId = newID
		return orig, true
	case *pb.UpstreamMessage_AuthorityRequestOp:
		orig := p.AuthorityRequestOp.GetClientRequestId()
		p.AuthorityRequestOp.ClientRequestId = newID
		return orig, true
	case *pb.UpstreamMessage_TaskSubscriptionOp:
		orig := p.TaskSubscriptionOp.GetClientRequestId()
		p.TaskSubscriptionOp.ClientRequestId = newID
		return orig, true
	}
	return "", false
}

// downstreamGetRequestID reads the correlation id from a correlated downstream
// reply, returning the id and whether the payload was a correlated type. For
// broadcast / tunnel payloads it returns ("", false).
//
// ErrorResponse is special-cased by the caller (it carries an optional
// request_id that may or may not correlate to a pending mux id) and is NOT
// reported as correlated here; see RelayMux.routeDownstream.
func downstreamGetRequestID(msg *pb.DownstreamMessage) (id string, correlated bool) {
	if msg == nil {
		return "", false
	}
	switch p := msg.Payload.(type) {
	case *pb.DownstreamMessage_Kv:
		return p.Kv.GetRequestId(), true
	case *pb.DownstreamMessage_TaskOp:
		return p.TaskOp.GetRequestId(), true
	case *pb.DownstreamMessage_TaskQuery:
		return p.TaskQuery.GetRequestId(), true
	case *pb.DownstreamMessage_CreateTask:
		return p.CreateTask.GetRequestId(), true
	case *pb.DownstreamMessage_Checkpoint:
		return p.Checkpoint.GetRequestId(), true
	case *pb.DownstreamMessage_Admin:
		return p.Admin.GetRequestId(), true
	case *pb.DownstreamMessage_SessionResponse:
		return p.SessionResponse.GetRequestId(), true
	case *pb.DownstreamMessage_Workspace:
		return p.Workspace.GetRequestId(), true
	case *pb.DownstreamMessage_Agent:
		return p.Agent.GetRequestId(), true
	case *pb.DownstreamMessage_Acl:
		return p.Acl.GetRequestId(), true
	case *pb.DownstreamMessage_Token:
		return p.Token.GetRequestId(), true
	case *pb.DownstreamMessage_AuditResponse:
		return p.AuditResponse.GetRequestId(), true
	case *pb.DownstreamMessage_AuthorityGrant:
		return p.AuthorityGrant.GetRequestId(), true
	case *pb.DownstreamMessage_ResolveAuthorityResponse:
		return p.ResolveAuthorityResponse.GetRequestId(), true
	case *pb.DownstreamMessage_ConnectionStatusResponse:
		return p.ConnectionStatusResponse.GetRequestId(), true
	case *pb.DownstreamMessage_WorkflowResponse:
		return p.WorkflowResponse.GetRequestId(), true
	case *pb.DownstreamMessage_SubmitAuditEventResponse:
		return p.SubmitAuditEventResponse.GetClientRequestId(), true
	case *pb.DownstreamMessage_AuthorityRequestResponse:
		return p.AuthorityRequestResponse.GetClientRequestId(), true
	case *pb.DownstreamMessage_TaskSubscriptionResponse:
		return p.TaskSubscriptionResponse.GetClientRequestId(), true
	}
	return "", false
}

// downstreamErrorRequestID returns the request_id carried on an ErrorResponse
// downstream frame (and true) when the frame is an error, else ("", false).
// ErrorResponse.request_id is optional: empty means the error is not tied to a
// specific correlated request and should be broadcast.
func downstreamErrorRequestID(msg *pb.DownstreamMessage) (id string, isError bool) {
	if msg == nil {
		return "", false
	}
	if p, ok := msg.Payload.(*pb.DownstreamMessage_Error); ok {
		return p.Error.GetRequestId(), true
	}
	return "", false
}

// proxyReqIDSentinel marks a request_id that was rewritten by the relay mux for
// a proxy frame. It is chosen so it cannot be confused with a normal
// "req-N" / "rpc-<hex>" / "rmx-N" id: it contains a '~' (never produced by the
// SDK's counter-based or hex ids, nor by the "rmx-" mux prefix).
const proxyReqIDSentinel = "rmxp~"

// encodeProxyReqID builds a deterministic, reversible per-sub-client prefix for
// a proxy frame's request_id. The format is:
//
//	"rmxp~" + len(subID) + "~" + subID + origID
//
// The length prefix makes the split unambiguous for ANY subID/origID content
// (origID may itself contain '~'). encode/decode round-trip exactly.
func encodeProxyReqID(subID, origID string) string {
	return proxyReqIDSentinel + strconv.Itoa(len(subID)) + "~" + subID + origID
}

// decodeProxyReqID reverses encodeProxyReqID. It returns ok=false for any id
// that was not produced by encodeProxyReqID (e.g. the terminator's own "req-N"
// proxy response ids), in which case subID/origID are empty.
func decodeProxyReqID(s string) (subID, origID string, ok bool) {
	if !strings.HasPrefix(s, proxyReqIDSentinel) {
		return "", "", false
	}
	rest := s[len(proxyReqIDSentinel):]
	sep := strings.IndexByte(rest, '~')
	if sep < 0 {
		return "", "", false
	}
	n, err := strconv.Atoi(rest[:sep])
	if err != nil || n < 0 {
		return "", "", false
	}
	body := rest[sep+1:]
	if len(body) < n {
		return "", "", false
	}
	return body[:n], body[n:], true
}

// rewriteUpstreamProxyReqID rewrites the request_id on an outbound proxy frame
// (ProxyHttpRequest / ProxyHttpBodyChunk) to encodeProxyReqID(subID, <orig>),
// returning true. For non-proxy payloads it returns false and leaves msg
// unchanged. Deterministic: every frame of one chunked request (which shares an
// orig request_id) rewrites to the same encoded id, preserving correlation.
func rewriteUpstreamProxyReqID(msg *pb.UpstreamMessage, subID string) bool {
	if msg == nil {
		return false
	}
	switch p := msg.Payload.(type) {
	case *pb.UpstreamMessage_ProxyHttpRequest:
		p.ProxyHttpRequest.RequestId = encodeProxyReqID(subID, p.ProxyHttpRequest.GetRequestId())
		return true
	case *pb.UpstreamMessage_ProxyHttpBodyChunk:
		p.ProxyHttpBodyChunk.RequestId = encodeProxyReqID(subID, p.ProxyHttpBodyChunk.GetRequestId())
		return true
	}
	return false
}

// decodeDownstreamProxyReqID decodes the request_id on an inbound proxy
// response frame (ProxyHttpResponse / ProxyHttpBodyChunk) back to its owning
// (subID, origID). It returns ok=false for non-proxy frames and for proxy
// frames whose request_id was not relay-encoded (the terminator's own).
func decodeDownstreamProxyReqID(msg *pb.DownstreamMessage) (subID, origID string, ok bool) {
	if msg == nil {
		return "", "", false
	}
	switch p := msg.Payload.(type) {
	case *pb.DownstreamMessage_ProxyHttpResponse:
		return decodeProxyReqID(p.ProxyHttpResponse.GetRequestId())
	case *pb.DownstreamMessage_ProxyHttpBodyChunk:
		return decodeProxyReqID(p.ProxyHttpBodyChunk.GetRequestId())
	}
	return "", "", false
}

// restoreDownstreamProxyReqID writes orig back onto a proxy downstream frame's
// request_id (the inverse of the rewriteUpstreamProxyReqID encode, applied to
// the matching response/chunk types) so the owning sub-client sees its own id.
func restoreDownstreamProxyReqID(msg *pb.DownstreamMessage, orig string) {
	if msg == nil {
		return
	}
	switch p := msg.Payload.(type) {
	case *pb.DownstreamMessage_ProxyHttpResponse:
		p.ProxyHttpResponse.RequestId = orig
	case *pb.DownstreamMessage_ProxyHttpBodyChunk:
		p.ProxyHttpBodyChunk.RequestId = orig
	}
}

// downstreamTunnelID returns the tunnel id (and true) for tunnel downstream
// frames routed by tunnel id, else ("", false).
func downstreamTunnelID(msg *pb.DownstreamMessage) (id string, isTunnel bool) {
	if msg == nil {
		return "", false
	}
	switch p := msg.Payload.(type) {
	case *pb.DownstreamMessage_TunnelData:
		return p.TunnelData.GetTunnelId(), true
	case *pb.DownstreamMessage_TunnelAck:
		return p.TunnelAck.GetTunnelId(), true
	case *pb.DownstreamMessage_TunnelClose:
		return p.TunnelClose.GetTunnelId(), true
	}
	return "", false
}
