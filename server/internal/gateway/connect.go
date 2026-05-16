package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/internal/tracing"
	aerrors "github.com/scitrera/aether/pkg/errors"
	"github.com/scitrera/aether/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// initConnectionTimeout is the maximum time to wait for an InitConnection message
// from a newly connected client before rejecting the connection.
const initConnectionTimeout = 30 * time.Second

// Connect handles the bidirectional gRPC streaming connection lifecycle.
func (s *GatewayServer) Connect(stream pb.AetherGateway_ConnectServer) error {
	s.activeConns.Add(1)
	defer s.activeConns.Done()

	ctx := stream.Context()
	// `gateway.connect.init` covers the connect handshake — mTLS auth,
	// InitConnection, identity resolution, lock acquisition, session
	// registration, ACL check, subscription setup, baseline config. It is
	// ended explicitly right before the main message loop so the span
	// duration reflects the connect cost, not the multi-hour stream
	// lifetime. Per-message handlers in the loop run with `sessionCtx`
	// (derived from `stream.Context()`, not from this span ctx), so they
	// create their own root spans and don't inherit this one as a parent.
	// `defer span.End()` is kept as a safety net for early error returns
	// during init; OTel Span.End() is idempotent so the pre-loop call wins
	// when init succeeds.
	ctx, span := tracing.Tracer.Start(ctx, "gateway.connect.init")
	defer span.End()

	// Span attributes are set after identity is resolved (step 3 below).

	// 1. Handle mTLS authentication
	identity, certPrincipalType, hasCertificate, isAnonymous, err := s.authenticateMTLS(ctx)
	if err != nil {
		return err
	}

	// 2. Wait for InitConnection (with timeout)
	type recvResult struct {
		msg *pb.UpstreamMessage
		err error
	}
	ch := make(chan recvResult, 1)
	go func() {
		msg, err := stream.Recv()
		ch <- recvResult{msg, err}
	}()
	var req *pb.UpstreamMessage
	select {
	case result := <-ch:
		req, err = result.msg, result.err
		if err != nil {
			return err
		}
	case <-time.After(initConnectionTimeout):
		return status.Error(codes.DeadlineExceeded, "timeout waiting for InitConnection")
	case <-ctx.Done():
		return ctx.Err()
	}

	init := req.GetInit()
	if init == nil {
		// No InitConnection provided
		if s.authHandler.mtlsRequired && !hasCertificate {
			return status.Error(codes.Unauthenticated, "mTLS is required but no client certificate provided")
		}
		return status.Error(codes.InvalidArgument, "first message must be InitConnection")
	}

	// 3. Resolve identity based on mTLS mode
	identity, err = s.resolveConnectionIdentity(ctx, init, identity, certPrincipalType, hasCertificate, isAnonymous)
	if err != nil {
		return err
	}

	sessionID := uuid.New().String()

	// 4. Authenticate credentials (task tokens + API key/OAuth)
	var associatedTaskID string
	associatedTaskID, identity, err = s.authenticateCredentials(ctx, init, identity, hasCertificate)
	if err != nil {
		return err
	}

	// Create a cancelable context for this session (used for admin disconnect)
	sessionCtx, sessionCancel := context.WithCancel(stream.Context())
	defer sessionCancel() // Ensure cleanup if we exit early

	cs := &connectionState{
		identity:         identity,
		sessionID:        sessionID,
		sessionCtx:       sessionCtx,
		sessionCancel:    sessionCancel,
		associatedTaskID: associatedTaskID,
	}

	// Set span attributes now that identity is fully resolved.
	span.SetAttributes(
		attribute.String("aether.principal_type", string(identity.Type)),
		attribute.String("aether.workspace", identity.Workspace),
		attribute.String("aether.identity", identity.String()),
		attribute.String("aether.session_id", sessionID),
	)

	// 5. Lock acquisition + session registration (before quota to avoid quota leak on lock failure)
	resumeSessionID := init.ResumeSessionId
	if err := s.acquireSessionLock(ctx, cs, resumeSessionID); err != nil {
		connectionAttempts.WithLabelValues(identity.Workspace, string(identity.Type), "lock_failed").Inc()
		return err
	}

	// 5.1 Connection quota check and atomic increment (after lock acquisition)
	// Moved after lock to prevent quota leak: if lock fails, we never incremented the quota.
	if s.quotaEnforcer.quotaManager != nil {
		if err := s.quotaEnforcer.quotaManager.CheckAndIncrementConnections(ctx, identity.Workspace); err != nil {
			// Rollback the lock since we can't proceed
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rollbackCancel()
			_ = s.sessions.UnregisterSession(rollbackCtx, cs.sessionID)
			_ = s.sessions.ReleaseLock(rollbackCtx, cs.identity, cs.sessionID)
			connectionAttempts.WithLabelValues(identity.Workspace, string(identity.Type), "quota_exceeded").Inc()
			var qErr *aerrors.QuotaExceededError
			if errors.As(err, &qErr) {
				return aerrors.ToGRPCStatus(qErr)
			}
			return status.Errorf(codes.ResourceExhausted, "connection quota exceeded: %v", err)
		}
	}

	connectionAttempts.WithLabelValues(identity.Workspace, string(identity.Type), "success").Inc()

	// 5.2 ACL Check - verify connection permission BEFORE the session becomes
	// discoverable in activeStreams or identityIndex.
	// Note: System principals (orchestrators, workflow engines, metrics bridges) skip
	// workspace ACL checks - they operate at system level. See checkConnection.
	if err := s.checkConnection(stream.Context(), identity, sessionID); err != nil {
		// Rollback lock and session registration (session not yet in activeStreams)
		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rollbackCancel()
		_ = s.sessions.UnregisterSession(rollbackCtx, cs.sessionID)
		_ = s.sessions.ReleaseLock(rollbackCtx, cs.identity, cs.sessionID)
		if s.quotaEnforcer.quotaManager != nil {
			_ = s.quotaEnforcer.quotaManager.DecrementConnections(rollbackCtx, cs.identity.Workspace)
		}
		connectionAttempts.WithLabelValues(identity.Workspace, string(identity.Type), "acl_denied").Inc()
		return status.Errorf(codes.PermissionDenied, "%v", err)
	}

	sessionUUID, _ := uuid.Parse(sessionID)
	client := &ClientSession{
		ID:               sessionID,
		SessionUUID:      sessionUUID,
		Identity:         identity,
		AssociatedTaskID: associatedTaskID,
		Stream:           stream,
		Cancel:           sessionCancel,
		ConnectedAt:      time.Now(),
		rateLimiter:      rate.NewLimiter(rate.Limit(s.quotaEnforcer.messageRateLimit), s.quotaEnforcer.messageRateBurst),
		deliveryCh:       make(chan *pb.DownstreamMessage, s.deliveryBufferSize),
	}
	client.startDeliveryLoop(sessionCtx)
	cs.client = client
	s.activeStreams.Store(sessionID, client)
	s.identityIndex.Store(identity.String(), sessionID)

	// Add agent to implementation index for pool task routing
	if identity.Type == models.PrincipalAgent {
		s.addToImplIndex(identity, client)
	}

	// 6. Start lock refresh goroutine
	s.startLockRefresh(sessionCtx, sessionCancel, identity, sessionID)

	// 4.45 Mark associated task as running (for orchestrated agents)
	// This transitions the task from "assigned" (set when delivered to orchestrator) to "running"
	// We use StartTaskWithAgent to record the agent identity for reconciliation purposes
	if associatedTaskID != "" && s.orchestration != nil && s.orchestration.TaskService != nil {
		if err := s.orchestration.TaskService.StartTaskWithAgent(sessionCtx, associatedTaskID, identity.String()); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", associatedTaskID).Msg("failed to mark task as running")
			// Non-fatal - agent connection should proceed
		} else {
			logging.Logger.Info().Str("task_id", associatedTaskID).Str("identity", identity.String()).Msg("task marked as running")
			s.notifyTaskStatusChangeFromTaskID(sessionCtx, associatedTaskID, "running", "")
		}
		// Clear any disconnect grace marker — worker is back. Idempotent
		// no-op when the task wasn't previously disconnected (fresh connect).
		if err := s.orchestration.TaskService.ClearTaskDisconnected(sessionCtx, associatedTaskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", associatedTaskID).Msg("failed to clear disconnect marker on (re)connect")
		}
		if err := s.maybeRenewTaskAuthorityGrants(sessionCtx, associatedTaskID); err != nil {
			logging.Logger.Warn().Err(err).Str("task_id", associatedTaskID).Msg("failed to renew task authority grants on connect")
		}
	}

	// 4.5 Subscribe to topics (if applicable)
	// Uses shared consumers with local fan-out for efficiency
	if err := s.setupClientSubscriptions(client); err != nil {
		s.rollbackSession(cs)
		logging.Logger.Error().Err(err).Str("identity", cs.identity.String()).Msg("failed to setup subscriptions")
		return status.Error(codes.Internal, "failed to setup subscriptions")
	}

	// Track whether the agent disconnected gracefully (clean EOF) vs crashed/errored
	gracefulExit := false

	// CRIT-2: Wire activeConnections and connectionDuration metrics
	activeConnections.WithLabelValues(identity.Workspace, string(identity.Type)).Inc()

	// 7. Cleanup on disconnect
	defer func() {
		activeConnections.WithLabelValues(identity.Workspace, string(identity.Type)).Dec()
		connectionDuration.WithLabelValues(identity.Workspace, string(identity.Type)).Observe(time.Since(client.ConnectedAt).Seconds())
		s.cleanupSession(cs, gracefulExit)
	}()

	logging.Logger.Info().Str("session_id", sessionID).Str("identity", identity.String()).Msg("session connected")

	// Phase 6: extension negotiation. Union the proto-side InitConnection
	// Extensions list with any URIs supplied via the `Aether-Extensions`
	// gRPC metadata header (comma-separated). Header-sourced entries are
	// always non-required; the proto path is authoritative for `required`.
	// On a required-but-unsupported extension we record an audit row,
	// release session state, and reject the connection with
	// codes.FailedPrecondition so the SDK can distinguish "your declared
	// extension is unsupported" from generic auth or quota failures.
	extDecls := append([]*pb.ExtensionDeclaration(nil), init.Extensions...)
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		extDecls = append(extDecls, parseExtensionMetadataHeader(md.Get(extensionMetadataHeader))...)
	}
	var negotiated *extensionNegotiationResult
	if len(extDecls) > 0 || len(KnownExtensions) > 0 {
		var extLookup agentDeclaredLookup
		if s.orchestration != nil && s.orchestration.Registry != nil {
			extLookup = registryAgentExtensions{store: s.orchestration.Registry}
		}
		result := negotiateExtensions(stream.Context(), extDecls, identity, extLookup)
		negotiated = &result

		// Audit one row per declaration (success or failure) so operators
		// can see extension negotiation outcomes alongside connection
		// lifecycle events. Uses the existing OpConnectionEstablished
		// audit lane under a distinct operation key.
		sessionUUID, _ := uuid.Parse(sessionID)
		for _, nx := range result.negotiated {
			required := false
			for _, decl := range extDecls {
				if decl != nil && decl.Uri == nx.Uri {
					required = decl.Required
					break
				}
			}
			s.auditLog(stream.Context(), audit.NewConnectionEvent(
				string(identity.Type), identity.String(), "extension_negotiated",
				sessionUUID, nx.Supported, nx.RejectionReason,
				map[string]interface{}{
					"workspace": identity.Workspace,
					"uri":       nx.Uri,
					"version":   nx.Version,
					"supported": nx.Supported,
					"required":  required,
				}))
		}

		if result.rejectURI != "" {
			// Send the failed ack so the client can pick the rejection
			// reason out of negotiated_extensions before the stream closes.
			rejectMsg := result.rejectReason
			if rejectMsg == "" {
				rejectMsg = "extension required but not supported: " + result.rejectURI
			}
			_ = client.SafeSend(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_ConnectionAck{
					ConnectionAck: &pb.ConnectionAck{
						SessionId:                 sessionID,
						Resumed:                   cs.resumed,
						NegotiatedExtensions:      result.negotiated,
						ServerSupportedExtensions: result.serverSupported,
					},
				},
			})
			logging.Logger.Warn().Str("uri", result.rejectURI).Str("identity", identity.String()).Msg("rejecting connection: required extension unsupported")
			return status.Errorf(codes.FailedPrecondition, "ERR_EXTENSION_UNSUPPORTED: %s", rejectMsg)
		}

		// Snapshot the negotiated active set onto the session for any
		// future per-message gates (Phase 6 has none, but the data
		// structure is in place).
		client.activeExtensions = result.activeURIs
	}

	// Send ConnectionAck with session ID so client can store it for reconnection
	ack := &pb.ConnectionAck{
		SessionId: sessionID,
		Resumed:   cs.resumed,
	}
	if negotiated != nil {
		ack.NegotiatedExtensions = negotiated.negotiated
		ack.ServerSupportedExtensions = negotiated.serverSupported
	}
	if err := client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: ack,
		},
	}); err != nil {
		logging.Logger.Warn().Err(err).Str("identity", identity.String()).Msg("failed to send ConnectionAck")
	}

	// 4.6 Send Baseline Config (Tenant/Workspace KV)
	s.sendBaselineConfig(stream.Context(), client)

	// Orchestration: Handle orchestrator profile registration
	if init != nil {
		if err := s.handleOrchestratorConnection(stream.Context(), identity, init); err != nil {
			logging.Logger.Error().Err(err).Str("identity", identity.String()).Msg("failed to handle orchestrator connection setup")
		} else if identity.Type == models.PrincipalOrchestrator {
			// Register orchestrator in the O(1) profile index using the same profile list
			// declared in InitConnection, mirroring what handleOrchestratorConnection stored.
			if orchInit, ok := init.ClientType.(*pb.InitConnection_Orchestrator); ok {
				s.registerOrchestratorInIndex(client, orchInit.Orchestrator.SupportedProfiles)
			}
		}
	}

	// Orchestration: Deliver queued tasks to agents
	if identity.Type == models.PrincipalAgent {
		if err := s.deliverQueuedTasksToAgent(stream.Context(), identity, client); err != nil {
			logging.Logger.Error().Err(err).Str("identity", identity.String()).Msg("error delivering queued tasks")
		}
	}

	// Close the init span now — the message loop below runs for the full
	// connection lifetime (potentially hours) and the span should reflect
	// only the handshake/setup cost. End() is idempotent so the deferred
	// safety-net End() at function exit is a no-op when we reach here.
	span.End()

	// 8. Main message loop
	for {
		// Check if session was cancelled (e.g., by admin disconnect)
		select {
		case <-sessionCtx.Done():
			logging.Logger.Info().Str("session_id", sessionID).Err(sessionCtx.Err()).Msg("session cancelled")
			return status.Error(codes.Canceled, "session disconnected by administrator")
		default:
		}

		req, err := stream.Recv()
		if err == io.EOF {
			gracefulExit = true
			return nil
		}
		if err != nil {
			// Check if this was due to context cancellation
			if sessionCtx.Err() != nil {
				return status.Error(codes.Canceled, "session disconnected by administrator")
			}
			return err
		}

		// Handle other message types
		switch p := req.Payload.(type) {
		case *pb.UpstreamMessage_Send:
			s.routeMessage(sessionCtx, client, p.Send)
		case *pb.UpstreamMessage_SwitchWorkspace:
			s.handleSwitchWorkspace(sessionCtx, client, p.SwitchWorkspace)
		case *pb.UpstreamMessage_KvOp:
			s.handleKVOp(sessionCtx, client, p.KvOp)
		case *pb.UpstreamMessage_CheckpointOp:
			s.handleCheckpointOp(sessionCtx, client, p.CheckpointOp)
		case *pb.UpstreamMessage_CreateTask:
			// Orchestration: Handle task creation
			client.identityMu.RLock()
			currentIdentity := client.Identity
			client.identityMu.RUnlock()
			if err := s.handleCreateTask(sessionCtx, client, currentIdentity, p.CreateTask); err != nil {
				logging.Logger.Error().Err(err).Str("identity", currentIdentity.String()).Msg("error handling CreateTask")
			}
		case *pb.UpstreamMessage_Progress:
			s.handleProgressReport(sessionCtx, client, p.Progress)
		case *pb.UpstreamMessage_TaskQuery:
			s.handleTaskQuery(sessionCtx, client, p.TaskQuery)
		case *pb.UpstreamMessage_TaskOp:
			s.handleTaskOp(sessionCtx, client, p.TaskOp)
		case *pb.UpstreamMessage_WorkspaceOp:
			if !s.isAllowedAdminOp(client, identity, "workspaces") {
				continue
			}
			s.handleWorkspaceOp(sessionCtx, client, p.WorkspaceOp)
		case *pb.UpstreamMessage_AgentOp:
			if !s.isAllowedAdminOp(client, identity, "agents") {
				continue
			}
			s.handleAgentOp(sessionCtx, client, p.AgentOp)
		case *pb.UpstreamMessage_AclOp:
			if !s.isAllowedACLOp(client, identity, p.AclOp) {
				continue
			}
			s.handleACLOp(sessionCtx, client, p.AclOp)
		case *pb.UpstreamMessage_TokenOp:
			if !s.isAllowedAdminOp(client, identity, "tokens") {
				continue
			}
			s.handleTokenOp(sessionCtx, client, p.TokenOp)
		case *pb.UpstreamMessage_AuditQuery:
			client.identityMu.RLock()
			currentIdentity := client.Identity
			client.identityMu.RUnlock()
			s.handleAuditQuery(sessionCtx, client, currentIdentity, p.AuditQuery)
		case *pb.UpstreamMessage_SubmitAuditEvent:
			s.handleSubmitAuditEvent(sessionCtx, client, p.SubmitAuditEvent)
		case *pb.UpstreamMessage_AuthorityGrantOp:
			s.handleAuthorityGrantOp(sessionCtx, client, p.AuthorityGrantOp)
		case *pb.UpstreamMessage_AuthorityRequestOp:
			// Phase 2 Stage C: authority-request lifecycle ("sudo"). The
			// handler routes per OpType and pushes downstream responses +
			// AuthorityRequestEvent notifications directly to this session.
			go s.handleAuthorityRequestOp(sessionCtx, client, p.AuthorityRequestOp)
		case *pb.UpstreamMessage_TaskSubscriptionOp:
			// Phase 4 Stage B: per-task event subscription primitive. Handler
			// pushes a TaskSubscriptionOperationResponse and (for SUBSCRIBE)
			// keeps streaming TaskEvent deliveries until UNSUBSCRIBE or
			// disconnect.
			go s.handleTaskSubscriptionOp(sessionCtx, client, p.TaskSubscriptionOp)
		case *pb.UpstreamMessage_WorkflowOp:
			s.handleWorkflowOp(sessionCtx, client, p.WorkflowOp)
		case *pb.UpstreamMessage_WorkflowResponse:
			// Only accept workflow responses from workflow engine clients
			if identity.Type == models.PrincipalWorkflowEngine {
				s.handleWorkflowResponse(sessionCtx, client, p.WorkflowResponse)
			}
		case *pb.UpstreamMessage_SessionOp:
			// ACL check (including OBO authority resolution) happens inside
			// handleSessionOp — mirrors the audit_query pattern so admin
			// dashboards can run on behalf of the authenticated user.
			s.handleSessionOp(sessionCtx, client, identity, p.SessionOp)
		case *pb.UpstreamMessage_ResolveAuthorityRequest:
			// ACL gating runs inside the handler so it can short-circuit on
			// the implicit-self-grant rule (caller is grant.actor or
			// audience) before consulting capability/resolve_authority.
			s.handleResolveAuthority(sessionCtx, client, identity, p.ResolveAuthorityRequest)
		case *pb.UpstreamMessage_ConnectionStatusRequest:
			// ACL gating runs inside the handler so self-checks short-circuit
			// without consulting capability/query_connections.
			s.handleConnectionStatus(sessionCtx, client, identity, p.ConnectionStatusRequest)
		case *pb.UpstreamMessage_ProxyHttpRequest, *pb.UpstreamMessage_ProxyHttpBodyChunk,
			*pb.UpstreamMessage_ProxyHttpResponse,
			*pb.UpstreamMessage_TunnelOpen, *pb.UpstreamMessage_TunnelData,
			*pb.UpstreamMessage_TunnelAck, *pb.UpstreamMessage_TunnelClose:
			env, ok := proxyEnvelopeFromUpstream(req)
			if ok {
				s.routeProxyEnvelope(sessionCtx, client, env)
			}
		case *pb.UpstreamMessage_Init:
			return status.Error(codes.InvalidArgument, "InitConnection already sent")
		default:
			// AdminQuery is not yet implemented over the streaming API
			sendClientError(client, "ERR_NOT_IMPLEMENTED", "This operation is not supported over the streaming API. Use the REST admin API.")
		}
	}
}

// checkConnection verifies that a principal has workspace access to connect.
// System principals (orchestrators, workflow engines, metrics bridges, bridges,
// and services) operate outside the workspace model and are unconditionally allowed.
// For all other principals the ACL service is consulted.
func (s *GatewayServer) checkConnection(ctx context.Context, identity models.Identity, sessionID string) error {
	if s.acl == nil {
		return nil // ACL not enabled
	}

	// Parse sessionID to UUID for ACL audit logging
	sessionUUID, err := uuid.Parse(sessionID)
	if err != nil {
		sessionUUID = uuid.Nil
	}

	// System-level principals operate outside the workspace model.
	switch identity.Type {
	case models.PrincipalOrchestrator, models.PrincipalWorkflowEngine, models.PrincipalMetricsBridge, models.PrincipalBridge, models.PrincipalService:
		logging.Logger.Info().Str("identity", identity.ToTopic()).Str("type", string(identity.Type)).Msg("system principal allowed to connect (workspace ACL not applicable)")
		return nil
	}

	decision, err := s.acl.CanConnect(ctx, identity, acl.ResourceTypeWorkspace, identity.Workspace, identity.Workspace, sessionUUID)
	if err != nil {
		return fmt.Errorf("ACL check failed: %w", err)
	}
	if decision.Denied() {
		return fmt.Errorf("access denied: %s", decision.Reason)
	}
	logging.Logger.Info().Str("identity", identity.ToTopic()).Str("workspace", identity.Workspace).Str("level", acl.AccessLevelName(decision.EffectiveAccessLevel)).Msg("connection allowed")
	return nil
}

// isAllowedAdminOp checks admin operation permission with operation-type granularity.
// System principals (WorkflowEngine, Orchestrator) bypass all checks.
// Other principals need either:
//   - admin/* (global admin) for full access, OR
//   - admin/{category} for category-specific access (e.g., admin/acl)
//
// Returns true if the operation is allowed. On denial, sends an error to the client.
// Note: this does NOT send an error for ACL operations — isAllowedACLOp handles that
// with its own workspace-scoped fallback path.
func (s *GatewayServer) isAllowedAdminOp(client *ClientSession, identity models.Identity, category string) bool {
	// System principals are implicitly allowed
	switch identity.Type {
	case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator:
		return true
	}

	if s.acl == nil {
		sendClientError(client, "ERR_PERMISSION_DENIED",
			fmt.Sprintf("admin %s operations require system-level privileges", category))
		return false
	}

	// Single check against admin/<category>. The umbrella rule admin/*
	// (granted to ops/super-users) glob-matches via Casbin's path.Match
	// pass; a category-specific rule (e.g. admin/acl) exact-matches.
	categoryPerm := "admin/" + category
	decision, err := s.acl.CheckAccess(
		context.Background(), identity,
		acl.ResourceTypeAdmin, categoryPerm,
		"admin_op_"+category, identity.Workspace, client.SessionUUID, acl.AccessAdmin,
	)
	if err == nil && decision != nil && decision.Allowed {
		return true
	}

	sendClientError(client, "ERR_PERMISSION_DENIED",
		fmt.Sprintf("admin %s operations require system-level privileges or an explicit ACL grant", category))
	return false
}

// isAllowedACLOp checks whether the caller can perform a specific ACL operation.
// Beyond the global/category admin checks, users with sufficient workspace-level
// access can manage ACL rules scoped to workspaces they have rights on.
//
// Required workspace access levels by operation:
//
//	LIST_RULES:                         AccessRead    (10) — see who has access
//	GRANT at level ≤ AccessReadWrite:   AccessManage  (30) — "manage" = can share
//	GRANT at level ≥ AccessManage:      AccessAdmin   (40) — only admins promote
//	REVOKE (rule level ≤ RW):           AccessManage  (30) — can remove grants ≤ own level
//	REVOKE (rule level > RW):           AccessAdmin   (40)
//	Non-workspace resources:            global admin only
func (s *GatewayServer) isAllowedACLOp(client *ClientSession, identity models.Identity, aclOp *pb.ACLOperation) bool {
	// System principals are implicitly allowed for all ACL operations.
	switch identity.Type {
	case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator:
		return true
	}

	if s.acl == nil {
		sendACLError(client, "ACL operations require admin privileges or workspace-level access")
		return false
	}

	// Determine the required permission level based on operation type.
	// Read operations (LIST_RULES, GET_RULE) only need AccessRead on admin/acl;
	// mutations (GRANT, REVOKE, etc.) need AccessAdmin.
	permLevel := acl.AccessAdmin
	switch aclOp.Op {
	case pb.ACLOperation_LIST_RULES, pb.ACLOperation_GET_RULE, pb.ACLOperation_QUERY_AUDIT,
		pb.ACLOperation_GET_FALLBACK_POLICY:
		permLevel = acl.AccessRead
	}

	// Check global admin permission (admin/* at required level)
	decision, err := s.acl.CheckAccess(
		context.Background(), identity,
		acl.ResourceTypeAdmin, acl.PermissionAdminOperations,
		"admin_op", identity.Workspace, client.SessionUUID, permLevel,
	)
	if err == nil && decision != nil && decision.Allowed {
		return true
	}

	// Check category-specific permission (admin/acl at required level)
	decision, err = s.acl.CheckAccess(
		context.Background(), identity,
		acl.ResourceTypeAdmin, acl.PermissionAdminACL,
		"admin_op_acl", identity.Workspace, client.SessionUUID, permLevel,
	)
	if err == nil && decision != nil && decision.Allowed {
		return true
	}

	// Second check: workspace-scoped ACL operations.
	// Users with sufficient workspace-level access can read/manage ACL rules
	// scoped to workspaces they have rights on.
	targetWorkspace := ""
	requiredLevel := acl.AccessAdmin // default: require admin

	switch aclOp.Op {
	case pb.ACLOperation_LIST_RULES:
		if f := aclOp.RuleFilter; f != nil && f.ResourceType == models.ResourceTypeWorkspace {
			targetWorkspace = f.ResourceId
		}
		requiredLevel = acl.AccessRead // anyone who can see the workspace can see its ACL

	case pb.ACLOperation_GET_RULE:
		if f := aclOp.RuleFilter; f != nil && f.ResourceType == models.ResourceTypeWorkspace {
			targetWorkspace = f.ResourceId
		}
		requiredLevel = acl.AccessRead

	case pb.ACLOperation_GRANT:
		if req := aclOp.GrantRequest; req != nil && req.ResourceType == models.ResourceTypeWorkspace {
			targetWorkspace = req.ResourceId
			if int(req.AccessLevel) <= acl.AccessReadWrite {
				requiredLevel = acl.AccessManage // manage can grant up to RW
			}
			// else: granting Manage or Admin requires Admin (default)
		}

	case pb.ACLOperation_REVOKE:
		// REVOKE uses rule_filter fields (principal + resource) to identify the target rule
		if f := aclOp.RuleFilter; f != nil && f.ResourceType == models.ResourceTypeWorkspace {
			targetWorkspace = f.ResourceId
			// Look up the existing rule to check its access level
			if f.PrincipalType != "" && f.PrincipalId != "" {
				rule, err := s.acl.GetRule(context.Background(),
					f.PrincipalType, f.PrincipalId, f.ResourceType, f.ResourceId)
				if err == nil && rule != nil && rule.AccessLevel <= acl.AccessReadWrite {
					requiredLevel = acl.AccessManage // can revoke grants ≤ RW
				}
			}
			// else: revoking Manage/Admin grants requires Admin (default)
		}

	case pb.ACLOperation_GET_FALLBACK_POLICY, pb.ACLOperation_QUERY_AUDIT:
		// Read-only operations that aren't workspace-scoped — require admin_acl grant
		sendACLError(client, "this ACL operation requires an admin_acl grant")
		return false

	case pb.ACLOperation_SET_FALLBACK_POLICY, pb.ACLOperation_CLEANUP_EXPIRED,
		pb.ACLOperation_CLEANUP_AUDIT_LOGS:
		// Global admin mutations — no workspace scoping
		sendACLError(client, "this ACL operation requires global admin privileges")
		return false
	}

	if targetWorkspace == "" {
		// No workspace scope derivable — requires admin_acl grant
		sendACLError(client, "ACL operations on non-workspace resources require an admin_acl grant")
		return false
	}

	// Check caller's access level on the target workspace
	decision, err = s.acl.CheckAccess(
		context.Background(), identity,
		acl.ResourceTypeWorkspace, targetWorkspace,
		"acl_manage", identity.Workspace, client.SessionUUID, requiredLevel,
	)
	if err == nil && decision != nil && decision.Allowed {
		return true
	}

	sendACLError(client, fmt.Sprintf("insufficient access on workspace %q for this ACL operation (need %s)",
		targetWorkspace, acl.AccessLevelName(requiredLevel)))
	return false
}

// isAllowedAdminOpQuiet checks admin operation permission without sending an error
// to the client on denial. Used by isAllowedACLOp which has its own fallback path.
func (s *GatewayServer) isAllowedAdminOpQuiet(client *ClientSession, identity models.Identity, category string) bool {
	// System principals are implicitly allowed
	switch identity.Type {
	case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator:
		return true
	}

	if s.acl == nil {
		return false
	}

	// Single check against admin/<category>; admin/* umbrella glob-matches.
	categoryPerm := "admin/" + category
	decision, err := s.acl.CheckAccess(
		context.Background(), identity,
		acl.ResourceTypeAdmin, categoryPerm,
		"admin_op_"+category, identity.Workspace, client.SessionUUID, acl.AccessAdmin,
	)
	if err == nil && decision != nil && decision.Allowed {
		return true
	}

	return false
}

// acquireSessionLock acquires a distributed lock for the identity, with optional session resume.
// It also registers the session. On success, the caller must ensure cleanup on disconnect.
func (s *GatewayServer) acquireSessionLock(ctx context.Context, cs *connectionState, resumeSessionID string) error {
	ctx, span := tracing.Tracer.Start(ctx, "gateway.AcquireSessionLock")
	defer span.End()
	span.SetAttributes(
		attribute.String("identity", cs.identity.String()),
		attribute.String("session_id", cs.sessionID),
	)
	if resumeSessionID != "" {
		span.SetAttributes(attribute.String("resume_session_id", resumeSessionID))
	}

	lockStart := time.Now()
	// Force-takeover threshold: use 1.5× LockRefreshInterval (15s) rather than
	// LockRefreshInterval (10s) so a single missed refresh cycle does not
	// prematurely declare the holder dead.
	forceTakeoverThresholdMs := (state.LockRefreshInterval * 3 / 2).Milliseconds()
	success, resumed, forced, err := s.sessions.AcquireOrResumeLock(cs.sessionCtx, cs.identity, cs.sessionID, resumeSessionID, forceTakeoverThresholdMs)
	sessionLockDuration.Observe(time.Since(lockStart).Seconds())
	if err != nil {
		redisOperations.WithLabelValues("lock_acquire", "failure").Inc()
		// Audit: Lock acquisition failure (internal error)
		sessionUUID, _ := uuid.Parse(cs.sessionID)
		s.auditLog(ctx, audit.NewConnectionEvent(string(cs.identity.Type), cs.identity.String(), audit.OpLockAcquired, sessionUUID, false, err.Error(), map[string]interface{}{
			"workspace":         cs.identity.Workspace,
			"resume_session_id": resumeSessionID,
		}))
		logging.Logger.Error().Err(err).Str("identity", cs.identity.String()).Msg("failed to acquire session lock")
		return status.Error(codes.Internal, "failed to acquire session lock")
	}
	if !success {
		redisOperations.WithLabelValues("lock_acquire", "failure").Inc()
		// Audit: Lock rejected (duplicate identity)
		sessionUUID, _ := uuid.Parse(cs.sessionID)
		s.auditLog(ctx, audit.NewConnectionEvent(string(cs.identity.Type), cs.identity.String(), audit.OpLockRejected, sessionUUID, false, "identity already connected", map[string]interface{}{
			"workspace":         cs.identity.Workspace,
			"resume_session_id": resumeSessionID,
		}))
		return status.Error(codes.AlreadyExists, fmt.Sprintf("DuplicateIdentityError: identity (%s) already connected", cs.identity.String()))
	}
	redisOperations.WithLabelValues("lock_acquire", "success").Inc()
	cs.resumed = resumed
	if resumed {
		logging.Logger.Info().Str("session_id", cs.sessionID).Str("resume_session_id", resumeSessionID).Str("identity", cs.identity.String()).Msg("session resumed")
	}
	if forced {
		logging.Logger.Warn().Str("session_id", cs.sessionID).Str("identity", cs.identity.String()).Msg("session lock force-takeover: previous holder appears dead")
	}
	// Audit: Lock acquired successfully
	sessionUUID, _ := uuid.Parse(cs.sessionID)
	s.auditLog(ctx, audit.NewConnectionEvent(string(cs.identity.Type), cs.identity.String(), audit.OpLockAcquired, sessionUUID, true, "", map[string]interface{}{
		"workspace":         cs.identity.Workspace,
		"resumed":           resumed,
		"forced":            forced,
		"resume_session_id": resumeSessionID,
	}))

	// Register Session
	err = s.sessions.RegisterSession(cs.sessionCtx, cs.identity, cs.sessionID, s.gatewayID)
	if err != nil {
		redisOperations.WithLabelValues("session_register", "failure").Inc()
		regCleanupCtx, regCleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer regCleanupCancel()
		// Best-effort cleanup of the just-acquired lock after a registration
		// failure. The TTL will expire the lock anyway, so a failure here is
		// non-fatal — log and proceed with the registration-failure return.
		if releaseErr := s.sessions.ReleaseLock(regCleanupCtx, cs.identity, cs.sessionID); releaseErr != nil {
			logging.Logger.Warn().Err(releaseErr).Str("session_id", cs.sessionID).Msg("failed to release lock after session register failure; relying on TTL expiry")
		}
		// Audit: Session registration failure
		sessionUUID, _ := uuid.Parse(cs.sessionID)
		s.auditLog(ctx, audit.NewConnectionEvent(string(cs.identity.Type), cs.identity.String(), audit.OpSessionRegistered, sessionUUID, false, err.Error(), map[string]interface{}{
			"workspace": cs.identity.Workspace,
		}))
		logging.Logger.Error().Err(err).Str("session_id", cs.sessionID).Msg("failed to register session")
		return status.Error(codes.Internal, "failed to register session")
	}
	redisOperations.WithLabelValues("session_register", "success").Inc()
	// Audit: Session registered successfully
	sessionUUID, _ = uuid.Parse(cs.sessionID)
	s.auditLog(ctx, audit.NewConnectionEvent(string(cs.identity.Type), cs.identity.String(), audit.OpSessionRegistered, sessionUUID, true, "", map[string]interface{}{
		"workspace": cs.identity.Workspace,
	}))

	return nil
}

// startLockRefresh launches a goroutine that periodically refreshes the distributed lock
// and session metadata. If the lock is lost, it cancels the session context.
func (s *GatewayServer) startLockRefresh(sessionCtx context.Context, sessionCancel context.CancelFunc, identity models.Identity, sessionID string) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "lockRefresh").Msg("recovered from panic in background goroutine")
				sessionCancel()
			}
		}()
		ticker := time.NewTicker(state.LockRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sessionCtx.Done():
				return
			case <-ticker.C:
				var refreshed bool
				refreshCtx, refreshCancel := context.WithTimeout(sessionCtx, 5*time.Second)
				err := s.redisBreaker.Execute(func() error {
					var refreshErr error
					// RefreshLockAndSession combines lock TTL refresh and session TTL
					// refresh into a single Lua script, eliminating a sequential
					// Redis round-trip on every heartbeat tick.
					refreshed, refreshErr = s.sessions.RefreshLockAndSession(refreshCtx, identity, sessionID)
					return refreshErr
				})
				refreshCancel()
				// Update circuit breaker state gauge after each refresh attempt
				circuitBreakerState.WithLabelValues("redis").Set(float64(s.redisBreaker.State()))
				if err == circuitbreaker.ErrCircuitOpen {
					// Redis is down transiently; skip this refresh cycle.
					// The lock TTL provides a grace period before expiry.
					logging.Logger.Warn().Str("identity", identity.String()).Str("circuit", s.redisBreaker.Name()).Msg("lock refresh skipped: circuit breaker open")
					continue
				}
				if err != nil {
					redisOperations.WithLabelValues("lock_refresh", "failure").Inc()
					logging.Logger.Error().Err(err).Str("identity", identity.String()).Msg("error refreshing lock")
					sessionCancel() // Disconnect the client
					return
				}
				if !refreshed {
					redisOperations.WithLabelValues("lock_refresh", "failure").Inc()
					logging.Logger.Warn().Str("identity", identity.String()).Str("session_id", sessionID).Msg("lock lost, disconnecting")
					sessionCancel() // Lock was taken by someone else
					return
				}
				redisOperations.WithLabelValues("lock_refresh", "success").Inc()

				// Update orchestrator profile heartbeat to keep it alive in the database
				if identity.Type == models.PrincipalOrchestrator && s.orchestration != nil && s.orchestration.Registry != nil {
					heartbeatCtx, heartbeatCancel := context.WithTimeout(sessionCtx, 5*time.Second)
					if err := s.orchestration.Registry.UpdateHeartbeat(heartbeatCtx, identity.String()); err != nil {
						logging.Logger.Warn().Err(err).Str("identity", identity.String()).Msg("failed to update orchestrator heartbeat")
						// Non-fatal - orchestrator will just appear offline in UI after 60s, but will remain functional
					}
					heartbeatCancel()
				}
			}
		}
	}()
}

// rollbackSession performs early cleanup when connection setup fails after lock acquisition.
// It removes session tracking, unregisters the session, and releases the lock with proper error logging.
func (s *GatewayServer) rollbackSession(cs *connectionState) {
	s.activeStreams.Delete(cs.sessionID)
	s.identityIndex.Delete(cs.identity.String())

	// Remove agent from implementation index
	if cs.identity.Type == models.PrincipalAgent && cs.client != nil {
		s.removeFromImplIndex(cs.identity, cs.client)
	}

	// Remove orchestrator from profile index
	if cs.identity.Type == models.PrincipalOrchestrator && cs.client != nil {
		s.unregisterOrchestratorFromIndex(cs.client)
	}

	rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rollbackCancel()

	if err := s.sessions.UnregisterSession(rollbackCtx, cs.sessionID); err != nil {
		redisOperations.WithLabelValues("session_unregister", "failure").Inc()
		logging.Logger.Warn().Err(err).Str("session_id", cs.sessionID).Msg("failed to unregister session during rollback")
	} else {
		redisOperations.WithLabelValues("session_unregister", "success").Inc()
	}
	if err := s.sessions.ReleaseLock(rollbackCtx, cs.identity, cs.sessionID); err != nil {
		redisOperations.WithLabelValues("lock_release", "failure").Inc()
		logging.Logger.Warn().Err(err).Str("identity", cs.identity.String()).Msg("failed to release lock during rollback")
	} else {
		redisOperations.WithLabelValues("lock_release", "success").Inc()
	}
	// Decrement workspace connection count on rollback
	if s.quotaEnforcer.quotaManager != nil {
		if err := s.quotaEnforcer.quotaManager.DecrementConnections(rollbackCtx, cs.identity.Workspace); err != nil {
			logging.Logger.Warn().Err(err).Str("workspace", cs.identity.Workspace).Msg("failed to decrement connection count during rollback")
		}
	}
}

// cleanupSession performs all cleanup when a client disconnects: unsubscribes from topics,
// removes session tracking, releases the lock, updates orchestration task state, and audits.
func (s *GatewayServer) cleanupSession(cs *connectionState, gracefulExit bool) {
	_, span := tracing.Tracer.Start(context.Background(), "gateway.CleanupSession")
	defer span.End()
	span.SetAttributes(
		attribute.String("session_id", cs.sessionID),
		attribute.String("identity", cs.identity.String()),
		attribute.Bool("graceful_exit", gracefulExit),
	)

	// Clean up any pending workflow requests for this client
	s.cleanupPendingWorkflowRequests(cs.client)

	// Unsubscribe from all topics
	cs.client.UnsubscribeAll()
	s.activeStreams.Delete(cs.sessionID)
	s.identityIndex.Delete(cs.identity.String())

	// Remove agent from implementation index
	if cs.identity.Type == models.PrincipalAgent {
		s.removeFromImplIndex(cs.identity, cs.client)
	}

	// Remove orchestrator from profile index
	if cs.identity.Type == models.PrincipalOrchestrator {
		s.unregisterOrchestratorFromIndex(cs.client)
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanupCancel()

	if err := s.sessions.UnregisterSession(cleanupCtx, cs.sessionID); err != nil {
		redisOperations.WithLabelValues("session_unregister", "failure").Inc()
		logging.Logger.Warn().Err(err).Str("session_id", cs.sessionID).Msg("failed to unregister session during cleanup")
	} else {
		redisOperations.WithLabelValues("session_unregister", "success").Inc()
	}
	if err := s.sessions.ReleaseLock(cleanupCtx, cs.identity, cs.sessionID); err != nil {
		redisOperations.WithLabelValues("lock_release", "failure").Inc()
		logging.Logger.Warn().Err(err).Str("identity", cs.identity.String()).Msg("failed to release lock during cleanup")
	} else {
		redisOperations.WithLabelValues("lock_release", "success").Inc()
	}
	// Decrement workspace connection count on disconnect
	if s.quotaEnforcer.quotaManager != nil {
		if err := s.quotaEnforcer.quotaManager.DecrementConnections(cleanupCtx, cs.identity.Workspace); err != nil {
			logging.Logger.Warn().Err(err).Str("workspace", cs.identity.Workspace).Msg("failed to decrement connection count during cleanup")
		}
	}

	// Update associated task state per the connection-as-heartbeat model:
	//
	// serverInitiated  | gracefulExit | Action
	// -----------------+--------------+----------------------------------
	// true             | any          | mark disconnected_at; no terminal
	// false            | true         | CompleteTask (worker EOF = "done")
	// false            | false        | mark disconnected_at; reaper handles
	//
	// Token + authority-grant revocation only fire on terminal transitions
	// (Complete/Fail/Cancel), so reconnect-with-same-token works for both
	// horizontal scaling and transient network blips.
	if cs.client != nil && cs.associatedTaskID != "" && s.orchestration != nil && s.orchestration.TaskService != nil {
		serverInitiated := cs.client.serverInitiatedDisconnect.Load()
		switch {
		case serverInitiated:
			if err := s.orchestration.TaskService.MarkTaskDisconnected(cleanupCtx, cs.associatedTaskID, time.Now()); err != nil {
				logging.Logger.Warn().Err(err).Str("task_id", cs.associatedTaskID).Msg("failed to mark task disconnected (server-initiated)")
			} else {
				logging.Logger.Info().Str("task_id", cs.associatedTaskID).Str("identity", cs.identity.String()).Msg("session ended; task left in current state (server-initiated disconnect — worker may reconnect)")
			}
		case gracefulExit:
			if err := s.orchestration.TaskService.CompleteTask(cleanupCtx, cs.associatedTaskID); err != nil {
				logging.Logger.Warn().Err(err).Str("task_id", cs.associatedTaskID).Msg("failed to mark task as completed on graceful exit")
			} else {
				logging.Logger.Info().Str("task_id", cs.associatedTaskID).Str("identity", cs.identity.String()).Msg("task marked as completed (worker graceful exit)")
				s.notifyTaskStatusChangeFromTaskID(cleanupCtx, cs.associatedTaskID, "completed", "")
			}
		default:
			if err := s.orchestration.TaskService.MarkTaskDisconnected(cleanupCtx, cs.associatedTaskID, time.Now()); err != nil {
				logging.Logger.Warn().Err(err).Str("task_id", cs.associatedTaskID).Msg("failed to mark task disconnected on unexpected drop")
			} else {
				logging.Logger.Info().Str("task_id", cs.associatedTaskID).Str("identity", cs.identity.String()).Msg("session ended unexpectedly; task in disconnect grace window (reaper will fail if no reconnect)")
			}
		}
	}

	// Audit: connection close with reason
	var reason string
	if cs.sessionCtx.Err() != nil {
		reason = "admin_disconnect"
	} else if gracefulExit {
		reason = "graceful_close"
	} else {
		reason = "error"
	}
	metadata := map[string]interface{}{
		"reason":    reason,
		"workspace": cs.identity.Workspace,
	}
	if cs.associatedTaskID != "" {
		metadata["associated_task_id"] = cs.associatedTaskID
	}
	sessionUUID, _ := uuid.Parse(cs.sessionID)
	s.auditLog(context.Background(), audit.NewConnectionEvent(string(cs.identity.Type), cs.identity.String(), audit.OpConnectionClosed, sessionUUID, true, "", metadata))

	logging.Logger.Info().Str("session_id", cs.sessionID).Str("identity", cs.identity.String()).Msg("session disconnected")
}
