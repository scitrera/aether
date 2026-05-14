package gateway

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/scitrera/aether/internal/logging"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/kv"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	auditstore "github.com/scitrera/aether/internal/storage/audit"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVACLChecker abstracts ACL access checks for testability.
type KVACLChecker interface {
	CheckAccess(ctx context.Context, principal models.Identity, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*acl.ACLDecision, error)
	CheckAccessWithAuthority(ctx context.Context, actor models.Identity, authority *acl.ResolvedAuthority, resourceType, resourceID, operation, workspace string, sessionID uuid.UUID, requiredLevel int) (*acl.ACLDecision, error)
}

// KVHandler handles KV operations for the gateway
type KVHandler struct {
	kvStore KVReadWriter
	// auditLogger is the audit domain Store (internal/storage/audit).
	auditLogger auditstore.Store
	aclService  KVACLChecker // nil when PostgreSQL unavailable
}

// NewKVHandler creates a new KV handler
func NewKVHandler(store KVReadWriter, auditLogger auditstore.Store, aclService KVACLChecker) *KVHandler {
	return &KVHandler{
		kvStore:     store,
		auditLogger: auditLogger,
		aclService:  aclService,
	}
}

// newKVHandlerFromService creates a KV handler from an ACL store implementation,
// correctly handling nil (no PostgreSQL) to avoid non-nil interface with nil value.
func newKVHandlerFromService(store KVReadWriter, auditLogger auditstore.Store, aclService aclstore.Store) *KVHandler {
	var checker KVACLChecker
	if aclService != nil {
		// aclstore.Store satisfies KVACLChecker — both CheckAccess and
		// CheckAccessWithAuthority are part of the storage interface.
		checker = aclService
	}
	return NewKVHandler(store, auditLogger, checker)
}

// checkKeyPermission checks key-level then scope-level ACL for the given operation.
// Key-level rules (kv_key/<key>) take precedence; if no explicit key rule exists
// (FallbackApplied), falls through to scope-level check (kv_scope/<scope>).
// When no ACL service is configured (no PostgreSQL), all operations are allowed.
//
// Owner fast-path: for EXCLUSIVE scopes, the namespace is isolated to
// the caller's `agent:{impl}|{spec}` segment by storage layout — there
// is no way for another agent to ever read or write the same key. We
// short-circuit ALLOW at AccessReadWrite without consulting the DB.
// Cross-agent access on exclusive scopes via OBO authority still flows
// through the regular ACL path (the actor identity differs from the
// authority subject in that case, so the fast-path skips it).
func (h *KVHandler) checkKeyPermission(ctx context.Context, identity models.Identity, authority *acl.ResolvedAuthority, scope kv.KVScope, key, operation, workspace string, sessionID uuid.UUID, requiredLevel int) error {
	if scope.IsExclusive() && authority == nil {
		// Caller is operating on their own per-agent namespace under
		// direct authority; ownership is implicit by storage layout.
		return nil
	}
	if h.aclService == nil {
		return nil
	}

	// 1. Check key-level rule
	decision, err := h.aclService.CheckAccessWithAuthority(ctx, identity, authority, acl.ResourceTypeKVKey, key, operation, workspace, sessionID, requiredLevel)
	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("key", key).Msg("ACL check failed for KV key, denying")
		return status.Errorf(codes.Internal, "ACL check failed: %v", err)
	}
	if !decision.FallbackApplied {
		// Explicit rule exists for this key — use it
		if decision.Denied() {
			return status.Errorf(codes.PermissionDenied, "KV access denied for key %s: %s", key, decision.Reason)
		}
		return nil
	}

	// 2. No key-level rule — fall through to scope-level
	decision, err = h.aclService.CheckAccessWithAuthority(ctx, identity, authority, acl.ResourceTypeKVScope, string(scope), operation, workspace, sessionID, requiredLevel)
	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Msg("ACL check failed for KV scope, denying")
		return status.Errorf(codes.Internal, "ACL check failed: %v", err)
	}
	if decision.Denied() {
		return status.Errorf(codes.PermissionDenied, "KV access denied for scope %s: %s", scope, decision.Reason)
	}
	return nil
}

// checkScopeReadPermission checks scope-level read permission (used for LIST which has no specific key).
func (h *KVHandler) checkScopeReadPermission(ctx context.Context, identity models.Identity, authority *acl.ResolvedAuthority, scope kv.KVScope, operation, workspace string, sessionID uuid.UUID) error {
	if h.aclService == nil {
		return nil
	}
	decision, err := h.aclService.CheckAccessWithAuthority(ctx, identity, authority, acl.ResourceTypeKVScope, string(scope), operation, workspace, sessionID, acl.AccessRead)
	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Msg("ACL check failed for KV scope read, denying")
		return status.Errorf(codes.Internal, "ACL check failed: %v", err)
	}
	if decision.Denied() {
		return status.Errorf(codes.PermissionDenied, "KV read denied for scope %s: %s", scope, decision.Reason)
	}
	return nil
}

// HandleKVOperation processes a KV operation from a client.
// The request_id from the operation is echoed back in the response for correlation.
func (h *KVHandler) HandleKVOperation(
	ctx context.Context,
	identity models.Identity,
	sessionID uuid.UUID,
	authority *acl.ResolvedAuthority,
	op *pb.KVOperation,
	sendResponse func(*pb.DownstreamMessage),
) error {
	// Agents, tasks, and services can access the KV store.
	if identity.Type != models.PrincipalAgent && identity.Type != models.PrincipalTask && identity.Type != models.PrincipalService {
		return status.Error(codes.PermissionDenied, "only agents, tasks, and services can access KV store")
	}

	// Map proto enum scope to internal KVScope (default to workspace for backward compatibility)
	scope := kv.ScopeWorkspace
	switch op.Scope {
	case pb.KVOperation_GLOBAL:
		scope = kv.ScopeGlobal
	case pb.KVOperation_WORKSPACE:
		scope = kv.ScopeWorkspace
	case pb.KVOperation_USER:
		scope = kv.ScopeUser
	case pb.KVOperation_USER_WORKSPACE:
		scope = kv.ScopeUserWorkspace
	case pb.KVOperation_GLOBAL_EXCLUSIVE:
		scope = kv.ScopeGlobalExclusive
	case pb.KVOperation_WORKSPACE_EXCLUSIVE:
		scope = kv.ScopeWorkspaceExclusive
	case pb.KVOperation_USER_SHARED:
		scope = kv.ScopeUserShared
	case pb.KVOperation_USER_WORKSPACE_SHARED:
		scope = kv.ScopeUserWorkspaceShared
	}

	// Extract user ID and workspace from operation or identity
	userID := op.UserId
	workspace := op.Workspace
	if workspace == "" {
		workspace = identity.Workspace
	}

	// Validate scope configuration
	if err := kv.ValidateScopeConfig(scope, identity, userID, workspace); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid scope config: %v", err)
	}

	// Parse TTL
	var ttl time.Duration
	if op.Ttl > 0 {
		ttl = time.Duration(op.Ttl) * time.Second
	}

	requestID := op.GetRequestId()

	// CRIT-2: Observe KV operation latency
	opStart := time.Now()
	opName := op.Op.String()
	var opErr error
	defer func() {
		kvOperationLatency.WithLabelValues(opName, string(scope)).Observe(time.Since(opStart).Seconds())
		opStatus := "ok"
		if opErr != nil {
			opStatus = "error"
		}
		kvOperations.WithLabelValues(opName, string(scope), opStatus).Inc()
	}()

	switch op.Op {
	case pb.KVOperation_GET:
		opErr = h.handleGet(ctx, identity, authority, sessionID, scope, op.Key, userID, workspace, requestID, sendResponse)

	case pb.KVOperation_PUT:
		opErr = h.handlePut(ctx, identity, authority, sessionID, scope, op.Key, string(op.Value), userID, workspace, ttl, requestID, sendResponse)

	case pb.KVOperation_DELETE:
		opErr = h.handleDelete(ctx, identity, authority, sessionID, scope, op.Key, userID, workspace, requestID, sendResponse)

	case pb.KVOperation_LIST:
		opErr = h.handleList(ctx, identity, authority, sessionID, scope, op.Key, userID, workspace, requestID, sendResponse)

	case pb.KVOperation_INCREMENT:
		opErr = h.handleIncrement(ctx, identity, authority, sessionID, scope, op.Key, userID, workspace, ttl, requestID, sendResponse)

	case pb.KVOperation_DECREMENT:
		opErr = h.handleDecrement(ctx, identity, authority, sessionID, scope, op.Key, userID, workspace, requestID, sendResponse)

	case pb.KVOperation_INCREMENT_IF:
		opErr = h.handleIncrementIf(ctx, identity, authority, sessionID, scope, op.Key, userID, workspace, op.DeltaValue, op.GuardValue, requestID, sendResponse)

	case pb.KVOperation_DECREMENT_IF:
		opErr = h.handleDecrementIf(ctx, identity, authority, sessionID, scope, op.Key, userID, workspace, op.DeltaValue, op.GuardValue, requestID, sendResponse)

	default:
		opErr = status.Error(codes.InvalidArgument, "unknown KV operation")
	}
	return opErr
}

func (h *KVHandler) handleGet(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	key string,
	userID string,
	workspace string,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.get")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.key", key),
		attribute.String("aether.kv.workspace", workspace),
	)

	// ACL-based read permission check (key-level then scope-level)
	if err := h.checkKeyPermission(ctx, identity, authority, scope, key, audit.OpKVGet, workspace, sessionID, acl.AccessRead); err != nil {
		return err
	}

	value, err := h.kvStore.Get(ctx, identity, scope, key, userID, workspace)

	// Audit logging
	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}

		metadata := map[string]interface{}{
			"scope":     string(scope),
			"key":       key,
			"workspace": workspace,
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if success {
			metadata["value_length"] = len(value)
		}

		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVGet,
			key,
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			// Key not found is a normal GET miss — return success with empty value
			logging.Logger.Debug().Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV GET key not found")
			sendResponse(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_Kv{
					Kv: &pb.KVResponse{
						Success:   true,
						Value:     nil,
						RequestId: requestID,
					},
				},
			})
			return nil
		}
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV GET failed")
		return status.Errorf(codes.Internal, "failed to get key: %v", err)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:   true,
				Value:     []byte(value),
				RequestId: requestID,
			},
		},
	})

	return nil
}

func (h *KVHandler) handlePut(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	key string,
	value string,
	userID string,
	workspace string,
	ttl time.Duration,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.put")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.key", key),
		attribute.String("aether.kv.workspace", workspace),
	)

	// ACL-based write permission check (key-level then scope-level)
	if err := h.checkKeyPermission(ctx, identity, authority, scope, key, audit.OpKVPut, workspace, sessionID, acl.AccessReadWrite); err != nil {
		return err
	}

	// Get old value for audit logging only when verbosity requires it
	var oldValue string
	if h.auditLogger != nil {
		verbosity := h.auditLogger.GetConfig().VerbosityLevel
		if audit.ShouldIncludeMessageMetadata(verbosity) {
			oldValue, _ = h.kvStore.Get(ctx, identity, scope, key, userID, workspace)
		}
	}

	err := h.kvStore.Set(ctx, identity, scope, key, value, userID, workspace, ttl)

	// Audit logging
	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}

		metadata := map[string]interface{}{
			"scope":          string(scope),
			"key":            key,
			"workspace":      workspace,
			"new_value_size": len(value),
			"is_update":      oldValue != "",
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if ttl > 0 {
			metadata["ttl_seconds"] = int(ttl.Seconds())
		}
		if oldValue != "" {
			metadata["old_value_size"] = len(oldValue)
			metadata["value_changed"] = oldValue != value
		}

		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVPut,
			key,
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV PUT failed")
		return status.Errorf(codes.Internal, "failed to set key: %v", err)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:   true,
				RequestId: requestID,
			},
		},
	})

	return nil
}

func (h *KVHandler) handleDelete(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	key string,
	userID string,
	workspace string,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.delete")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.key", key),
		attribute.String("aether.kv.workspace", workspace),
	)

	// ACL-based write permission check (key-level then scope-level)
	if err := h.checkKeyPermission(ctx, identity, authority, scope, key, audit.OpKVDelete, workspace, sessionID, acl.AccessReadWrite); err != nil {
		return err
	}

	// Get old value for audit logging only when verbosity requires it
	var oldValue string
	if h.auditLogger != nil {
		verbosity := h.auditLogger.GetConfig().VerbosityLevel
		if audit.ShouldIncludeMessageMetadata(verbosity) {
			oldValue, _ = h.kvStore.Get(ctx, identity, scope, key, userID, workspace)
		}
	}

	err := h.kvStore.Delete(ctx, identity, scope, key, userID, workspace)

	// Audit logging
	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}

		metadata := map[string]interface{}{
			"scope":     string(scope),
			"key":       key,
			"workspace": workspace,
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if oldValue != "" {
			metadata["old_value_size"] = len(oldValue)
		}

		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVDelete,
			key,
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV DELETE failed")
		return status.Errorf(codes.Internal, "failed to delete key: %v", err)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:   true,
				RequestId: requestID,
			},
		},
	})

	return nil
}

func (h *KVHandler) handleList(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	keyPrefix string,
	userID string,
	workspace string,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.list")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.workspace", workspace),
		attribute.String("aether.kv.key_prefix", keyPrefix),
	)

	// ACL-based scope read permission check (LIST has no specific key)
	if err := h.checkScopeReadPermission(ctx, identity, authority, scope, audit.OpKVList, workspace, sessionID); err != nil {
		return err
	}

	items, err := h.kvStore.List(ctx, identity, scope, userID, workspace)
	// Apply caller-supplied key prefix filter. Pre-Solution-A this was
	// implicitly handled by the per-agent storage namespace (each caller
	// only saw its own writes). With shared global/workspace namespaces
	// the store now returns every key in the scope; the prefix filter
	// must be enforced explicitly so callers like
	// AetherKVHelper.get_by_prefix actually see only the keys they asked
	// for and don't try to deserialize unrelated payloads from sibling
	// agents.
	if err == nil && keyPrefix != "" && len(items) > 0 {
		filtered := make(map[string]string, len(items))
		for k, v := range items {
			if strings.HasPrefix(k, keyPrefix) {
				filtered[k] = v
			}
		}
		items = filtered
	}

	// Audit logging
	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}

		metadata := map[string]interface{}{
			"scope":     string(scope),
			"workspace": workspace,
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if success {
			metadata["result_count"] = len(items)
		}

		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVList,
			"", // No specific key for LIST operation
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Msg("KV LIST failed")
		return status.Errorf(codes.Internal, "failed to list keys: %v", err)
	}

	// Convert map[string]string → map[string][]byte for the proto wire.
	// The store's internal Go strings hold arbitrary byte sequences; the
	// proto field is now `bytes` so non-UTF-8 binary payloads (msgpack,
	// protobuf, raw blobs) survive transit to non-Go clients.
	itemsBytes := make(map[string][]byte, len(items))
	for k, v := range items {
		itemsBytes[k] = []byte(v)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:   true,
				Keys:      itemsToKeys(items),
				KvMap:     itemsBytes,
				RequestId: requestID,
			},
		},
	})

	return nil
}

func (h *KVHandler) handleIncrement(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	key string,
	userID string,
	workspace string,
	ttl time.Duration,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.increment")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.key", key),
		attribute.String("aether.kv.workspace", workspace),
	)

	// ACL-based write permission check (key-level then scope-level)
	if err := h.checkKeyPermission(ctx, identity, authority, scope, key, audit.OpKVIncrement, workspace, sessionID, acl.AccessReadWrite); err != nil {
		return err
	}

	counterVal, err := h.kvStore.Increment(ctx, identity, scope, key, userID, workspace)

	// If a TTL is specified and this is the first increment (counterVal == 1),
	// set the expiry on the key. We re-set the key with the string representation
	// of the counter value so the TTL takes effect without losing the numeric value.
	// NOTE: This two-step approach (INCR then EXPIRE via SET) is not fully atomic.
	// For strict atomicity (e.g., sliding rate limit windows), a Lua script should
	// be used instead. This is acceptable for fixed-window rate limit use cases
	// where the window is established on the first increment.
	if err == nil && ttl > 0 && counterVal == 1 {
		// Only set TTL on the first increment to establish the window boundary
		if setErr := h.kvStore.Set(ctx, identity, scope, key, "1", userID, workspace, ttl); setErr != nil {
			logging.Logger.Error().Err(setErr).Str("identity", identity.String()).Str("key", key).Msg("KV INCREMENT: failed to set TTL after first increment")
		}
	}

	// Audit logging
	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}

		metadata := map[string]interface{}{
			"scope":     string(scope),
			"key":       key,
			"workspace": workspace,
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if success {
			metadata["counter_value"] = counterVal
		}
		if ttl > 0 {
			metadata["ttl_seconds"] = int(ttl.Seconds())
		}

		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVIncrement,
			key,
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV INCREMENT failed")
		return status.Errorf(codes.Internal, "failed to increment key: %v", err)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:      true,
				CounterValue: counterVal,
				RequestId:    requestID,
			},
		},
	})

	return nil
}

func (h *KVHandler) handleDecrement(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	key string,
	userID string,
	workspace string,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.decrement")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.key", key),
		attribute.String("aether.kv.workspace", workspace),
	)

	// ACL-based write permission check (key-level then scope-level)
	if err := h.checkKeyPermission(ctx, identity, authority, scope, key, audit.OpKVDecrement, workspace, sessionID, acl.AccessReadWrite); err != nil {
		return err
	}

	counterVal, err := h.kvStore.Decrement(ctx, identity, scope, key, userID, workspace)

	// Audit logging
	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}

		metadata := map[string]interface{}{
			"scope":     string(scope),
			"key":       key,
			"workspace": workspace,
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if success {
			metadata["counter_value"] = counterVal
		}

		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVDecrement,
			key,
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV DECREMENT failed")
		return status.Errorf(codes.Internal, "failed to decrement key: %v", err)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:      true,
				CounterValue: counterVal,
				RequestId:    requestID,
			},
		},
	})

	return nil
}

func (h *KVHandler) handleIncrementIf(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	ceiling int64,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.increment_if")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.key", key),
		attribute.String("aether.kv.workspace", workspace),
		attribute.Int64("aether.kv.guard_value", ceiling),
	)

	if err := h.checkKeyPermission(ctx, identity, authority, scope, key, audit.OpKVIncrementIf, workspace, sessionID, acl.AccessReadWrite); err != nil {
		return err
	}

	if delta == 0 {
		delta = 1
	}
	counterVal, applied, err := h.kvStore.IncrementIf(ctx, identity, scope, key, userID, workspace, delta, ceiling)

	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}
		metadata := map[string]interface{}{
			"scope":       string(scope),
			"key":         key,
			"workspace":   workspace,
			"guard_value": ceiling,
			"delta":       delta,
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if success {
			metadata["counter_value"] = counterVal
			metadata["applied"] = applied
		}
		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVIncrementIf,
			key,
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV INCREMENT_IF failed")
		return status.Errorf(codes.Internal, "failed to increment_if key: %v", err)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:      true,
				CounterValue: counterVal,
				Applied:      applied,
				RequestId:    requestID,
			},
		},
	})
	return nil
}

func (h *KVHandler) handleDecrementIf(
	ctx context.Context,
	identity models.Identity,
	authority *acl.ResolvedAuthority,
	sessionID uuid.UUID,
	scope kv.KVScope,
	key string,
	userID string,
	workspace string,
	delta int64,
	floor int64,
	requestID string,
	sendResponse func(*pb.DownstreamMessage),
) error {
	ctx, span := tracing.Tracer.Start(ctx, "aether.kv.decrement_if")
	defer span.End()
	span.SetAttributes(
		attribute.String("aether.kv.scope", string(scope)),
		attribute.String("aether.kv.key", key),
		attribute.String("aether.kv.workspace", workspace),
		attribute.Int64("aether.kv.guard_value", floor),
	)

	if err := h.checkKeyPermission(ctx, identity, authority, scope, key, audit.OpKVDecrementIf, workspace, sessionID, acl.AccessReadWrite); err != nil {
		return err
	}

	if delta == 0 {
		delta = 1
	}
	counterVal, applied, err := h.kvStore.DecrementIf(ctx, identity, scope, key, userID, workspace, delta, floor)

	if h.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}
		metadata := map[string]interface{}{
			"scope":       string(scope),
			"key":         key,
			"workspace":   workspace,
			"guard_value": floor,
			"delta":       delta,
		}
		if userID != "" {
			metadata["user_id"] = userID
		}
		if success {
			metadata["counter_value"] = counterVal
			metadata["applied"] = applied
		}
		event := audit.NewKVEvent(
			string(identity.Type),
			identity.String(),
			audit.OpKVDecrementIf,
			key,
			workspace,
			sessionID,
			success,
			errorMsg,
			metadata,
		)
		applyResolvedAuthorityToAuditEvent(event, authority)
		h.auditLogger.LogEvent(ctx, event)
	}

	if err != nil {
		logging.Logger.Error().Err(err).Str("identity", identity.String()).Str("scope", string(scope)).Str("key", key).Msg("KV DECREMENT_IF failed")
		return status.Errorf(codes.Internal, "failed to decrement_if key: %v", err)
	}

	sendResponse(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{
			Kv: &pb.KVResponse{
				Success:      true,
				CounterValue: counterVal,
				Applied:      applied,
				RequestId:    requestID,
			},
		},
	})
	return nil
}

// itemsToKeys extracts just the keys from a map (for backward compatibility)
func itemsToKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
