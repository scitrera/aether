// Package aether KV operations for the Go SDK.
//
// This file provides KV (key-value) store operations with support for
// all scoped namespaces: global, workspace, user, and user-workspace.
//
// KV operations can be performed in two modes:
//   - Async: Fire-and-forget operations where responses are handled by the
//     OnKVResponse handler callback
//   - Sync: Blocking operations that wait for the response with a timeout
//
// Scope descriptions:
//   - Global: Accessible to all entities across all workspaces
//   - Workspace: Accessible within a specific workspace
//   - User: Accessible to a specific user across all workspaces
//   - UserWorkspace: Accessible to a specific user within a specific workspace

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Scope Conversion
// =============================================================================

// kvScopeToProto converts a KVScope string type to the proto enum value.
func kvScopeToProto(scope KVScope) pb.KVOperation_Scope {
	switch scope {
	case KVScopeGlobal:
		return pb.KVOperation_GLOBAL
	case KVScopeWorkspace:
		return pb.KVOperation_WORKSPACE
	case KVScopeUser:
		return pb.KVOperation_USER
	case KVScopeUserWorkspace:
		return pb.KVOperation_USER_WORKSPACE
	case KVScopeGlobalExclusive:
		return pb.KVOperation_GLOBAL_EXCLUSIVE
	case KVScopeWorkspaceExclusive:
		return pb.KVOperation_WORKSPACE_EXCLUSIVE
	case KVScopeUserShared:
		return pb.KVOperation_USER_SHARED
	case KVScopeUserWorkspaceShared:
		return pb.KVOperation_USER_WORKSPACE_SHARED
	default:
		return pb.KVOperation_SCOPE_UNSPECIFIED
	}
}

// =============================================================================
// Default Timeouts
// =============================================================================

const (
	// DefaultKVTimeout is the default timeout for synchronous KV operations.
	DefaultKVTimeout = 5 * time.Second
)

// =============================================================================
// KV Operations Interface
// =============================================================================

// KV provides KV store operations on a client.
//
// All operations support the following scopes:
//   - KVScopeGlobal: Accessible to all entities
//   - KVScopeWorkspace: Accessible within a workspace
//   - KVScopeUser: Accessible to a specific user
//   - KVScopeUserWorkspace: Accessible to a user within a workspace
//
// For workspace and user-workspace scopes, the workspace can be omitted
// to use the client's current workspace (for clients that have one).
type KV struct {
	client *BaseClient
	syncMu sync.Mutex // serializes synchronous KV operations
}

// newKV creates a new KV operations helper for a client.
func newKV(client *BaseClient) *KV {
	return &KV{client: client}
}

// =============================================================================
// Async KV Operations
// =============================================================================

// Get retrieves a value from the KV store (async).
//
// The response is delivered via the OnKVResponse handler callback.
// For synchronous operation, use GetSync.
//
// Parameters:
//   - key: The key to retrieve
//   - scope: The KV scope (default: KVScopeGlobal)
//   - userID: Required for KVScopeUser and KVScopeUserWorkspace
//   - workspace: Required for KVScopeWorkspace and KVScopeUserWorkspace
func (kv *KV) Get(key string, scope KVScope, userID, workspace string) error {
	return kv.GetWithRequestID(key, scope, userID, workspace, "")
}

// GetWithRequestID retrieves a value from the KV store with a specific request ID for correlation.
func (kv *KV) GetWithRequestID(key string, scope KVScope, userID, workspace, requestID string) error {
	if scope == "" {
		scope = KVScopeGlobal
	}

	op := &pb.KVOperation{
		Op:        pb.KVOperation_GET,
		Scope:     kvScopeToProto(scope),
		Key:       key,
		UserId:    userID,
		Workspace: workspace,
		RequestId: requestID,
	}

	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{
			KvOp: op,
		},
	})
}

// Put stores a value in the KV store (async).
//
// The response is delivered via the OnKVResponse handler callback.
// For synchronous operation, use PutSync.
//
// Parameters:
//   - key: The key to store
//   - value: The value to store (bytes)
//   - scope: The KV scope (default: KVScopeGlobal)
//   - userID: Required for KVScopeUser and KVScopeUserWorkspace
//   - workspace: Required for KVScopeWorkspace and KVScopeUserWorkspace
//   - ttl: Time-to-live in seconds (0 = no expiration)
func (kv *KV) Put(key string, value []byte, scope KVScope, userID, workspace string, ttl int64) error {
	return kv.PutWithRequestID(key, value, scope, userID, workspace, ttl, "")
}

// PutWithRequestID stores a value in the KV store with a specific request ID for correlation.
func (kv *KV) PutWithRequestID(key string, value []byte, scope KVScope, userID, workspace string, ttl int64, requestID string) error {
	if scope == "" {
		scope = KVScopeGlobal
	}

	op := &pb.KVOperation{
		Op:        pb.KVOperation_PUT,
		Scope:     kvScopeToProto(scope),
		Key:       key,
		Value:     value,
		UserId:    userID,
		Workspace: workspace,
		Ttl:       ttl,
		RequestId: requestID,
	}

	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{
			KvOp: op,
		},
	})
}

// List retrieves keys from the KV store matching a prefix (async).
//
// The response is delivered via the OnKVResponse handler callback.
// For synchronous operation, use ListSync.
//
// Parameters:
//   - keyPrefix: Prefix to filter keys (empty for all keys in scope)
//   - scope: The KV scope (default: KVScopeGlobal)
//   - userID: Required for KVScopeUser and KVScopeUserWorkspace
//   - workspace: Required for KVScopeWorkspace and KVScopeUserWorkspace
func (kv *KV) List(keyPrefix string, scope KVScope, userID, workspace string) error {
	return kv.ListWithRequestID(keyPrefix, scope, userID, workspace, "")
}

// ListWithRequestID lists keys with a specific request ID for correlation.
func (kv *KV) ListWithRequestID(keyPrefix string, scope KVScope, userID, workspace, requestID string) error {
	if scope == "" {
		scope = KVScopeGlobal
	}

	op := &pb.KVOperation{
		Op:        pb.KVOperation_LIST,
		Scope:     kvScopeToProto(scope),
		Key:       keyPrefix,
		UserId:    userID,
		Workspace: workspace,
		RequestId: requestID,
	}

	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{
			KvOp: op,
		},
	})
}

// Delete removes a key from the KV store (async).
//
// The response is delivered via the OnKVResponse handler callback.
// For synchronous operation, use DeleteSync.
//
// Parameters:
//   - key: The key to delete
//   - scope: The KV scope (default: KVScopeGlobal)
//   - userID: Required for KVScopeUser and KVScopeUserWorkspace
//   - workspace: Required for KVScopeWorkspace and KVScopeUserWorkspace
func (kv *KV) Delete(key string, scope KVScope, userID, workspace string) error {
	return kv.DeleteWithRequestID(key, scope, userID, workspace, "")
}

// DeleteWithRequestID removes a key with a specific request ID for correlation.
func (kv *KV) DeleteWithRequestID(key string, scope KVScope, userID, workspace, requestID string) error {
	if scope == "" {
		scope = KVScopeGlobal
	}

	op := &pb.KVOperation{
		Op:        pb.KVOperation_DELETE,
		Scope:     kvScopeToProto(scope),
		Key:       key,
		UserId:    userID,
		Workspace: workspace,
		RequestId: requestID,
	}

	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{
			KvOp: op,
		},
	})
}

// Increment atomically increments a counter in the KV store (async).
//
// The response is delivered via the OnKVResponse handler callback.
// The CounterValue field of the KVResponse contains the resulting value.
func (kv *KV) Increment(key string, scope KVScope, userID, workspace string) error {
	return kv.IncrementWithRequestID(key, scope, userID, workspace, "")
}

// IncrementWithRequestID atomically increments a counter with a specific request ID for correlation.
func (kv *KV) IncrementWithRequestID(key string, scope KVScope, userID, workspace, requestID string) error {
	if scope == "" {
		scope = KVScopeGlobal
	}
	op := &pb.KVOperation{
		Op:        pb.KVOperation_INCREMENT,
		Scope:     kvScopeToProto(scope),
		Key:       key,
		UserId:    userID,
		Workspace: workspace,
		RequestId: requestID,
	}
	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{KvOp: op},
	})
}

// Decrement atomically decrements a counter in the KV store (async).
//
// The response is delivered via the OnKVResponse handler callback.
// The CounterValue field of the KVResponse contains the resulting value.
func (kv *KV) Decrement(key string, scope KVScope, userID, workspace string) error {
	return kv.DecrementWithRequestID(key, scope, userID, workspace, "")
}

// DecrementWithRequestID atomically decrements a counter with a specific request ID for correlation.
func (kv *KV) DecrementWithRequestID(key string, scope KVScope, userID, workspace, requestID string) error {
	if scope == "" {
		scope = KVScopeGlobal
	}
	op := &pb.KVOperation{
		Op:        pb.KVOperation_DECREMENT,
		Scope:     kvScopeToProto(scope),
		Key:       key,
		UserId:    userID,
		Workspace: workspace,
		RequestId: requestID,
	}
	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{KvOp: op},
	})
}

// IncrementSync atomically increments a counter and waits for the response.
func (kv *KV) IncrementSync(ctx context.Context, key string, scope KVScope, userID, workspace string, timeout time.Duration) (*KVResponse, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultKVTimeout
	}
	if scope == "" {
		scope = KVScopeGlobal
	}

	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID)

	if err := kv.IncrementWithRequestID(key, scope, userID, workspace, requestID); err != nil {
		return nil, err
	}
	return kv.waitForCorrelatedResponse(ctx, ch, timeout)
}

// DecrementSync atomically decrements a counter and waits for the response.
func (kv *KV) DecrementSync(ctx context.Context, key string, scope KVScope, userID, workspace string, timeout time.Duration) (*KVResponse, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultKVTimeout
	}
	if scope == "" {
		scope = KVScopeGlobal
	}

	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID)

	if err := kv.DecrementWithRequestID(key, scope, userID, workspace, requestID); err != nil {
		return nil, err
	}
	return kv.waitForCorrelatedResponse(ctx, ch, timeout)
}

// IncrementGlobal atomically increments a counter in the global scope (async).
func (kv *KV) IncrementGlobal(key string) error {
	return kv.Increment(key, KVScopeGlobal, "", "")
}

// DecrementGlobal atomically decrements a counter in the global scope (async).
func (kv *KV) DecrementGlobal(key string) error {
	return kv.Decrement(key, KVScopeGlobal, "", "")
}

// IncrementIf atomically increments a counter only if the result would not exceed ceiling (async).
// delta specifies the increment amount; pass 0 for server default (1).
func (kv *KV) IncrementIf(key string, scope KVScope, userID, workspace string, delta, ceiling int64) error {
	if scope == "" {
		scope = KVScopeGlobal
	}
	op := &pb.KVOperation{
		Op:         pb.KVOperation_INCREMENT_IF,
		Scope:      kvScopeToProto(scope),
		Key:        key,
		UserId:     userID,
		Workspace:  workspace,
		DeltaValue: delta,
		GuardValue: ceiling,
	}
	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{KvOp: op},
	})
}

// DecrementIf atomically decrements a counter only if the result would not go below floor (async).
// delta specifies the decrement amount; pass 0 for server default (1).
func (kv *KV) DecrementIf(key string, scope KVScope, userID, workspace string, delta, floor int64) error {
	if scope == "" {
		scope = KVScopeGlobal
	}
	op := &pb.KVOperation{
		Op:         pb.KVOperation_DECREMENT_IF,
		Scope:      kvScopeToProto(scope),
		Key:        key,
		UserId:     userID,
		Workspace:  workspace,
		DeltaValue: delta,
		GuardValue: floor,
	}
	return kv.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_KvOp{KvOp: op},
	})
}

// IncrementIfSync atomically increments if result <= ceiling; returns (newValue, applied, error).
// delta specifies the increment amount; pass 0 for server default (1).
func (kv *KV) IncrementIfSync(ctx context.Context, key string, scope KVScope, userID, workspace string, delta, ceiling int64, timeout time.Duration) (int64, bool, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultKVTimeout
	}
	if scope == "" {
		scope = KVScopeGlobal
	}

	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID)

	op := &pb.KVOperation{
		Op:         pb.KVOperation_INCREMENT_IF,
		Scope:      kvScopeToProto(scope),
		Key:        key,
		UserId:     userID,
		Workspace:  workspace,
		DeltaValue: delta,
		GuardValue: ceiling,
		RequestId:  requestID,
	}
	if err := kv.client.Send(&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_KvOp{KvOp: op}}); err != nil {
		return 0, false, err
	}
	resp, err := kv.waitForCorrelatedResponse(ctx, ch, timeout)
	if err != nil {
		return 0, false, err
	}
	return resp.CounterValue, resp.Applied, nil
}

// DecrementIfSync atomically decrements if result >= floor; returns (newValue, applied, error).
// delta specifies the decrement amount; pass 0 for server default (1).
func (kv *KV) DecrementIfSync(ctx context.Context, key string, scope KVScope, userID, workspace string, delta, floor int64, timeout time.Duration) (int64, bool, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	if timeout == 0 {
		timeout = DefaultKVTimeout
	}
	if scope == "" {
		scope = KVScopeGlobal
	}

	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID)

	op := &pb.KVOperation{
		Op:         pb.KVOperation_DECREMENT_IF,
		Scope:      kvScopeToProto(scope),
		Key:        key,
		UserId:     userID,
		Workspace:  workspace,
		DeltaValue: delta,
		GuardValue: floor,
		RequestId:  requestID,
	}
	if err := kv.client.Send(&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_KvOp{KvOp: op}}); err != nil {
		return 0, false, err
	}
	resp, err := kv.waitForCorrelatedResponse(ctx, ch, timeout)
	if err != nil {
		return 0, false, err
	}
	return resp.CounterValue, resp.Applied, nil
}

// =============================================================================
// Synchronous KV Operations
// =============================================================================

// GetSync retrieves a value from the KV store and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// Parameters:
//   - ctx: Context for cancellation
//   - opts: Get operation options
//
// Returns:
//   - KVResponse containing the value if successful
//   - error if the operation fails or times out
func (kv *KV) GetSync(ctx context.Context, opts KVGetOptions) (*KVResponse, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	// Determine timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultKVTimeout
	}

	// Generate correlation ID and register pending request
	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID) // cleanup on timeout/cancel

	// Send the request with correlation ID
	scope := opts.Scope
	if scope == "" {
		scope = KVScopeGlobal
	}

	if err := kv.GetWithRequestID(opts.Key, scope, opts.UserID, opts.Workspace, requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return kv.waitForCorrelatedResponse(ctx, ch, timeout)
}

// PutSync stores a value in the KV store and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// Parameters:
//   - ctx: Context for cancellation
//   - opts: Put operation options
//
// Returns:
//   - KVResponse indicating success or failure
//   - error if the operation fails or times out
func (kv *KV) PutSync(ctx context.Context, opts KVPutOptions) (*KVResponse, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	// Determine timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultKVTimeout
	}

	// Generate correlation ID and register pending request
	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID)

	// Send the request with correlation ID
	scope := opts.Scope
	if scope == "" {
		scope = KVScopeGlobal
	}

	// Convert TTL from time.Duration to seconds
	var ttlSeconds int64
	if opts.TTL > 0 {
		ttlSeconds = int64(opts.TTL.Seconds())
	}

	if err := kv.PutWithRequestID(opts.Key, opts.Value, scope, opts.UserID, opts.Workspace, ttlSeconds, requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return kv.waitForCorrelatedResponse(ctx, ch, timeout)
}

// ListSync retrieves keys from the KV store and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// Parameters:
//   - ctx: Context for cancellation
//   - opts: List operation options
//
// Returns:
//   - KVResponse containing the keys (and optionally values) if successful
//   - error if the operation fails or times out
func (kv *KV) ListSync(ctx context.Context, opts KVListOptions) (*KVResponse, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	// Determine timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultKVTimeout
	}

	// Generate correlation ID and register pending request
	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID)

	// Send the request with correlation ID
	scope := opts.Scope
	if scope == "" {
		scope = KVScopeGlobal
	}

	if err := kv.ListWithRequestID(opts.KeyPrefix, scope, opts.UserID, opts.Workspace, requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return kv.waitForCorrelatedResponse(ctx, ch, timeout)
}

// DeleteSync removes a key from the KV store and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// Parameters:
//   - ctx: Context for cancellation
//   - opts: Delete operation options
//
// Returns:
//   - KVResponse indicating success or failure
//   - error if the operation fails or times out
func (kv *KV) DeleteSync(ctx context.Context, opts KVDeleteOptions) (*KVResponse, error) {
	kv.syncMu.Lock()
	defer kv.syncMu.Unlock()

	// Determine timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultKVTimeout
	}

	// Generate correlation ID and register pending request
	requestID := kv.client.NextRequestID()
	ch := kv.client.RegisterPendingKVRequest(requestID)
	defer kv.client.pendingKVRequests.Delete(requestID)

	// Send the request with correlation ID
	scope := opts.Scope
	if scope == "" {
		scope = KVScopeGlobal
	}

	if err := kv.DeleteWithRequestID(opts.Key, scope, opts.UserID, opts.Workspace, requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return kv.waitForCorrelatedResponse(ctx, ch, timeout)
}

// =============================================================================
// Helper Methods
// =============================================================================

// drainResponseQueue clears any pending responses from the queue.
func (kv *KV) drainResponseQueue() {
	queue := kv.client.KVResponseQueue()
	for {
		select {
		case <-queue:
			// Drain the queue
		default:
			return
		}
	}
}

// waitForResponse waits for a KV response with timeout (legacy queue-based).
func (kv *KV) waitForResponse(ctx context.Context, timeout time.Duration) (*KVResponse, error) {
	// Create a timer for the timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	queue := kv.client.KVResponseQueue()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("KV operation timed out", timeout.Seconds())
	case resp := <-queue:
		return resp, nil
	}
}

// waitForCorrelatedResponse waits for a KV response on a correlated channel with timeout.
func (kv *KV) waitForCorrelatedResponse(ctx context.Context, ch chan *KVResponse, timeout time.Duration) (*KVResponse, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("KV operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// =============================================================================
// Convenience Methods (Simpler API)
// =============================================================================

// GetGlobal retrieves a value from the global scope (async).
func (kv *KV) GetGlobal(key string) error {
	return kv.Get(key, KVScopeGlobal, "", "")
}

// PutGlobal stores a value in the global scope (async).
func (kv *KV) PutGlobal(key string, value []byte) error {
	return kv.Put(key, value, KVScopeGlobal, "", "", 0)
}

// DeleteGlobal removes a key from the global scope (async).
func (kv *KV) DeleteGlobal(key string) error {
	return kv.Delete(key, KVScopeGlobal, "", "")
}

// ListGlobal lists keys from the global scope (async).
func (kv *KV) ListGlobal(keyPrefix string) error {
	return kv.List(keyPrefix, KVScopeGlobal, "", "")
}

// GetWorkspace retrieves a value from a workspace scope (async).
func (kv *KV) GetWorkspace(key, workspace string) error {
	return kv.Get(key, KVScopeWorkspace, "", workspace)
}

// PutWorkspace stores a value in a workspace scope (async).
func (kv *KV) PutWorkspace(key string, value []byte, workspace string) error {
	return kv.Put(key, value, KVScopeWorkspace, "", workspace, 0)
}

// DeleteWorkspace removes a key from a workspace scope (async).
func (kv *KV) DeleteWorkspace(key, workspace string) error {
	return kv.Delete(key, KVScopeWorkspace, "", workspace)
}

// ListWorkspace lists keys from a workspace scope (async).
func (kv *KV) ListWorkspace(keyPrefix, workspace string) error {
	return kv.List(keyPrefix, KVScopeWorkspace, "", workspace)
}

// GetUser retrieves a value from a user scope (async).
func (kv *KV) GetUser(key, userID string) error {
	return kv.Get(key, KVScopeUser, userID, "")
}

// PutUser stores a value in a user scope (async).
func (kv *KV) PutUser(key string, value []byte, userID string) error {
	return kv.Put(key, value, KVScopeUser, userID, "", 0)
}

// DeleteUser removes a key from a user scope (async).
func (kv *KV) DeleteUser(key, userID string) error {
	return kv.Delete(key, KVScopeUser, userID, "")
}

// ListUser lists keys from a user scope (async).
func (kv *KV) ListUser(keyPrefix, userID string) error {
	return kv.List(keyPrefix, KVScopeUser, userID, "")
}

// GetUserWorkspace retrieves a value from a user-workspace scope (async).
func (kv *KV) GetUserWorkspace(key, userID, workspace string) error {
	return kv.Get(key, KVScopeUserWorkspace, userID, workspace)
}

// PutUserWorkspace stores a value in a user-workspace scope (async).
func (kv *KV) PutUserWorkspace(key string, value []byte, userID, workspace string) error {
	return kv.Put(key, value, KVScopeUserWorkspace, userID, workspace, 0)
}

// DeleteUserWorkspace removes a key from a user-workspace scope (async).
func (kv *KV) DeleteUserWorkspace(key, userID, workspace string) error {
	return kv.Delete(key, KVScopeUserWorkspace, userID, workspace)
}

// ListUserWorkspace lists keys from a user-workspace scope (async).
func (kv *KV) ListUserWorkspace(keyPrefix, userID, workspace string) error {
	return kv.List(keyPrefix, KVScopeUserWorkspace, userID, workspace)
}

// =============================================================================
// BaseClient Extension
// =============================================================================

// KV returns the KV operations helper for this client.
//
// Use this to perform KV store operations:
//
//	// Async (fire-and-forget)
//	client.KV().Put("key", []byte("value"), aether.KVScopeWorkspace, "", "my-workspace", 0)
//
//	// Sync (blocking)
//	resp, err := client.KV().GetSync(ctx, aether.KVGetOptions{
//	    Key:       "key",
//	    Scope:     aether.KVScopeWorkspace,
//	    Workspace: "my-workspace",
//	})
func (c *BaseClient) KV() *KV {
	c.kvOnce.Do(func() {
		c.kvInstance = newKV(c)
	})
	return c.kvInstance
}

// =============================================================================
// Direct KV Methods on BaseClient (Python API Compatibility)
// =============================================================================

// KVGet retrieves a value from the KV store (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.KV().Get() or client.KV().GetSync().
func (c *BaseClient) KVGet(key string, scope KVScope, userID, workspace string) error {
	return c.KV().Get(key, scope, userID, workspace)
}

// KVPut stores a value in the KV store (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.KV().Put() or client.KV().PutSync().
func (c *BaseClient) KVPut(key string, value []byte, scope KVScope, userID, workspace string, ttl int64) error {
	return c.KV().Put(key, value, scope, userID, workspace, ttl)
}

// KVList lists keys from the KV store (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.KV().List() or client.KV().ListSync().
func (c *BaseClient) KVList(keyPrefix string, scope KVScope, userID, workspace string) error {
	return c.KV().List(keyPrefix, scope, userID, workspace)
}

// KVDelete removes a key from the KV store (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.KV().Delete() or client.KV().DeleteSync().
func (c *BaseClient) KVDelete(key string, scope KVScope, userID, workspace string) error {
	return c.KV().Delete(key, scope, userID, workspace)
}

// KVGetSync retrieves a value from the KV store and waits for the response.
//
// This is a convenience method for synchronous KV get operations.
// For async operations, use client.KV().Get().
func (c *BaseClient) KVGetSync(ctx context.Context, key string, scope KVScope, userID, workspace string, timeout time.Duration) (*KVResponse, error) {
	return c.KV().GetSync(ctx, KVGetOptions{
		Key:       key,
		Scope:     scope,
		UserID:    userID,
		Workspace: workspace,
		Timeout:   timeout,
	})
}

// KVPutSync stores a value in the KV store and waits for the response.
//
// This is a convenience method for synchronous KV put operations.
// For async operations, use client.KV().Put().
func (c *BaseClient) KVPutSync(ctx context.Context, key string, value []byte, scope KVScope, userID, workspace string, ttl time.Duration, timeout time.Duration) (*KVResponse, error) {
	return c.KV().PutSync(ctx, KVPutOptions{
		Key:       key,
		Value:     value,
		Scope:     scope,
		UserID:    userID,
		Workspace: workspace,
		TTL:       ttl,
		Timeout:   timeout,
	})
}

// KVListSync lists keys from the KV store and waits for the response.
//
// This is a convenience method for synchronous KV list operations.
// For async operations, use client.KV().List().
func (c *BaseClient) KVListSync(ctx context.Context, keyPrefix string, scope KVScope, userID, workspace string, timeout time.Duration) (*KVResponse, error) {
	return c.KV().ListSync(ctx, KVListOptions{
		KeyPrefix: keyPrefix,
		Scope:     scope,
		UserID:    userID,
		Workspace: workspace,
		Timeout:   timeout,
	})
}

// KVDeleteSync removes a key from the KV store and waits for the response.
//
// This is a convenience method for synchronous KV delete operations.
// For async operations, use client.KV().Delete().
func (c *BaseClient) KVDeleteSync(ctx context.Context, key string, scope KVScope, userID, workspace string, timeout time.Duration) (*KVResponse, error) {
	return c.KV().DeleteSync(ctx, KVDeleteOptions{
		Key:       key,
		Scope:     scope,
		UserID:    userID,
		Workspace: workspace,
		Timeout:   timeout,
	})
}
