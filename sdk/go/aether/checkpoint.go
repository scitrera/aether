// Package aether Checkpoint operations for the Go SDK.
//
// This file provides checkpoint operations for persisting application-specific
// state that needs to survive restarts. Checkpoints allow agents/tasks to save
// and load custom state, separate from message offset tracking (which is handled
// automatically by RabbitMQ Streams).
//
// Checkpoint operations can be performed in two modes:
//   - Async: Fire-and-forget operations where responses are handled by the
//     OnCheckpointResponse handler callback
//   - Sync: Blocking operations that wait for the response with a timeout
//
// Key concepts:
//   - Each identity can have multiple named checkpoints (using the key parameter)
//   - The "default" key is used when no key is specified
//   - Checkpoints can have TTL (time-to-live) for automatic expiration
//   - TTL values: -1 = server default, 0 = no expiration, >0 = specific TTL

package aether

import (
	"context"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Default Timeouts
// =============================================================================

const (
	// DefaultCheckpointTimeout is the default timeout for synchronous checkpoint operations.
	DefaultCheckpointTimeout = 5 * time.Second
)

// =============================================================================
// Checkpoint Operations Interface
// =============================================================================

// Checkpoint provides checkpoint store operations on a client.
//
// Checkpoints allow agents and tasks to persist arbitrary state that survives
// restarts. This is separate from message offset tracking (handled automatically).
// Use checkpoints to save application-specific state like progress markers,
// configuration, or any data that needs to be restored on reconnection.
//
// Each identity can have multiple named checkpoints. The "default" key is used
// when no key is specified.
type Checkpoint struct {
	client *BaseClient
	syncMu sync.Mutex // serializes synchronous checkpoint operations
}

// newCheckpoint creates a new Checkpoint operations helper for a client.
func newCheckpoint(client *BaseClient) *Checkpoint {
	return &Checkpoint{client: client}
}

// =============================================================================
// Async Checkpoint Operations
// =============================================================================

// Save saves checkpoint data (async).
//
// The response is delivered via the OnCheckpointResponse handler callback.
// For synchronous operation, use SaveSync.
//
// Parameters:
//   - data: The checkpoint data to save (bytes)
//   - key: Checkpoint key (empty for "default")
//   - ttl: Time-to-live in seconds (-1 = server default, 0 = no expiration, >0 = specific TTL)
func (cp *Checkpoint) Save(data []byte, key string, ttl int64) error {
	return cp.SaveWithRequestID(data, key, ttl, "")
}

// SaveWithRequestID saves checkpoint data with a specific request ID for correlation.
func (cp *Checkpoint) SaveWithRequestID(data []byte, key string, ttl int64, requestID string) error {
	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_SAVE,
		Key:       key,
		Data:      data,
		Ttl:       ttl,
		RequestId: requestID,
	}

	return cp.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CheckpointOp{
			CheckpointOp: op,
		},
	})
}

// Load requests checkpoint data (async).
//
// The response is delivered via the OnCheckpointResponse handler callback.
// For synchronous operation, use LoadSync.
//
// Parameters:
//   - key: Checkpoint key (empty for "default")
func (cp *Checkpoint) Load(key string) error {
	return cp.LoadWithRequestID(key, "")
}

// LoadWithRequestID loads checkpoint data with a specific request ID for correlation.
func (cp *Checkpoint) LoadWithRequestID(key string, requestID string) error {
	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_LOAD,
		Key:       key,
		RequestId: requestID,
	}

	return cp.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CheckpointOp{
			CheckpointOp: op,
		},
	})
}

// Delete deletes a checkpoint (async).
//
// The response is delivered via the OnCheckpointResponse handler callback.
// For synchronous operation, use DeleteSync.
//
// Parameters:
//   - key: Checkpoint key (empty for "default")
func (cp *Checkpoint) Delete(key string) error {
	return cp.DeleteWithRequestID(key, "")
}

// DeleteWithRequestID deletes checkpoint data with a specific request ID for correlation.
func (cp *Checkpoint) DeleteWithRequestID(key string, requestID string) error {
	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_DELETE,
		Key:       key,
		RequestId: requestID,
	}

	return cp.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CheckpointOp{
			CheckpointOp: op,
		},
	})
}

// List requests the list of checkpoint keys (async).
//
// The response is delivered via the OnCheckpointResponse handler callback.
// For synchronous operation, use ListSync.
func (cp *Checkpoint) List() error {
	return cp.ListWithRequestID("")
}

// ListWithRequestID lists checkpoint keys with a specific request ID for correlation.
func (cp *Checkpoint) ListWithRequestID(requestID string) error {
	op := &pb.CheckpointOperation{
		Op:        pb.CheckpointOperation_LIST,
		RequestId: requestID,
	}

	return cp.client.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_CheckpointOp{
			CheckpointOp: op,
		},
	})
}

// =============================================================================
// Synchronous Checkpoint Operations
// =============================================================================

// SaveSync saves checkpoint data and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// Parameters:
//   - ctx: Context for cancellation
//   - opts: Save operation options
//
// Returns:
//   - CheckpointResponse indicating success or failure
//   - error if the operation fails or times out
func (cp *Checkpoint) SaveSync(ctx context.Context, opts CheckpointSaveOptions) (*CheckpointResponse, error) {
	cp.syncMu.Lock()
	defer cp.syncMu.Unlock()

	// Determine timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultCheckpointTimeout
	}

	// Generate correlation ID and register pending request
	requestID := cp.client.NextRequestID()
	ch := cp.client.RegisterPendingCheckpointRequest(requestID)
	defer cp.client.pendingCheckpointRequests.Delete(requestID)

	// Convert TTL from time.Duration to seconds
	var ttlSeconds int64 = -1 // Default: server default
	if opts.TTL > 0 {
		ttlSeconds = int64(opts.TTL.Seconds())
	} else if opts.TTL == 0 {
		ttlSeconds = 0 // No expiration
	}

	// Send the request with correlation ID
	if err := cp.SaveWithRequestID(opts.Data, opts.Key, ttlSeconds, requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return cp.waitForCorrelatedResponse(ctx, ch, timeout)
}

// LoadSync loads checkpoint data and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// If the checkpoint doesn't exist, response.Success=true but response.Data
// will be empty.
//
// Parameters:
//   - ctx: Context for cancellation
//   - opts: Load operation options
//
// Returns:
//   - CheckpointResponse containing the data if successful
//   - error if the operation fails or times out
func (cp *Checkpoint) LoadSync(ctx context.Context, opts CheckpointLoadOptions) (*CheckpointResponse, error) {
	cp.syncMu.Lock()
	defer cp.syncMu.Unlock()

	// Determine timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultCheckpointTimeout
	}

	// Generate correlation ID and register pending request
	requestID := cp.client.NextRequestID()
	ch := cp.client.RegisterPendingCheckpointRequest(requestID)
	defer cp.client.pendingCheckpointRequests.Delete(requestID)

	// Send the request with correlation ID
	if err := cp.LoadWithRequestID(opts.Key, requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return cp.waitForCorrelatedResponse(ctx, ch, timeout)
}

// DeleteSync deletes a checkpoint and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// Parameters:
//   - ctx: Context for cancellation
//   - opts: Delete operation options
//
// Returns:
//   - CheckpointResponse indicating success or failure
//   - error if the operation fails or times out
func (cp *Checkpoint) DeleteSync(ctx context.Context, opts CheckpointDeleteOptions) (*CheckpointResponse, error) {
	cp.syncMu.Lock()
	defer cp.syncMu.Unlock()

	// Determine timeout
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultCheckpointTimeout
	}

	// Generate correlation ID and register pending request
	requestID := cp.client.NextRequestID()
	ch := cp.client.RegisterPendingCheckpointRequest(requestID)
	defer cp.client.pendingCheckpointRequests.Delete(requestID)

	// Send the request with correlation ID
	if err := cp.DeleteWithRequestID(opts.Key, requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return cp.waitForCorrelatedResponse(ctx, ch, timeout)
}

// ListSync lists all checkpoint keys and waits for the response.
//
// This is a blocking operation that waits for the response with a timeout.
// Returns the response or an error if the operation times out.
//
// Parameters:
//   - ctx: Context for cancellation
//   - timeout: Timeout for the operation (0 uses default)
//
// Returns:
//   - CheckpointResponse containing the list of keys
//   - error if the operation fails or times out
func (cp *Checkpoint) ListSync(ctx context.Context, timeout time.Duration) (*CheckpointResponse, error) {
	cp.syncMu.Lock()
	defer cp.syncMu.Unlock()

	// Determine timeout
	if timeout == 0 {
		timeout = DefaultCheckpointTimeout
	}

	// Generate correlation ID and register pending request
	requestID := cp.client.NextRequestID()
	ch := cp.client.RegisterPendingCheckpointRequest(requestID)
	defer cp.client.pendingCheckpointRequests.Delete(requestID)

	// Send the request with correlation ID
	if err := cp.ListWithRequestID(requestID); err != nil {
		return nil, err
	}

	// Wait for correlated response with timeout
	return cp.waitForCorrelatedResponse(ctx, ch, timeout)
}

// =============================================================================
// Helper Methods
// =============================================================================

// drainResponseQueue clears any pending responses from the queue.
func (cp *Checkpoint) drainResponseQueue() {
	queue := cp.client.CheckpointResponseQueue()
	for {
		select {
		case <-queue:
			// Drain the queue
		default:
			return
		}
	}
}

// waitForResponse waits for a checkpoint response with timeout (legacy queue-based).
func (cp *Checkpoint) waitForResponse(ctx context.Context, timeout time.Duration) (*CheckpointResponse, error) {
	// Create a timer for the timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	queue := cp.client.CheckpointResponseQueue()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("checkpoint operation timed out", timeout.Seconds())
	case resp := <-queue:
		return resp, nil
	}
}

// waitForCorrelatedResponse waits for a checkpoint response on a correlated channel with timeout.
func (cp *Checkpoint) waitForCorrelatedResponse(ctx context.Context, ch chan *CheckpointResponse, timeout time.Duration) (*CheckpointResponse, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("checkpoint operation timed out", timeout.Seconds())
	case resp := <-ch:
		return resp, nil
	}
}

// =============================================================================
// Convenience Methods (Simpler API)
// =============================================================================

// SaveDefault saves data to the default checkpoint key (async).
func (cp *Checkpoint) SaveDefault(data []byte) error {
	return cp.Save(data, "", -1)
}

// LoadDefault loads data from the default checkpoint key (async).
func (cp *Checkpoint) LoadDefault() error {
	return cp.Load("")
}

// DeleteDefault deletes the default checkpoint (async).
func (cp *Checkpoint) DeleteDefault() error {
	return cp.Delete("")
}

// SaveWithTTL saves checkpoint data with a specific TTL (async).
//
// Parameters:
//   - data: The checkpoint data to save
//   - key: Checkpoint key (empty for "default")
//   - ttl: Time-to-live duration (converted to seconds)
func (cp *Checkpoint) SaveWithTTL(data []byte, key string, ttl time.Duration) error {
	var ttlSeconds int64
	if ttl > 0 {
		ttlSeconds = int64(ttl.Seconds())
	}
	return cp.Save(data, key, ttlSeconds)
}

// SavePermanent saves checkpoint data with no expiration (async).
//
// Parameters:
//   - data: The checkpoint data to save
//   - key: Checkpoint key (empty for "default")
func (cp *Checkpoint) SavePermanent(data []byte, key string) error {
	return cp.Save(data, key, 0)
}

// =============================================================================
// BaseClient Extension
// =============================================================================

// Checkpoint returns the Checkpoint operations helper for this client.
//
// Use this to perform checkpoint operations:
//
//	// Async (fire-and-forget)
//	client.Checkpoint().Save([]byte("state data"), "my-checkpoint", -1)
//
//	// Sync (blocking)
//	resp, err := client.Checkpoint().LoadSync(ctx, aether.CheckpointLoadOptions{
//	    Key: "my-checkpoint",
//	})
func (c *BaseClient) Checkpoint() *Checkpoint {
	c.cpOnce.Do(func() {
		c.cpInstance = newCheckpoint(c)
	})
	return c.cpInstance
}

// =============================================================================
// Direct Checkpoint Methods on BaseClient (Python API Compatibility)
// =============================================================================

// CheckpointSave saves checkpoint data (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.Checkpoint().Save() or client.Checkpoint().SaveSync().
//
// Parameters:
//   - data: The checkpoint data to save (bytes)
//   - key: Checkpoint key (empty for "default")
//   - ttl: Time-to-live in seconds (-1 = server default, 0 = no expiration, >0 = specific TTL)
func (c *BaseClient) CheckpointSave(data []byte, key string, ttl int64) error {
	return c.Checkpoint().Save(data, key, ttl)
}

// CheckpointLoad requests checkpoint data (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.Checkpoint().Load() or client.Checkpoint().LoadSync().
//
// Parameters:
//   - key: Checkpoint key (empty for "default")
func (c *BaseClient) CheckpointLoad(key string) error {
	return c.Checkpoint().Load(key)
}

// CheckpointDelete deletes a checkpoint (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.Checkpoint().Delete() or client.Checkpoint().DeleteSync().
//
// Parameters:
//   - key: Checkpoint key (empty for "default")
func (c *BaseClient) CheckpointDelete(key string) error {
	return c.Checkpoint().Delete(key)
}

// CheckpointList requests the list of checkpoint keys (async).
//
// This is a convenience method that matches the Python client API.
// For more options, use client.Checkpoint().List() or client.Checkpoint().ListSync().
func (c *BaseClient) CheckpointList() error {
	return c.Checkpoint().List()
}

// CheckpointSaveSync saves checkpoint data and waits for the response.
//
// This is a convenience method for synchronous checkpoint save operations.
// For async operations, use client.Checkpoint().Save().
//
// Parameters:
//   - ctx: Context for cancellation
//   - data: The checkpoint data to save
//   - key: Checkpoint key (empty for "default")
//   - ttl: Time-to-live duration (converted to seconds)
//   - timeout: Timeout for the operation (0 uses default)
func (c *BaseClient) CheckpointSaveSync(ctx context.Context, data []byte, key string, ttl time.Duration, timeout time.Duration) (*CheckpointResponse, error) {
	return c.Checkpoint().SaveSync(ctx, CheckpointSaveOptions{
		Data:    data,
		Key:     key,
		TTL:     ttl,
		Timeout: timeout,
	})
}

// CheckpointLoadSync loads checkpoint data and waits for the response.
//
// This is a convenience method for synchronous checkpoint load operations.
// For async operations, use client.Checkpoint().Load().
//
// Parameters:
//   - ctx: Context for cancellation
//   - key: Checkpoint key (empty for "default")
//   - timeout: Timeout for the operation (0 uses default)
func (c *BaseClient) CheckpointLoadSync(ctx context.Context, key string, timeout time.Duration) (*CheckpointResponse, error) {
	return c.Checkpoint().LoadSync(ctx, CheckpointLoadOptions{
		Key:     key,
		Timeout: timeout,
	})
}

// CheckpointDeleteSync deletes a checkpoint and waits for the response.
//
// This is a convenience method for synchronous checkpoint delete operations.
// For async operations, use client.Checkpoint().Delete().
//
// Parameters:
//   - ctx: Context for cancellation
//   - key: Checkpoint key (empty for "default")
//   - timeout: Timeout for the operation (0 uses default)
func (c *BaseClient) CheckpointDeleteSync(ctx context.Context, key string, timeout time.Duration) (*CheckpointResponse, error) {
	return c.Checkpoint().DeleteSync(ctx, CheckpointDeleteOptions{
		Key:     key,
		Timeout: timeout,
	})
}

// CheckpointListSync lists all checkpoint keys and waits for the response.
//
// This is a convenience method for synchronous checkpoint list operations.
// For async operations, use client.Checkpoint().List().
//
// Parameters:
//   - ctx: Context for cancellation
//   - timeout: Timeout for the operation (0 uses default)
func (c *BaseClient) CheckpointListSync(ctx context.Context, timeout time.Duration) (*CheckpointResponse, error) {
	return c.Checkpoint().ListSync(ctx, timeout)
}
