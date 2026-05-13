// Package aether generic pending request tracker.
//
// pendingRequests is a type-safe wrapper around sync.Map for correlating
// async requests to their responses via a string request ID.

package aether

import "sync"

// pendingRequests tracks in-flight requests keyed by request ID.
// Each request is represented by a buffered channel of size 1.
type pendingRequests[T any] struct {
	m sync.Map
}

// Register stores a new pending request channel keyed by id.
// Returns the channel that will receive exactly one response.
func (p *pendingRequests[T]) Register(id string) chan T {
	ch := make(chan T, 1)
	p.m.Store(id, ch)
	return ch
}

// Resolve delivers resp to the pending request identified by id.
// Returns true if the request was found and the response was delivered.
func (p *pendingRequests[T]) Resolve(id string, resp T) bool {
	val, ok := p.m.LoadAndDelete(id)
	if !ok {
		return false
	}
	ch := val.(chan T)
	ch <- resp
	return true
}

// ResolveFirst delivers resp to the first pending request found.
// Uses CompareAndDelete for safe concurrent access.
// Returns true if any request was resolved.
// This is provided for backward compatibility with servers that do not echo
// a request_id in their responses.
func (p *pendingRequests[T]) ResolveFirst(resp T) bool {
	var resolved bool
	p.m.Range(func(key, value any) bool {
		if p.m.CompareAndDelete(key, value) {
			ch := value.(chan T)
			ch <- resp
			resolved = true
			return false
		}
		return true
	})
	return resolved
}

// Delete removes a pending request without delivering a response.
// Useful for cleanup after a timeout.
// The id parameter may be a string or any key value returned from Range.
func (p *pendingRequests[T]) Delete(id any) {
	p.m.Delete(id)
}

// Range iterates over all pending requests, calling f for each key/value pair.
// The value passed to f is the underlying chan T.
// Iteration stops if f returns false.
// This matches the sync.Map.Range signature for compatibility with test helpers
// that need to inspect or manually drain pending requests.
func (p *pendingRequests[T]) Range(f func(key, value any) bool) {
	p.m.Range(f)
}

// Clear removes all pending requests without delivering responses.
func (p *pendingRequests[T]) Clear() {
	p.m.Range(func(key, _ any) bool {
		p.m.Delete(key)
		return true
	})
}
