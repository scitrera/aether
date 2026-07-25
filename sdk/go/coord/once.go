package coord

import (
	"context"
	"time"
)

// Once provides cluster-wide run-exactly-once semantics for a named action,
// backed by an atomic set-if-absent on a marker key. The first caller to claim
// the key runs fn; concurrent and later callers observe the claim and skip.
//
// On fn error the marker is released so the action can be retried by a
// subsequent caller (the failed run does not "consume" the once). On success
// the marker persists (subject to the optional TTL) so the action is not
// repeated.
type Once struct {
	locker Locker
	key    string
	owner  string
	ttl    time.Duration // 0 = marker never expires
}

// OnceOption customizes a Once.
type OnceOption func(*Once)

// WithOnceTTL bounds how long the completion marker persists. After it expires
// the action becomes runnable again. Zero (default) means the marker is
// permanent. Use a TTL for periodic "once per window" semantics.
func WithOnceTTL(ttl time.Duration) OnceOption {
	return func(o *Once) { o.ttl = ttl }
}

// NewOnce creates a Once guarding the action identified by key.
func NewOnce(locker Locker, key string, opts ...OnceOption) *Once {
	o := &Once{locker: locker, key: key, owner: NewOwnerID()}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Do runs fn exactly once cluster-wide for this key. It returns ran=true iff
// this caller won the claim and executed fn (in which case err is fn's error).
// ran=false means another caller already claimed the action; err is then any
// backend error from attempting the claim (nil on a normal skip).
func (o *Once) Do(ctx context.Context, fn func(ctx context.Context) error) (ran bool, err error) {
	claimed, err := o.locker.TryAcquire(ctx, o.key, o.owner, o.ttl)
	if err != nil {
		return false, err
	}
	if !claimed {
		return false, nil
	}
	if fnErr := fn(ctx); fnErr != nil {
		// Release the marker so the action can be retried by someone else.
		// Best-effort: if release fails the marker's TTL (when set) still
		// eventually frees it.
		_, _ = o.locker.Release(context.WithoutCancel(ctx), o.key, o.owner)
		return true, fnErr
	}
	return true, nil
}
