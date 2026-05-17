package state

import (
	"context"

	"github.com/scitrera/aether/pkg/models"
)

// acquireLegacy adapts the new ConnectMeta/ConnectResult signature to the
// legacy (acquired, resumed, forced, err) tuple. Used by tests written
// against the previous shape so the spec-driven signature change does
// not require a churn-heavy mass rewrite of assertions.
type acquireOrResumer interface {
	AcquireOrResumeLock(ctx context.Context, identity models.Identity, sessionID, resumeSessionID string, forceTakeoverThresholdMs int64, meta ConnectMeta) (ConnectResult, error)
}

func acquireLegacy(reg acquireOrResumer, ctx context.Context, id models.Identity, sessionID, resumeSessionID string, thresholdMs int64) (bool, bool, bool, error) {
	r, err := reg.AcquireOrResumeLock(ctx, id, sessionID, resumeSessionID, thresholdMs, ConnectMeta{})
	return r.Acquired, r.Resumed, r.Forced, err
}
