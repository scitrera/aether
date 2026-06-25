package state

// SPIKE (feasibility): sandbox-provider tenant-relay redesign ("Direction A").
//
// Goal of this file: prove that Aether session resume is keyed on the PRINCIPAL
// IDENTITY (lock:<identity>), independent of the TCP peer / cert / connection, in
// EVERY session backend Aether ships. That is the property the two-hop relay
// tunnel depends on: when a tenant-relay restarts (or a different provider replica
// reconnects), a fresh connection presenting the same identity + resume_session_id
// must resume the existing session rather than be rejected or fork a new one.
//
// All three backends key the lock by identity.String():
//   - Redis (full):        lock:<identity>            (session.go)
//   - Badger (lite/embed): lockKey(identity.String()) (badger_session.go)
//   - JetStream (lite/NATS):encodeKVKey(identity.String()) (jetstream_session.go)
//
// This test exercises the real AcquireOrResumeLock of each against in-process
// backends (miniredis, embedded badger, embedded NATS/JetStream).

import (
	"context"
	"testing"

	"github.com/scitrera/aether/pkg/models"
)

// resumeLocker is the slice of the session-registry surface the relay design
// relies on. All three production registries implement it.
type resumeLocker interface {
	AcquireOrResumeLock(ctx context.Context, identity models.Identity, sessionID, resumeSessionID string, forceTakeoverThresholdMs int64, meta ConnectMeta) (ConnectResult, error)
}

func TestSpike_SessionResumeIsIdentityKeyedAcrossBackends(t *testing.T) {
	backends := []struct {
		name string
		make func(t *testing.T) resumeLocker
	}{
		{"badger_embedded_lite", func(t *testing.T) resumeLocker { return newBadgerSessionRegistry(t) }},
		{"redis_full", func(t *testing.T) resumeLocker { reg, _ := newTestSessionRegistry(t); return reg }},
		{"jetstream_nats_lite", func(t *testing.T) resumeLocker { return newTestJetStreamSession(t) }},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			reg := b.make(t)

			// The identity the tenant-relay registers on behalf of the remote
			// provider. Within a tenant gateway this is THE sandbox-provider
			// service principal; tenant-binding comes from which CA signed the
			// relay's cert + which gateway it dialed, not from this specifier.
			id := models.Identity{
				Type:           models.PrincipalService,
				Implementation: "sandbox-provider",
				Specifier:      "pod-7",
			}
			const session = "sess-A"
			const forceThresholdMs = int64(50) // fresh locks have multi-second TTL

			// Connection #1: relay instance A dials the gateway (fresh acquire).
			r1, err := reg.AcquireOrResumeLock(ctx, id, session, "", forceThresholdMs, ConnectMeta{})
			if err != nil {
				t.Fatalf("acquire #1: %v", err)
			}
			if !r1.Acquired || r1.Resumed || r1.ReconnectionCount != 0 {
				t.Fatalf("conn#1 = %+v, want Acquired && !Resumed && count==0", r1)
			}

			// A DIFFERENT session cannot steal the still-held lock. Proves the
			// lock is real and identity-scoped (not first-write-wins).
			steal, err := reg.AcquireOrResumeLock(ctx, id, "sess-OTHER", "", forceThresholdMs, ConnectMeta{})
			if err != nil {
				t.Fatalf("steal attempt: %v", err)
			}
			if steal.Acquired {
				t.Fatalf("a different session stole an active lock: %+v", steal)
			}

			// Connection #2: relay instance A died ungracefully (lock NOT
			// released). A FRESH connection — new TCP peer, possibly a different
			// relay/provider replica — reconnects with the same identity and
			// resume_session_id. Must resume (peer-independent), bumping the
			// reconnection count rather than forking a new session.
			r2, err := reg.AcquireOrResumeLock(ctx, id, session, session, forceThresholdMs, ConnectMeta{})
			if err != nil {
				t.Fatalf("resume #2: %v", err)
			}
			if !r2.Acquired || !r2.Resumed || r2.ReconnectionCount != 1 {
				t.Fatalf("conn#2 = %+v, want Acquired && Resumed && count==1", r2)
			}
		})
	}
}
