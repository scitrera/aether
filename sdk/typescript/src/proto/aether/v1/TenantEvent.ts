// Original file: sandbox_relay_tunnel.proto


/**
 * TenantEvent reports a tenant's relay coming online (online=true) or going
 * offline (online=false).
 * 
 * snapshot_complete is a terminal sentinel emitted exactly once per
 * (re)subscribe, after the burst of currently-online tenants is replayed and
 * before any live transitions stream. The provider uses it to deterministically
 * prune tenants that left during a watch disconnect (they are absent from the
 * replay set). Live events always carry snapshot_complete=false (the zero
 * value); the sentinel carries an empty tenant and online is irrelevant.
 */
export interface TenantEvent {
  'tenant'?: (string);
  'online'?: (boolean);
  'snapshotComplete'?: (boolean);
}

/**
 * TenantEvent reports a tenant's relay coming online (online=true) or going
 * offline (online=false).
 * 
 * snapshot_complete is a terminal sentinel emitted exactly once per
 * (re)subscribe, after the burst of currently-online tenants is replayed and
 * before any live transitions stream. The provider uses it to deterministically
 * prune tenants that left during a watch disconnect (they are absent from the
 * replay set). Live events always carry snapshot_complete=false (the zero
 * value); the sentinel carries an empty tenant and online is irrelevant.
 */
export interface TenantEvent__Output {
  'tenant': (string);
  'online': (boolean);
  'snapshotComplete': (boolean);
}
