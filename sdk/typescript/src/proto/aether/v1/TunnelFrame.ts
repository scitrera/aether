// Original file: sandbox_relay_tunnel.proto

import type { UpstreamMessage as _aether_v1_UpstreamMessage, UpstreamMessage__Output as _aether_v1_UpstreamMessage__Output } from '../../aether/v1/UpstreamMessage';
import type { DownstreamMessage as _aether_v1_DownstreamMessage, DownstreamMessage__Output as _aether_v1_DownstreamMessage__Output } from '../../aether/v1/DownstreamMessage';
import type { TunnelHello as _aether_v1_TunnelHello, TunnelHello__Output as _aether_v1_TunnelHello__Output } from '../../aether/v1/TunnelHello';

/**
 * TunnelFrame wraps either direction of a relayed gateway session, or the
 * relay's opening hello. Provider->relay carries UpstreamMessage (up);
 * relay->provider carries DownstreamMessage (down).
 */
export interface TunnelFrame {
  'up'?: (_aether_v1_UpstreamMessage | null);
  'down'?: (_aether_v1_DownstreamMessage | null);
  'hello'?: (_aether_v1_TunnelHello | null);
  'f'?: "up"|"down"|"hello";
}

/**
 * TunnelFrame wraps either direction of a relayed gateway session, or the
 * relay's opening hello. Provider->relay carries UpstreamMessage (up);
 * relay->provider carries DownstreamMessage (down).
 */
export interface TunnelFrame__Output {
  'up'?: (_aether_v1_UpstreamMessage__Output | null);
  'down'?: (_aether_v1_DownstreamMessage__Output | null);
  'hello'?: (_aether_v1_TunnelHello__Output | null);
  'f'?: "up"|"down"|"hello";
}
