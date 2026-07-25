// Original file: sandbox_relay_tunnel.proto


/**
 * TunnelHello is the relay's opening frame, announcing the tenant it serves.
 * The tenant string is a hint that MUST be validated against the relay's
 * peer-certificate CN before any pairing.
 */
export interface TunnelHello {
  'tenant'?: (string);
}

/**
 * TunnelHello is the relay's opening frame, announcing the tenant it serves.
 * The tenant string is a hint that MUST be validated against the relay's
 * peer-certificate CN before any pairing.
 */
export interface TunnelHello__Output {
  'tenant': (string);
}
