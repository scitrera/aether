// Original file: aether.proto


// Original file: aether.proto

export const _aether_v1_TunnelClose_Reason = {
  NORMAL: 'NORMAL',
  PEER_RESET: 'PEER_RESET',
  IDLE_TIMEOUT: 'IDLE_TIMEOUT',
  QUOTA: 'QUOTA',
  ERROR: 'ERROR',
} as const;

export type _aether_v1_TunnelClose_Reason =
  | 'NORMAL'
  | 0
  | 'PEER_RESET'
  | 1
  | 'IDLE_TIMEOUT'
  | 2
  | 'QUOTA'
  | 3
  | 'ERROR'
  | 4

export type _aether_v1_TunnelClose_Reason__Output = typeof _aether_v1_TunnelClose_Reason[keyof typeof _aether_v1_TunnelClose_Reason]

export interface TunnelClose {
  'tunnelId'?: (string);
  'reason'?: (_aether_v1_TunnelClose_Reason);
  'detail'?: (string);
}

export interface TunnelClose__Output {
  'tunnelId': (string);
  'reason': (_aether_v1_TunnelClose_Reason__Output);
  'detail': (string);
}
