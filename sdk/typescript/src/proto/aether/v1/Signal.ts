// Original file: aether.proto


// Original file: aether.proto

export const _aether_v1_Signal_SignalType = {
  FORCE_DISCONNECT: 'FORCE_DISCONNECT',
  GRACEFUL_DISCONNECT: 'GRACEFUL_DISCONNECT',
} as const;

export type _aether_v1_Signal_SignalType =
  | 'FORCE_DISCONNECT'
  | 0
  | 'GRACEFUL_DISCONNECT'
  | 1

export type _aether_v1_Signal_SignalType__Output = typeof _aether_v1_Signal_SignalType[keyof typeof _aether_v1_Signal_SignalType]

export interface Signal {
  'type'?: (_aether_v1_Signal_SignalType);
  'reason'?: (string);
}

export interface Signal__Output {
  'type': (_aether_v1_Signal_SignalType__Output);
  'reason': (string);
}
