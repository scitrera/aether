// Original file: aether.proto


export interface TunnelData {
  'tunnelId'?: (string);
  'seq'?: (number);
  'data'?: (Buffer | Uint8Array | string);
  'fin'?: (boolean);
}

export interface TunnelData__Output {
  'tunnelId': (string);
  'seq': (number);
  'data': (Buffer);
  'fin': (boolean);
}
