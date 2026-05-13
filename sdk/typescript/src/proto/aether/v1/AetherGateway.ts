// Original file: aether.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { DownstreamMessage as _aether_v1_DownstreamMessage, DownstreamMessage__Output as _aether_v1_DownstreamMessage__Output } from '../../aether/v1/DownstreamMessage';
import type { UpstreamMessage as _aether_v1_UpstreamMessage, UpstreamMessage__Output as _aether_v1_UpstreamMessage__Output } from '../../aether/v1/UpstreamMessage';

export interface AetherGatewayClient extends grpc.Client {
  /**
   * Bi-directional stream.
   * First message must be InitConnection.
   * Subsequent messages are opaque Envelopes.
   */
  Connect(metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_UpstreamMessage, _aether_v1_DownstreamMessage__Output>;
  Connect(options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_UpstreamMessage, _aether_v1_DownstreamMessage__Output>;
  /**
   * Bi-directional stream.
   * First message must be InitConnection.
   * Subsequent messages are opaque Envelopes.
   */
  connect(metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_UpstreamMessage, _aether_v1_DownstreamMessage__Output>;
  connect(options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_UpstreamMessage, _aether_v1_DownstreamMessage__Output>;
  
}

export interface AetherGatewayHandlers extends grpc.UntypedServiceImplementation {
  /**
   * Bi-directional stream.
   * First message must be InitConnection.
   * Subsequent messages are opaque Envelopes.
   */
  Connect: grpc.handleBidiStreamingCall<_aether_v1_UpstreamMessage__Output, _aether_v1_DownstreamMessage>;
  
}

export interface AetherGatewayDefinition extends grpc.ServiceDefinition {
  Connect: MethodDefinition<_aether_v1_UpstreamMessage, _aether_v1_DownstreamMessage, _aether_v1_UpstreamMessage__Output, _aether_v1_DownstreamMessage__Output>
}
