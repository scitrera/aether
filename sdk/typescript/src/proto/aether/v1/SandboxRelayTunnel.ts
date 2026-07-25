// Original file: sandbox_relay_tunnel.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { TenantEvent as _aether_v1_TenantEvent, TenantEvent__Output as _aether_v1_TenantEvent__Output } from '../../aether/v1/TenantEvent';
import type { TunnelFrame as _aether_v1_TunnelFrame, TunnelFrame__Output as _aether_v1_TunnelFrame__Output } from '../../aether/v1/TunnelFrame';
import type { WatchTenantsRequest as _aether_v1_WatchTenantsRequest, WatchTenantsRequest__Output as _aether_v1_WatchTenantsRequest__Output } from '../../aether/v1/WatchTenantsRequest';

/**
 * SandboxRelayTunnel is the aggregator's relay-facing surface. A tenant-relay
 * sidecar dials Tunnel() and announces its tenant via TunnelHello; the
 * aggregator pairs that relay with a provider's AetherGateway.Connect stream
 * for the same tenant and splices the two 1:1 (NOT a mux — each pair owns its
 * own gateway session, lock, and session id). WatchTenants lets a provider
 * learn which tenants currently have an online relay so it can dial in.
 */
export interface SandboxRelayTunnelClient extends grpc.Client {
  /**
   * Tunnel carries one provider<->gateway session, framed in both directions.
   * The relay's first frame MUST be a TunnelHello announcing its tenant.
   */
  Tunnel(metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_TunnelFrame, _aether_v1_TunnelFrame__Output>;
  Tunnel(options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_TunnelFrame, _aether_v1_TunnelFrame__Output>;
  /**
   * Tunnel carries one provider<->gateway session, framed in both directions.
   * The relay's first frame MUST be a TunnelHello announcing its tenant.
   */
  tunnel(metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_TunnelFrame, _aether_v1_TunnelFrame__Output>;
  tunnel(options?: grpc.CallOptions): grpc.ClientDuplexStream<_aether_v1_TunnelFrame, _aether_v1_TunnelFrame__Output>;
  
  /**
   * WatchTenants streams tenant online/offline transitions to a provider.
   */
  WatchTenants(argument: _aether_v1_WatchTenantsRequest, metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientReadableStream<_aether_v1_TenantEvent__Output>;
  WatchTenants(argument: _aether_v1_WatchTenantsRequest, options?: grpc.CallOptions): grpc.ClientReadableStream<_aether_v1_TenantEvent__Output>;
  /**
   * WatchTenants streams tenant online/offline transitions to a provider.
   */
  watchTenants(argument: _aether_v1_WatchTenantsRequest, metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientReadableStream<_aether_v1_TenantEvent__Output>;
  watchTenants(argument: _aether_v1_WatchTenantsRequest, options?: grpc.CallOptions): grpc.ClientReadableStream<_aether_v1_TenantEvent__Output>;
  
}

/**
 * SandboxRelayTunnel is the aggregator's relay-facing surface. A tenant-relay
 * sidecar dials Tunnel() and announces its tenant via TunnelHello; the
 * aggregator pairs that relay with a provider's AetherGateway.Connect stream
 * for the same tenant and splices the two 1:1 (NOT a mux — each pair owns its
 * own gateway session, lock, and session id). WatchTenants lets a provider
 * learn which tenants currently have an online relay so it can dial in.
 */
export interface SandboxRelayTunnelHandlers extends grpc.UntypedServiceImplementation {
  /**
   * Tunnel carries one provider<->gateway session, framed in both directions.
   * The relay's first frame MUST be a TunnelHello announcing its tenant.
   */
  Tunnel: grpc.handleBidiStreamingCall<_aether_v1_TunnelFrame__Output, _aether_v1_TunnelFrame>;
  
  /**
   * WatchTenants streams tenant online/offline transitions to a provider.
   */
  WatchTenants: grpc.handleServerStreamingCall<_aether_v1_WatchTenantsRequest__Output, _aether_v1_TenantEvent>;
  
}

export interface SandboxRelayTunnelDefinition extends grpc.ServiceDefinition {
  Tunnel: MethodDefinition<_aether_v1_TunnelFrame, _aether_v1_TunnelFrame, _aether_v1_TunnelFrame__Output, _aether_v1_TunnelFrame__Output>
  WatchTenants: MethodDefinition<_aether_v1_WatchTenantsRequest, _aether_v1_TenantEvent, _aether_v1_WatchTenantsRequest__Output, _aether_v1_TenantEvent__Output>
}
