import type * as grpc from '@grpc/grpc-js';
import type { EnumTypeDefinition, MessageTypeDefinition } from '@grpc/proto-loader';

import type { ACLAccessContributionInfo as _aether_v1_ACLAccessContributionInfo, ACLAccessContributionInfo__Output as _aether_v1_ACLAccessContributionInfo__Output } from './aether/v1/ACLAccessContributionInfo';
import type { ACLAccessExplanationInfo as _aether_v1_ACLAccessExplanationInfo, ACLAccessExplanationInfo__Output as _aether_v1_ACLAccessExplanationInfo__Output } from './aether/v1/ACLAccessExplanationInfo';
import type { ACLAuditEntryInfo as _aether_v1_ACLAuditEntryInfo, ACLAuditEntryInfo__Output as _aether_v1_ACLAuditEntryInfo__Output } from './aether/v1/ACLAuditEntryInfo';
import type { ACLAuditFilter as _aether_v1_ACLAuditFilter, ACLAuditFilter__Output as _aether_v1_ACLAuditFilter__Output } from './aether/v1/ACLAuditFilter';
import type { ACLAuthorityGrantFilter as _aether_v1_ACLAuthorityGrantFilter, ACLAuthorityGrantFilter__Output as _aether_v1_ACLAuthorityGrantFilter__Output } from './aether/v1/ACLAuthorityGrantFilter';
import type { ACLAuthorityGrantInfo as _aether_v1_ACLAuthorityGrantInfo, ACLAuthorityGrantInfo__Output as _aether_v1_ACLAuthorityGrantInfo__Output } from './aether/v1/ACLAuthorityGrantInfo';
import type { ACLAuthorityGrantRequest as _aether_v1_ACLAuthorityGrantRequest, ACLAuthorityGrantRequest__Output as _aether_v1_ACLAuthorityGrantRequest__Output } from './aether/v1/ACLAuthorityGrantRequest';
import type { ACLAuthorityGrantResourceScopeEntry as _aether_v1_ACLAuthorityGrantResourceScopeEntry, ACLAuthorityGrantResourceScopeEntry__Output as _aether_v1_ACLAuthorityGrantResourceScopeEntry__Output } from './aether/v1/ACLAuthorityGrantResourceScopeEntry';
import type { ACLCleanupResult as _aether_v1_ACLCleanupResult, ACLCleanupResult__Output as _aether_v1_ACLCleanupResult__Output } from './aether/v1/ACLCleanupResult';
import type { ACLFallbackPolicyInfo as _aether_v1_ACLFallbackPolicyInfo, ACLFallbackPolicyInfo__Output as _aether_v1_ACLFallbackPolicyInfo__Output } from './aether/v1/ACLFallbackPolicyInfo';
import type { ACLGrantRequest as _aether_v1_ACLGrantRequest, ACLGrantRequest__Output as _aether_v1_ACLGrantRequest__Output } from './aether/v1/ACLGrantRequest';
import type { ACLGroupInfo as _aether_v1_ACLGroupInfo, ACLGroupInfo__Output as _aether_v1_ACLGroupInfo__Output } from './aether/v1/ACLGroupInfo';
import type { ACLGroupMemberInfo as _aether_v1_ACLGroupMemberInfo, ACLGroupMemberInfo__Output as _aether_v1_ACLGroupMemberInfo__Output } from './aether/v1/ACLGroupMemberInfo';
import type { ACLGroupMemberRequest as _aether_v1_ACLGroupMemberRequest, ACLGroupMemberRequest__Output as _aether_v1_ACLGroupMemberRequest__Output } from './aether/v1/ACLGroupMemberRequest';
import type { ACLGroupRequest as _aether_v1_ACLGroupRequest, ACLGroupRequest__Output as _aether_v1_ACLGroupRequest__Output } from './aether/v1/ACLGroupRequest';
import type { ACLOperation as _aether_v1_ACLOperation, ACLOperation__Output as _aether_v1_ACLOperation__Output } from './aether/v1/ACLOperation';
import type { ACLRenewAuthorityGrantRequest as _aether_v1_ACLRenewAuthorityGrantRequest, ACLRenewAuthorityGrantRequest__Output as _aether_v1_ACLRenewAuthorityGrantRequest__Output } from './aether/v1/ACLRenewAuthorityGrantRequest';
import type { ACLResponse as _aether_v1_ACLResponse, ACLResponse__Output as _aether_v1_ACLResponse__Output } from './aether/v1/ACLResponse';
import type { ACLRoleAssignmentInfo as _aether_v1_ACLRoleAssignmentInfo, ACLRoleAssignmentInfo__Output as _aether_v1_ACLRoleAssignmentInfo__Output } from './aether/v1/ACLRoleAssignmentInfo';
import type { ACLRoleAssignmentRequest as _aether_v1_ACLRoleAssignmentRequest, ACLRoleAssignmentRequest__Output as _aether_v1_ACLRoleAssignmentRequest__Output } from './aether/v1/ACLRoleAssignmentRequest';
import type { ACLRoleInfo as _aether_v1_ACLRoleInfo, ACLRoleInfo__Output as _aether_v1_ACLRoleInfo__Output } from './aether/v1/ACLRoleInfo';
import type { ACLRoleRequest as _aether_v1_ACLRoleRequest, ACLRoleRequest__Output as _aether_v1_ACLRoleRequest__Output } from './aether/v1/ACLRoleRequest';
import type { ACLRuleFilter as _aether_v1_ACLRuleFilter, ACLRuleFilter__Output as _aether_v1_ACLRuleFilter__Output } from './aether/v1/ACLRuleFilter';
import type { ACLRuleInfo as _aether_v1_ACLRuleInfo, ACLRuleInfo__Output as _aether_v1_ACLRuleInfo__Output } from './aether/v1/ACLRuleInfo';
import type { ACLSetFallbackRequest as _aether_v1_ACLSetFallbackRequest, ACLSetFallbackRequest__Output as _aether_v1_ACLSetFallbackRequest__Output } from './aether/v1/ACLSetFallbackRequest';
import type { AdminQuery as _aether_v1_AdminQuery, AdminQuery__Output as _aether_v1_AdminQuery__Output } from './aether/v1/AdminQuery';
import type { AdminResponse as _aether_v1_AdminResponse, AdminResponse__Output as _aether_v1_AdminResponse__Output } from './aether/v1/AdminResponse';
import type { AetherGatewayClient as _aether_v1_AetherGatewayClient, AetherGatewayDefinition as _aether_v1_AetherGatewayDefinition } from './aether/v1/AetherGateway';
import type { AgentFilter as _aether_v1_AgentFilter, AgentFilter__Output as _aether_v1_AgentFilter__Output } from './aether/v1/AgentFilter';
import type { AgentIdentity as _aether_v1_AgentIdentity, AgentIdentity__Output as _aether_v1_AgentIdentity__Output } from './aether/v1/AgentIdentity';
import type { AgentLaunchParams as _aether_v1_AgentLaunchParams, AgentLaunchParams__Output as _aether_v1_AgentLaunchParams__Output } from './aether/v1/AgentLaunchParams';
import type { AgentLaunchResult as _aether_v1_AgentLaunchResult, AgentLaunchResult__Output as _aether_v1_AgentLaunchResult__Output } from './aether/v1/AgentLaunchResult';
import type { AgentOperation as _aether_v1_AgentOperation, AgentOperation__Output as _aether_v1_AgentOperation__Output } from './aether/v1/AgentOperation';
import type { AgentRegistrationInfo as _aether_v1_AgentRegistrationInfo, AgentRegistrationInfo__Output as _aether_v1_AgentRegistrationInfo__Output } from './aether/v1/AgentRegistrationInfo';
import type { AgentResourceSchemaEntry as _aether_v1_AgentResourceSchemaEntry, AgentResourceSchemaEntry__Output as _aether_v1_AgentResourceSchemaEntry__Output } from './aether/v1/AgentResourceSchemaEntry';
import type { AgentResponse as _aether_v1_AgentResponse, AgentResponse__Output as _aether_v1_AgentResponse__Output } from './aether/v1/AgentResponse';
import type { AuditEntry as _aether_v1_AuditEntry, AuditEntry__Output as _aether_v1_AuditEntry__Output } from './aether/v1/AuditEntry';
import type { AuditQuery as _aether_v1_AuditQuery, AuditQuery__Output as _aether_v1_AuditQuery__Output } from './aether/v1/AuditQuery';
import type { AuditQueryResponse as _aether_v1_AuditQueryResponse, AuditQueryResponse__Output as _aether_v1_AuditQueryResponse__Output } from './aether/v1/AuditQueryResponse';
import type { AuthorityGrantBatchExchangeRequest as _aether_v1_AuthorityGrantBatchExchangeRequest, AuthorityGrantBatchExchangeRequest__Output as _aether_v1_AuthorityGrantBatchExchangeRequest__Output } from './aether/v1/AuthorityGrantBatchExchangeRequest';
import type { AuthorityGrantDeriveForTargetRequest as _aether_v1_AuthorityGrantDeriveForTargetRequest, AuthorityGrantDeriveForTargetRequest__Output as _aether_v1_AuthorityGrantDeriveForTargetRequest__Output } from './aether/v1/AuthorityGrantDeriveForTargetRequest';
import type { AuthorityGrantDeriveRequest as _aether_v1_AuthorityGrantDeriveRequest, AuthorityGrantDeriveRequest__Output as _aether_v1_AuthorityGrantDeriveRequest__Output } from './aether/v1/AuthorityGrantDeriveRequest';
import type { AuthorityGrantExchangeRequest as _aether_v1_AuthorityGrantExchangeRequest, AuthorityGrantExchangeRequest__Output as _aether_v1_AuthorityGrantExchangeRequest__Output } from './aether/v1/AuthorityGrantExchangeRequest';
import type { AuthorityGrantInfo as _aether_v1_AuthorityGrantInfo, AuthorityGrantInfo__Output as _aether_v1_AuthorityGrantInfo__Output } from './aether/v1/AuthorityGrantInfo';
import type { AuthorityGrantListRequest as _aether_v1_AuthorityGrantListRequest, AuthorityGrantListRequest__Output as _aether_v1_AuthorityGrantListRequest__Output } from './aether/v1/AuthorityGrantListRequest';
import type { AuthorityGrantOperation as _aether_v1_AuthorityGrantOperation, AuthorityGrantOperation__Output as _aether_v1_AuthorityGrantOperation__Output } from './aether/v1/AuthorityGrantOperation';
import type { AuthorityGrantResponse as _aether_v1_AuthorityGrantResponse, AuthorityGrantResponse__Output as _aether_v1_AuthorityGrantResponse__Output } from './aether/v1/AuthorityGrantResponse';
import type { AuthorityGrantRevocation as _aether_v1_AuthorityGrantRevocation, AuthorityGrantRevocation__Output as _aether_v1_AuthorityGrantRevocation__Output } from './aether/v1/AuthorityGrantRevocation';
import type { AuthorityIdentity as _aether_v1_AuthorityIdentity, AuthorityIdentity__Output as _aether_v1_AuthorityIdentity__Output } from './aether/v1/AuthorityIdentity';
import type { AuthorityRequest as _aether_v1_AuthorityRequest, AuthorityRequest__Output as _aether_v1_AuthorityRequest__Output } from './aether/v1/AuthorityRequest';
import type { AuthorityRequestEvent as _aether_v1_AuthorityRequestEvent, AuthorityRequestEvent__Output as _aether_v1_AuthorityRequestEvent__Output } from './aether/v1/AuthorityRequestEvent';
import type { AuthorityRequestListFilter as _aether_v1_AuthorityRequestListFilter, AuthorityRequestListFilter__Output as _aether_v1_AuthorityRequestListFilter__Output } from './aether/v1/AuthorityRequestListFilter';
import type { AuthorityRequestOperation as _aether_v1_AuthorityRequestOperation, AuthorityRequestOperation__Output as _aether_v1_AuthorityRequestOperation__Output } from './aether/v1/AuthorityRequestOperation';
import type { AuthorityRequestOperationResponse as _aether_v1_AuthorityRequestOperationResponse, AuthorityRequestOperationResponse__Output as _aether_v1_AuthorityRequestOperationResponse__Output } from './aether/v1/AuthorityRequestOperationResponse';
import type { AuthorityRequestResourceScopeEntry as _aether_v1_AuthorityRequestResourceScopeEntry, AuthorityRequestResourceScopeEntry__Output as _aether_v1_AuthorityRequestResourceScopeEntry__Output } from './aether/v1/AuthorityRequestResourceScopeEntry';
import type { AuthorityRequestRoutingTarget as _aether_v1_AuthorityRequestRoutingTarget, AuthorityRequestRoutingTarget__Output as _aether_v1_AuthorityRequestRoutingTarget__Output } from './aether/v1/AuthorityRequestRoutingTarget';
import type { AuthoritySpan as _aether_v1_AuthoritySpan, AuthoritySpan__Output as _aether_v1_AuthoritySpan__Output } from './aether/v1/AuthoritySpan';
import type { AuthorizationContext as _aether_v1_AuthorizationContext, AuthorizationContext__Output as _aether_v1_AuthorizationContext__Output } from './aether/v1/AuthorizationContext';
import type { BridgeIdentity as _aether_v1_BridgeIdentity, BridgeIdentity__Output as _aether_v1_BridgeIdentity__Output } from './aether/v1/BridgeIdentity';
import type { BuildInfo as _aether_v1_BuildInfo, BuildInfo__Output as _aether_v1_BuildInfo__Output } from './aether/v1/BuildInfo';
import type { CheckpointOperation as _aether_v1_CheckpointOperation, CheckpointOperation__Output as _aether_v1_CheckpointOperation__Output } from './aether/v1/CheckpointOperation';
import type { CheckpointResponse as _aether_v1_CheckpointResponse, CheckpointResponse__Output as _aether_v1_CheckpointResponse__Output } from './aether/v1/CheckpointResponse';
import type { ConfigSnapshot as _aether_v1_ConfigSnapshot, ConfigSnapshot__Output as _aether_v1_ConfigSnapshot__Output } from './aether/v1/ConfigSnapshot';
import type { ConnectionAck as _aether_v1_ConnectionAck, ConnectionAck__Output as _aether_v1_ConnectionAck__Output } from './aether/v1/ConnectionAck';
import type { ConnectionFilter as _aether_v1_ConnectionFilter, ConnectionFilter__Output as _aether_v1_ConnectionFilter__Output } from './aether/v1/ConnectionFilter';
import type { ConnectionInfo as _aether_v1_ConnectionInfo, ConnectionInfo__Output as _aether_v1_ConnectionInfo__Output } from './aether/v1/ConnectionInfo';
import type { ConnectionStatusRequest as _aether_v1_ConnectionStatusRequest, ConnectionStatusRequest__Output as _aether_v1_ConnectionStatusRequest__Output } from './aether/v1/ConnectionStatusRequest';
import type { ConnectionStatusResponse as _aether_v1_ConnectionStatusResponse, ConnectionStatusResponse__Output as _aether_v1_ConnectionStatusResponse__Output } from './aether/v1/ConnectionStatusResponse';
import type { CreateAuthorityRequestPayload as _aether_v1_CreateAuthorityRequestPayload, CreateAuthorityRequestPayload__Output as _aether_v1_CreateAuthorityRequestPayload__Output } from './aether/v1/CreateAuthorityRequestPayload';
import type { CreateTaskRequest as _aether_v1_CreateTaskRequest, CreateTaskRequest__Output as _aether_v1_CreateTaskRequest__Output } from './aether/v1/CreateTaskRequest';
import type { CreateTaskResponse as _aether_v1_CreateTaskResponse, CreateTaskResponse__Output as _aether_v1_CreateTaskResponse__Output } from './aether/v1/CreateTaskResponse';
import type { DownstreamMessage as _aether_v1_DownstreamMessage, DownstreamMessage__Output as _aether_v1_DownstreamMessage__Output } from './aether/v1/DownstreamMessage';
import type { ErrorResponse as _aether_v1_ErrorResponse, ErrorResponse__Output as _aether_v1_ErrorResponse__Output } from './aether/v1/ErrorResponse';
import type { ExtensionDeclaration as _aether_v1_ExtensionDeclaration, ExtensionDeclaration__Output as _aether_v1_ExtensionDeclaration__Output } from './aether/v1/ExtensionDeclaration';
import type { FlowEdge as _aether_v1_FlowEdge, FlowEdge__Output as _aether_v1_FlowEdge__Output } from './aether/v1/FlowEdge';
import type { FlowNode as _aether_v1_FlowNode, FlowNode__Output as _aether_v1_FlowNode__Output } from './aether/v1/FlowNode';
import type { GatewayInfo as _aether_v1_GatewayInfo, GatewayInfo__Output as _aether_v1_GatewayInfo__Output } from './aether/v1/GatewayInfo';
import type { GatewayStats as _aether_v1_GatewayStats, GatewayStats__Output as _aether_v1_GatewayStats__Output } from './aether/v1/GatewayStats';
import type { HealthCheck as _aether_v1_HealthCheck, HealthCheck__Output as _aether_v1_HealthCheck__Output } from './aether/v1/HealthCheck';
import type { HealthInfo as _aether_v1_HealthInfo, HealthInfo__Output as _aether_v1_HealthInfo__Output } from './aether/v1/HealthInfo';
import type { HibernationDescriptor as _aether_v1_HibernationDescriptor, HibernationDescriptor__Output as _aether_v1_HibernationDescriptor__Output } from './aether/v1/HibernationDescriptor';
import type { IncomingMessage as _aether_v1_IncomingMessage, IncomingMessage__Output as _aether_v1_IncomingMessage__Output } from './aether/v1/IncomingMessage';
import type { InitConnection as _aether_v1_InitConnection, InitConnection__Output as _aether_v1_InitConnection__Output } from './aether/v1/InitConnection';
import type { KVOperation as _aether_v1_KVOperation, KVOperation__Output as _aether_v1_KVOperation__Output } from './aether/v1/KVOperation';
import type { KVResponse as _aether_v1_KVResponse, KVResponse__Output as _aether_v1_KVResponse__Output } from './aether/v1/KVResponse';
import type { MessageEnvelope as _aether_v1_MessageEnvelope, MessageEnvelope__Output as _aether_v1_MessageEnvelope__Output } from './aether/v1/MessageEnvelope';
import type { MessageFlowInfo as _aether_v1_MessageFlowInfo, MessageFlowInfo__Output as _aether_v1_MessageFlowInfo__Output } from './aether/v1/MessageFlowInfo';
import type { Metric as _aether_v1_Metric, Metric__Output as _aether_v1_Metric__Output } from './aether/v1/Metric';
import type { MetricEntry as _aether_v1_MetricEntry, MetricEntry__Output as _aether_v1_MetricEntry__Output } from './aether/v1/MetricEntry';
import type { MetricsBridgeIdentity as _aether_v1_MetricsBridgeIdentity, MetricsBridgeIdentity__Output as _aether_v1_MetricsBridgeIdentity__Output } from './aether/v1/MetricsBridgeIdentity';
import type { NegotiatedExtension as _aether_v1_NegotiatedExtension, NegotiatedExtension__Output as _aether_v1_NegotiatedExtension__Output } from './aether/v1/NegotiatedExtension';
import type { OrchestratorIdentity as _aether_v1_OrchestratorIdentity, OrchestratorIdentity__Output as _aether_v1_OrchestratorIdentity__Output } from './aether/v1/OrchestratorIdentity';
import type { OrchestratorInfo as _aether_v1_OrchestratorInfo, OrchestratorInfo__Output as _aether_v1_OrchestratorInfo__Output } from './aether/v1/OrchestratorInfo';
import type { PrincipalRef as _aether_v1_PrincipalRef, PrincipalRef__Output as _aether_v1_PrincipalRef__Output } from './aether/v1/PrincipalRef';
import type { ProgressReport as _aether_v1_ProgressReport, ProgressReport__Output as _aether_v1_ProgressReport__Output } from './aether/v1/ProgressReport';
import type { ProgressStep as _aether_v1_ProgressStep, ProgressStep__Output as _aether_v1_ProgressStep__Output } from './aether/v1/ProgressStep';
import type { ProgressUpdate as _aether_v1_ProgressUpdate, ProgressUpdate__Output as _aether_v1_ProgressUpdate__Output } from './aether/v1/ProgressUpdate';
import type { ProxyError as _aether_v1_ProxyError, ProxyError__Output as _aether_v1_ProxyError__Output } from './aether/v1/ProxyError';
import type { ProxyHttpBodyChunk as _aether_v1_ProxyHttpBodyChunk, ProxyHttpBodyChunk__Output as _aether_v1_ProxyHttpBodyChunk__Output } from './aether/v1/ProxyHttpBodyChunk';
import type { ProxyHttpRequest as _aether_v1_ProxyHttpRequest, ProxyHttpRequest__Output as _aether_v1_ProxyHttpRequest__Output } from './aether/v1/ProxyHttpRequest';
import type { ProxyHttpResponse as _aether_v1_ProxyHttpResponse, ProxyHttpResponse__Output as _aether_v1_ProxyHttpResponse__Output } from './aether/v1/ProxyHttpResponse';
import type { ResolveAuthorityRequest as _aether_v1_ResolveAuthorityRequest, ResolveAuthorityRequest__Output as _aether_v1_ResolveAuthorityRequest__Output } from './aether/v1/ResolveAuthorityRequest';
import type { ResolveAuthorityRequestPayload as _aether_v1_ResolveAuthorityRequestPayload, ResolveAuthorityRequestPayload__Output as _aether_v1_ResolveAuthorityRequestPayload__Output } from './aether/v1/ResolveAuthorityRequestPayload';
import type { ResolveAuthorityResponse as _aether_v1_ResolveAuthorityResponse, ResolveAuthorityResponse__Output as _aether_v1_ResolveAuthorityResponse__Output } from './aether/v1/ResolveAuthorityResponse';
import type { ResolvedAuthority as _aether_v1_ResolvedAuthority, ResolvedAuthority__Output as _aether_v1_ResolvedAuthority__Output } from './aether/v1/ResolvedAuthority';
import type { ResolvedAuthorityInfo as _aether_v1_ResolvedAuthorityInfo, ResolvedAuthorityInfo__Output as _aether_v1_ResolvedAuthorityInfo__Output } from './aether/v1/ResolvedAuthorityInfo';
import type { RetryPolicy as _aether_v1_RetryPolicy, RetryPolicy__Output as _aether_v1_RetryPolicy__Output } from './aether/v1/RetryPolicy';
import type { SendMessage as _aether_v1_SendMessage, SendMessage__Output as _aether_v1_SendMessage__Output } from './aether/v1/SendMessage';
import type { ServiceIdentity as _aether_v1_ServiceIdentity, ServiceIdentity__Output as _aether_v1_ServiceIdentity__Output } from './aether/v1/ServiceIdentity';
import type { SessionOperation as _aether_v1_SessionOperation, SessionOperation__Output as _aether_v1_SessionOperation__Output } from './aether/v1/SessionOperation';
import type { SessionOperationResponse as _aether_v1_SessionOperationResponse, SessionOperationResponse__Output as _aether_v1_SessionOperationResponse__Output } from './aether/v1/SessionOperationResponse';
import type { Signal as _aether_v1_Signal, Signal__Output as _aether_v1_Signal__Output } from './aether/v1/Signal';
import type { SubmitAuditEventRequest as _aether_v1_SubmitAuditEventRequest, SubmitAuditEventRequest__Output as _aether_v1_SubmitAuditEventRequest__Output } from './aether/v1/SubmitAuditEventRequest';
import type { SubmitAuditEventResponse as _aether_v1_SubmitAuditEventResponse, SubmitAuditEventResponse__Output as _aether_v1_SubmitAuditEventResponse__Output } from './aether/v1/SubmitAuditEventResponse';
import type { SwitchWorkspace as _aether_v1_SwitchWorkspace, SwitchWorkspace__Output as _aether_v1_SwitchWorkspace__Output } from './aether/v1/SwitchWorkspace';
import type { TaskAssignment as _aether_v1_TaskAssignment, TaskAssignment__Output as _aether_v1_TaskAssignment__Output } from './aether/v1/TaskAssignment';
import type { TaskAuthorityRequestEventRelay as _aether_v1_TaskAuthorityRequestEventRelay, TaskAuthorityRequestEventRelay__Output as _aether_v1_TaskAuthorityRequestEventRelay__Output } from './aether/v1/TaskAuthorityRequestEventRelay';
import type { TaskChildLifecycleEvent as _aether_v1_TaskChildLifecycleEvent, TaskChildLifecycleEvent__Output as _aether_v1_TaskChildLifecycleEvent__Output } from './aether/v1/TaskChildLifecycleEvent';
import type { TaskCompletionEvent as _aether_v1_TaskCompletionEvent, TaskCompletionEvent__Output as _aether_v1_TaskCompletionEvent__Output } from './aether/v1/TaskCompletionEvent';
import type { TaskEvent as _aether_v1_TaskEvent, TaskEvent__Output as _aether_v1_TaskEvent__Output } from './aether/v1/TaskEvent';
import type { TaskFilter as _aether_v1_TaskFilter, TaskFilter__Output as _aether_v1_TaskFilter__Output } from './aether/v1/TaskFilter';
import type { TaskHibernated as _aether_v1_TaskHibernated, TaskHibernated__Output as _aether_v1_TaskHibernated__Output } from './aether/v1/TaskHibernated';
import type { TaskIdentity as _aether_v1_TaskIdentity, TaskIdentity__Output as _aether_v1_TaskIdentity__Output } from './aether/v1/TaskIdentity';
import type { TaskInfo as _aether_v1_TaskInfo, TaskInfo__Output as _aether_v1_TaskInfo__Output } from './aether/v1/TaskInfo';
import type { TaskOperation as _aether_v1_TaskOperation, TaskOperation__Output as _aether_v1_TaskOperation__Output } from './aether/v1/TaskOperation';
import type { TaskOperationResponse as _aether_v1_TaskOperationResponse, TaskOperationResponse__Output as _aether_v1_TaskOperationResponse__Output } from './aether/v1/TaskOperationResponse';
import type { TaskProgressEvent as _aether_v1_TaskProgressEvent, TaskProgressEvent__Output as _aether_v1_TaskProgressEvent__Output } from './aether/v1/TaskProgressEvent';
import type { TaskQuery as _aether_v1_TaskQuery, TaskQuery__Output as _aether_v1_TaskQuery__Output } from './aether/v1/TaskQuery';
import type { TaskQueryResponse as _aether_v1_TaskQueryResponse, TaskQueryResponse__Output as _aether_v1_TaskQueryResponse__Output } from './aether/v1/TaskQueryResponse';
import type { TaskStatusChangedEvent as _aether_v1_TaskStatusChangedEvent, TaskStatusChangedEvent__Output as _aether_v1_TaskStatusChangedEvent__Output } from './aether/v1/TaskStatusChangedEvent';
import type { TaskSubscriptionOperation as _aether_v1_TaskSubscriptionOperation, TaskSubscriptionOperation__Output as _aether_v1_TaskSubscriptionOperation__Output } from './aether/v1/TaskSubscriptionOperation';
import type { TaskSubscriptionOperationResponse as _aether_v1_TaskSubscriptionOperationResponse, TaskSubscriptionOperationResponse__Output as _aether_v1_TaskSubscriptionOperationResponse__Output } from './aether/v1/TaskSubscriptionOperationResponse';
import type { TokenCreateRequest as _aether_v1_TokenCreateRequest, TokenCreateRequest__Output as _aether_v1_TokenCreateRequest__Output } from './aether/v1/TokenCreateRequest';
import type { TokenFilter as _aether_v1_TokenFilter, TokenFilter__Output as _aether_v1_TokenFilter__Output } from './aether/v1/TokenFilter';
import type { TokenInfo as _aether_v1_TokenInfo, TokenInfo__Output as _aether_v1_TokenInfo__Output } from './aether/v1/TokenInfo';
import type { TokenOperation as _aether_v1_TokenOperation, TokenOperation__Output as _aether_v1_TokenOperation__Output } from './aether/v1/TokenOperation';
import type { TokenResponse as _aether_v1_TokenResponse, TokenResponse__Output as _aether_v1_TokenResponse__Output } from './aether/v1/TokenResponse';
import type { TunnelAck as _aether_v1_TunnelAck, TunnelAck__Output as _aether_v1_TunnelAck__Output } from './aether/v1/TunnelAck';
import type { TunnelClose as _aether_v1_TunnelClose, TunnelClose__Output as _aether_v1_TunnelClose__Output } from './aether/v1/TunnelClose';
import type { TunnelData as _aether_v1_TunnelData, TunnelData__Output as _aether_v1_TunnelData__Output } from './aether/v1/TunnelData';
import type { TunnelOpen as _aether_v1_TunnelOpen, TunnelOpen__Output as _aether_v1_TunnelOpen__Output } from './aether/v1/TunnelOpen';
import type { UpstreamMessage as _aether_v1_UpstreamMessage, UpstreamMessage__Output as _aether_v1_UpstreamMessage__Output } from './aether/v1/UpstreamMessage';
import type { UserIdentity as _aether_v1_UserIdentity, UserIdentity__Output as _aether_v1_UserIdentity__Output } from './aether/v1/UserIdentity';
import type { WaitSpec as _aether_v1_WaitSpec, WaitSpec__Output as _aether_v1_WaitSpec__Output } from './aether/v1/WaitSpec';
import type { WorkflowEngineIdentity as _aether_v1_WorkflowEngineIdentity, WorkflowEngineIdentity__Output as _aether_v1_WorkflowEngineIdentity__Output } from './aether/v1/WorkflowEngineIdentity';
import type { WorkflowOperation as _aether_v1_WorkflowOperation, WorkflowOperation__Output as _aether_v1_WorkflowOperation__Output } from './aether/v1/WorkflowOperation';
import type { WorkflowResponse as _aether_v1_WorkflowResponse, WorkflowResponse__Output as _aether_v1_WorkflowResponse__Output } from './aether/v1/WorkflowResponse';
import type { WorkspaceFilter as _aether_v1_WorkspaceFilter, WorkspaceFilter__Output as _aether_v1_WorkspaceFilter__Output } from './aether/v1/WorkspaceFilter';
import type { WorkspaceInfo as _aether_v1_WorkspaceInfo, WorkspaceInfo__Output as _aether_v1_WorkspaceInfo__Output } from './aether/v1/WorkspaceInfo';
import type { WorkspaceOperation as _aether_v1_WorkspaceOperation, WorkspaceOperation__Output as _aether_v1_WorkspaceOperation__Output } from './aether/v1/WorkspaceOperation';
import type { WorkspaceResponse as _aether_v1_WorkspaceResponse, WorkspaceResponse__Output as _aether_v1_WorkspaceResponse__Output } from './aether/v1/WorkspaceResponse';

type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
  new(...args: ConstructorParameters<Constructor>): Subtype;
};

export interface ProtoGrpcType {
  aether: {
    v1: {
      ACLAccessContributionInfo: MessageTypeDefinition<_aether_v1_ACLAccessContributionInfo, _aether_v1_ACLAccessContributionInfo__Output>
      ACLAccessExplanationInfo: MessageTypeDefinition<_aether_v1_ACLAccessExplanationInfo, _aether_v1_ACLAccessExplanationInfo__Output>
      ACLAuditEntryInfo: MessageTypeDefinition<_aether_v1_ACLAuditEntryInfo, _aether_v1_ACLAuditEntryInfo__Output>
      ACLAuditFilter: MessageTypeDefinition<_aether_v1_ACLAuditFilter, _aether_v1_ACLAuditFilter__Output>
      ACLAuthorityGrantFilter: MessageTypeDefinition<_aether_v1_ACLAuthorityGrantFilter, _aether_v1_ACLAuthorityGrantFilter__Output>
      ACLAuthorityGrantInfo: MessageTypeDefinition<_aether_v1_ACLAuthorityGrantInfo, _aether_v1_ACLAuthorityGrantInfo__Output>
      ACLAuthorityGrantRequest: MessageTypeDefinition<_aether_v1_ACLAuthorityGrantRequest, _aether_v1_ACLAuthorityGrantRequest__Output>
      ACLAuthorityGrantResourceScopeEntry: MessageTypeDefinition<_aether_v1_ACLAuthorityGrantResourceScopeEntry, _aether_v1_ACLAuthorityGrantResourceScopeEntry__Output>
      ACLCleanupResult: MessageTypeDefinition<_aether_v1_ACLCleanupResult, _aether_v1_ACLCleanupResult__Output>
      ACLFallbackPolicyInfo: MessageTypeDefinition<_aether_v1_ACLFallbackPolicyInfo, _aether_v1_ACLFallbackPolicyInfo__Output>
      ACLGrantRequest: MessageTypeDefinition<_aether_v1_ACLGrantRequest, _aether_v1_ACLGrantRequest__Output>
      ACLGroupInfo: MessageTypeDefinition<_aether_v1_ACLGroupInfo, _aether_v1_ACLGroupInfo__Output>
      ACLGroupMemberInfo: MessageTypeDefinition<_aether_v1_ACLGroupMemberInfo, _aether_v1_ACLGroupMemberInfo__Output>
      ACLGroupMemberRequest: MessageTypeDefinition<_aether_v1_ACLGroupMemberRequest, _aether_v1_ACLGroupMemberRequest__Output>
      ACLGroupRequest: MessageTypeDefinition<_aether_v1_ACLGroupRequest, _aether_v1_ACLGroupRequest__Output>
      ACLOperation: MessageTypeDefinition<_aether_v1_ACLOperation, _aether_v1_ACLOperation__Output>
      ACLRenewAuthorityGrantRequest: MessageTypeDefinition<_aether_v1_ACLRenewAuthorityGrantRequest, _aether_v1_ACLRenewAuthorityGrantRequest__Output>
      ACLResponse: MessageTypeDefinition<_aether_v1_ACLResponse, _aether_v1_ACLResponse__Output>
      ACLRoleAssignmentInfo: MessageTypeDefinition<_aether_v1_ACLRoleAssignmentInfo, _aether_v1_ACLRoleAssignmentInfo__Output>
      ACLRoleAssignmentRequest: MessageTypeDefinition<_aether_v1_ACLRoleAssignmentRequest, _aether_v1_ACLRoleAssignmentRequest__Output>
      ACLRoleInfo: MessageTypeDefinition<_aether_v1_ACLRoleInfo, _aether_v1_ACLRoleInfo__Output>
      ACLRoleRequest: MessageTypeDefinition<_aether_v1_ACLRoleRequest, _aether_v1_ACLRoleRequest__Output>
      ACLRuleFilter: MessageTypeDefinition<_aether_v1_ACLRuleFilter, _aether_v1_ACLRuleFilter__Output>
      ACLRuleInfo: MessageTypeDefinition<_aether_v1_ACLRuleInfo, _aether_v1_ACLRuleInfo__Output>
      ACLSetFallbackRequest: MessageTypeDefinition<_aether_v1_ACLSetFallbackRequest, _aether_v1_ACLSetFallbackRequest__Output>
      AccessLevel: EnumTypeDefinition
      AdminQuery: MessageTypeDefinition<_aether_v1_AdminQuery, _aether_v1_AdminQuery__Output>
      AdminResponse: MessageTypeDefinition<_aether_v1_AdminResponse, _aether_v1_AdminResponse__Output>
      AetherGateway: SubtypeConstructor<typeof grpc.Client, _aether_v1_AetherGatewayClient> & { service: _aether_v1_AetherGatewayDefinition }
      AgentFilter: MessageTypeDefinition<_aether_v1_AgentFilter, _aether_v1_AgentFilter__Output>
      AgentIdentity: MessageTypeDefinition<_aether_v1_AgentIdentity, _aether_v1_AgentIdentity__Output>
      AgentLaunchParams: MessageTypeDefinition<_aether_v1_AgentLaunchParams, _aether_v1_AgentLaunchParams__Output>
      AgentLaunchResult: MessageTypeDefinition<_aether_v1_AgentLaunchResult, _aether_v1_AgentLaunchResult__Output>
      AgentOperation: MessageTypeDefinition<_aether_v1_AgentOperation, _aether_v1_AgentOperation__Output>
      AgentRegistrationInfo: MessageTypeDefinition<_aether_v1_AgentRegistrationInfo, _aether_v1_AgentRegistrationInfo__Output>
      AgentResourceSchemaEntry: MessageTypeDefinition<_aether_v1_AgentResourceSchemaEntry, _aether_v1_AgentResourceSchemaEntry__Output>
      AgentResponse: MessageTypeDefinition<_aether_v1_AgentResponse, _aether_v1_AgentResponse__Output>
      AuditEntry: MessageTypeDefinition<_aether_v1_AuditEntry, _aether_v1_AuditEntry__Output>
      AuditQuery: MessageTypeDefinition<_aether_v1_AuditQuery, _aether_v1_AuditQuery__Output>
      AuditQueryResponse: MessageTypeDefinition<_aether_v1_AuditQueryResponse, _aether_v1_AuditQueryResponse__Output>
      AuthorityGrantBatchExchangeRequest: MessageTypeDefinition<_aether_v1_AuthorityGrantBatchExchangeRequest, _aether_v1_AuthorityGrantBatchExchangeRequest__Output>
      AuthorityGrantDeriveForTargetRequest: MessageTypeDefinition<_aether_v1_AuthorityGrantDeriveForTargetRequest, _aether_v1_AuthorityGrantDeriveForTargetRequest__Output>
      AuthorityGrantDeriveRequest: MessageTypeDefinition<_aether_v1_AuthorityGrantDeriveRequest, _aether_v1_AuthorityGrantDeriveRequest__Output>
      AuthorityGrantExchangeRequest: MessageTypeDefinition<_aether_v1_AuthorityGrantExchangeRequest, _aether_v1_AuthorityGrantExchangeRequest__Output>
      AuthorityGrantInfo: MessageTypeDefinition<_aether_v1_AuthorityGrantInfo, _aether_v1_AuthorityGrantInfo__Output>
      AuthorityGrantListRequest: MessageTypeDefinition<_aether_v1_AuthorityGrantListRequest, _aether_v1_AuthorityGrantListRequest__Output>
      AuthorityGrantOperation: MessageTypeDefinition<_aether_v1_AuthorityGrantOperation, _aether_v1_AuthorityGrantOperation__Output>
      AuthorityGrantResponse: MessageTypeDefinition<_aether_v1_AuthorityGrantResponse, _aether_v1_AuthorityGrantResponse__Output>
      AuthorityGrantRevocation: MessageTypeDefinition<_aether_v1_AuthorityGrantRevocation, _aether_v1_AuthorityGrantRevocation__Output>
      AuthorityIdentity: MessageTypeDefinition<_aether_v1_AuthorityIdentity, _aether_v1_AuthorityIdentity__Output>
      AuthorityRequest: MessageTypeDefinition<_aether_v1_AuthorityRequest, _aether_v1_AuthorityRequest__Output>
      AuthorityRequestEvent: MessageTypeDefinition<_aether_v1_AuthorityRequestEvent, _aether_v1_AuthorityRequestEvent__Output>
      AuthorityRequestListFilter: MessageTypeDefinition<_aether_v1_AuthorityRequestListFilter, _aether_v1_AuthorityRequestListFilter__Output>
      AuthorityRequestOperation: MessageTypeDefinition<_aether_v1_AuthorityRequestOperation, _aether_v1_AuthorityRequestOperation__Output>
      AuthorityRequestOperationResponse: MessageTypeDefinition<_aether_v1_AuthorityRequestOperationResponse, _aether_v1_AuthorityRequestOperationResponse__Output>
      AuthorityRequestResourceScopeEntry: MessageTypeDefinition<_aether_v1_AuthorityRequestResourceScopeEntry, _aether_v1_AuthorityRequestResourceScopeEntry__Output>
      AuthorityRequestRoutingTarget: MessageTypeDefinition<_aether_v1_AuthorityRequestRoutingTarget, _aether_v1_AuthorityRequestRoutingTarget__Output>
      AuthorityRequestStatus: EnumTypeDefinition
      AuthoritySpan: MessageTypeDefinition<_aether_v1_AuthoritySpan, _aether_v1_AuthoritySpan__Output>
      AuthorizationContext: MessageTypeDefinition<_aether_v1_AuthorizationContext, _aether_v1_AuthorizationContext__Output>
      BackoffStrategy: EnumTypeDefinition
      BridgeIdentity: MessageTypeDefinition<_aether_v1_BridgeIdentity, _aether_v1_BridgeIdentity__Output>
      BuildInfo: MessageTypeDefinition<_aether_v1_BuildInfo, _aether_v1_BuildInfo__Output>
      CheckpointOperation: MessageTypeDefinition<_aether_v1_CheckpointOperation, _aether_v1_CheckpointOperation__Output>
      CheckpointResponse: MessageTypeDefinition<_aether_v1_CheckpointResponse, _aether_v1_CheckpointResponse__Output>
      ConfigSnapshot: MessageTypeDefinition<_aether_v1_ConfigSnapshot, _aether_v1_ConfigSnapshot__Output>
      ConnectionAck: MessageTypeDefinition<_aether_v1_ConnectionAck, _aether_v1_ConnectionAck__Output>
      ConnectionFilter: MessageTypeDefinition<_aether_v1_ConnectionFilter, _aether_v1_ConnectionFilter__Output>
      ConnectionInfo: MessageTypeDefinition<_aether_v1_ConnectionInfo, _aether_v1_ConnectionInfo__Output>
      ConnectionStatusRequest: MessageTypeDefinition<_aether_v1_ConnectionStatusRequest, _aether_v1_ConnectionStatusRequest__Output>
      ConnectionStatusResponse: MessageTypeDefinition<_aether_v1_ConnectionStatusResponse, _aether_v1_ConnectionStatusResponse__Output>
      CreateAuthorityRequestPayload: MessageTypeDefinition<_aether_v1_CreateAuthorityRequestPayload, _aether_v1_CreateAuthorityRequestPayload__Output>
      CreateTaskRequest: MessageTypeDefinition<_aether_v1_CreateTaskRequest, _aether_v1_CreateTaskRequest__Output>
      CreateTaskResponse: MessageTypeDefinition<_aether_v1_CreateTaskResponse, _aether_v1_CreateTaskResponse__Output>
      DownstreamMessage: MessageTypeDefinition<_aether_v1_DownstreamMessage, _aether_v1_DownstreamMessage__Output>
      ErrorResponse: MessageTypeDefinition<_aether_v1_ErrorResponse, _aether_v1_ErrorResponse__Output>
      ExtensionDeclaration: MessageTypeDefinition<_aether_v1_ExtensionDeclaration, _aether_v1_ExtensionDeclaration__Output>
      FlowEdge: MessageTypeDefinition<_aether_v1_FlowEdge, _aether_v1_FlowEdge__Output>
      FlowNode: MessageTypeDefinition<_aether_v1_FlowNode, _aether_v1_FlowNode__Output>
      GatewayInfo: MessageTypeDefinition<_aether_v1_GatewayInfo, _aether_v1_GatewayInfo__Output>
      GatewayStats: MessageTypeDefinition<_aether_v1_GatewayStats, _aether_v1_GatewayStats__Output>
      HealthCheck: MessageTypeDefinition<_aether_v1_HealthCheck, _aether_v1_HealthCheck__Output>
      HealthCheckStatus: EnumTypeDefinition
      HealthInfo: MessageTypeDefinition<_aether_v1_HealthInfo, _aether_v1_HealthInfo__Output>
      HealthStatus: EnumTypeDefinition
      HibernationDescriptor: MessageTypeDefinition<_aether_v1_HibernationDescriptor, _aether_v1_HibernationDescriptor__Output>
      IncomingMessage: MessageTypeDefinition<_aether_v1_IncomingMessage, _aether_v1_IncomingMessage__Output>
      InitConnection: MessageTypeDefinition<_aether_v1_InitConnection, _aether_v1_InitConnection__Output>
      KVOperation: MessageTypeDefinition<_aether_v1_KVOperation, _aether_v1_KVOperation__Output>
      KVResponse: MessageTypeDefinition<_aether_v1_KVResponse, _aether_v1_KVResponse__Output>
      MessageEnvelope: MessageTypeDefinition<_aether_v1_MessageEnvelope, _aether_v1_MessageEnvelope__Output>
      MessageFlowInfo: MessageTypeDefinition<_aether_v1_MessageFlowInfo, _aether_v1_MessageFlowInfo__Output>
      MessageType: EnumTypeDefinition
      Metric: MessageTypeDefinition<_aether_v1_Metric, _aether_v1_Metric__Output>
      MetricEntry: MessageTypeDefinition<_aether_v1_MetricEntry, _aether_v1_MetricEntry__Output>
      MetricsBridgeIdentity: MessageTypeDefinition<_aether_v1_MetricsBridgeIdentity, _aether_v1_MetricsBridgeIdentity__Output>
      NegotiatedExtension: MessageTypeDefinition<_aether_v1_NegotiatedExtension, _aether_v1_NegotiatedExtension__Output>
      OrchestratorIdentity: MessageTypeDefinition<_aether_v1_OrchestratorIdentity, _aether_v1_OrchestratorIdentity__Output>
      OrchestratorInfo: MessageTypeDefinition<_aether_v1_OrchestratorInfo, _aether_v1_OrchestratorInfo__Output>
      PrincipalRef: MessageTypeDefinition<_aether_v1_PrincipalRef, _aether_v1_PrincipalRef__Output>
      PrincipalType: EnumTypeDefinition
      ProgressKind: EnumTypeDefinition
      ProgressReport: MessageTypeDefinition<_aether_v1_ProgressReport, _aether_v1_ProgressReport__Output>
      ProgressStep: MessageTypeDefinition<_aether_v1_ProgressStep, _aether_v1_ProgressStep__Output>
      ProgressUpdate: MessageTypeDefinition<_aether_v1_ProgressUpdate, _aether_v1_ProgressUpdate__Output>
      ProxyError: MessageTypeDefinition<_aether_v1_ProxyError, _aether_v1_ProxyError__Output>
      ProxyHttpBodyChunk: MessageTypeDefinition<_aether_v1_ProxyHttpBodyChunk, _aether_v1_ProxyHttpBodyChunk__Output>
      ProxyHttpRequest: MessageTypeDefinition<_aether_v1_ProxyHttpRequest, _aether_v1_ProxyHttpRequest__Output>
      ProxyHttpResponse: MessageTypeDefinition<_aether_v1_ProxyHttpResponse, _aether_v1_ProxyHttpResponse__Output>
      ResolveAuthorityRequest: MessageTypeDefinition<_aether_v1_ResolveAuthorityRequest, _aether_v1_ResolveAuthorityRequest__Output>
      ResolveAuthorityRequestPayload: MessageTypeDefinition<_aether_v1_ResolveAuthorityRequestPayload, _aether_v1_ResolveAuthorityRequestPayload__Output>
      ResolveAuthorityResponse: MessageTypeDefinition<_aether_v1_ResolveAuthorityResponse, _aether_v1_ResolveAuthorityResponse__Output>
      ResolvedAuthority: MessageTypeDefinition<_aether_v1_ResolvedAuthority, _aether_v1_ResolvedAuthority__Output>
      ResolvedAuthorityInfo: MessageTypeDefinition<_aether_v1_ResolvedAuthorityInfo, _aether_v1_ResolvedAuthorityInfo__Output>
      RetryPolicy: MessageTypeDefinition<_aether_v1_RetryPolicy, _aether_v1_RetryPolicy__Output>
      SendMessage: MessageTypeDefinition<_aether_v1_SendMessage, _aether_v1_SendMessage__Output>
      ServiceIdentity: MessageTypeDefinition<_aether_v1_ServiceIdentity, _aether_v1_ServiceIdentity__Output>
      SessionOperation: MessageTypeDefinition<_aether_v1_SessionOperation, _aether_v1_SessionOperation__Output>
      SessionOperationResponse: MessageTypeDefinition<_aether_v1_SessionOperationResponse, _aether_v1_SessionOperationResponse__Output>
      Signal: MessageTypeDefinition<_aether_v1_Signal, _aether_v1_Signal__Output>
      SubmitAuditEventRequest: MessageTypeDefinition<_aether_v1_SubmitAuditEventRequest, _aether_v1_SubmitAuditEventRequest__Output>
      SubmitAuditEventResponse: MessageTypeDefinition<_aether_v1_SubmitAuditEventResponse, _aether_v1_SubmitAuditEventResponse__Output>
      SwitchWorkspace: MessageTypeDefinition<_aether_v1_SwitchWorkspace, _aether_v1_SwitchWorkspace__Output>
      TaskAssignment: MessageTypeDefinition<_aether_v1_TaskAssignment, _aether_v1_TaskAssignment__Output>
      TaskAssignmentMode: EnumTypeDefinition
      TaskAuthorityRequestEventRelay: MessageTypeDefinition<_aether_v1_TaskAuthorityRequestEventRelay, _aether_v1_TaskAuthorityRequestEventRelay__Output>
      TaskChildLifecycleEvent: MessageTypeDefinition<_aether_v1_TaskChildLifecycleEvent, _aether_v1_TaskChildLifecycleEvent__Output>
      TaskClass: EnumTypeDefinition
      TaskCompletionEvent: MessageTypeDefinition<_aether_v1_TaskCompletionEvent, _aether_v1_TaskCompletionEvent__Output>
      TaskEvent: MessageTypeDefinition<_aether_v1_TaskEvent, _aether_v1_TaskEvent__Output>
      TaskFilter: MessageTypeDefinition<_aether_v1_TaskFilter, _aether_v1_TaskFilter__Output>
      TaskHibernated: MessageTypeDefinition<_aether_v1_TaskHibernated, _aether_v1_TaskHibernated__Output>
      TaskIdentity: MessageTypeDefinition<_aether_v1_TaskIdentity, _aether_v1_TaskIdentity__Output>
      TaskInfo: MessageTypeDefinition<_aether_v1_TaskInfo, _aether_v1_TaskInfo__Output>
      TaskOperation: MessageTypeDefinition<_aether_v1_TaskOperation, _aether_v1_TaskOperation__Output>
      TaskOperationResponse: MessageTypeDefinition<_aether_v1_TaskOperationResponse, _aether_v1_TaskOperationResponse__Output>
      TaskPriority: EnumTypeDefinition
      TaskProgressEvent: MessageTypeDefinition<_aether_v1_TaskProgressEvent, _aether_v1_TaskProgressEvent__Output>
      TaskQuery: MessageTypeDefinition<_aether_v1_TaskQuery, _aether_v1_TaskQuery__Output>
      TaskQueryResponse: MessageTypeDefinition<_aether_v1_TaskQueryResponse, _aether_v1_TaskQueryResponse__Output>
      TaskStatus: EnumTypeDefinition
      TaskStatusChangedEvent: MessageTypeDefinition<_aether_v1_TaskStatusChangedEvent, _aether_v1_TaskStatusChangedEvent__Output>
      TaskSubscriptionOperation: MessageTypeDefinition<_aether_v1_TaskSubscriptionOperation, _aether_v1_TaskSubscriptionOperation__Output>
      TaskSubscriptionOperationResponse: MessageTypeDefinition<_aether_v1_TaskSubscriptionOperationResponse, _aether_v1_TaskSubscriptionOperationResponse__Output>
      TokenCreateRequest: MessageTypeDefinition<_aether_v1_TokenCreateRequest, _aether_v1_TokenCreateRequest__Output>
      TokenFilter: MessageTypeDefinition<_aether_v1_TokenFilter, _aether_v1_TokenFilter__Output>
      TokenInfo: MessageTypeDefinition<_aether_v1_TokenInfo, _aether_v1_TokenInfo__Output>
      TokenOperation: MessageTypeDefinition<_aether_v1_TokenOperation, _aether_v1_TokenOperation__Output>
      TokenResponse: MessageTypeDefinition<_aether_v1_TokenResponse, _aether_v1_TokenResponse__Output>
      TunnelAck: MessageTypeDefinition<_aether_v1_TunnelAck, _aether_v1_TunnelAck__Output>
      TunnelClose: MessageTypeDefinition<_aether_v1_TunnelClose, _aether_v1_TunnelClose__Output>
      TunnelData: MessageTypeDefinition<_aether_v1_TunnelData, _aether_v1_TunnelData__Output>
      TunnelOpen: MessageTypeDefinition<_aether_v1_TunnelOpen, _aether_v1_TunnelOpen__Output>
      UpstreamMessage: MessageTypeDefinition<_aether_v1_UpstreamMessage, _aether_v1_UpstreamMessage__Output>
      UserIdentity: MessageTypeDefinition<_aether_v1_UserIdentity, _aether_v1_UserIdentity__Output>
      WaitReason: EnumTypeDefinition
      WaitSpec: MessageTypeDefinition<_aether_v1_WaitSpec, _aether_v1_WaitSpec__Output>
      WorkflowEngineIdentity: MessageTypeDefinition<_aether_v1_WorkflowEngineIdentity, _aether_v1_WorkflowEngineIdentity__Output>
      WorkflowOperation: MessageTypeDefinition<_aether_v1_WorkflowOperation, _aether_v1_WorkflowOperation__Output>
      WorkflowResponse: MessageTypeDefinition<_aether_v1_WorkflowResponse, _aether_v1_WorkflowResponse__Output>
      WorkspaceFilter: MessageTypeDefinition<_aether_v1_WorkspaceFilter, _aether_v1_WorkspaceFilter__Output>
      WorkspaceInfo: MessageTypeDefinition<_aether_v1_WorkspaceInfo, _aether_v1_WorkspaceInfo__Output>
      WorkspaceOperation: MessageTypeDefinition<_aether_v1_WorkspaceOperation, _aether_v1_WorkspaceOperation__Output>
      WorkspaceResponse: MessageTypeDefinition<_aether_v1_WorkspaceResponse, _aether_v1_WorkspaceResponse__Output>
    }
  }
}

