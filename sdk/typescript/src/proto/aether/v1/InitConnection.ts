// Original file: aether.proto

import type { AgentIdentity as _aether_v1_AgentIdentity, AgentIdentity__Output as _aether_v1_AgentIdentity__Output } from '../../aether/v1/AgentIdentity';
import type { TaskIdentity as _aether_v1_TaskIdentity, TaskIdentity__Output as _aether_v1_TaskIdentity__Output } from '../../aether/v1/TaskIdentity';
import type { UserIdentity as _aether_v1_UserIdentity, UserIdentity__Output as _aether_v1_UserIdentity__Output } from '../../aether/v1/UserIdentity';
import type { OrchestratorIdentity as _aether_v1_OrchestratorIdentity, OrchestratorIdentity__Output as _aether_v1_OrchestratorIdentity__Output } from '../../aether/v1/OrchestratorIdentity';
import type { WorkflowEngineIdentity as _aether_v1_WorkflowEngineIdentity, WorkflowEngineIdentity__Output as _aether_v1_WorkflowEngineIdentity__Output } from '../../aether/v1/WorkflowEngineIdentity';
import type { MetricsBridgeIdentity as _aether_v1_MetricsBridgeIdentity, MetricsBridgeIdentity__Output as _aether_v1_MetricsBridgeIdentity__Output } from '../../aether/v1/MetricsBridgeIdentity';
import type { BridgeIdentity as _aether_v1_BridgeIdentity, BridgeIdentity__Output as _aether_v1_BridgeIdentity__Output } from '../../aether/v1/BridgeIdentity';
import type { ServiceIdentity as _aether_v1_ServiceIdentity, ServiceIdentity__Output as _aether_v1_ServiceIdentity__Output } from '../../aether/v1/ServiceIdentity';

export interface InitConnection {
  'agent'?: (_aether_v1_AgentIdentity | null);
  'task'?: (_aether_v1_TaskIdentity | null);
  'user'?: (_aether_v1_UserIdentity | null);
  'orchestrator'?: (_aether_v1_OrchestratorIdentity | null);
  'workflowEngine'?: (_aether_v1_WorkflowEngineIdentity | null);
  'metricsBridge'?: (_aether_v1_MetricsBridgeIdentity | null);
  'bridge'?: (_aether_v1_BridgeIdentity | null);
  'service'?: (_aether_v1_ServiceIdentity | null);
  'credentials'?: ({[key: string]: string});
  /**
   * Optional: session ID from a previous connection to resume.
   * If provided and the lock still exists with this session ID,
   * the connection will take over the existing lock instead of failing.
   */
  'resumeSessionId'?: (string);
  'clientType'?: "agent"|"task"|"user"|"orchestrator"|"workflowEngine"|"metricsBridge"|"bridge"|"service";
}

export interface InitConnection__Output {
  'agent'?: (_aether_v1_AgentIdentity__Output | null);
  'task'?: (_aether_v1_TaskIdentity__Output | null);
  'user'?: (_aether_v1_UserIdentity__Output | null);
  'orchestrator'?: (_aether_v1_OrchestratorIdentity__Output | null);
  'workflowEngine'?: (_aether_v1_WorkflowEngineIdentity__Output | null);
  'metricsBridge'?: (_aether_v1_MetricsBridgeIdentity__Output | null);
  'bridge'?: (_aether_v1_BridgeIdentity__Output | null);
  'service'?: (_aether_v1_ServiceIdentity__Output | null);
  'credentials': ({[key: string]: string});
  /**
   * Optional: session ID from a previous connection to resume.
   * If provided and the lock still exists with this session ID,
   * the connection will take over the existing lock instead of failing.
   */
  'resumeSessionId': (string);
  'clientType'?: "agent"|"task"|"user"|"orchestrator"|"workflowEngine"|"metricsBridge"|"bridge"|"service";
}
