// Original file: aether.proto

/**
 * PrincipalType enumerates the types of principals that can connect to the gateway.
 */
export const PrincipalType = {
  PRINCIPAL_TYPE_UNSPECIFIED: 'PRINCIPAL_TYPE_UNSPECIFIED',
  PRINCIPAL_AGENT: 'PRINCIPAL_AGENT',
  PRINCIPAL_TASK: 'PRINCIPAL_TASK',
  PRINCIPAL_USER: 'PRINCIPAL_USER',
  PRINCIPAL_ORCHESTRATOR: 'PRINCIPAL_ORCHESTRATOR',
  PRINCIPAL_WORKFLOW_ENGINE: 'PRINCIPAL_WORKFLOW_ENGINE',
  PRINCIPAL_METRICS_BRIDGE: 'PRINCIPAL_METRICS_BRIDGE',
  PRINCIPAL_BRIDGE: 'PRINCIPAL_BRIDGE',
  PRINCIPAL_SERVICE: 'PRINCIPAL_SERVICE',
} as const;

/**
 * PrincipalType enumerates the types of principals that can connect to the gateway.
 */
export type PrincipalType =
  | 'PRINCIPAL_TYPE_UNSPECIFIED'
  | 0
  | 'PRINCIPAL_AGENT'
  | 1
  | 'PRINCIPAL_TASK'
  | 2
  | 'PRINCIPAL_USER'
  | 3
  | 'PRINCIPAL_ORCHESTRATOR'
  | 4
  | 'PRINCIPAL_WORKFLOW_ENGINE'
  | 5
  | 'PRINCIPAL_METRICS_BRIDGE'
  | 6
  | 'PRINCIPAL_BRIDGE'
  | 7
  | 'PRINCIPAL_SERVICE'
  | 8

/**
 * PrincipalType enumerates the types of principals that can connect to the gateway.
 */
export type PrincipalType__Output = typeof PrincipalType[keyof typeof PrincipalType]
