// Original file: aether.proto

export const WorkflowAuthorityLifetimeMode = {
  WORKFLOW_AUTHORITY_LIFETIME_SOURCE_BOUND: 'WORKFLOW_AUTHORITY_LIFETIME_SOURCE_BOUND',
  WORKFLOW_AUTHORITY_LIFETIME_DURABLE: 'WORKFLOW_AUTHORITY_LIFETIME_DURABLE',
} as const;

export type WorkflowAuthorityLifetimeMode =
  | 'WORKFLOW_AUTHORITY_LIFETIME_SOURCE_BOUND'
  | 0
  | 'WORKFLOW_AUTHORITY_LIFETIME_DURABLE'
  | 1

export type WorkflowAuthorityLifetimeMode__Output = typeof WorkflowAuthorityLifetimeMode[keyof typeof WorkflowAuthorityLifetimeMode]
