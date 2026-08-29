// Original file: aether.proto

/**
 * Controls what TARGETED task creation does when the exact target identity is
 * not connected. UNSPECIFIED deliberately preserves the released behavior:
 * validate the implementation and ask an orchestrator to start the worker.
 */
export const TargetOfflinePolicy = {
  TARGET_OFFLINE_POLICY_UNSPECIFIED: 'TARGET_OFFLINE_POLICY_UNSPECIFIED',
  TARGET_OFFLINE_POLICY_ORCHESTRATE: 'TARGET_OFFLINE_POLICY_ORCHESTRATE',
  TARGET_OFFLINE_POLICY_QUEUE: 'TARGET_OFFLINE_POLICY_QUEUE',
  TARGET_OFFLINE_POLICY_REJECT: 'TARGET_OFFLINE_POLICY_REJECT',
} as const;

/**
 * Controls what TARGETED task creation does when the exact target identity is
 * not connected. UNSPECIFIED deliberately preserves the released behavior:
 * validate the implementation and ask an orchestrator to start the worker.
 */
export type TargetOfflinePolicy =
  | 'TARGET_OFFLINE_POLICY_UNSPECIFIED'
  | 0
  | 'TARGET_OFFLINE_POLICY_ORCHESTRATE'
  | 1
  | 'TARGET_OFFLINE_POLICY_QUEUE'
  | 2
  | 'TARGET_OFFLINE_POLICY_REJECT'
  | 3

/**
 * Controls what TARGETED task creation does when the exact target identity is
 * not connected. UNSPECIFIED deliberately preserves the released behavior:
 * validate the implementation and ask an orchestrator to start the worker.
 */
export type TargetOfflinePolicy__Output = typeof TargetOfflinePolicy[keyof typeof TargetOfflinePolicy]
