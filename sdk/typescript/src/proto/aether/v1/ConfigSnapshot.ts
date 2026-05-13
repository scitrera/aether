// Original file: aether.proto


export interface ConfigSnapshot {
  /**
   * Legacy fields. The server stops auto-populating these as part of the
   * KV scope revamp — shared workspace/global data must be queried on
   * demand via KVOperation. Old SDK clients deserializing these will see
   * empty maps; no consumer relied on the eager hydration path.
   * @deprecated
   */
  'kv'?: ({[key: string]: Buffer | Uint8Array | string});
  /**
   * legacy: was global-shared
   * @deprecated
   */
  'globalKv'?: ({[key: string]: Buffer | Uint8Array | string});
  /**
   * Per-agent task context populated from TaskAssignment on connect.
   * Contains: task_id, workspace, user, implementation, specifier, profile,
   * plus all metadata entries and launch_params (prefixed with "lp.").
   * Empty if no TaskAssignment exists for the connecting identity.
   */
  'taskContext'?: ({[key: string]: string});
  /**
   * Per-agent (exclusive) baseline KV pre-loaded at connect time.
   */
  'workspaceExclusiveKv'?: ({[key: string]: Buffer | Uint8Array | string});
  /**
   * per-agent, tenant-wide
   */
  'globalExclusiveKv'?: ({[key: string]: Buffer | Uint8Array | string});
}

export interface ConfigSnapshot__Output {
  /**
   * Legacy fields. The server stops auto-populating these as part of the
   * KV scope revamp — shared workspace/global data must be queried on
   * demand via KVOperation. Old SDK clients deserializing these will see
   * empty maps; no consumer relied on the eager hydration path.
   * @deprecated
   */
  'kv': ({[key: string]: Buffer});
  /**
   * legacy: was global-shared
   * @deprecated
   */
  'globalKv': ({[key: string]: Buffer});
  /**
   * Per-agent task context populated from TaskAssignment on connect.
   * Contains: task_id, workspace, user, implementation, specifier, profile,
   * plus all metadata entries and launch_params (prefixed with "lp.").
   * Empty if no TaskAssignment exists for the connecting identity.
   */
  'taskContext': ({[key: string]: string});
  /**
   * Per-agent (exclusive) baseline KV pre-loaded at connect time.
   */
  'workspaceExclusiveKv': ({[key: string]: Buffer});
  /**
   * per-agent, tenant-wide
   */
  'globalExclusiveKv': ({[key: string]: Buffer});
}
