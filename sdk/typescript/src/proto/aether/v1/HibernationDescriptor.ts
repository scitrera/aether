// Original file: aether.proto


/**
 * HibernationDescriptor captures the parameters needed to release a worker on
 * hibernation and re-spawn it with full state on wake.
 */
export interface HibernationDescriptor {
  /**
   * Required: the checkpoint key the worker SAVE'd before requesting
   * hibernation. The gateway validates this checkpoint exists before allowing
   * the HIBERNATE transition. On wake, the next TaskAssignment carries this
   * key so the spawned worker can LOAD it.
   */
  'checkpointKey'?: (string);
  /**
   * Optional: session id the worker held when hibernating. If set, the new
   * worker should resume that session rather than create a fresh one.
   */
  'resumeSessionId'?: (string);
  /**
   * Optional: additional wake-event triggers (beyond timer / scheduled wake).
   * Reserved for future use; the Phase 1 waker honors scheduled_wake_unix_ms
   * and timeout_ms on the parent WaitSpec already.
   */
  'wakeEventTypes'?: (string)[];
  /**
   * Optional escalation policy when max duration is hit and no wake event has
   * fired. Default: "fail" (transition task to FAILED with reason "hibernation timeout").
   * Other allowed values: "retry" (reset to pending), "alert" (status stays but emit event).
   */
  'escalationPolicy'?: (string);
}

/**
 * HibernationDescriptor captures the parameters needed to release a worker on
 * hibernation and re-spawn it with full state on wake.
 */
export interface HibernationDescriptor__Output {
  /**
   * Required: the checkpoint key the worker SAVE'd before requesting
   * hibernation. The gateway validates this checkpoint exists before allowing
   * the HIBERNATE transition. On wake, the next TaskAssignment carries this
   * key so the spawned worker can LOAD it.
   */
  'checkpointKey': (string);
  /**
   * Optional: session id the worker held when hibernating. If set, the new
   * worker should resume that session rather than create a fresh one.
   */
  'resumeSessionId': (string);
  /**
   * Optional: additional wake-event triggers (beyond timer / scheduled wake).
   * Reserved for future use; the Phase 1 waker honors scheduled_wake_unix_ms
   * and timeout_ms on the parent WaitSpec already.
   */
  'wakeEventTypes': (string)[];
  /**
   * Optional escalation policy when max duration is hit and no wake event has
   * fired. Default: "fail" (transition task to FAILED with reason "hibernation timeout").
   * Other allowed values: "retry" (reset to pending), "alert" (status stays but emit event).
   */
  'escalationPolicy': (string);
}
