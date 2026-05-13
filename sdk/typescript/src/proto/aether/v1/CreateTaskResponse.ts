// Original file: aether.proto


/**
 * CreateTaskResponse is sent in response to CreateTaskRequest when the
 * request carries a non-empty request_id. Gives the creator the server-
 * assigned task_id so it can later COMPLETE/FAIL/CANCEL the task.
 */
export interface CreateTaskResponse {
  'success'?: (boolean);
  /**
   * server-assigned task ID on success
   */
  'taskId'?: (string);
  /**
   * e.g., "pending", "assigned", "pending_pool"
   */
  'status'?: (string);
  /**
   * populated on failure: e.g. "ERR_PERMISSION_DENIED"
   */
  'errorCode'?: (string);
  /**
   * human-readable error message
   */
  'errorMessage'?: (string);
  /**
   * Echoed from the originating CreateTaskRequest for correlation
   */
  'requestId'?: (string);
  /**
   * Optional: for TARGETED tasks successfully delivered, the receiving
   * identity string. Empty for SELF_ASSIGN or if not yet assigned.
   */
  'assignedTo'?: (string);
  /**
   * Plaintext task auth token (HMAC-stored server-side, 24h TTL). Populated
   * ONLY when CreateTaskRequest.target_identity was non-empty AND the issue-
   * token ACL check passed (creator has AccessManage on the workspace, the
   * target_identity's workspace matches the task workspace, and the
   * workspace is not platform-blocked). The worker presents this token via
   * Credentials{"token": ...} at connection init to authenticate as the
   * declared target_identity. Plaintext is only available here; the server
   * stores HMAC-SHA256 only. Empty string means "no token issued" — either
   * not requested (no target_identity), or the issue-token check denied it
   * (the underlying CreateTask still succeeded, audit log records the
   * denial). Token is auto-revoked when RevokeTokensForTask runs at task
   * completion.
   */
  'taskToken'?: (string);
  /**
   * Authority grant ID derived for this task by establishTaskAuthorityGrant.
   * Populated only when the caller passed an AuthorizationContext that
   * resolved to an OBO subject (otherwise empty). Lets the creator forward
   * this grant to a downstream worker that needs to act with the task's
   * exact per-task workspace/scope — for example, the platform-server
   * ws-server stamps it onto the chat_message envelope so the running
   * CoworkAgent uses the per-message grant (which reflects the user's
   * current workspace) instead of its long-lived startup-task grant
   * (which was minted at agent boot with a stale workspace list).
   */
  'authorityGrantId'?: (string);
}

/**
 * CreateTaskResponse is sent in response to CreateTaskRequest when the
 * request carries a non-empty request_id. Gives the creator the server-
 * assigned task_id so it can later COMPLETE/FAIL/CANCEL the task.
 */
export interface CreateTaskResponse__Output {
  'success': (boolean);
  /**
   * server-assigned task ID on success
   */
  'taskId': (string);
  /**
   * e.g., "pending", "assigned", "pending_pool"
   */
  'status': (string);
  /**
   * populated on failure: e.g. "ERR_PERMISSION_DENIED"
   */
  'errorCode': (string);
  /**
   * human-readable error message
   */
  'errorMessage': (string);
  /**
   * Echoed from the originating CreateTaskRequest for correlation
   */
  'requestId': (string);
  /**
   * Optional: for TARGETED tasks successfully delivered, the receiving
   * identity string. Empty for SELF_ASSIGN or if not yet assigned.
   */
  'assignedTo': (string);
  /**
   * Plaintext task auth token (HMAC-stored server-side, 24h TTL). Populated
   * ONLY when CreateTaskRequest.target_identity was non-empty AND the issue-
   * token ACL check passed (creator has AccessManage on the workspace, the
   * target_identity's workspace matches the task workspace, and the
   * workspace is not platform-blocked). The worker presents this token via
   * Credentials{"token": ...} at connection init to authenticate as the
   * declared target_identity. Plaintext is only available here; the server
   * stores HMAC-SHA256 only. Empty string means "no token issued" — either
   * not requested (no target_identity), or the issue-token check denied it
   * (the underlying CreateTask still succeeded, audit log records the
   * denial). Token is auto-revoked when RevokeTokensForTask runs at task
   * completion.
   */
  'taskToken': (string);
  /**
   * Authority grant ID derived for this task by establishTaskAuthorityGrant.
   * Populated only when the caller passed an AuthorizationContext that
   * resolved to an OBO subject (otherwise empty). Lets the creator forward
   * this grant to a downstream worker that needs to act with the task's
   * exact per-task workspace/scope — for example, the platform-server
   * ws-server stamps it onto the chat_message envelope so the running
   * CoworkAgent uses the per-message grant (which reflects the user's
   * current workspace) instead of its long-lived startup-task grant
   * (which was minted at agent boot with a stale workspace list).
   */
  'authorityGrantId': (string);
}
