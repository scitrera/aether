// Original file: aether.proto

/**
 * AuthorityRequestStatus tracks the lifecycle of an AuthorityRequest.
 */
export const AuthorityRequestStatus = {
  AUTHORITY_REQUEST_STATUS_UNSPECIFIED: 'AUTHORITY_REQUEST_STATUS_UNSPECIFIED',
  /**
   * awaiting an approver
   */
  AUTHORITY_REQUEST_STATUS_PENDING: 'AUTHORITY_REQUEST_STATUS_PENDING',
  /**
   * resolved approved -- a grant has been minted
   */
  AUTHORITY_REQUEST_STATUS_APPROVED: 'AUTHORITY_REQUEST_STATUS_APPROVED',
  /**
   * resolved denied with reason
   */
  AUTHORITY_REQUEST_STATUS_DENIED: 'AUTHORITY_REQUEST_STATUS_DENIED',
  /**
   * never resolved within the timeout window
   */
  AUTHORITY_REQUEST_STATUS_EXPIRED: 'AUTHORITY_REQUEST_STATUS_EXPIRED',
  /**
   * requester withdrew / task was cancelled
   */
  AUTHORITY_REQUEST_STATUS_CANCELLED: 'AUTHORITY_REQUEST_STATUS_CANCELLED',
} as const;

/**
 * AuthorityRequestStatus tracks the lifecycle of an AuthorityRequest.
 */
export type AuthorityRequestStatus =
  | 'AUTHORITY_REQUEST_STATUS_UNSPECIFIED'
  | 0
  /**
   * awaiting an approver
   */
  | 'AUTHORITY_REQUEST_STATUS_PENDING'
  | 1
  /**
   * resolved approved -- a grant has been minted
   */
  | 'AUTHORITY_REQUEST_STATUS_APPROVED'
  | 2
  /**
   * resolved denied with reason
   */
  | 'AUTHORITY_REQUEST_STATUS_DENIED'
  | 3
  /**
   * never resolved within the timeout window
   */
  | 'AUTHORITY_REQUEST_STATUS_EXPIRED'
  | 4
  /**
   * requester withdrew / task was cancelled
   */
  | 'AUTHORITY_REQUEST_STATUS_CANCELLED'
  | 5

/**
 * AuthorityRequestStatus tracks the lifecycle of an AuthorityRequest.
 */
export type AuthorityRequestStatus__Output = typeof AuthorityRequestStatus[keyof typeof AuthorityRequestStatus]
