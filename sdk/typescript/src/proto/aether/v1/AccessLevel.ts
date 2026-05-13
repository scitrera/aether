// Original file: aether.proto

/**
 * AccessLevel enumerates permission levels for ACL rules.
 */
export const AccessLevel = {
  ACCESS_LEVEL_UNSPECIFIED: 'ACCESS_LEVEL_UNSPECIFIED',
  /**
   * 0 in legacy int32
   */
  ACCESS_LEVEL_NONE: 'ACCESS_LEVEL_NONE',
  /**
   * 10 in legacy int32
   */
  ACCESS_LEVEL_READ: 'ACCESS_LEVEL_READ',
  /**
   * 20 in legacy int32
   */
  ACCESS_LEVEL_READWRITE: 'ACCESS_LEVEL_READWRITE',
  /**
   * 30 in legacy int32
   */
  ACCESS_LEVEL_MANAGE: 'ACCESS_LEVEL_MANAGE',
  /**
   * 40 in legacy int32
   */
  ACCESS_LEVEL_ADMIN: 'ACCESS_LEVEL_ADMIN',
  /**
   * 50 in legacy int32
   */
  ACCESS_LEVEL_SUPERADMIN: 'ACCESS_LEVEL_SUPERADMIN',
} as const;

/**
 * AccessLevel enumerates permission levels for ACL rules.
 */
export type AccessLevel =
  | 'ACCESS_LEVEL_UNSPECIFIED'
  | 0
  /**
   * 0 in legacy int32
   */
  | 'ACCESS_LEVEL_NONE'
  | 1
  /**
   * 10 in legacy int32
   */
  | 'ACCESS_LEVEL_READ'
  | 2
  /**
   * 20 in legacy int32
   */
  | 'ACCESS_LEVEL_READWRITE'
  | 3
  /**
   * 30 in legacy int32
   */
  | 'ACCESS_LEVEL_MANAGE'
  | 4
  /**
   * 40 in legacy int32
   */
  | 'ACCESS_LEVEL_ADMIN'
  | 5
  /**
   * 50 in legacy int32
   */
  | 'ACCESS_LEVEL_SUPERADMIN'
  | 6

/**
 * AccessLevel enumerates permission levels for ACL rules.
 */
export type AccessLevel__Output = typeof AccessLevel[keyof typeof AccessLevel]
