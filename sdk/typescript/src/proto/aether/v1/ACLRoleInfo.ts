// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLRoleInfo represents a role definition.
 */
export interface ACLRoleInfo {
  'roleId'?: (string);
  'roleName'?: (string);
  'description'?: (string);
  'createdBy'?: (string);
  'createdAt'?: (number | string | Long);
  'metadata'?: ({[key: string]: string});
}

/**
 * ACLRoleInfo represents a role definition.
 */
export interface ACLRoleInfo__Output {
  'roleId': (string);
  'roleName': (string);
  'description': (string);
  'createdBy': (string);
  'createdAt': (string);
  'metadata': ({[key: string]: string});
}
