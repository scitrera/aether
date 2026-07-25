// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * ACLGroupInfo represents a group definition.
 */
export interface ACLGroupInfo {
  'groupId'?: (string);
  'groupName'?: (string);
  'description'?: (string);
  'createdBy'?: (string);
  'createdAt'?: (number | string | Long);
  'metadata'?: ({[key: string]: string});
}

/**
 * ACLGroupInfo represents a group definition.
 */
export interface ACLGroupInfo__Output {
  'groupId': (string);
  'groupName': (string);
  'description': (string);
  'createdBy': (string);
  'createdAt': (string);
  'metadata': ({[key: string]: string});
}
