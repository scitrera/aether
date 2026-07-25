// Original file: aether.proto


/**
 * ACLRoleRequest contains data for creating a role.
 */
export interface ACLRoleRequest {
  'name'?: (string);
  'description'?: (string);
  'createdBy'?: (string);
  'metadata'?: ({[key: string]: string});
}

/**
 * ACLRoleRequest contains data for creating a role.
 */
export interface ACLRoleRequest__Output {
  'name': (string);
  'description': (string);
  'createdBy': (string);
  'metadata': ({[key: string]: string});
}
