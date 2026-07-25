// Original file: aether.proto


/**
 * ACLGroupRequest contains data for creating a group.
 */
export interface ACLGroupRequest {
  'name'?: (string);
  'description'?: (string);
  'createdBy'?: (string);
  'metadata'?: ({[key: string]: string});
}

/**
 * ACLGroupRequest contains data for creating a group.
 */
export interface ACLGroupRequest__Output {
  'name': (string);
  'description': (string);
  'createdBy': (string);
  'metadata': ({[key: string]: string});
}
