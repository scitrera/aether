// Original file: aether.proto


/**
 * ResourceAccessRequest is the portable runtime authorization tuple evaluated
 * by the gateway. It is intentionally independent of any tool protocol: the
 * same primitive gates workspace views, catalog providers/entries, and future
 * logical resources. All string fields are required except workspace.
 */
export interface ResourceAccessRequest {
  'resourceType'?: (string);
  'resourceId'?: (string);
  'operation'?: (string);
  'workspace'?: (string);
  'requiredAccessLevel'?: (number);
  /**
   * Caller-generated correlation binding for a single logical action. A
   * recipient compares this value with its application payload/request.
   */
  'correlationId'?: (string);
}

/**
 * ResourceAccessRequest is the portable runtime authorization tuple evaluated
 * by the gateway. It is intentionally independent of any tool protocol: the
 * same primitive gates workspace views, catalog providers/entries, and future
 * logical resources. All string fields are required except workspace.
 */
export interface ResourceAccessRequest__Output {
  'resourceType': (string);
  'resourceId': (string);
  'operation': (string);
  'workspace': (string);
  'requiredAccessLevel': (number);
  /**
   * Caller-generated correlation binding for a single logical action. A
   * recipient compares this value with its application payload/request.
   */
  'correlationId': (string);
}
