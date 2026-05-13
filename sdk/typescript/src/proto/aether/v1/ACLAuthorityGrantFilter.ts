// Original file: aether.proto


export interface ACLAuthorityGrantFilter {
  'rootGrantId'?: (string);
  'subjectType'?: (string);
  'subjectId'?: (string);
  'delegateType'?: (string);
  'delegateId'?: (string);
  'audienceType'?: (string);
  'audienceId'?: (string);
  'includeRevoked'?: (boolean);
  'activeOnly'?: (boolean);
  'limit'?: (number);
  'offset'?: (number);
}

export interface ACLAuthorityGrantFilter__Output {
  'rootGrantId': (string);
  'subjectType': (string);
  'subjectId': (string);
  'delegateType': (string);
  'delegateId': (string);
  'audienceType': (string);
  'audienceId': (string);
  'includeRevoked': (boolean);
  'activeOnly': (boolean);
  'limit': (number);
  'offset': (number);
}
