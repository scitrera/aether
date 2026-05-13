// Original file: aether.proto

import type { Long } from '@grpc/proto-loader';

/**
 * FlowEdge represents a message routing path between two entities.
 * Edges indicate that messages can flow from source to destination.
 */
export interface FlowEdge {
  /**
   * Source node ID
   */
  'from'?: (string);
  /**
   * Destination node ID
   */
  'to'?: (string);
  /**
   * Edge description (e.g., message type, topic pattern)
   */
  'label'?: (string);
  /**
   * Number of messages sent over this edge (if tracked)
   */
  'count'?: (number | string | Long);
}

/**
 * FlowEdge represents a message routing path between two entities.
 * Edges indicate that messages can flow from source to destination.
 */
export interface FlowEdge__Output {
  /**
   * Source node ID
   */
  'from': (string);
  /**
   * Destination node ID
   */
  'to': (string);
  /**
   * Edge description (e.g., message type, topic pattern)
   */
  'label': (string);
  /**
   * Number of messages sent over this edge (if tracked)
   */
  'count': (string);
}
