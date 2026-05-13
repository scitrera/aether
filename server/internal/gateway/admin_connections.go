package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
	"google.golang.org/protobuf/proto"
)

// =============================================================================
// Connections
// =============================================================================

func (p *GatewayStateProvider) GetConnections(ctx context.Context, filter *admin.ConnectionFilter) ([]*admin.ConnectionInfo, error) {
	var connections []*admin.ConnectionInfo

	if p.gateway == nil {
		return connections, nil
	}

	p.gateway.activeStreams.Range(func(key, value interface{}) bool {
		session, ok := value.(*ClientSession)
		if !ok {
			return true
		}

		// Apply filters
		if filter != nil {
			if filter.Type != "" && string(session.Identity.Type) != filter.Type {
				return true
			}
			if filter.Workspace != "" && session.Identity.Workspace != filter.Workspace {
				return true
			}
		}

		// Use in-memory ConnectedAt time, fallback to Redis if not available
		connectedAt := session.ConnectedAt
		if connectedAt.IsZero() && p.sessions != nil {
			// Fallback: fetch from Redis (e.g., during upgrades or if in-memory field not set)
			redis := p.sessions.GetRedisClient()
			sessionKey := fmt.Sprintf("session:%s", session.ID)
			startUnix, err := redis.HGet(ctx, sessionKey, "start").Int64()
			if err == nil {
				connectedAt = time.Unix(startUnix, 0)
			} else {
				// Last resort: use current time (will show 0s duration)
				connectedAt = time.Now()
			}
		}

		conn := &admin.ConnectionInfo{
			SessionID:      session.ID,
			Type:           string(session.Identity.Type),
			Identity:       session.Identity.String(),
			Workspace:      session.Identity.Workspace,
			Implementation: session.Identity.Implementation,
			Specifier:      session.Identity.Specifier,
			ConnectedAt:    connectedAt,
			Duration:       formatDuration(time.Since(connectedAt)),
		}

		connections = append(connections, conn)
		return true
	})

	// Apply limit/offset
	if filter != nil {
		if filter.Offset > 0 && filter.Offset < len(connections) {
			connections = connections[filter.Offset:]
		}
		if filter.Limit > 0 && filter.Limit < len(connections) {
			connections = connections[:filter.Limit]
		}
	}

	return connections, nil
}

func (p *GatewayStateProvider) GetConnectionByID(ctx context.Context, sessionID string) (*admin.ConnectionInfo, error) {
	if p.gateway == nil {
		return nil, fmt.Errorf("gateway not available")
	}

	value, ok := p.gateway.activeStreams.Load(sessionID)
	if !ok {
		return nil, fmt.Errorf("session not found: %s", sessionID)
	}

	session, ok := value.(*ClientSession)
	if !ok {
		return nil, fmt.Errorf("invalid session data")
	}

	// Use in-memory ConnectedAt time, fallback to Redis if not available
	connectedAt := session.ConnectedAt
	duration := "unknown"
	if connectedAt.IsZero() && p.sessions != nil {
		// Fallback: fetch from Redis (e.g., during upgrades or if in-memory field not set)
		redis := p.sessions.GetRedisClient()
		sessionKey := fmt.Sprintf("session:%s", session.ID)
		startUnix, err := redis.HGet(ctx, sessionKey, "start").Int64()
		if err == nil {
			connectedAt = time.Unix(startUnix, 0)
			duration = formatDuration(time.Since(connectedAt))
		} else {
			// Last resort: use current time (will show 0s duration)
			connectedAt = time.Now()
			duration = "0s"
		}
	} else {
		duration = formatDuration(time.Since(connectedAt))
	}

	return &admin.ConnectionInfo{
		SessionID:      session.ID,
		Type:           string(session.Identity.Type),
		Identity:       session.Identity.String(),
		Workspace:      session.Identity.Workspace,
		Implementation: session.Identity.Implementation,
		Specifier:      session.Identity.Specifier,
		ConnectedAt:    connectedAt,
		Duration:       duration,
	}, nil
}

func (p *GatewayStateProvider) DisconnectSession(ctx context.Context, sessionID string) error {
	if p.gateway == nil {
		return fmt.Errorf("gateway not available")
	}

	value, ok := p.gateway.activeStreams.Load(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	session, ok := value.(*ClientSession)
	if !ok {
		return fmt.Errorf("invalid session type")
	}

	logging.Logger.Info().Str("session_id", sessionID).Str("identity", session.Identity.String()).Msg("admin disconnecting session")

	// Mark this disconnect as server-initiated so cleanupSession leaves the
	// associated task alone. Admin force-kick of a session is not a task
	// transition; if the worker shouldn't continue, the admin should also
	// call cancel_task explicitly.
	session.serverInitiatedDisconnect.Store(true)

	// Send FORCE_DISCONNECT signal to the client
	// The client should close its connection when it receives this
	if session.Stream != nil {
		err := session.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Signal{
				Signal: &pb.Signal{
					Type:   pb.Signal_FORCE_DISCONNECT,
					Reason: "disconnected by administrator",
				},
			},
		})
		if err != nil {
			logging.Logger.Error().Err(err).Str("session_id", sessionID).Msg("admin failed to send disconnect signal")
		}
	}

	// Cancel the session context
	// This will cause the server's main loop to exit when it next checks context.
	// cleanupSession (triggered by session.Cancel()) handles removal from activeStreams
	// and identityIndex; deleting here would create stale identityIndex entries.
	if session.Cancel != nil {
		session.Cancel()
	}

	return nil
}

// =============================================================================
// Messaging
// =============================================================================

// SendMessage sends a message to a topic as the admin
func (p *GatewayStateProvider) SendMessage(ctx context.Context, req *admin.SendMessageRequest) error {
	if p.router == nil {
		return fmt.Errorf("router not available")
	}

	// Convert payload string to bytes
	payload := []byte(req.Payload)

	// Parse message type
	msgType := pb.MessageType_CHAT
	switch req.MessageType {
	case "CONTROL":
		msgType = pb.MessageType_CONTROL
	case "TOOL_CALL":
		msgType = pb.MessageType_TOOL_CALL
	case "EVENT":
		msgType = pb.MessageType_EVENT
	case "METRIC":
		msgType = pb.MessageType_METRIC
	case "OPAQUE":
		msgType = pb.MessageType_OPAQUE
	}

	// Admin-side METRIC payloads still must conform to the structured Metric
	// proto. Shape validation is independent of ACL (which the admin path
	// intentionally bypasses) and protects the downstream MetricsBridge from
	// malformed inputs that would otherwise corrupt aggregations.
	if msgType == pb.MessageType_METRIC {
		if _, err := validateMetricShape(payload); err != nil {
			return fmt.Errorf("admin metric payload invalid: %w", err)
		}
	}

	// Wrap in MessageEnvelope with admin as source
	envelope := &pb.MessageEnvelope{
		Source:      "admin",
		Payload:     payload,
		MessageType: msgType,
		TimestampMs: time.Now().UnixMilli(),
	}

	envelopeBytes, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal message envelope: %w", err)
	}

	// Publish to the topic
	// Admin messages bypass normal ACL checks
	logging.Logger.Info().Str("topic", req.TargetTopic).Str("type", req.MessageType).Int("size", len(payload)).Msg("admin sending message")
	return p.router.Publish(ctx, req.TargetTopic, envelopeBytes)
}

// SubscribeToTopic creates a monitoring subscription on a topic
func (p *GatewayStateProvider) SubscribeToTopic(ctx context.Context, topic string, handler func(*admin.MonitoredMessage)) (func(), error) {
	if p.router == nil {
		return nil, fmt.Errorf("router not available")
	}

	// Create subscription with message handler
	cancel, err := p.router.Subscribe(topic, func(envelopeBytes []byte) {
		// Unwrap MessageEnvelope to get source and payload
		var envelope pb.MessageEnvelope
		if err := proto.Unmarshal(envelopeBytes, &envelope); err != nil {
			logging.Logger.Error().Err(err).Str("topic", topic).Msg("admin monitor failed to unmarshal envelope")
			return
		}

		// Create monitored message with envelope data
		msg := &admin.MonitoredMessage{
			ID:          fmt.Sprintf("%d", time.Now().UnixNano()),
			Topic:       topic,
			SourceTopic: envelope.Source,
			Payload:     string(envelope.Payload),
			MessageType: envelope.MessageType.String(),
			Timestamp:   time.UnixMilli(envelope.TimestampMs),
		}

		// Try to parse payload as JSON for pretty display
		var jsonData any
		if err := json.Unmarshal(envelope.Payload, &jsonData); err == nil {
			msg.PayloadJSON = jsonData
		}

		handler(msg)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	logging.Logger.Info().Str("topic", topic).Msg("admin created monitor subscription")
	return cancel, nil
}
