package msgbridge

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/msgbridge/platforms"
)

// bridgeClient is the interface the router needs for sending Aether messages.
type bridgeClient interface {
	SendToAgent(workspace, impl, spec string, payload []byte) error
	SendToUser(userID, windowID string, payload []byte) error
	SendToUserWorkspace(userID, workspace string, payload []byte) error
	BroadcastToAgents(workspace string, payload []byte) error
	BroadcastToUsers(workspace string, payload []byte) error
	SendMessage(targetTopic string, payload []byte, msgType pb.MessageType) error
}

// Router handles bidirectional message routing between platforms and Aether.
type Router struct {
	store    *Store
	adapters map[string]platforms.PlatformAdapter // keyed by platform name
	client   bridgeClient
}

// NewRouter creates a new Router with the given store, adapters, and bridge client.
func NewRouter(store *Store, adapters map[string]platforms.PlatformAdapter, client bridgeClient) *Router {
	return &Router{
		store:    store,
		adapters: adapters,
		client:   client,
	}
}

// inboundPayload is the JSON envelope sent into Aether for inbound platform messages.
type inboundPayload struct {
	BridgeSource bridgeSource           `json:"bridge_source"`
	Author       inboundAuthor          `json:"author"`
	Content      string                 `json:"content"`
	Subject      string                 `json:"subject,omitempty"`
	Attachments  []platforms.Attachment `json:"attachments,omitempty"`
	Metadata     map[string]string      `json:"metadata,omitempty"`
}

type bridgeSource struct {
	Platform  string `json:"platform"`
	ChannelID string `json:"channel_id"`
	MessageID string `json:"message_id"`
	ThreadID  string `json:"thread_id,omitempty"`
}

type inboundAuthor struct {
	PlatformID   string `json:"platform_id"`
	PlatformName string `json:"platform_name"`
	AetherUserID string `json:"aether_user_id,omitempty"`
}

// HandleInbound receives a message from a platform adapter and routes it into Aether.
// Never returns an error — failures are logged and the message is dropped.
func (r *Router) HandleInbound(ctx context.Context, msg platforms.InboundMessage) error {
	start := time.Now()

	// Look up channel mapping
	mapping, err := r.store.GetChannelMappingByChannel(ctx, msg.Platform, msg.ChannelID)
	if err != nil {
		log.Error().Err(err).Str("platform", msg.Platform).Str("channel", msg.ChannelID).Msg("msgbridge: inbound channel mapping lookup failed")
		messagesRoutedTotal.WithLabelValues("inbound", msg.Platform, "failed").Inc()
		messageRoutingDuration.WithLabelValues("inbound", msg.Platform).Observe(time.Since(start).Seconds())
		return nil
	}
	if mapping == nil {
		log.Debug().Str("platform", msg.Platform).Str("channel", msg.ChannelID).Msg("msgbridge: no channel mapping found, dropping inbound message")
		messagesRoutedTotal.WithLabelValues("inbound", msg.Platform, "dropped").Inc()
		messageRoutingDuration.WithLabelValues("inbound", msg.Platform).Observe(time.Since(start).Seconds())
		return nil
	}
	if !mapping.Enabled {
		log.Debug().Str("mapping", mapping.Name).Msg("msgbridge: channel mapping disabled, dropping inbound message")
		messagesRoutedTotal.WithLabelValues("inbound", msg.Platform, "dropped").Inc()
		messageRoutingDuration.WithLabelValues("inbound", msg.Platform).Observe(time.Since(start).Seconds())
		return nil
	}
	if mapping.Direction == "outbound" {
		log.Debug().Str("mapping", mapping.Name).Msg("msgbridge: mapping direction is outbound-only, dropping inbound message")
		messagesRoutedTotal.WithLabelValues("inbound", msg.Platform, "dropped").Inc()
		messageRoutingDuration.WithLabelValues("inbound", msg.Platform).Observe(time.Since(start).Seconds())
		return nil
	}

	// Resolve Aether user from user mapping if available
	aetherUserID := ""
	if msg.AuthorID != "" {
		userMap, err := r.store.GetUserMapping(ctx, msg.Platform, msg.AuthorID)
		if err != nil {
			log.Warn().Err(err).Str("platform", msg.Platform).Str("author_id", msg.AuthorID).Msg("msgbridge: user mapping lookup failed")
		} else if userMap != nil {
			aetherUserID = userMap.AetherUserID
		}
	}

	// Build payload
	envelope := inboundPayload{
		BridgeSource: bridgeSource{
			Platform:  msg.Platform,
			ChannelID: msg.ChannelID,
			MessageID: msg.MessageID,
			ThreadID:  msg.ThreadID,
		},
		Author: inboundAuthor{
			PlatformID:   msg.AuthorID,
			PlatformName: msg.AuthorName,
			AetherUserID: aetherUserID,
		},
		Content:     msg.Content,
		Subject:     msg.Subject,
		Attachments: msg.Attachments,
		Metadata:    msg.Metadata,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		log.Error().Err(err).Msg("msgbridge: failed to marshal inbound payload")
		r.logMessage(ctx, "inbound", msg.Platform, msg.ChannelID, msg.MessageID, "", "error", err.Error())
		messagesRoutedTotal.WithLabelValues("inbound", msg.Platform, "failed").Inc()
		messageRoutingDuration.WithLabelValues("inbound", msg.Platform).Observe(time.Since(start).Seconds())
		return nil
	}

	// Route to Aether based on target_type
	aetherTopic, routeErr := r.routeInbound(ctx, mapping, payloadBytes)
	if routeErr != nil {
		log.Error().Err(routeErr).Str("target_type", mapping.TargetType).Msg("msgbridge: failed to route inbound message to Aether")
		r.logMessage(ctx, "inbound", msg.Platform, msg.ChannelID, msg.MessageID, aetherTopic, "error", routeErr.Error())
		messagesRoutedTotal.WithLabelValues("inbound", msg.Platform, "failed").Inc()
		messageRoutingDuration.WithLabelValues("inbound", msg.Platform).Observe(time.Since(start).Seconds())
		return nil
	}

	log.Info().
		Str("platform", msg.Platform).
		Str("channel", msg.ChannelID).
		Str("target_type", mapping.TargetType).
		Str("aether_topic", aetherTopic).
		Msg("msgbridge: inbound message routed to Aether")
	r.logMessage(ctx, "inbound", msg.Platform, msg.ChannelID, msg.MessageID, aetherTopic, "ok", "")
	messagesRoutedTotal.WithLabelValues("inbound", msg.Platform, "delivered").Inc()
	messageRoutingDuration.WithLabelValues("inbound", msg.Platform).Observe(time.Since(start).Seconds())

	// Publish workflow event if configured on this mapping
	r.publishWorkflowEvent(ctx, mapping, msg, aetherUserID)

	return nil
}

// routeInbound dispatches the payload to the correct Aether topic and returns the topic string.
func (r *Router) routeInbound(ctx context.Context, mapping *ChannelMapping, payload []byte) (string, error) {
	workspace := mapping.TargetWorkspace
	targetID := mapping.TargetID

	switch mapping.TargetType {
	case "agent":
		// targetID is "impl.spec"
		impl, spec, _ := splitTwo(targetID)
		topic := "ag." + workspace + "." + impl + "." + spec
		return topic, r.client.SendToAgent(workspace, impl, spec, payload)

	case "user":
		if workspace != "" {
			topic := "uw." + targetID + "." + workspace
			return topic, r.client.SendToUserWorkspace(targetID, workspace, payload)
		}
		topic := "us." + targetID + "."
		return topic, r.client.SendToUser(targetID, "", payload)

	case "broadcast_agents":
		topic := "ga." + workspace
		return topic, r.client.BroadcastToAgents(workspace, payload)

	case "broadcast_users":
		topic := "gu." + workspace
		return topic, r.client.BroadcastToUsers(workspace, payload)

	default:
		topic := ""
		return topic, nil
	}
}

// splitTwo splits s on the first "." into two parts.
func splitTwo(s string) (string, string, bool) {
	idx := strings.Index(s, ".")
	if idx < 0 {
		return s, "", false
	}
	return s[:idx], s[idx+1:], true
}

// workflowConfig is parsed from the ChannelMapping.Metadata JSONB field.
type workflowConfig struct {
	WorkflowEnabled bool     `json:"workflow_enabled"`
	WorkflowEvents  []string `json:"workflow_events"`
}

// parseWorkflowConfig extracts workflow trigger settings from mapping metadata.
// Returns nil if workflow is not enabled or metadata is empty/invalid.
func parseWorkflowConfig(metadata json.RawMessage) *workflowConfig {
	if len(metadata) == 0 {
		return nil
	}
	var cfg workflowConfig
	if err := json.Unmarshal(metadata, &cfg); err != nil {
		return nil
	}
	if !cfg.WorkflowEnabled {
		return nil
	}
	return &cfg
}

// workflowEventPayload matches the EventPayload struct expected by the workflow engine
// (server/internal/workflow/router.go).
type workflowEventPayload struct {
	SourceAgent string         `json:"source_agent"`
	EventNames  []string       `json:"event_names"`
	Data        map[string]any `json:"data"`
	Workspace   string         `json:"workspace"`
}

// publishWorkflowEvent publishes an event to the workflow engine if the mapping
// has workflow_enabled=true in its metadata. Events are published to the
// event.{workspace} topic so the workflow engine can match rules and trigger
// DAG executions or state machine transitions.
func (r *Router) publishWorkflowEvent(ctx context.Context, mapping *ChannelMapping, msg platforms.InboundMessage, aetherUserID string) {
	cfg := parseWorkflowConfig(mapping.Metadata)
	if cfg == nil {
		return
	}

	// Determine event names: use configured list or defaults
	eventNames := cfg.WorkflowEvents
	if len(eventNames) == 0 {
		eventNames = []string{"message.received", "message." + msg.Platform}
	}

	workspace := mapping.TargetWorkspace
	if workspace == "" {
		workspace = "default"
	}

	event := workflowEventPayload{
		SourceAgent: "aether-msgbridge",
		EventNames:  eventNames,
		Data: map[string]any{
			"platform":       msg.Platform,
			"channel_id":     msg.ChannelID,
			"message_id":     msg.MessageID,
			"thread_id":      msg.ThreadID,
			"author_id":      msg.AuthorID,
			"author_name":    msg.AuthorName,
			"aether_user_id": aetherUserID,
			"content":        msg.Content,
			"subject":        msg.Subject,
			"mapping_name":   mapping.Name,
		},
		Workspace: workspace,
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Error().Err(err).Str("mapping", mapping.Name).Msg("msgbridge: failed to marshal workflow event")
		return
	}

	eventTopic := "event." + workspace
	if err := r.client.SendMessage(eventTopic, eventBytes, pb.MessageType_EVENT); err != nil {
		log.Error().Err(err).Str("topic", eventTopic).Str("mapping", mapping.Name).Msg("msgbridge: failed to publish workflow event")
		messagesRoutedTotal.WithLabelValues("event", msg.Platform, "failed").Inc()
		return
	}

	log.Info().
		Str("topic", eventTopic).
		Str("mapping", mapping.Name).
		Strs("events", eventNames).
		Msg("msgbridge: workflow event published")
	messagesRoutedTotal.WithLabelValues("event", msg.Platform, "delivered").Inc()
}

// logMessage writes an entry to the message log, ignoring write errors.
func (r *Router) logMessage(ctx context.Context, direction, platform, channelID, messageID, aetherTopic, status, errMsg string) {
	entry := &MessageLogEntry{
		Direction:   direction,
		Platform:    platform,
		ChannelID:   channelID,
		MessageID:   messageID,
		AetherTopic: aetherTopic,
		Status:      status,
		ErrorMsg:    errMsg,
	}
	if err := r.store.LogMessage(ctx, entry); err != nil {
		log.Error().Err(err).Msg("msgbridge: failed to write message log entry")
	}
}
