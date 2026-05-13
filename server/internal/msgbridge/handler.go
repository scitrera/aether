package msgbridge

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/internal/msgbridge/platforms"
)

// BridgePayload is the JSON structure agents send to the bridge for outbound delivery.
type BridgePayload struct {
	BridgeAction string            `json:"bridge_action"` // "send"
	Target       BridgeTarget      `json:"target"`
	Content      string            `json:"content"`
	HTMLContent  string            `json:"html_content,omitempty"`
	Subject      string            `json:"subject,omitempty"`
	Embeds       []platforms.Embed `json:"embeds,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// BridgeTarget identifies the destination channel for an outbound message.
type BridgeTarget struct {
	Platform  string `json:"platform,omitempty"`
	ChannelID string `json:"channel_id,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
	Mapping   string `json:"mapping,omitempty"` // alias name
}

// handleOutbound processes a raw Aether message payload as an outbound bridge request.
// It never returns an error — failures are logged and the message is dropped.
func (r *Router) handleOutbound(ctx context.Context, sourceTopic string, raw []byte) {
	var payload BridgePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		log.Error().Err(err).Str("source", sourceTopic).Msg("msgbridge: failed to parse bridge payload")
		return
	}
	r.HandleOutbound(ctx, sourceTopic, payload)
}

// HandleOutbound resolves and dispatches an outbound BridgePayload to the target platform.
func (r *Router) HandleOutbound(ctx context.Context, sourceTopic string, payload BridgePayload) {
	start := time.Now()

	if payload.BridgeAction != "send" {
		log.Warn().Str("action", payload.BridgeAction).Str("source", sourceTopic).Msg("msgbridge: unknown bridge_action, dropping")
		return
	}

	// Resolve channel mapping
	var (
		platform  string
		channelID string
		threadID  string
	)

	if payload.Target.Mapping != "" {
		mapping, err := r.store.GetChannelMappingByName(ctx, payload.Target.Mapping)
		if err != nil {
			log.Error().Err(err).Str("mapping", payload.Target.Mapping).Msg("msgbridge: failed to look up channel mapping")
			r.logMessage(ctx, "outbound", "", "", "", sourceTopic, "error", err.Error())
			messagesRoutedTotal.WithLabelValues("outbound", "", "failed").Inc()
			messageRoutingDuration.WithLabelValues("outbound", "").Observe(time.Since(start).Seconds())
			return
		}
		if mapping == nil {
			log.Warn().Str("mapping", payload.Target.Mapping).Msg("msgbridge: channel mapping not found, dropping")
			r.logMessage(ctx, "outbound", "", "", "", sourceTopic, "dropped", "mapping not found")
			messagesRoutedTotal.WithLabelValues("outbound", "", "dropped").Inc()
			messageRoutingDuration.WithLabelValues("outbound", "").Observe(time.Since(start).Seconds())
			return
		}
		if !mapping.Enabled {
			log.Warn().Str("mapping", payload.Target.Mapping).Msg("msgbridge: channel mapping disabled, dropping")
			r.logMessage(ctx, "outbound", mapping.Platform, mapping.ChannelID, "", sourceTopic, "dropped", "mapping disabled")
			messagesRoutedTotal.WithLabelValues("outbound", mapping.Platform, "dropped").Inc()
			messageRoutingDuration.WithLabelValues("outbound", mapping.Platform).Observe(time.Since(start).Seconds())
			return
		}
		platform = mapping.Platform
		channelID = mapping.ChannelID
		threadID = payload.Target.ThreadID
	} else if payload.Target.Platform != "" && payload.Target.ChannelID != "" {
		platform = payload.Target.Platform
		channelID = payload.Target.ChannelID
		threadID = payload.Target.ThreadID
	} else {
		log.Warn().Str("source", sourceTopic).Msg("msgbridge: outbound target has neither mapping nor platform+channel_id, dropping")
		r.logMessage(ctx, "outbound", "", "", "", sourceTopic, "dropped", "missing target")
		messagesRoutedTotal.WithLabelValues("outbound", "", "dropped").Inc()
		messageRoutingDuration.WithLabelValues("outbound", "").Observe(time.Since(start).Seconds())
		return
	}

	// Find adapter
	adapter, ok := r.adapters[platform]
	if !ok {
		log.Warn().Str("platform", platform).Msg("msgbridge: no adapter registered for platform, dropping")
		r.logMessage(ctx, "outbound", platform, channelID, "", sourceTopic, "dropped", "no adapter")
		messagesRoutedTotal.WithLabelValues("outbound", platform, "dropped").Inc()
		messageRoutingDuration.WithLabelValues("outbound", platform).Observe(time.Since(start).Seconds())
		return
	}

	outMsg := platforms.OutboundMessage{
		ChannelID:   channelID,
		Content:     payload.Content,
		HTMLContent: payload.HTMLContent,
		Subject:     payload.Subject,
		ThreadID:    threadID,
		Embeds:      payload.Embeds,
		Metadata:    payload.Metadata,
	}

	msgID, err := adapter.SendMessage(ctx, outMsg)
	if err != nil {
		log.Error().Err(err).Str("platform", platform).Str("channel", channelID).Msg("msgbridge: failed to send outbound message")
		r.logMessage(ctx, "outbound", platform, channelID, "", sourceTopic, "error", err.Error())
		messagesRoutedTotal.WithLabelValues("outbound", platform, "failed").Inc()
		messageRoutingDuration.WithLabelValues("outbound", platform).Observe(time.Since(start).Seconds())
		return
	}

	log.Info().Str("platform", platform).Str("channel", channelID).Str("message_id", msgID).Msg("msgbridge: outbound message sent")
	r.logMessage(ctx, "outbound", platform, channelID, msgID, sourceTopic, "ok", "")
	messagesRoutedTotal.WithLabelValues("outbound", platform, "delivered").Inc()
	messageRoutingDuration.WithLabelValues("outbound", platform).Observe(time.Since(start).Seconds())
}
