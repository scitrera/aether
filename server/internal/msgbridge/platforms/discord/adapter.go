package discord

import (
	"context"
	"fmt"
	"sync"

	"github.com/bwmarrin/discordgo"
	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/internal/msgbridge/platforms"
)

// Adapter implements platforms.PlatformAdapter for Discord.
type Adapter struct {
	session        *discordgo.Session
	botToken       string
	applicationID  string
	inboundHandler func(ctx context.Context, msg platforms.InboundMessage) error
	healthy        bool
	mu             sync.RWMutex
}

// NewAdapter creates a new Discord adapter with the given bot token and application ID.
func NewAdapter(botToken, applicationID string) *Adapter {
	return &Adapter{
		botToken:      botToken,
		applicationID: applicationID,
	}
}

// Name returns the platform identifier.
func (a *Adapter) Name() string { return "discord" }

// Start creates the Discord session, registers handlers, sets intents, and opens the connection.
func (a *Adapter) Start(ctx context.Context) error {
	session, err := discordgo.New("Bot " + a.botToken)
	if err != nil {
		return fmt.Errorf("discord: failed to create session: %w", err)
	}

	session.Identify.Intents = discordgo.IntentsGuildMessages |
		discordgo.IntentsDirectMessages |
		discordgo.IntentsMessageContent

	session.AddHandler(a.handleMessage)

	if err := session.Open(); err != nil {
		return fmt.Errorf("discord: failed to open session: %w", err)
	}

	a.mu.Lock()
	a.session = session
	a.healthy = true
	a.mu.Unlock()

	log.Info().Str("platform", "discord").Msg("Discord adapter started")
	return nil
}

// Stop closes the Discord session.
func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.session != nil {
		if err := a.session.Close(); err != nil {
			return fmt.Errorf("discord: failed to close session: %w", err)
		}
		a.session = nil
	}
	a.healthy = false
	log.Info().Str("platform", "discord").Msg("Discord adapter stopped")
	return nil
}

// SendMessage sends a message to a Discord channel, optionally into a thread and/or as a reply.
func (a *Adapter) SendMessage(ctx context.Context, msg platforms.OutboundMessage) (string, error) {
	a.mu.RLock()
	session := a.session
	a.mu.RUnlock()

	if session == nil {
		return "", fmt.Errorf("discord: adapter not started")
	}

	channelID := msg.ChannelID
	if msg.ThreadID != "" {
		channelID = msg.ThreadID
	}

	data := &discordgo.MessageSend{
		Content: msg.Content,
	}

	if msg.ReplyTo != "" {
		data.Reference = &discordgo.MessageReference{
			MessageID: msg.ReplyTo,
		}
	}

	if len(msg.Embeds) > 0 {
		embeds := make([]*discordgo.MessageEmbed, 0, len(msg.Embeds))
		for _, e := range msg.Embeds {
			embed := &discordgo.MessageEmbed{
				Title:       e.Title,
				Description: e.Description,
				URL:         e.URL,
				Color:       e.Color,
			}
			embeds = append(embeds, embed)
		}
		data.Embeds = embeds
	}

	sent, err := session.ChannelMessageSendComplex(channelID, data)
	if err != nil {
		return "", fmt.Errorf("discord: failed to send message to channel %s: %w", channelID, err)
	}

	return sent.ID, nil
}

// SetInboundHandler registers the callback for inbound messages from Discord.
func (a *Adapter) SetInboundHandler(fn func(ctx context.Context, msg platforms.InboundMessage) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inboundHandler = fn
}

// IsHealthy returns whether the Discord session is active and healthy.
func (a *Adapter) IsHealthy() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.healthy
}

// handleMessage is the discordgo MessageCreate event handler.
func (a *Adapter) handleMessage(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Skip messages from the bot itself.
	if m.Author == nil || m.Author.ID == s.State.User.ID {
		return
	}

	a.mu.RLock()
	handler := a.inboundHandler
	a.mu.RUnlock()

	if handler == nil {
		return
	}

	attachments := make([]platforms.Attachment, 0, len(m.Attachments))
	for _, att := range m.Attachments {
		attachments = append(attachments, platforms.Attachment{
			Filename:    att.Filename,
			URL:         att.URL,
			ContentType: att.ContentType,
			Size:        int64(att.Size),
		})
	}

	inbound := platforms.InboundMessage{
		Platform:    "discord",
		ChannelID:   m.ChannelID,
		MessageID:   m.ID,
		AuthorID:    m.Author.ID,
		AuthorName:  m.Author.Username,
		Content:     m.Content,
		Attachments: attachments,
	}

	// Populate ThreadID if the message is in a thread.
	if m.Message != nil && m.Thread != nil {
		inbound.ThreadID = m.Thread.ID
	}

	if err := handler(context.Background(), inbound); err != nil {
		log.Error().Err(err).
			Str("platform", "discord").
			Str("channel_id", m.ChannelID).
			Str("message_id", m.ID).
			Msg("inbound handler error")
	}
}
