package platforms

import "context"

// Attachment represents a file attachment.
type Attachment struct {
	Filename    string `json:"filename"`
	URL         string `json:"url"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
}

// Embed represents a rich embed (Discord/Teams).
type Embed struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	URL         string `json:"url,omitempty"`
	Color       int    `json:"color,omitempty"`
}

// InboundMessage is a message received from an external platform.
type InboundMessage struct {
	Platform    string            `json:"platform"`
	ChannelID   string            `json:"channel_id"`
	MessageID   string            `json:"message_id"`
	ThreadID    string            `json:"thread_id"`
	AuthorID    string            `json:"author_id"`
	AuthorName  string            `json:"author_name"`
	Content     string            `json:"content"`
	Subject     string            `json:"subject"`
	Attachments []Attachment      `json:"attachments,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// OutboundMessage is a message to send to an external platform.
type OutboundMessage struct {
	ChannelID   string            `json:"channel_id"`
	Content     string            `json:"content"`
	HTMLContent string            `json:"html_content,omitempty"`
	Subject     string            `json:"subject,omitempty"`
	ThreadID    string            `json:"thread_id,omitempty"`
	ReplyTo     string            `json:"reply_to,omitempty"`
	Embeds      []Embed           `json:"embeds,omitempty"`
	Attachments []Attachment      `json:"attachments,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// PlatformAdapter is the interface all platform implementations must satisfy.
type PlatformAdapter interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	SendMessage(ctx context.Context, msg OutboundMessage) (messageID string, err error)
	SetInboundHandler(fn func(ctx context.Context, msg InboundMessage) error)
	IsHealthy() bool
}
