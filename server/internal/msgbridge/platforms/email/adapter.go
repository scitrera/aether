package email

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"mime/multipart"
	"net/smtp"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/internal/msgbridge/platforms"
)

// SMTPConfig holds SMTP connection settings for the email adapter.
type SMTPConfig struct {
	Host        string
	Port        int
	Username    string
	Password    string
	FromAddress string
	UseTLS      bool
}

// IMAPConfig holds IMAP polling settings for inbound email.
type IMAPConfig struct {
	Host         string
	Port         int
	Username     string
	Password     string
	Mailbox      string
	PollInterval time.Duration
	UseTLS       bool
}

// Adapter implements platforms.PlatformAdapter for email via SMTP outbound
// and (stubbed) IMAP inbound polling.
type Adapter struct {
	smtp           SMTPConfig
	imap           *IMAPConfig
	inboundHandler func(ctx context.Context, msg platforms.InboundMessage) error
	healthy        bool
	mu             sync.RWMutex
	cancel         context.CancelFunc
}

// NewAdapter creates a new email Adapter. imapCfg may be nil to disable inbound polling.
func NewAdapter(smtpCfg SMTPConfig, imapCfg *IMAPConfig) *Adapter {
	return &Adapter{
		smtp: smtpCfg,
		imap: imapCfg,
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string { return "email" }

// Start validates the SMTP connection and optionally starts the IMAP polling goroutine.
func (a *Adapter) Start(ctx context.Context) error {
	logger := log.With().Str("adapter", "email").Logger()

	// Test SMTP connectivity by dialing and immediately quitting.
	if err := a.testSMTP(); err != nil {
		return fmt.Errorf("email adapter: SMTP connectivity check failed: %w", err)
	}
	logger.Info().Str("host", a.smtp.Host).Int("port", a.smtp.Port).Msg("SMTP connection verified")

	// Start IMAP polling goroutine if configured.
	if a.imap != nil {
		pollCtx, cancel := context.WithCancel(ctx)
		a.mu.Lock()
		a.cancel = cancel
		a.mu.Unlock()
		go a.pollIMAP(pollCtx)
	}

	a.mu.Lock()
	a.healthy = true
	a.mu.Unlock()

	return nil
}

// Stop cancels the IMAP polling goroutine if running.
func (a *Adapter) Stop(_ context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cancel != nil {
		a.cancel()
		a.cancel = nil
	}
	a.healthy = false
	return nil
}

// SendMessage sends an outbound email. msg.ChannelID is used as the To address.
// Returns the generated Message-ID on success.
func (a *Adapter) SendMessage(ctx context.Context, msg platforms.OutboundMessage) (string, error) {
	to := msg.ChannelID
	if to == "" {
		return "", fmt.Errorf("email adapter: ChannelID (recipient address) is required")
	}

	subject := msg.Subject
	if subject == "" {
		subject = "Message from Aether"
	}

	// Build extra headers from metadata.
	headers := make(map[string]string)

	if cc, ok := msg.Metadata["cc"]; ok && cc != "" {
		headers["Cc"] = cc
	}
	if replyTo, ok := msg.Metadata["reply_to"]; ok && replyTo != "" {
		headers["Reply-To"] = replyTo
	}
	if msg.ThreadID != "" {
		headers["In-Reply-To"] = msg.ThreadID
		headers["References"] = msg.ThreadID
	}

	messageID := generateMessageID(a.smtp.FromAddress)
	headers["Message-ID"] = messageID

	raw := a.buildMessage(to, subject, msg.Content, msg.HTMLContent, headers)

	recipients := []string{to}
	// Add CC recipients to the SMTP envelope as well.
	if cc, ok := headers["Cc"]; ok {
		for _, addr := range strings.Split(cc, ",") {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				recipients = append(recipients, addr)
			}
		}
	}

	if err := a.sendSMTP(recipients, raw); err != nil {
		return "", fmt.Errorf("email adapter: failed to send email to %s: %w", to, err)
	}

	log.Debug().
		Str("adapter", "email").
		Str("to", to).
		Str("subject", subject).
		Str("message_id", messageID).
		Msg("email sent")

	return messageID, nil
}

// SetInboundHandler registers the handler called for each inbound message.
func (a *Adapter) SetInboundHandler(fn func(ctx context.Context, msg platforms.InboundMessage) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inboundHandler = fn
}

// IsHealthy returns the current health status of the adapter.
func (a *Adapter) IsHealthy() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.healthy
}

// testSMTP dials the SMTP server and immediately issues QUIT to verify connectivity.
func (a *Adapter) testSMTP() error {
	addr := fmt.Sprintf("%s:%d", a.smtp.Host, a.smtp.Port)

	var c *smtp.Client
	var err error

	if a.smtp.UseTLS {
		tlsCfg := &tls.Config{ServerName: a.smtp.Host} //nolint:gosec
		conn, dialErr := tls.Dial("tcp", addr, tlsCfg)
		if dialErr != nil {
			return fmt.Errorf("TLS dial failed: %w", dialErr)
		}
		c, err = smtp.NewClient(conn, a.smtp.Host)
	} else {
		c, err = smtp.Dial(addr)
	}
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	return c.Quit()
}

// buildMessage constructs an RFC 2822 / MIME email message.
// If htmlBody is non-empty a multipart/alternative body is produced;
// otherwise a plain text body is used.
func (a *Adapter) buildMessage(to, subject, plainBody, htmlBody string, headers map[string]string) []byte {
	var buf bytes.Buffer

	// Standard headers.
	fmt.Fprintf(&buf, "From: %s\r\n", a.smtp.FromAddress)
	fmt.Fprintf(&buf, "To: %s\r\n", to)
	fmt.Fprintf(&buf, "Subject: %s\r\n", subject)
	fmt.Fprintf(&buf, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z))
	fmt.Fprintf(&buf, "MIME-Version: 1.0\r\n")

	// Extra headers (Message-ID, Cc, Reply-To, In-Reply-To, References, …).
	for k, v := range headers {
		fmt.Fprintf(&buf, "%s: %s\r\n", k, v)
	}

	if htmlBody != "" {
		// Build the multipart body first so we know the boundary before writing headers.
		var partsBuf bytes.Buffer
		mw := multipart.NewWriter(&partsBuf)

		plainHdr := textproto.MIMEHeader{}
		plainHdr.Set("Content-Type", "text/plain; charset=utf-8")
		plainHdr.Set("Content-Transfer-Encoding", "quoted-printable")
		pw, _ := mw.CreatePart(plainHdr)
		pw.Write([]byte(plainBody)) //nolint:errcheck

		htmlHdr := textproto.MIMEHeader{}
		htmlHdr.Set("Content-Type", "text/html; charset=utf-8")
		htmlHdr.Set("Content-Transfer-Encoding", "quoted-printable")
		hw, _ := mw.CreatePart(htmlHdr)
		hw.Write([]byte(htmlBody)) //nolint:errcheck

		mw.Close() //nolint:errcheck

		// Now write headers (including the boundary) then append the pre-built parts.
		fmt.Fprintf(&buf, "Content-Type: multipart/alternative; boundary=%q\r\n", mw.Boundary())
		buf.WriteString("\r\n")
		buf.Write(partsBuf.Bytes()) //nolint:errcheck
	} else {
		// Plain text only.
		buf.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
		buf.WriteString("\r\n")
		buf.WriteString(plainBody)
	}

	return buf.Bytes()
}

// sendSMTP sends a pre-built RFC 2822 message via SMTP.
func (a *Adapter) sendSMTP(to []string, msg []byte) error {
	addr := fmt.Sprintf("%s:%d", a.smtp.Host, a.smtp.Port)

	var c *smtp.Client
	var err error

	if a.smtp.UseTLS {
		tlsCfg := &tls.Config{ServerName: a.smtp.Host} //nolint:gosec
		conn, dialErr := tls.Dial("tcp", addr, tlsCfg)
		if dialErr != nil {
			return fmt.Errorf("TLS dial failed: %w", dialErr)
		}
		c, err = smtp.NewClient(conn, a.smtp.Host)
	} else {
		c, err = smtp.Dial(addr)
		if err != nil {
			return fmt.Errorf("dial failed: %w", err)
		}
		// Attempt STARTTLS upgrade if the server supports it.
		if ok, _ := c.Extension("STARTTLS"); ok {
			tlsCfg := &tls.Config{ServerName: a.smtp.Host} //nolint:gosec
			if tlsErr := c.StartTLS(tlsCfg); tlsErr != nil {
				log.Warn().Err(tlsErr).Str("adapter", "email").Msg("STARTTLS upgrade failed, continuing without TLS")
			}
		}
	}
	if err != nil {
		return fmt.Errorf("failed to create SMTP client: %w", err)
	}
	defer c.Close() //nolint:errcheck

	if a.smtp.Username != "" {
		auth := smtp.PlainAuth("", a.smtp.Username, a.smtp.Password, a.smtp.Host)
		if err := c.Auth(auth); err != nil {
			return fmt.Errorf("SMTP auth failed: %w", err)
		}
	}

	if err := c.Mail(a.smtp.FromAddress); err != nil {
		return fmt.Errorf("SMTP MAIL FROM failed: %w", err)
	}

	for _, recipient := range to {
		if err := c.Rcpt(recipient); err != nil {
			return fmt.Errorf("SMTP RCPT TO %s failed: %w", recipient, err)
		}
	}

	wc, err := c.Data()
	if err != nil {
		return fmt.Errorf("SMTP DATA failed: %w", err)
	}
	if _, err := wc.Write(msg); err != nil {
		wc.Close() //nolint:errcheck
		return fmt.Errorf("SMTP write body failed: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("SMTP close data writer failed: %w", err)
	}

	return c.Quit()
}

// pollIMAP is the stub inbound polling loop. Full IMAP support is not yet implemented.
func (a *Adapter) pollIMAP(ctx context.Context) {
	interval := a.imap.PollInterval
	if interval <= 0 {
		interval = time.Minute
	}

	logger := log.With().Str("adapter", "email").Str("component", "imap-poller").Logger()
	logger.Info().
		Str("host", a.imap.Host).
		Str("mailbox", a.imap.Mailbox).
		Dur("poll_interval", interval).
		Msg("IMAP polling started (not yet implemented)")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("IMAP polling stopped")
			return
		case <-ticker.C:
			logger.Debug().Msg("IMAP poll tick: inbound email polling not yet implemented")
		}
	}
}

// generateMessageID generates a unique RFC 2822 Message-ID header value.
func generateMessageID(fromAddress string) string {
	domain := "localhost"
	if parts := strings.SplitN(fromAddress, "@", 2); len(parts) == 2 {
		domain = parts[1]
	}
	ts := time.Now().UnixNano()
	rnd := rand.Int63() //nolint:gosec
	return fmt.Sprintf("<%d.%d@%s>", ts, rnd, domain)
}

// Ensure Adapter satisfies the PlatformAdapter interface at compile time.
var _ platforms.PlatformAdapter = (*Adapter)(nil)
