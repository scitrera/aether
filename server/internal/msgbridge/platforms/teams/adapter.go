package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/internal/msgbridge/platforms"
)

const (
	authURLTemplate = "https://login.microsoftonline.com/%s/oauth2/v2.0/token"
	defaultScope    = "https://api.botframework.com/.default"
)

// tokenResponse is the OAuth2 token endpoint response.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// activity is a Bot Framework Activity for sending messages.
type activity struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	ReplyToID string `json:"replyToId,omitempty"`
}

// activityResponse is the response from the Bot Framework when sending an activity.
type activityResponse struct {
	ID string `json:"id"`
}

// inboundActivity represents an incoming Bot Framework Activity from Teams.
type inboundActivity struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	From struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"from"`
	Conversation struct {
		ID      string `json:"id"`
		IsGroup bool   `json:"isGroup"`
	} `json:"conversation"`
	ReplyToID   string `json:"replyToId"`
	Text        string `json:"text"`
	ChannelID   string `json:"channelId"`
	ServiceURL  string `json:"serviceUrl"`
	Attachments []struct {
		Name        string `json:"name"`
		ContentURL  string `json:"contentUrl"`
		ContentType string `json:"contentType"`
	} `json:"attachments"`
}

// Adapter implements platforms.PlatformAdapter for Microsoft Teams via the Bot Framework REST API.
type Adapter struct {
	appID       string
	appPassword string
	tenantID    string
	serviceURL  string

	httpClient     *http.Client
	accessToken    string
	tokenExpiry    time.Time
	inboundHandler func(ctx context.Context, msg platforms.InboundMessage) error

	webhookServer *http.Server
	webhookPort   int

	healthy bool
	mu      sync.RWMutex
}

// NewAdapter creates a new Teams adapter.
func NewAdapter(appID, appPassword, tenantID string, webhookPort int) *Adapter {
	if webhookPort <= 0 {
		webhookPort = 8081
	}
	return &Adapter{
		appID:       appID,
		appPassword: appPassword,
		tenantID:    tenantID,
		serviceURL:  "https://smba.trafficmanager.net/teams",
		webhookPort: webhookPort,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
	}
}

// Name returns the platform name.
func (a *Adapter) Name() string { return "teams" }

// Start acquires an initial OAuth2 token and starts the webhook HTTP server.
func (a *Adapter) Start(ctx context.Context) error {
	if err := a.refreshToken(ctx); err != nil {
		return fmt.Errorf("teams: failed to acquire initial token: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleWebhook)

	a.webhookServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", a.webhookPort),
		Handler: mux,
	}

	go func() {
		log.Info().Int("port", a.webhookPort).Msg("teams: webhook server starting")
		if err := a.webhookServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("teams: webhook server error")
			a.mu.Lock()
			a.healthy = false
			a.mu.Unlock()
		}
	}()

	a.mu.Lock()
	a.healthy = true
	a.mu.Unlock()

	log.Info().Str("platform", "teams").Msg("teams: adapter started")
	return nil
}

// Stop shuts down the webhook server.
func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.Lock()
	a.healthy = false
	a.mu.Unlock()

	if a.webhookServer != nil {
		if err := a.webhookServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("teams: webhook server shutdown error: %w", err)
		}
	}
	log.Info().Str("platform", "teams").Msg("teams: adapter stopped")
	return nil
}

// SendMessage sends a message to a Teams channel via the Bot Framework REST API.
// msg.ChannelID should be the conversation ID.
// msg.ThreadID or msg.ReplyTo can carry the replyToId for threaded replies.
func (a *Adapter) SendMessage(ctx context.Context, msg platforms.OutboundMessage) (string, error) {
	token, err := a.getToken(ctx)
	if err != nil {
		return "", fmt.Errorf("teams: failed to get token: %w", err)
	}

	text := msg.Content
	if text == "" {
		text = msg.HTMLContent
	}

	replyToID := msg.ThreadID
	if replyToID == "" {
		replyToID = msg.ReplyTo
	}

	act := activity{
		Type:      "message",
		Text:      text,
		ReplyToID: replyToID,
	}

	body, err := json.Marshal(act)
	if err != nil {
		return "", fmt.Errorf("teams: failed to marshal activity: %w", err)
	}

	endpointURL := fmt.Sprintf("%s/v3/conversations/%s/activities",
		strings.TrimRight(a.serviceURL, "/"),
		url.PathEscape(msg.ChannelID),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("teams: failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("teams: failed to send activity: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("teams: failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("teams: send activity returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var actResp activityResponse
	if err := json.Unmarshal(respBody, &actResp); err != nil {
		return "", fmt.Errorf("teams: failed to parse activity response: %w", err)
	}

	log.Debug().Str("activity_id", actResp.ID).Str("channel_id", msg.ChannelID).Msg("teams: message sent")
	return actResp.ID, nil
}

// SetInboundHandler registers the handler for messages received from Teams.
func (a *Adapter) SetInboundHandler(fn func(ctx context.Context, msg platforms.InboundMessage) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inboundHandler = fn
}

// IsHealthy returns whether the adapter is operational.
func (a *Adapter) IsHealthy() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.healthy
}

// refreshToken fetches a new OAuth2 token from the Microsoft identity platform.
func (a *Adapter) refreshToken(ctx context.Context) error {
	authURL := fmt.Sprintf(authURLTemplate, a.tenantID)

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", a.appID)
	form.Set("client_secret", a.appPassword)
	form.Set("scope", defaultScope)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var tok tokenResponse
	if err := json.Unmarshal(respBody, &tok); err != nil {
		return fmt.Errorf("failed to parse token response: %w", err)
	}

	a.mu.Lock()
	a.accessToken = tok.AccessToken
	// Subtract 60s to refresh before actual expiry.
	a.tokenExpiry = time.Now().Add(time.Duration(tok.ExpiresIn-60) * time.Second)
	a.mu.Unlock()

	log.Debug().Msg("teams: OAuth2 token refreshed")
	return nil
}

// getToken returns a valid access token, refreshing if expired.
func (a *Adapter) getToken(ctx context.Context) (string, error) {
	a.mu.RLock()
	token := a.accessToken
	expiry := a.tokenExpiry
	a.mu.RUnlock()

	if token != "" && time.Now().Before(expiry) {
		return token, nil
	}

	if err := a.refreshToken(ctx); err != nil {
		return "", err
	}

	a.mu.RLock()
	token = a.accessToken
	a.mu.RUnlock()
	return token, nil
}

// handleWebhook processes incoming Bot Framework Activity payloads from Teams.
func (a *Adapter) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error().Err(err).Msg("teams: failed to read webhook body")
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var act inboundActivity
	if err := json.Unmarshal(body, &act); err != nil {
		log.Error().Err(err).Msg("teams: failed to parse webhook activity")
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if act.Type != "message" {
		// Acknowledge non-message activities silently.
		w.WriteHeader(http.StatusOK)
		return
	}

	var attachments []platforms.Attachment
	for _, a := range act.Attachments {
		attachments = append(attachments, platforms.Attachment{
			Filename:    a.Name,
			URL:         a.ContentURL,
			ContentType: a.ContentType,
		})
	}

	inbound := platforms.InboundMessage{
		Platform:    "teams",
		ChannelID:   act.Conversation.ID,
		MessageID:   act.ID,
		ThreadID:    act.ReplyToID,
		AuthorID:    act.From.ID,
		AuthorName:  act.From.Name,
		Content:     act.Text,
		Attachments: attachments,
		Metadata: map[string]string{
			"channel_id":  act.ChannelID,
			"service_url": act.ServiceURL,
		},
	}

	a.mu.RLock()
	handler := a.inboundHandler
	a.mu.RUnlock()

	if handler != nil {
		if err := handler(r.Context(), inbound); err != nil {
			log.Error().Err(err).Str("message_id", act.ID).Msg("teams: inbound handler error")
		}
	} else {
		log.Warn().Str("message_id", act.ID).Msg("teams: no inbound handler registered, dropping message")
	}

	w.WriteHeader(http.StatusOK)
}
