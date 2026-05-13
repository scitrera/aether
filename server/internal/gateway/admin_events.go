package gateway

import (
	"context"

	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
)

// =============================================================================
// Real-time Events
// =============================================================================

func (p *GatewayStateProvider) SubscribeEvents(ctx context.Context) (<-chan *admin.Event, error) {
	ch := make(chan *admin.Event, 100)

	p.eventMu.Lock()
	p.eventSubs[ch] = struct{}{}
	p.eventMu.Unlock()

	// Cleanup when context is done
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("goroutine", "eventSubCleanup").Msg("recovered from panic in background goroutine")
			}
		}()
		<-ctx.Done()
		p.eventMu.Lock()
		delete(p.eventSubs, ch)
		p.eventMu.Unlock()
		close(ch)
	}()

	return ch, nil
}

// PublishEvent broadcasts an event to all subscribers
func (p *GatewayStateProvider) PublishEvent(event *admin.Event) {
	p.eventMu.RLock()
	defer p.eventMu.RUnlock()

	for ch := range p.eventSubs {
		select {
		case ch <- event:
		default:
			// Channel full, skip
		}
	}
}

// IncrementMessageCount increments the message counter
func (p *GatewayStateProvider) IncrementMessageCount() {
	p.messageCount.Add(1)
}
