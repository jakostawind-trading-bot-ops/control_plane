package dispatch_routes

import (
	events_lifecycle "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/events/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

// BotStartedEvent
func BotStartedEventDispatchRoute() ports.BotEventDispatchRoute {
	return ports.BotEventDispatchRoute{
		BotEvent: &events_lifecycle.BotStartedEvent{},
		Handler:  &events_lifecycle.BotStartedEventHandler{},
	}
}
