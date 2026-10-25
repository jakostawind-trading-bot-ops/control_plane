package dispatch_routes

import (
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotEventDispatchRoutesRegistry() []ports.BotEventDispatchRoute {
	return []ports.BotEventDispatchRoute{
		// Lifecycle
		BotStartedEventDispatchRoute(),
	}
}