package dispatchroutes

import (
	dispatchroutes_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/dispatch_routes/v1"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotEventDispatchRoutesRegistry() []ports.BotEventDispatchRoute {
	return []ports.BotEventDispatchRoute{
		dispatchroutes_v1.BotStartedEventV1DispatchRoute(),
	}
}