package dispatchroutes_v1

import (
	events_lifecycle_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/v1/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotStartedEventV1DispatchRoute() ports.BotEventDispatchRoute {
	return ports.BotEventDispatchRoute{
		BotEvent: &events_lifecycle_v1.BotStartedEventV1{},
		Handler:  &events_lifecycle_v1.BotStartedEventV1Handler{},
	}
}
