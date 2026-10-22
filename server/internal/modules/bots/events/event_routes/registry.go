package eventroutes

import (
	eventroutesv1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/event_routes/v1"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotEventDecoderRoutesRegistry() []ports.BotEventRoute {
	return []ports.BotEventRoute{
		// V1 : Lifecycle
		eventroutesv1.BotStartedEventV1Route(),
	}
}
