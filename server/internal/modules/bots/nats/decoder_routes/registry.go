package decoder_routes

import (
	decoder_routes_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats/decoder_routes/v1"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotEventDecoderRoutesRegistry() []ports.BotEventDecoderRoute {
	return []ports.BotEventDecoderRoute{
		// V1 : Lifecycle
		decoder_routes_v1.BotStartedEventV1DecoderRoute(),
	}
}
