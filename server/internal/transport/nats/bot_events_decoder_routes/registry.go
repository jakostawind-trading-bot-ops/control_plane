package bot_event_decoder_routes

import (
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/ports"
	bot_event_decoder_routes_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/nats/bot_events_decoder_routes/v1"
)

func BotEventDecoderRoutesRegistry() []ports.BotEventDecoderRoute {
	return []ports.BotEventDecoderRoute{
		// V1 : Lifecycle
		bot_event_decoder_routes_v1.BotStartedEventV1DecoderRoute(),
	}
}
