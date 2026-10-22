package di_bots

import (
	bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotEventDecoderProvider(
	Routes []ports.BotEventRoute,
) ports.BotEventDecoderPort {
	RouteMap := make(map[string]ports.BotEventRoute, len(Routes))

	for _, Route := range Routes {
		RouteMap[Route.MessageType] = Route
	}

	return &bots_nats.NatsBotEventDecoder{
		BotEventRoutes: RouteMap,
	}
}