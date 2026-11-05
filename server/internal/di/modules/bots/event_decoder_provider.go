package di_bots

import (
	"log/slog"

	bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/nats"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/ports"
)

// BotEventDecoderProvider собирает декодер из Routes и общего OTel-логгера Logger.
// Возвращает порт декодирования; Logger должен быть создан LoggerProvider.
func BotEventDecoderProvider(
	Routes []ports.BotEventDecoderRoute,
	Logger *slog.Logger,
) ports.BotEventDecoderPort {
	RouteMap := make(map[string]ports.BotEventDecoderRoute, len(Routes))

	for _, Route := range Routes {
		RouteMap[Route.MessageType] = Route
	}

	return &bots_nats.NatsBotEventDecoder{
		Logger:                Logger.With("component", "bot_event_decoder"),
		BotEventDecoderRoutes: RouteMap,
	}
}
