package di_bots

import (
	"log/slog"

	bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

// BotEventDecoderProvider собирает декодер из Routes и общего JSON-логгера Logger.
// Возвращает порт декодирования; Logger должен быть создан JSONLoggerProvider.
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
