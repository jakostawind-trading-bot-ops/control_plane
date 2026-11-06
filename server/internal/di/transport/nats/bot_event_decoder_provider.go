package di

import (
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/ports"
	bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/nats"
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
