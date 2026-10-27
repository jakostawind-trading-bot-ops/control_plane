package dispatch_routes

import (
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

// BotEventDispatchRoutesRegistry возвращает маршруты с общим JSON-логгером Logger из DI.
func BotEventDispatchRoutesRegistry(Logger *slog.Logger) []ports.BotEventDispatchRoute {
	return []ports.BotEventDispatchRoute{
		// Lifecycle
		BotStartedEventDispatchRoute(Logger),
	}
}
