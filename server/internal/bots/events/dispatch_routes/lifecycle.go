package dispatch_routes

import (
	"log/slog"

	events_lifecycle "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events/events/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/ports"
	usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/usecases/lifecycle"
)

// BotStartedEventDispatchRoute возвращает маршрут запуска бота с JSON-логгером Logger из DI.
func BotStartedEventDispatchRoute(Logger *slog.Logger) ports.BotEventDispatchRoute {
	return ports.BotEventDispatchRoute{
		BotEvent: &events_lifecycle.BotStartedEvent{},
		Handler: &events_lifecycle.BotStartedEventHandler{
			Logger: Logger.With("component", "bot_started_event_handler"),
			BotStartedUC: usecases.BotStartedUC{
				Logger: Logger.With("component", "bot_started_use_case"),
			},
		},
	}
}
