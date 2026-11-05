package dispatch_routes

import (
	"log/slog"

	events_lifecycle "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events/events/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/ports"
	usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/usecases/lifecycle"
)

// BotStartedEventDispatchRoute возвращает маршрут события запуска бота.
// Logger — общий логгер из DI; должен быть задан.
// BotStartedUC — готовый use case из DI; должен быть задан.
func BotStartedEventDispatchRoute(
	Logger *slog.Logger,
	BotStartedUC *usecases.BotStartedUC,
) ports.BotEventDispatchRoute {
	return ports.BotEventDispatchRoute{
		BotEvent: &events_lifecycle.BotStartedEvent{},
		Handler: &events_lifecycle.BotStartedEventHandler{
			Logger:       Logger.With("component", "bot_started_event_handler"),
			BotStartedUC: BotStartedUC,
		},
	}
}
