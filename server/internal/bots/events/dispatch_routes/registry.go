package dispatch_routes

import (
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/ports"
	usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/usecases/lifecycle"
)

// BotEventDispatchRoutesRegistry возвращает маршруты с зависимостями из DI.
// Logger — общий логгер обработчиков; должен быть задан.
// BotStartedUC — готовый use case запуска бота; должен быть задан.
func BotEventDispatchRoutesRegistry(
	Logger *slog.Logger,
	BotStartedUC *usecases.BotStartedUC,
) []ports.BotEventDispatchRoute {
	return []ports.BotEventDispatchRoute{
		// Lifecycle
		BotStartedEventDispatchRoute(Logger, BotStartedUC),
	}
}
