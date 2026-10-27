package di_bots

import (
	"log/slog"
	"reflect"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

// BotEventDispatcherProvider собирает диспетчер из Routes и общего JSON-логгера Logger.
// Возвращает порт обработки; Logger должен быть создан JSONLoggerProvider.
func BotEventDispatcherProvider(
	Routes []ports.BotEventDispatchRoute,
	Logger *slog.Logger,
) ports.BotEventDispatcherPort {
	Handlers := make(map[reflect.Type]events.BotEventHandlerI, len(Routes))

	for _, Route := range Routes {
		Handlers[reflect.TypeOf(Route.BotEvent)] = Route.Handler
	}

	return &events.BotEventDispatcher{
		Logger:   Logger.With("component", "bot_event_dispatcher"),
		Handlers: Handlers,
	}
}
