package di

import (
	"log/slog"
	"reflect"

	events "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/ports"
)

// BotEventDispatcherProvider собирает диспетчер из Routes и общего OTel-логгера Logger.
// Возвращает порт обработки; Logger должен быть создан LoggerProvider.
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
