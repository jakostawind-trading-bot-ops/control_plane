package di_bots

import (
	"reflect"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotEventDispatcherProvider(
	Routes []ports.BotEventDispatchRoute,
) ports.BotEventDispatcherPort {
	Handlers := make(map[reflect.Type]events.BotEventHandlerI, len(Routes))

	for _, Route := range Routes {
		Handlers[reflect.TypeOf(Route.BotEvent)] = Route.Handler
	}

	return &events.BotEventDispatcher{
		Handlers: Handlers,
	}
}