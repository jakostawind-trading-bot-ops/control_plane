package events

import (
	"context"
	"fmt"
	"reflect"
)

type NatsBotEventDispatcher struct {
	Handlers map[reflect.Type]BotEventHandler
}

func (d *NatsBotEventDispatcher) BotEventDispatch(
    ctx context.Context,
    event BotEventI,
) error {
    handler, ok := d.Handlers[reflect.TypeOf(event)]
    if !ok {
        return fmt.Errorf(
            "обработчик события %T не зарегистрирован",
            event,
        )
    }

    return handler.HandleBotEvent(ctx, event)
}