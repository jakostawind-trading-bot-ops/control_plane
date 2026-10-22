package events

import (
	"context"
	"fmt"
	"reflect"

)

type BotEventDispatcher struct {
	Handlers map[reflect.Type]BotEventHandlerI
}

func (BotEventDispatcher *BotEventDispatcher) DispatchBotEvent(ctx context.Context, BotEvent BaseBotEventI) error {
    Handler, ok := BotEventDispatcher.Handlers[reflect.TypeOf(BotEvent)]

    if !ok {
        return fmt.Errorf("обработчик события %T не зарегистрирован", BotEvent)
    }

    return Handler.HandleBotEvent(ctx, BotEvent)

}