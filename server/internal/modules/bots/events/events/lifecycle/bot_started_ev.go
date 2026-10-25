package events_lifecycle

import (
	"context"
	"fmt"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
)

type BotStartedEvent struct {
	events.BaseBotEvent
}

type BotStartedEventHandler struct{}

var _ events.BotEventHandlerI = (*BotStartedEventHandler)(nil)

func (Handler *BotStartedEventHandler) HandleBotEvent(
	ctx context.Context,
	BotEvent events.BaseBotEventI,
) error {
	// TODO: привести BotEvent к *BotStartedEventV1 и вызвать use case.
	fmt.Println("BotStartedEventHandler отработал")
	return nil
}
