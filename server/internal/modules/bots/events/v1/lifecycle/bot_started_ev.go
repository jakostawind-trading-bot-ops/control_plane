package events_lifecycle_v1

import (
	"context"
	"fmt"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
)

type BotStartedEventV1 struct {
	events.BaseBotEvent
}

type BotStartedEventV1Handler struct{}

var _ events.BotEventHandlerI = (*BotStartedEventV1Handler)(nil)

func (Handler *BotStartedEventV1Handler) HandleBotEvent(
	ctx context.Context,
	BotEvent events.BaseBotEventI,
) error {
	// TODO: привести BotEvent к *BotStartedEventV1 и вызвать use case.
	fmt.Println("BotStartedEventV1Handler отработал")
	return nil
}
