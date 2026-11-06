package ports

import (
	"context"

	events "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots"
)

type BotEventDispatchRoute struct {
	BotEvent events.BaseBotEventI
	Handler  events.BotEventHandlerI
}

type BotEventDispatcherPort interface {
	BotEventDispatch(ctx context.Context, BotEvent events.BaseBotEventI) error
}
