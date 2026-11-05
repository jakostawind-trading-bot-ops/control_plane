package ports

import (
	"context"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events"
)

type BotEventDispatchRoute struct {
	BotEvent events.BaseBotEventI
	Handler  events.BotEventHandlerI
}

type BotEventDispatcherPort interface {
	BotEventDispatch(ctx context.Context, BotEvent events.BaseBotEventI) error
}
