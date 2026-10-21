package ports

import (
	"context"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
)


type BotEventDispatcherPort interface {
	DispatchBotEvent(ctx context.Context, BotEvent *events.BaseBotEventI) error
}