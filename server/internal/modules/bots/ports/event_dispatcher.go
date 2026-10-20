package ports

import (
	"context"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events"
)


type BotEventDispatcherPort interface {
	BotEventDispatch(ctx context.Context, e *events.BotEvent) error
}