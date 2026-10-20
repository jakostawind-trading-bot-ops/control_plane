package events

import "context"

type BotEventHandler interface {
	HandleBotEvent(
		ctx context.Context,
		event any,
	) error
}
