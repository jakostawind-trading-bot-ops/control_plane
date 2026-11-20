package events_bot

import "context"

type BotEventHandlerI interface {
	HandleBotEvent(
		ctx context.Context,
		BotEvent BaseBotEventI,
	) error
}
