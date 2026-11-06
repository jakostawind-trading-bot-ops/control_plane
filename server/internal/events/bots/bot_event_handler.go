package bot_events

import "context"

type BotEventHandlerI interface {
	HandleBotEvent(
		ctx context.Context,
		BotEvent BaseBotEventI,
		// todo: мб сюда надо будет передать общий интерфейс юзкейса
	) error
}
