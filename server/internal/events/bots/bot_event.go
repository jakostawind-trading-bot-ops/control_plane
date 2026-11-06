package bot_events

type BaseBotEvent struct {
	BotID     string
	Payload   map[any]any
	Timestamp string
}

type BaseBotEventI interface {
	IsBaseBotEvent() *BaseBotEvent
}

func (BaseBotEvent *BaseBotEvent) IsBaseBotEvent() *BaseBotEvent {
	return BaseBotEvent
}