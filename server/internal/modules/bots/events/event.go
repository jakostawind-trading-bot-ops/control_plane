package events

type BaseBotEvent struct {
	BotID     string
	Message   BaseBotEventMessage
	Payload   map[string]any
	Timestamp string
}

type BaseBotEventMessage struct {
	MsgID         string
	CorrelationID string
}

type BaseBotEventI interface {
	IsBaseBotEvent() *BaseBotEvent
}

func (BaseBotEvent *BaseBotEvent) IsBaseBotEvent() *BaseBotEvent {
	return BaseBotEvent
}