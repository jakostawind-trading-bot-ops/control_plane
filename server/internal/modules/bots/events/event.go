package events

type BotEvent struct {
	BotID     string
	Message   BotEventMessage
	Payload   map[string]any
	Timestamp string
}

type BotEventMessage struct {
	MsgID         string
	CorrelationID string
	MsgVersion    string
	MsgType       string
	MsgName       string
}

type BotEventI interface {
	IsBotEvent()
}

func (be *BotEvent) isBotEvent() {}