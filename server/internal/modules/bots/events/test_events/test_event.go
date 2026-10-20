package testevents

import (
	"context"
	"fmt"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events"
)

type TestEvent struct {
	events.BotEvent
}

type TestNatsBotEvent struct {
	BotID     string `json:"bot_id"`
	Message   TestNatsBotEventMessage `json:"message"`
	Payload   map[string]any `json:"payload"`
	Timestamp string `json:"timestamp"`
}

type TestEventHandler struct{}

// Проверка соответствия интерфейсу при компиляции.
var _ events.BotEventHandler = (*TestEventHandler)(nil)

func (h *TestEventHandler) HandleBotEvent(
    ctx context.Context,
    event any,
) error {
    e, ok := event.(*TestEvent)
    if !ok {
        return fmt.Errorf("ожидался *TestEvent, получен %T", event)
    }
    if e == nil {
        return fmt.Errorf("получен nil *TestEvent")
    }

    fmt.Println("Обработан TestEvent для бота:", e.BotID)

    // Здесь в будущем вызываешь usecase,
    // передавая ctx и необходимые данные из e.
    return nil
}


type TestNatsBotEventMessage struct {
	MsgID         string `json:"msg_id"`
	CorrelationID string `json:"correlation_id"`
	MsgVersion    string `json:"msg_version"`
	MsgType       string `json:"msg_type"`
	MsgName       string `json:"msg_name"`
}

var TestNatsEventExample = TestNatsBotEvent{
    BotID: "test_bot",

    Message: TestNatsBotEventMessage{
        MsgID:         "msg-123",
        CorrelationID: "corr-456",
        MsgVersion:    "1.0",
        MsgType:       "event",
        MsgName:       "bot.started",
    },

    Payload: map[string]any{
        "status":  "started",
        "version": "1.0.0",
        "uptime":  120,
    },

    Timestamp: "2026-10-16T03:46:00Z",
}
