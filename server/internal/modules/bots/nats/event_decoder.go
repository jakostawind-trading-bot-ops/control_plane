package nats

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

type NatsBotEventDecoder struct {
	BotEventRoutes map[string]ports.BotEventRoute
}

func (NatsBotEventDecoder *NatsBotEventDecoder) BotEventDecode(
	ctx context.Context,
	MessageType string,
	RawMessage []byte,
) (events.BaseBotEventI, error) {
	Route, ok := NatsBotEventDecoder.BotEventRoutes[MessageType]
	if !ok {
		return nil, fmt.Errorf("маршрут для типа %q не зарегистрирован", MessageType)
	}

	// 1) JSON -> Contract
	Contract := Route.Contract()
	
	if err := json.Unmarshal(RawMessage, Contract); err != nil {
		return nil, fmt.Errorf(
			"декодирование контракта %q: %w",
			MessageType, err,
		)
	}

	// 2) Проверка контракта
	if err := Contract.Validate(); err != nil {
		return nil, fmt.Errorf(
			"валидация контракта %q: %w",
			MessageType, err,
		)
	}

	// 3) Contarct -> BotEvent
	BotEvent, err := Route.BotEvent(Contract)
	if err != nil {
		return nil, fmt.Errorf(
			"преобразование контракта %q: %w",
			MessageType, err,
		)
	}

	return  BotEvent, nil
}